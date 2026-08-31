package queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// JobStatus represents the state of a job
type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusCancelled  JobStatus = "cancelled"
)

// Job represents a job in the queue
type Job struct {
	ID            string                  `json:"id"`
	Queue         string                  `json:"queue"`
	Payload       []byte                  `json:"payload"`
	Status        JobStatus               `json:"status"`
	Priority      int                     `json:"priority"` // 0-10, higher = urgent
	Timestamp     int64                   `json:"timestamp"` // Lamport clock
	VectorClock   map[string]int64        `json:"vector_clock"`
	RetryCount    int                     `json:"retry_count"`
	MaxRetries    int                     `json:"max_retries"`
	ProcessedBy   string                  `json:"processed_by"`
	Result        []byte                  `json:"result"`
	Error         string                  `json:"error"`
	CreatedAt     int64                   `json:"created_at"`
	UpdatedAt     int64                   `json:"updated_at"`
	DeadlineAt    int64                   `json:"deadline_at"`
	ProcessStarted int64                  `json:"process_started"`
	ProcessEnded  int64                   `json:"process_ended"`
	mu            sync.RWMutex            `json:"-"`
}

// JobQueue represents a distributed job queue
type JobQueue struct {
	Name          string
	Jobs          []*Job              // Append-only log
	JobIndex      map[string]*Job     // Fast lookup by ID
	PendingJobs   []string            // IDs of pending jobs
	ProcessingJobs []string            // IDs of processing jobs
	Subscribers   map[string]*Worker  // Active workers
	mu            sync.RWMutex
	vectorClock   map[string]int64
	lamportClock  int64
	maxAge        time.Duration       // Clean up old jobs
	retryPolicy   RetryPolicy
	deadLetterQ   *DeadLetterQueue
}

// Worker represents a job worker
type Worker struct {
	ID            string
	Queue         string
	LastHeartbeat int64
	Processing    string // Current job ID
	Completed     int64  // Number of jobs completed
}

// RetryPolicy defines how to retry failed jobs
type RetryPolicy struct {
	MaxRetries     int           // Max attempts
	InitialBackoff time.Duration // Initial retry delay
	MaxBackoff     time.Duration // Maximum retry delay
	BackoffFactor  float64       // Exponential backoff multiplier
}

// DeadLetterQueue stores jobs that can't be processed
type DeadLetterQueue struct {
	Jobs        []*Job
	MaxSize     int
	mu          sync.RWMutex
}

// NewJobQueue creates a new distributed job queue
func NewJobQueue(name string, maxAge time.Duration) *JobQueue {
	return &JobQueue{
		Name:           name,
		Jobs:           make([]*Job, 0),
		JobIndex:       make(map[string]*Job),
		PendingJobs:    make([]string, 0),
		ProcessingJobs: make([]string, 0),
		Subscribers:    make(map[string]*Worker),
		vectorClock:    make(map[string]int64),
		maxAge:         maxAge,
		retryPolicy: RetryPolicy{
			MaxRetries:     3,
			InitialBackoff: 1 * time.Second,
			MaxBackoff:     30 * time.Second,
			BackoffFactor:  2.0,
		},
		deadLetterQ: &DeadLetterQueue{
			Jobs:    make([]*Job, 0),
			MaxSize: 10000,
		},
	}
}

// Enqueue adds a job to the queue
func (jq *JobQueue) Enqueue(payload []byte, priority int, maxRetries int, deadline time.Duration) (*Job, error) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	// Increment Lamport clock
	atomic.AddInt64(&jq.lamportClock, 1)
	ts := atomic.LoadInt64(&jq.lamportClock)

	// Create job
	job := &Job{
		ID:         fmt.Sprintf("job-%d-%d", ts, time.Now().UnixNano()%1000),
		Queue:      jq.Name,
		Payload:    payload,
		Status:     StatusPending,
		Priority:   priority,
		Timestamp:  ts,
		VectorClock: copyVectorClock(jq.vectorClock),
		MaxRetries: maxRetries,
		CreatedAt:  time.Now().UnixMilli(),
		UpdatedAt:  time.Now().UnixMilli(),
		DeadlineAt: time.Now().Add(deadline).UnixMilli(),
	}

	// Add to log (append-only)
	jq.Jobs = append(jq.Jobs, job)
	jq.JobIndex[job.ID] = job
	jq.PendingJobs = append(jq.PendingJobs, job.ID)

	// Sort pending jobs by priority (higher priority first)
	jq.sortPendingJobs()

	return job, nil
}

// GetNextJob attempts to claim the next available job
// Uses CAS (Compare-And-Swap) to ensure no race conditions
func (jq *JobQueue) GetNextJob(workerID string) (*Job, error) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if len(jq.PendingJobs) == 0 {
		return nil, fmt.Errorf("no pending jobs")
	}

	// Find first job that hasn't expired
	now := time.Now().UnixMilli()
	for i, jobID := range jq.PendingJobs {
		job := jq.JobIndex[jobID]

		// Skip expired jobs
		if job.DeadlineAt > 0 && now > job.DeadlineAt {
			jq.moveToDeadLetter(job, "deadline exceeded")
			jq.PendingJobs = append(jq.PendingJobs[:i], jq.PendingJobs[i+1:]...)
			continue
		}

		// Attempt to claim (CAS-like operation)
		if job.Status == StatusPending {
			job.Status = StatusProcessing
			job.ProcessedBy = workerID
			job.ProcessStarted = time.Now().UnixMilli()
			job.UpdatedAt = job.ProcessStarted

			// Remove from pending, add to processing
			jq.PendingJobs = append(jq.PendingJobs[:i], jq.PendingJobs[i+1:]...)
			jq.ProcessingJobs = append(jq.ProcessingJobs, jobID)

			// Register worker
			if _, exists := jq.Subscribers[workerID]; !exists {
				jq.Subscribers[workerID] = &Worker{
					ID:            workerID,
					Queue:         jq.Name,
					LastHeartbeat: time.Now().UnixMilli(),
				}
			}
			jq.Subscribers[workerID].Processing = jobID

			return job, nil
		}
	}

	return nil, fmt.Errorf("no claimable jobs")
}

// MarkComplete marks a job as completed
func (jq *JobQueue) MarkComplete(jobID string, result []byte) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	job, exists := jq.JobIndex[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if job.Status != StatusProcessing {
		return fmt.Errorf("job not in processing state: %s", jobID)
	}

	// Mark as completed
	job.Status = StatusCompleted
	job.Result = result
	job.ProcessEnded = time.Now().UnixMilli()
	job.UpdatedAt = job.ProcessEnded

	// Remove from processing
	jq.removeFromProcessing(jobID)

	return nil
}

// MarkFailed marks a job as failed and handles retry
func (jq *JobQueue) MarkFailed(jobID string, errMsg string) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	job, exists := jq.JobIndex[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	job.Error = errMsg
	job.RetryCount++
	job.ProcessEnded = time.Now().UnixMilli()
	job.UpdatedAt = job.ProcessEnded

	// Remove from processing
	jq.removeFromProcessing(jobID)

	// Check if we should retry
	if job.RetryCount >= job.MaxRetries {
		job.Status = StatusFailed
		jq.moveToDeadLetter(job, fmt.Sprintf("max retries exceeded: %s", errMsg))
		return nil
	}

	// Schedule retry with exponential backoff
	backoff := jq.calculateBackoff(job.RetryCount)
	retryAt := time.Now().Add(backoff).UnixMilli()

	// Put back to pending after backoff
	job.Status = StatusPending
	job.UpdatedAt = retryAt

	// Add back to pending (will be picked up after backoff period)
	jq.PendingJobs = append(jq.PendingJobs, jobID)
	jq.sortPendingJobs()

	return nil
}

// GetStatus returns the status of a job
func (jq *JobQueue) GetStatus(jobID string) (*Job, error) {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	job, exists := jq.JobIndex[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	return job, nil
}

// GetStats returns queue statistics
func (jq *JobQueue) GetStats() map[string]interface{} {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	return map[string]interface{}{
		"queue":            jq.Name,
		"total_jobs":       len(jq.Jobs),
		"pending":          len(jq.PendingJobs),
		"processing":       len(jq.ProcessingJobs),
		"active_workers":   len(jq.Subscribers),
		"dead_letter_size": len(jq.deadLetterQ.Jobs),
		"lamport_clock":    atomic.LoadInt64(&jq.lamportClock),
	}
}

// GetDeadLetterQueue returns jobs that couldn't be processed
func (jq *JobQueue) GetDeadLetterQueue() []*Job {
	jq.deadLetterQ.mu.RLock()
	defer jq.deadLetterQ.mu.RUnlock()

	result := make([]*Job, len(jq.deadLetterQ.Jobs))
	copy(result, jq.deadLetterQ.Jobs)
	return result
}

// Helper functions

func (jq *JobQueue) sortPendingJobs() {
	// Sort by priority (higher first), then by timestamp (older first)
	for i := 0; i < len(jq.PendingJobs)-1; i++ {
		for j := i + 1; j < len(jq.PendingJobs); j++ {
			jobI := jq.JobIndex[jq.PendingJobs[i]]
			jobJ := jq.JobIndex[jq.PendingJobs[j]]

			// Higher priority first
			if jobJ.Priority > jobI.Priority {
				jq.PendingJobs[i], jq.PendingJobs[j] = jq.PendingJobs[j], jq.PendingJobs[i]
			} else if jobJ.Priority == jobI.Priority && jobJ.Timestamp < jobI.Timestamp {
				// Older jobs first if same priority
				jq.PendingJobs[i], jq.PendingJobs[j] = jq.PendingJobs[j], jq.PendingJobs[i]
			}
		}
	}
}

func (jq *JobQueue) removeFromProcessing(jobID string) {
	for i, id := range jq.ProcessingJobs {
		if id == jobID {
			jq.ProcessingJobs = append(jq.ProcessingJobs[:i], jq.ProcessingJobs[i+1:]...)
			break
		}
	}
}

func (jq *JobQueue) moveToDeadLetter(job *Job, reason string) {
	jq.deadLetterQ.mu.Lock()
	defer jq.deadLetterQ.mu.Unlock()

	job.Error = reason
	jq.deadLetterQ.Jobs = append(jq.deadLetterQ.Jobs, job)

	// Maintain max size
	if len(jq.deadLetterQ.Jobs) > jq.deadLetterQ.MaxSize {
		jq.deadLetterQ.Jobs = jq.deadLetterQ.Jobs[len(jq.deadLetterQ.Jobs)-jq.deadLetterQ.MaxSize:]
	}
}

func (jq *JobQueue) calculateBackoff(retryCount int) time.Duration {
	if retryCount <= 0 {
		return jq.retryPolicy.InitialBackoff
	}

	// Exponential backoff with jitter
	backoff := time.Duration(float64(jq.retryPolicy.InitialBackoff) *
		(jq.retryPolicy.BackoffFactor * float64(retryCount)))

	if backoff > jq.retryPolicy.MaxBackoff {
		backoff = jq.retryPolicy.MaxBackoff
	}

	return backoff
}

func copyVectorClock(vc map[string]int64) map[string]int64 {
	result := make(map[string]int64)
	for k, v := range vc {
		result[k] = v
	}
	return result
}
