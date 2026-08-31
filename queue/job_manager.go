package queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// JobManager manages multiple job queues with distributed coordination
type JobManager struct {
	mu              sync.RWMutex
	queues          map[string]*JobQueue
	nodeID          string
	acknowledgments map[string]map[string]int64 // queue -> jobID -> ack count
	peers           []string                     // Other nodes in cluster
	cleanupTicker   *time.Ticker
	stopChan        chan struct{}
}

// NewJobManager creates a new job manager
func NewJobManager(nodeID string) *JobManager {
	jm := &JobManager{
		queues:          make(map[string]*JobQueue),
		nodeID:          nodeID,
		acknowledgments: make(map[string]map[string]int64),
		peers:           make([]string, 0),
		stopChan:        make(chan struct{}),
	}

	// Start background cleanup
	jm.cleanupTicker = time.NewTicker(30 * time.Second)
	go jm.backgroundCleanup()

	return jm
}

// GetOrCreateQueue gets or creates a queue
func (jm *JobManager) GetOrCreateQueue(name string) *JobQueue {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if q, exists := jm.queues[name]; exists {
		return q
	}

	q := NewJobQueue(name, 24*time.Hour)
	jm.queues[name] = q

	// Initialize acknowledgments map for this queue
	jm.acknowledgments[name] = make(map[string]int64)

	return q
}

// RegisterPeer registers another node in the cluster
func (jm *JobManager) RegisterPeer(peerID string) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	for _, p := range jm.peers {
		if p == peerID {
			return
		}
	}
	jm.peers = append(jm.peers, peerID)
}

// Enqueue adds a job to a queue
func (jm *JobManager) Enqueue(queueName string, payload []byte, opts JobOptions) (*Job, error) {
	q := jm.GetOrCreateQueue(queueName)
	return q.Enqueue(payload, opts.Priority, opts.MaxRetries, opts.Deadline)
}

// ClaimJob attempts to claim a job for processing
func (jm *JobManager) ClaimJob(queueName string, workerID string) (*Job, error) {
	q, exists := jm.getQueue(queueName)
	if !exists {
		return nil, fmt.Errorf("queue not found: %s", queueName)
	}

	job, err := q.GetNextJob(workerID)
	if err != nil {
		return nil, err
	}

	// Broadcast claim to other nodes (gossip protocol simulation)
	jm.broadcastJobState(queueName, job)

	return job, nil
}

// CompleteJob marks a job as completed
func (jm *JobManager) CompleteJob(queueName string, jobID string, result []byte) error {
	q, exists := jm.getQueue(queueName)
	if !exists {
		return fmt.Errorf("queue not found: %s", queueName)
	}

	err := q.MarkComplete(jobID, result)
	if err != nil {
		return err
	}

	// Acknowledge across cluster
	jm.acknowledgeJob(queueName, jobID)

	// Broadcast completion to other nodes
	job, _ := q.GetStatus(jobID)
	jm.broadcastJobState(queueName, job)

	return nil
}

// FailJob marks a job as failed and handles retry
func (jm *JobManager) FailJob(queueName string, jobID string, errMsg string) error {
	q, exists := jm.getQueue(queueName)
	if !exists {
		return fmt.Errorf("queue not found: %s", queueName)
	}

	return q.MarkFailed(jobID, errMsg)
}

// GetJobStatus returns the status of a job
func (jm *JobManager) GetJobStatus(queueName string, jobID string) (*Job, error) {
	q, exists := jm.getQueue(queueName)
	if !exists {
		return nil, fmt.Errorf("queue not found: %s", queueName)
	}

	return q.GetStatus(jobID)
}

// GetQueueStats returns statistics for a queue
func (jm *JobManager) GetQueueStats(queueName string) (map[string]interface{}, error) {
	q, exists := jm.getQueue(queueName)
	if !exists {
		return nil, fmt.Errorf("queue not found: %s", queueName)
	}

	stats := q.GetStats()
	stats["node"] = jm.nodeID

	return stats, nil
}

// GetAllStats returns statistics for all queues
func (jm *JobManager) GetAllStats() map[string]map[string]interface{} {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	result := make(map[string]map[string]interface{})
	for name, q := range jm.queues {
		result[name] = q.GetStats()
	}

	return result
}

// GetDeadLetterQueue returns dead-lettered jobs for a queue
func (jm *JobManager) GetDeadLetterQueue(queueName string) ([]*Job, error) {
	q, exists := jm.getQueue(queueName)
	if !exists {
		return nil, fmt.Errorf("queue not found: %s", queueName)
	}

	return q.GetDeadLetterQueue(), nil
}

// GetClusterStatus returns status of all nodes
func (jm *JobManager) GetClusterStatus() map[string]interface{} {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	stats := make(map[string]map[string]interface{})
	for name, q := range jm.queues {
		stats[name] = q.GetStats()
	}

	return map[string]interface{}{
		"node_id": jm.nodeID,
		"peers":   jm.peers,
		"queues":  stats,
	}
}

// ReplayDeadLetter re-enqueues a dead-lettered job
func (jm *JobManager) ReplayDeadLetter(queueName string, jobID string) error {
	q, exists := jm.getQueue(queueName)
	if !exists {
		return fmt.Errorf("queue not found: %s", queueName)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Find job in dead-letter queue
	dlq := q.GetDeadLetterQueue()
	var targetJob *Job

	for _, job := range dlq {
		if job.ID == jobID {
			targetJob = job
			break
		}
	}

	if targetJob == nil {
		return fmt.Errorf("job not found in dead-letter queue: %s", jobID)
	}

	// Reset and move back to pending
	targetJob.Status = StatusPending
	targetJob.RetryCount = 0
	targetJob.Error = ""
	targetJob.UpdatedAt = time.Now().UnixMilli()

	q.PendingJobs = append(q.PendingJobs, jobID)
	q.sortPendingJobs()

	return nil
}

// Stop stops the job manager
func (jm *JobManager) Stop() {
	if jm.cleanupTicker != nil {
		jm.cleanupTicker.Stop()
	}
	close(jm.stopChan)
}

// Helper functions

func (jm *JobManager) getQueue(name string) (*JobQueue, bool) {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	q, exists := jm.queues[name]
	return q, exists
}

func (jm *JobManager) acknowledgeJob(queueName string, jobID string) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if _, exists := jm.acknowledgments[queueName]; !exists {
		jm.acknowledgments[queueName] = make(map[string]int64)
	}

	jm.acknowledgments[queueName][jobID]++
}

func (jm *JobManager) backgroundCleanup() {
	for {
		select {
		case <-jm.stopChan:
			return
		case <-jm.cleanupTicker.C:
			jm.cleanupExpiredJobs()
		}
	}
}

func (jm *JobManager) cleanupExpiredJobs() {
	jm.mu.RLock()
	queues := make([]*JobQueue, 0, len(jm.queues))
	for _, q := range jm.queues {
		queues = append(queues, q)
	}
	jm.mu.RUnlock()

	now := time.Now().UnixMilli()

	for _, q := range queues {
		q.mu.Lock()

		// Find and move expired jobs
		newPending := make([]string, 0)
		for _, jobID := range q.PendingJobs {
			job := q.JobIndex[jobID]
			if job.DeadlineAt > 0 && now > job.DeadlineAt {
				q.moveToDeadLetter(job, "deadline exceeded")
			} else {
				newPending = append(newPending, jobID)
			}
		}
		q.PendingJobs = newPending

		// Timeout processing jobs (stuck workers)
		const PROCESSING_TIMEOUT = 5 * time.Minute
		newProcessing := make([]string, 0)
		for _, jobID := range q.ProcessingJobs {
			job := q.JobIndex[jobID]
			if job.ProcessStarted > 0 && now-job.ProcessStarted > PROCESSING_TIMEOUT.Milliseconds() {
				// Job stuck, move back to pending for retry
				job.Status = StatusPending
				job.ProcessedBy = ""
				newPending = append(newPending, jobID)
			} else {
				newProcessing = append(newProcessing, jobID)
			}
		}
		q.ProcessingJobs = newProcessing
		q.PendingJobs = newPending
		q.sortPendingJobs()

		q.mu.Unlock()
	}
}

func (jm *JobManager) broadcastJobState(queueName string, job *Job) {
	// This simulates gossip protocol
	// In real implementation, this would send to other nodes
	// For now, it's a no-op (state is already replicated in memory)
	_ = job
	_ = queueName
}

// JobOptions contains options for enqueueing a job
type JobOptions struct {
	Priority   int
	MaxRetries int
	Deadline   time.Duration
}

// DefaultJobOptions returns default job options
func DefaultJobOptions() JobOptions {
	return JobOptions{
		Priority:   5,
		MaxRetries: 3,
		Deadline:   24 * time.Hour,
	}
}
