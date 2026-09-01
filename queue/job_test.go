package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestEnqueueJob(t *testing.T) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	payload := []byte("test job")
	job, err := jm.Enqueue("test-queue", payload, JobOptions{
		Priority:   5,
		MaxRetries: 3,
		Deadline:   1 * time.Hour,
	})

	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if job == nil {
		t.Fatal("job is nil")
	}

	if job.Status != StatusPending {
		t.Errorf("expected status pending, got %s", job.Status)
	}

	if job.Priority != 5 {
		t.Errorf("expected priority 5, got %d", job.Priority)
	}
}

func TestClaimJob(t *testing.T) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	// Enqueue job
	_, err := jm.Enqueue("test-queue", []byte("test"), DefaultJobOptions())
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Claim job
	job, err := jm.ClaimJob("test-queue", "worker-1")
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}

	if job.Status != StatusProcessing {
		t.Errorf("expected status processing, got %s", job.Status)
	}

	if job.ProcessedBy != "worker-1" {
		t.Errorf("expected worker worker-1, got %s", job.ProcessedBy)
	}
}

func TestCompleteJob(t *testing.T) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	// Enqueue and claim
	job, _ := jm.Enqueue("test-queue", []byte("test"), DefaultJobOptions())
	jm.ClaimJob("test-queue", "worker-1")

	// Complete job
	err := jm.CompleteJob("test-queue", job.ID, []byte("result"))
	if err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	// Check status
	completed, _ := jm.GetJobStatus("test-queue", job.ID)
	if completed.Status != StatusCompleted {
		t.Errorf("expected status completed, got %s", completed.Status)
	}
}

func TestJobRetry(t *testing.T) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	// Enqueue job
	job, _ := jm.Enqueue("test-queue", []byte("test"), JobOptions{
		MaxRetries: 3,
	})

	// Claim and fail
	jm.ClaimJob("test-queue", "worker-1")
	jm.FailJob("test-queue", job.ID, "worker crashed")

	// Should still be pending (not in dead letter yet)
	status, _ := jm.GetJobStatus("test-queue", job.ID)
	if status.Status != StatusPending {
		t.Errorf("expected status pending after retry, got %s", status.Status)
	}

	if status.RetryCount != 1 {
		t.Errorf("expected retry count 1, got %d", status.RetryCount)
	}
}

func TestMaxRetryExceeded(t *testing.T) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	// Enqueue job with 1 max retry
	job, _ := jm.Enqueue("test-queue", []byte("test"), JobOptions{
		MaxRetries: 1,
	})

	// Fail twice
	jm.ClaimJob("test-queue", "worker-1")
	jm.FailJob("test-queue", job.ID, "error 1")

	jm.ClaimJob("test-queue", "worker-2")
	jm.FailJob("test-queue", job.ID, "error 2")

	// Should be in dead letter queue
	status, _ := jm.GetJobStatus("test-queue", job.ID)
	if status.Status != StatusFailed {
		t.Errorf("expected status failed, got %s", status.Status)
	}

	dlq, _ := jm.GetDeadLetterQueue("test-queue")
	if len(dlq) == 0 {
		t.Fatal("expected job in dead letter queue")
	}
}

func TestPriorityOrdering(t *testing.T) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	// Enqueue multiple jobs with different priorities
	jm.Enqueue("test-queue", []byte("low"), JobOptions{Priority: 1})
	jm.Enqueue("test-queue", []byte("medium"), JobOptions{Priority: 5})
	jm.Enqueue("test-queue", []byte("high"), JobOptions{Priority: 10})

	// Jobs should be claimed in priority order
	job1, _ := jm.ClaimJob("test-queue", "worker-1")
	job2, _ := jm.ClaimJob("test-queue", "worker-2")
	job3, _ := jm.ClaimJob("test-queue", "worker-3")

	if job1.Priority != 10 {
		t.Errorf("first job should be high priority (10), got %d", job1.Priority)
	}

	if job2.Priority != 5 {
		t.Errorf("second job should be medium priority (5), got %d", job2.Priority)
	}

	if job3.Priority != 1 {
		t.Errorf("third job should be low priority (1), got %d", job3.Priority)
	}
}

func TestConcurrentClaiming(t *testing.T) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	// Enqueue 100 jobs
	for i := 0; i < 100; i++ {
		jm.Enqueue("test-queue", []byte(fmt.Sprintf("job-%d", i)), DefaultJobOptions())
	}

	// Have 10 workers claim jobs concurrently
	var wg sync.WaitGroup
	jobs := make(map[string]bool)
	var mu sync.Mutex

	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				job, err := jm.ClaimJob("test-queue", workerID)
				if err != nil {
					break
				}
				mu.Lock()
				jobs[job.ID] = true
				mu.Unlock()
			}
		}(fmt.Sprintf("worker-%d", w))
	}

	wg.Wait()

	// All 100 jobs should be claimed exactly once
	if len(jobs) != 100 {
		t.Errorf("expected 100 unique jobs claimed, got %d", len(jobs))
	}
}

func TestDeadlineExpiry(t *testing.T) {
	jm := NewJobManager("node-1")

	// Enqueue job with 1 second deadline
	job, _ := jm.Enqueue("test-queue", []byte("test"), JobOptions{
		Deadline: 1 * time.Second,
	})

	// Wait for expiry
	time.Sleep(2 * time.Second)

	// Manually trigger cleanup (don't use background loop in test)
	q := jm.GetOrCreateQueue("test-queue")
	q.mu.Lock()

	now := time.Now().UnixMilli()
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
	q.mu.Unlock()

	jm.Stop()

	// Job should be in dead letter queue
	dlq, _ := jm.GetDeadLetterQueue("test-queue")
	found := false
	for _, dlqJob := range dlq {
		if dlqJob.ID == job.ID {
			found = true
			break
		}
	}

	if !found {
		t.Fatal("expected expired job in dead letter queue")
	}
}

func TestProcessingTimeout(t *testing.T) {
	jm := NewJobManager("node-1")

	// Enqueue and claim job
	job, _ := jm.Enqueue("test-queue", []byte("test"), DefaultJobOptions())
	claimedJob, _ := jm.ClaimJob("test-queue", "worker-1")

	// Manually set process start time to 10 minutes ago
	claimedJob.ProcessStarted = time.Now().Add(-10 * time.Minute).UnixMilli()

	// Manually trigger cleanup
	q := jm.GetOrCreateQueue("test-queue")
	q.mu.Lock()

	const PROCESSING_TIMEOUT = 5 * time.Minute
	now := time.Now().UnixMilli()
	newProcessing := make([]string, 0)
	newPending := make([]string, 0)

	for _, jobID := range q.ProcessingJobs {
		job := q.JobIndex[jobID]
		if job.ProcessStarted > 0 && now-job.ProcessStarted > PROCESSING_TIMEOUT.Milliseconds() {
			job.Status = StatusPending
			job.ProcessedBy = ""
			newPending = append(newPending, jobID)
		} else {
			newProcessing = append(newProcessing, jobID)
		}
	}

	q.ProcessingJobs = newProcessing
	q.PendingJobs = append(q.PendingJobs, newPending...)
	q.mu.Unlock()

	jm.Stop()

	// Job should be back to pending
	status, _ := jm.GetJobStatus("test-queue", job.ID)
	if status.Status != StatusPending {
		t.Errorf("expected status pending after timeout, got %s", status.Status)
	}

	if status.ProcessedBy != "" {
		t.Errorf("expected processed_by empty after timeout, got %s", status.ProcessedBy)
	}
}

func TestQueueStats(t *testing.T) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	// Enqueue 3 jobs
	jm.Enqueue("test-queue", []byte("1"), DefaultJobOptions())
	jm.Enqueue("test-queue", []byte("2"), DefaultJobOptions())
	jm.Enqueue("test-queue", []byte("3"), DefaultJobOptions())

	// Claim two, leaving 1 pending
	claimedA, err := jm.ClaimJob("test-queue", "worker-1")
	if err != nil {
		t.Fatalf("claim A failed: %v", err)
	}
	_, err = jm.ClaimJob("test-queue", "worker-2")
	if err != nil {
		t.Fatalf("claim B failed: %v", err)
	}

	// Complete one of the two claimed jobs, leaving 1 processing
	if err := jm.CompleteJob("test-queue", claimedA.ID, []byte("result")); err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	// Check stats
	stats, _ := jm.GetQueueStats("test-queue")

	if stats["total_jobs"] != 3 {
		t.Errorf("expected 3 total jobs, got %v", stats["total_jobs"])
	}

	if stats["pending"] != 1 {
		t.Errorf("expected 1 pending, got %v", stats["pending"])
	}

	if stats["processing"] != 1 {
		t.Errorf("expected 1 processing, got %v", stats["processing"])
	}
}

func TestReplayDeadLetter(t *testing.T) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	// Enqueue and fail job
	job, _ := jm.Enqueue("test-queue", []byte("test"), JobOptions{MaxRetries: 1})
	jm.ClaimJob("test-queue", "worker-1")
	jm.FailJob("test-queue", job.ID, "error")

	// Should be in dead letter
	dlq, _ := jm.GetDeadLetterQueue("test-queue")
	if len(dlq) == 0 {
		t.Fatal("expected job in dead letter queue")
	}

	// Replay job
	err := jm.ReplayDeadLetter("test-queue", job.ID)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	// Should be back to pending
	status, _ := jm.GetJobStatus("test-queue", job.ID)
	if status.Status != StatusPending {
		t.Errorf("expected status pending after replay, got %s", status.Status)
	}

	if status.RetryCount != 0 {
		t.Errorf("expected retry count reset to 0, got %d", status.RetryCount)
	}
}

func BenchmarkEnqueue(b *testing.B) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jm.Enqueue("bench-queue", []byte("test"), DefaultJobOptions())
	}
}

func BenchmarkClaim(b *testing.B) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	// Enqueue 1000 jobs
	for i := 0; i < 1000; i++ {
		jm.Enqueue("bench-queue", []byte("test"), DefaultJobOptions())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jm.ClaimJob("bench-queue", "worker-1")
	}
}

func BenchmarkComplete(b *testing.B) {
	jm := NewJobManager("node-1")
	defer jm.Stop()

	// Enqueue jobs
	jobs := make([]*Job, b.N)
	for i := 0; i < b.N; i++ {
		job, _ := jm.Enqueue("bench-queue", []byte("test"), DefaultJobOptions())
		jm.ClaimJob("bench-queue", "worker-1")
		jobs[i] = job
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jm.CompleteJob("bench-queue", jobs[i].ID, []byte("result"))
	}
}
