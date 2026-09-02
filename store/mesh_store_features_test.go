package store

import (
	"context"
	"testing"
	"time"

	"github.com/toll-mesh/store/core"
)

func newTestStore(t *testing.T) *MeshStore {
	t.Helper()
	config := &core.ClusterConfig{
		NodeName: "node1",
		BindAddr: "127.0.0.1",
		BindPort: 8000,
		DataDir:  t.TempDir(),
	}
	s, err := NewMeshStore(config)
	if err != nil {
		t.Fatalf("Failed to create mesh store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestMeshStore_JobQueue_EndToEnd(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job, err := s.Enqueue(ctx, "tasks", []byte("payload"), 5, 3, time.Hour)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	claimed, err := s.ClaimJob(ctx, "tasks", "worker-1")
	if err != nil {
		t.Fatalf("ClaimJob failed: %v", err)
	}
	if claimed.ID != job.ID {
		t.Errorf("expected to claim job %s, got %s", job.ID, claimed.ID)
	}

	if err := s.CompleteJob(ctx, "tasks", claimed.ID, []byte("done")); err != nil {
		t.Fatalf("CompleteJob failed: %v", err)
	}

	status, err := s.GetJobStatus(ctx, "tasks", job.ID)
	if err != nil {
		t.Fatalf("GetJobStatus failed: %v", err)
	}
	if status.Status != "completed" {
		t.Errorf("expected status completed, got %v", status.Status)
	}

	stats, err := s.GetQueueStats(ctx, "tasks")
	if err != nil {
		t.Fatalf("GetQueueStats failed: %v", err)
	}
	if stats["total_jobs"] != 1 {
		t.Errorf("expected 1 total job, got %v", stats["total_jobs"])
	}
}

func TestMeshStore_JobQueue_FailAndRetry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job, err := s.Enqueue(ctx, "tasks", []byte("payload"), 5, 3, time.Hour)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	if _, err := s.ClaimJob(ctx, "tasks", "worker-1"); err != nil {
		t.Fatalf("ClaimJob failed: %v", err)
	}

	if err := s.FailJob(ctx, "tasks", job.ID, "boom"); err != nil {
		t.Fatalf("FailJob failed: %v", err)
	}

	status, err := s.GetJobStatus(ctx, "tasks", job.ID)
	if err != nil {
		t.Fatalf("GetJobStatus failed: %v", err)
	}
	if status.Status != "pending" {
		t.Errorf("expected job to be re-queued as pending after failure, got %v", status.Status)
	}
}

func TestMeshStore_SortedSet_EndToEnd(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.ZAdd(ctx, "leaderboard", "alice", 100); err != nil {
		t.Fatalf("ZAdd failed: %v", err)
	}
	if err := s.ZAdd(ctx, "leaderboard", "bob", 200); err != nil {
		t.Fatalf("ZAdd failed: %v", err)
	}
	if err := s.ZAdd(ctx, "leaderboard", "carol", 50); err != nil {
		t.Fatalf("ZAdd failed: %v", err)
	}

	score, ok := s.ZScore(ctx, "leaderboard", "bob")
	if !ok || score != 200 {
		t.Errorf("expected bob's score 200, got %v (ok=%v)", score, ok)
	}

	rank, ok := s.ZRank(ctx, "leaderboard", "carol")
	if !ok || rank != 0 {
		t.Errorf("expected carol's rank 0 (lowest), got %v (ok=%v)", rank, ok)
	}

	revRank, ok := s.ZRevRank(ctx, "leaderboard", "bob")
	if !ok || revRank != 0 {
		t.Errorf("expected bob's rev rank 0 (highest), got %v (ok=%v)", revRank, ok)
	}

	top := s.ZRevRange(ctx, "leaderboard", 1e18, -1e18, 2)
	if len(top) != 2 || top[0].Member != "bob" || top[1].Member != "alice" {
		t.Errorf("expected top-2 [bob, alice], got %+v", top)
	}

	if card := s.ZCard(ctx, "leaderboard"); card != 3 {
		t.Errorf("expected card 3, got %d", card)
	}

	if err := s.ZRem(ctx, "leaderboard", "carol"); err != nil {
		t.Fatalf("ZRem failed: %v", err)
	}
	if card := s.ZCard(ctx, "leaderboard"); card != 2 {
		t.Errorf("expected card 2 after removal, got %d", card)
	}
}

func TestMeshStore_Stream_EndToEnd(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	e1, err := s.XAdd(ctx, "events", map[string]string{"type": "login"})
	if err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}
	if _, err := s.XAdd(ctx, "events", map[string]string{"type": "logout"}); err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}

	if length := s.XLen(ctx, "events"); length != 2 {
		t.Errorf("expected stream length 2, got %d", length)
	}

	if err := s.XGroupCreate(ctx, "events", "analytics"); err != nil {
		t.Fatalf("XGroupCreate failed: %v", err)
	}

	entries, err := s.XReadGroup(ctx, "events", "analytics", "consumer-1", 10)
	if err != nil {
		t.Fatalf("XReadGroup failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected to read 2 entries, got %d", len(entries))
	}

	if err := s.XAck(ctx, "events", "analytics", "consumer-1", e1.ID); err != nil {
		t.Fatalf("XAck failed: %v", err)
	}

	// A second read after acking only entry 1 should still return both entries
	// (offset only advances to e1, entry 2 is still unacknowledged/re-deliverable).
	entriesAfterAck, err := s.XReadGroup(ctx, "events", "analytics", "consumer-1", 10)
	if err != nil {
		t.Fatalf("XReadGroup after ack failed: %v", err)
	}
	if len(entriesAfterAck) == 0 {
		t.Error("expected at least the unacked second entry to be re-readable")
	}
}
