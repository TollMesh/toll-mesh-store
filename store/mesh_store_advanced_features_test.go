package store

import (
	"context"
	"testing"
	"time"

	"github.com/toll-mesh/store/ranking"
	"github.com/toll-mesh/store/scripting"
	"github.com/toll-mesh/store/search"
	"github.com/toll-mesh/store/transactions"
)

func TestMeshStore_PubSub_EndToEnd(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.Subscribe(ctx, "sub-1", "news", ""); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	count, err := s.Publish(ctx, "news", "publisher-1", []byte("hello"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 delivery, got %d", count)
	}

	messages, err := s.PollMessages(ctx, "sub-1", 10, time.Second)
	if err != nil {
		t.Fatalf("poll failed: %v", err)
	}
	if len(messages) != 1 || string(messages[0].Payload) != "hello" {
		t.Fatalf("unexpected messages: %+v", messages)
	}

	topics := s.GetTopics(ctx)
	if len(topics) != 1 || topics[0] != "news" {
		t.Errorf("expected [news], got %v", topics)
	}
}

func TestMeshStore_Transaction_CommitAppliesToRealCache(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, err := s.BeginTransaction(ctx, "txn-1"); err != nil {
		t.Fatalf("begin failed: %v", err)
	}

	err := s.AddTransactionOperation(ctx, "txn-1", transactions.Operation{
		Type: transactions.OpSet, Namespace: "ns", Key: "k", Value: "v",
	})
	if err != nil {
		t.Fatalf("add operation failed: %v", err)
	}

	// Before commit, the write must not be visible.
	_, exists, _ := s.Get(ctx, "ns", "k")
	if exists {
		t.Fatal("value should not be visible before commit")
	}

	if err := s.CommitTransaction(ctx, "txn-1"); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	value, exists, _ := s.Get(ctx, "ns", "k")
	if !exists || string(value) != "v" {
		t.Fatalf("expected committed value 'v', got %q (exists=%v)", value, exists)
	}
}

func TestMeshStore_Transaction_RollbackDoesNotApply(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.BeginTransaction(ctx, "txn-1")
	s.AddTransactionOperation(ctx, "txn-1", transactions.Operation{
		Type: transactions.OpSet, Namespace: "ns", Key: "k", Value: "v",
	})
	if err := s.RollbackTransaction(ctx, "txn-1"); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	_, exists, _ := s.Get(ctx, "ns", "k")
	if exists {
		t.Error("rolled-back operation should never have been applied")
	}
}

func TestMeshStore_Persistence_SnapshotAndRestore(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Set(ctx, "ns", "key1", []byte("value1"), time.Hour)
	s.Consume(ctx, "rate-key", 10, time.Minute)
	s.Seen(ctx, "nonce-1", time.Minute)

	if err := s.CreateSnapshot(ctx); err != nil {
		t.Fatalf("create snapshot failed: %v", err)
	}

	snap, err := s.GetLatestSnapshot(ctx)
	if err != nil {
		t.Fatalf("get latest snapshot failed: %v", err)
	}
	if snap == nil {
		t.Fatal("expected a snapshot, got nil")
	}
	if string(snap.Cache["ns"]["key1"]) != "value1" {
		t.Errorf("snapshot missing cache value: %+v", snap.Cache)
	}

	// Mutate live state, then restore from snapshot and verify it reverts.
	s.Set(ctx, "ns", "key2", []byte("value2"), time.Hour)
	if err := s.RestoreFromLatestSnapshot(ctx); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	_, exists, _ := s.Get(ctx, "ns", "key1")
	if !exists {
		t.Error("expected key1 to survive restore")
	}
	seen, _ := s.Seen(ctx, "nonce-1", time.Minute)
	if !seen {
		t.Error("expected replay protection to be restored (nonce-1 already seen)")
	}
}

func TestMeshStore_Pipeline_RealOperationsCompose(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	result, err := s.ExecuteInlinePipeline(ctx, []scripting.Step{
		{Op: "set", Args: map[string]interface{}{"namespace": "ns", "key": "k", "value": "hello"}},
		{Op: "get", Args: map[string]interface{}{"namespace": "ns", "key": "k"}, SaveAs: "got"},
	})
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	if len(result.Steps) != 2 {
		t.Fatalf("expected 2 step results, got %d", len(result.Steps))
	}

	// Verify the pipeline's "set" step had a real effect on the store, not
	// just on some pipeline-local state.
	value, exists, _ := s.Get(ctx, "ns", "k")
	if !exists || string(value) != "hello" {
		t.Errorf("pipeline's set step did not affect real store state: %q exists=%v", value, exists)
	}
}

func TestMeshStore_Search_EndToEnd(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.IndexDocument(ctx, &search.Document{ID: "1", Content: "distributed systems"}); err != nil {
		t.Fatalf("index failed: %v", err)
	}
	if err := s.IndexDocument(ctx, &search.Document{ID: "2", Content: "cooking recipes"}); err != nil {
		t.Fatalf("index failed: %v", err)
	}

	results := s.SearchBM25(ctx, "distributed", 10)
	if len(results) != 1 || results[0].Document.ID != "1" {
		t.Fatalf("expected doc 1 to match, got %+v", results)
	}

	if err := s.DeleteSearchDocument(ctx, "1"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	results = s.SearchBM25(ctx, "distributed", 10)
	if len(results) != 0 {
		t.Errorf("expected no results after delete, got %+v", results)
	}
}

func TestMeshStore_Rank(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	items := []ranking.RankedItem{{ID: "a", Score: 1}, {ID: "b", Score: 3}}
	result := s.Rank(ctx, items, "bm25", nil)
	if result[0].ID != "b" {
		t.Errorf("expected 'b' to rank first, got %s", result[0].ID)
	}

	boosted := s.Rank(ctx, items, "context", map[string]float32{"a": 10})
	if boosted[0].ID != "a" {
		t.Errorf("expected boosted 'a' to rank first, got %s", boosted[0].ID)
	}
}

func TestMeshStore_Metrics_RecordsRealOperations(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Consume(ctx, "key", 10, time.Minute)
	s.Set(ctx, "ns", "k", []byte("v"), time.Hour)
	s.Get(ctx, "ns", "k")

	stats := s.GetMetrics(ctx)
	if stats["consume_total"] != int64(1) {
		t.Errorf("expected 1 consume recorded, got %v", stats["consume_total"])
	}
	if stats["set_total"] != int64(1) {
		t.Errorf("expected 1 set recorded, got %v", stats["set_total"])
	}
	if stats["get_hits"] != int64(1) {
		t.Errorf("expected 1 get hit recorded, got %v", stats["get_hits"])
	}

	prom := s.GetPrometheusMetrics(ctx)
	if len(prom) == 0 {
		t.Error("expected non-empty Prometheus output")
	}
}
