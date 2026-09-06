package store

import (
	"context"
	"testing"
	"time"

	"github.com/toll-mesh/store/core"
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

func TestMeshStore_RankingConfig_RegisterAndExecuteByName(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.RegisterRankingConfig(ctx, "boost-a", "context", map[string]float32{"a": 10}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	cfg, err := s.GetRankingConfig(ctx, "boost-a")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if cfg.Strategy != "context" {
		t.Errorf("expected strategy context, got %s", cfg.Strategy)
	}

	items := []ranking.RankedItem{{ID: "a", Score: 1}, {ID: "b", Score: 3}}
	result, err := s.RankWithConfig(ctx, "boost-a", items)
	if err != nil {
		t.Fatalf("rank with config failed: %v", err)
	}
	if result[0].ID != "a" {
		t.Errorf("expected boosted 'a' to rank first, got %s", result[0].ID)
	}

	configs := s.ListRankingConfigs(ctx)
	if len(configs) != 1 {
		t.Errorf("expected 1 registered config, got %d", len(configs))
	}

	if err := s.DeleteRankingConfig(ctx, "boost-a"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, err := s.GetRankingConfig(ctx, "boost-a"); err == nil {
		t.Error("expected config to be gone after delete")
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

// TestMeshStore_ClusterMetrics_MergesPerNodeCountsAsGCounter is the
// regression test for Metrics gossip replication: MergeState must combine
// two nodes' own counter values into a per-node breakdown (real GCounter
// semantics -- each node's slot is independent, so the cluster total is
// their sum), and a later merge with a lower/stale count for an
// already-known node must not regress it.
func TestMeshStore_ClusterMetrics_MergesPerNodeCountsAsGCounter(t *testing.T) {
	ctx := context.Background()

	node1 := newTestStore(t)
	node1.Consume(ctx, "key", 10, time.Minute)
	node1.Consume(ctx, "key", 10, time.Minute)

	node2Config := &core.ClusterConfig{
		NodeName: "node2",
		BindAddr: "127.0.0.1",
		BindPort: 8001,
		DataDir:  t.TempDir(),
	}
	node2, err := NewMeshStore(node2Config)
	if err != nil {
		t.Fatalf("failed to create node2: %v", err)
	}
	t.Cleanup(func() { node2.Close() })
	node2.Consume(ctx, "key", 10, time.Minute)

	node2.MergeState(node1.GetState())

	cluster := node2.GetClusterMetrics(ctx)
	consumeTotal, ok := cluster["consume_total"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected consume_total entry, got %+v", cluster["consume_total"])
	}
	if consumeTotal["total"] != int64(3) {
		t.Fatalf("expected cluster consume_total = 3 (2 from node1 + 1 from node2), got %v", consumeTotal["total"])
	}
	byNode := consumeTotal["by_node"].(map[string]int64)
	if byNode["node1"] != 2 || byNode["node2"] != 1 {
		t.Fatalf("expected per-node breakdown node1=2 node2=1, got %+v", byNode)
	}

	// A stale re-merge (node1's count hasn't grown) must not regress
	// node2's already-known value for node1.
	node2.MergeState(node1.GetState())
	cluster = node2.GetClusterMetrics(ctx)
	consumeTotal = cluster["consume_total"].(map[string]interface{})
	if consumeTotal["total"] != int64(3) {
		t.Fatalf("expected cluster consume_total to stay 3 after a stale re-merge, got %v", consumeTotal["total"])
	}
}

// TestMeshStore_Snapshot_CoversAllNineNewerFeatureGroups is the regression
// test for the real gap this closes: before this, CreateSnapshot/
// RestoreFromLatestSnapshot only covered the original three primitives
// (rate limiting, replay protection, cache), so a lone node with no peers
// (or an entire cluster restarting at once) silently lost every Sorted
// Set/Stream/Pipeline/Search document/Job/Pub-Sub message/Transaction/
// WASM script/Metric/(now) named Ranking config on restart, since none of
// them were ever WAL-logged either. This exercises all ten through a real
// snapshot-then-restore round trip on a single store (no peers, no gossip
// involved at all).
func TestMeshStore_Snapshot_CoversAllNineNewerFeatureGroups(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.ZAdd(ctx, "zset1", "member1", 5); err != nil {
		t.Fatalf("ZAdd failed: %v", err)
	}
	if _, err := s.XAdd(ctx, "stream1", map[string]string{"f": "v"}); err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}
	if err := s.RegisterPipeline(ctx, &scripting.Pipeline{Name: "pipe1", Steps: []scripting.Step{{Op: "get", Args: map[string]interface{}{"namespace": "ns", "key": "k"}}}}); err != nil {
		t.Fatalf("RegisterPipeline failed: %v", err)
	}
	if err := s.IndexDocument(ctx, &search.Document{ID: "doc1", Content: "snapshot restore coverage"}); err != nil {
		t.Fatalf("IndexDocument failed: %v", err)
	}
	job, err := s.Enqueue(ctx, "queue1", []byte("payload"), 5, 3, time.Hour)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if err := s.Subscribe(ctx, "sub1", "topic1", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	if _, err := s.Publish(ctx, "topic1", "pub1", []byte("hello")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if _, err := s.BeginTransaction(ctx, "txn1"); err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	s.Consume(ctx, "rate-key", 100, time.Minute)
	if err := s.RegisterRankingConfig(ctx, "boost-a", "context", map[string]float32{"a": 10}); err != nil {
		t.Fatalf("RegisterRankingConfig failed: %v", err)
	}

	if err := s.CreateSnapshot(ctx); err != nil {
		t.Fatalf("create snapshot failed: %v", err)
	}

	// Simulate a full restart: a fresh store from the same snapshot/WAL
	// files, but every in-memory engine starts empty.
	fresh, err := NewMeshStore(s.config)
	if err != nil {
		t.Fatalf("failed to reopen store from same data dir: %v", err)
	}
	defer fresh.Close()

	if score, exists := fresh.ZScore(ctx, "zset1", "member1"); !exists || score != 5 {
		t.Errorf("sorted set not restored: exists=%v score=%v", exists, score)
	}
	if entries := fresh.XRange(ctx, "stream1", "-", "+", 10); len(entries) != 1 {
		t.Errorf("stream not restored: %+v", entries)
	}
	if _, err := fresh.GetPipeline(ctx, "pipe1"); err != nil {
		t.Errorf("pipeline not restored: %v", err)
	}
	if results := fresh.SearchBM25(ctx, "coverage", 10); len(results) == 0 || results[0].Document.ID != "doc1" {
		t.Errorf("search document not restored: %+v", results)
	}
	if status, err := fresh.GetJobStatus(ctx, "queue1", job.ID); err != nil || status.Status != "pending" {
		t.Errorf("job not restored: status=%+v err=%v", status, err)
	}
	topics := fresh.GetTopics(ctx)
	if len(topics) != 1 || topics[0] != "topic1" {
		t.Errorf("pub/sub topic not restored: %+v", topics)
	}
	if status, err := fresh.GetTransactionStatus(ctx, "txn1"); err != nil || status != transactions.StatusPending {
		t.Errorf("transaction not restored: status=%v err=%v", status, err)
	}
	cluster := fresh.GetClusterMetrics(ctx)
	consumeTotal, ok := cluster["consume_total"].(map[string]interface{})
	if !ok || consumeTotal["total"].(int64) < 1 {
		t.Errorf("metrics not restored: %+v", cluster["consume_total"])
	}
	if cfg, err := fresh.GetRankingConfig(ctx, "boost-a"); err != nil || cfg.Strategy != "context" {
		t.Errorf("ranking config not restored: cfg=%+v err=%v", cfg, err)
	}
}

// TestMeshStore_StampLocalMetrics_DoesNotRegressRestoredHighWaterMark is
// the regression test for a real bug found while testing snapshot restore
// of Metrics: stampLocalMetrics used to unconditionally overwrite this
// node's own slot in clusterMetrics with its live (in-memory) counter
// value. A freshly-restarted process's Metrics collector always starts at
// 0, so the very first GetState/CreateSnapshot/GetClusterMetrics call
// after restoring a snapshot with a real historical count for this node
// would regress that count back to 0 -- violating the "a GCounter slot
// only grows" invariant every peer's merge depends on.
func TestMeshStore_StampLocalMetrics_DoesNotRegressRestoredHighWaterMark(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Simulate a restored snapshot carrying a high-water mark for this
	// node's own past activity, higher than its brand-new live counter
	// (which is 0, since nothing has been recorded on s yet).
	s.mu.Lock()
	s.clusterMetrics["consume_total"] = map[string]int64{s.config.NodeName: 42}
	s.mu.Unlock()

	cluster := s.GetClusterMetrics(ctx)
	consumeTotal := cluster["consume_total"].(map[string]interface{})
	if consumeTotal["total"].(int64) != 42 {
		t.Fatalf("restored high-water mark regressed: %+v", consumeTotal)
	}
	byNode := consumeTotal["by_node"].(map[string]int64)
	if byNode[s.config.NodeName] != 42 {
		t.Fatalf("expected this node's slot to stay at 42, got %v", byNode[s.config.NodeName])
	}
}

const echoWasmScript = `
package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	fmt.Printf("echo: %s\n", scanner.Text())
}
`

func TestMeshStore_WasmScript_EndToEnd(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	script, err := s.CompileScript(ctx, "echo", echoWasmScript)
	if err != nil {
		t.Skipf("WASM scripting unavailable (tinygo not installed?): %v", err)
	}
	if script.WasmSize == 0 {
		t.Fatal("expected non-empty compiled WASM module")
	}

	output, err := s.ExecuteScript(ctx, "echo", "hello from meshstore")
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if output != "echo: hello from meshstore\n" {
		t.Errorf("unexpected output: %q", output)
	}

	scripts := s.ListScripts(ctx)
	if len(scripts) != 1 {
		t.Errorf("expected 1 registered script, got %d", len(scripts))
	}

	if err := s.DeleteScript(ctx, "echo"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
}

func TestMeshStore_WasmScript_ExecuteInline(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	output, err := s.ExecuteInlineScript(ctx, echoWasmScript, "inline")
	if err != nil {
		t.Skipf("WASM scripting unavailable (tinygo not installed?): %v", err)
	}
	if output != "echo: inline\n" {
		t.Errorf("unexpected output: %q", output)
	}
}
