package store

import (
	"context"
	"testing"
	"time"

	"github.com/toll-mesh/store/core"
)

// TestMergedCacheValueSurvivesRestart is the regression test for a real gap
// found while live-verifying the cache LWW-register CRDT merge:
// MergeState updated ms.cache in memory but never called
// PersistenceEngine.LogOperation, so a value this node only *learned via
// gossip* (as opposed to wrote itself) was never durable -- a crash on the
// receiving node reverted it to its own older local write on restart,
// silently undoing convergence that had already happened. Confirmed live
// against two real server processes before writing this test.
func TestMergedCacheValueSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dataDir1 := t.TempDir()

	node1, err := NewMeshStore(&core.ClusterConfig{NodeName: "node-1", DataDir: dataDir1})
	if err != nil {
		t.Fatalf("NewMeshStore(node1) failed: %v", err)
	}
	node2, err := NewMeshStore(&core.ClusterConfig{NodeName: "node-2", DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewMeshStore(node2) failed: %v", err)
	}
	defer node2.Close()

	if err := node1.Set(ctx, "shared", "key", []byte("from-node1-first"), 0); err != nil {
		t.Fatalf("node1 Set failed: %v", err)
	}
	time.Sleep(2 * time.Millisecond) // ensure a strictly later wall-clock write
	if err := node2.Set(ctx, "shared", "key", []byte("from-node2-later"), 0); err != nil {
		t.Fatalf("node2 Set failed: %v", err)
	}

	// Merge node2's state into node1 directly (skipping the HTTP/gossip
	// transport, which api/gossip_integration_test.go already covers) --
	// this is exactly what a real gossip round does under the hood.
	node1.MergeState(node2.GetState())

	value, exists, err := node1.Get(ctx, "shared", "key")
	if err != nil || !exists || string(value) != "from-node2-later" {
		t.Fatalf("node1 after merge: shared/key = %q exists=%v err=%v, want \"from-node2-later\"", value, exists, err)
	}

	if err := node1.Close(); err != nil {
		t.Fatalf("node1 Close failed: %v", err)
	}

	// Simulate a restart: a brand new MeshStore on node1's own DataDir.
	restarted, err := NewMeshStore(&core.ClusterConfig{NodeName: "node-1", DataDir: dataDir1})
	if err != nil {
		t.Fatalf("NewMeshStore (restarted node1) failed: %v", err)
	}
	defer restarted.Close()

	value, exists, err = restarted.Get(ctx, "shared", "key")
	if err != nil || !exists || string(value) != "from-node2-later" {
		t.Fatalf("node1 after restart: shared/key = %q exists=%v err=%v, want \"from-node2-later\" (the merged value, not node1's own stale write)", value, exists, err)
	}
}

// TestRecoverFromWALWithoutSnapshot is the regression test for a real gap:
// MeshStore.Set/Consume/Seen never called the WAL's write method at all, so
// a fresh MeshStore pointed at the same DataDir as a previous, never-
// explicitly-snapshotted process had no way to recover anything -- despite
// "write-ahead log" strongly implying every write is logged. This writes
// through a first MeshStore instance (no snapshot taken), closes it, opens
// a second MeshStore on the same DataDir, and asserts every write is
// visible without ever calling create_snapshot/restore_from_latest_snapshot
// -- i.e. recovery now happens automatically at startup, from the WAL
// alone.
func TestRecoverFromWALWithoutSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	ctx := context.Background()

	config := &core.ClusterConfig{NodeName: "node1", DataDir: dataDir}
	first, err := NewMeshStore(config)
	if err != nil {
		t.Fatalf("NewMeshStore (first) failed: %v", err)
	}

	if err := first.Set(ctx, "users", "alice", []byte("hello"), 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := first.Set(ctx, "users", "bob", []byte("world"), time.Hour); err != nil {
		t.Fatalf("Set with TTL failed: %v", err)
	}
	if _, err := first.Seen(ctx, "nonce-1", time.Minute); err != nil {
		t.Fatalf("Seen failed: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := first.Consume(ctx, "api-limit", 100, time.Minute); err != nil {
			t.Fatalf("Consume failed: %v", err)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// A brand new MeshStore, same DataDir, simulating a process restart.
	// No create_snapshot / restore_from_latest_snapshot call anywhere --
	// this must recover purely from the WAL written by `first`.
	second, err := NewMeshStore(&core.ClusterConfig{NodeName: "node1", DataDir: dataDir})
	if err != nil {
		t.Fatalf("NewMeshStore (second, recovering) failed: %v", err)
	}
	defer second.Close()

	value, exists, err := second.Get(ctx, "users", "alice")
	if err != nil || !exists || string(value) != "hello" {
		t.Errorf("users/alice after recovery = %q exists=%v err=%v, want \"hello\"", value, exists, err)
	}

	value, exists, err = second.Get(ctx, "users", "bob")
	if err != nil || !exists || string(value) != "world" {
		t.Errorf("users/bob after recovery = %q exists=%v err=%v, want \"world\"", value, exists, err)
	}

	seen, err := second.Seen(ctx, "nonce-1", time.Minute)
	if err != nil || !seen {
		t.Errorf("Seen(nonce-1) after recovery = %v err=%v, want true (should already be marked seen)", seen, err)
	}

	// 3 consumes already happened against a limit of 100; a 4th consume
	// should report 96 remaining (100 - 3 already recovered - 1 for this
	// call), proving the GCounter's count, not just its existence,
	// survived recovery.
	result, err := second.Consume(ctx, "api-limit", 100, time.Minute)
	if err != nil {
		t.Fatalf("Consume after recovery failed: %v", err)
	}
	if result.Remaining != 96 {
		t.Errorf("Remaining after recovery = %d, want 96 (recovered count of 3 + this call)", result.Remaining)
	}
}

// TestRecoverFromSnapshotAndWAL verifies the combined path: a snapshot
// captures state up to a point, the WAL (freshly rotated by
// CreateSnapshot) captures everything after, and recovery correctly
// layers the two -- not just one or the other.
func TestRecoverFromSnapshotAndWAL(t *testing.T) {
	dataDir := t.TempDir()
	ctx := context.Background()

	config := &core.ClusterConfig{NodeName: "node1", DataDir: dataDir}
	first, err := NewMeshStore(config)
	if err != nil {
		t.Fatalf("NewMeshStore (first) failed: %v", err)
	}

	if err := first.Set(ctx, "users", "alice", []byte("before-snapshot"), 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := first.CreateSnapshot(ctx); err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	if err := first.Set(ctx, "users", "carol", []byte("after-snapshot"), 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	second, err := NewMeshStore(&core.ClusterConfig{NodeName: "node1", DataDir: dataDir})
	if err != nil {
		t.Fatalf("NewMeshStore (second, recovering) failed: %v", err)
	}
	defer second.Close()

	value, exists, err := second.Get(ctx, "users", "alice")
	if err != nil || !exists || string(value) != "before-snapshot" {
		t.Errorf("users/alice (from snapshot) after recovery = %q exists=%v err=%v", value, exists, err)
	}
	value, exists, err = second.Get(ctx, "users", "carol")
	if err != nil || !exists || string(value) != "after-snapshot" {
		t.Errorf("users/carol (from post-snapshot WAL) after recovery = %q exists=%v err=%v", value, exists, err)
	}
}
