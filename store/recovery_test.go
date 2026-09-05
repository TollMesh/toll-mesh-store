package store

import (
	"context"
	"testing"
	"time"

	"github.com/toll-mesh/store/core"
)

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
