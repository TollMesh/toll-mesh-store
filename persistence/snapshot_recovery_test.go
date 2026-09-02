package persistence

import (
	"testing"
	"time"
)

func TestSnapshotManagerCreateAndRetrieve(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir, SnapshotConfig{SnapshotInterval: time.Hour, MaxSnapshots: 5})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	snap := &Snapshot{
		Timestamp:    time.Now().UnixMilli(),
		RateLimiters: map[string]interface{}{"k": float64(1)},
	}
	if err := sm.CreateSnapshot(snap); err != nil {
		t.Fatalf("create snapshot failed: %v", err)
	}

	latest, err := sm.GetLatestSnapshot()
	if err != nil {
		t.Fatalf("get latest failed: %v", err)
	}
	if latest.Timestamp != snap.Timestamp {
		t.Errorf("expected timestamp %d, got %d", snap.Timestamp, latest.Timestamp)
	}

	list := sm.ListSnapshots()
	if len(list) != 1 {
		t.Errorf("expected 1 snapshot in list, got %d", len(list))
	}
}

func TestSnapshotManagerMaxSnapshotsCleanup(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir, SnapshotConfig{SnapshotInterval: time.Hour, MaxSnapshots: 2})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	for i := 0; i < 4; i++ {
		sm.CreateSnapshot(&Snapshot{Timestamp: time.Now().UnixMilli()})
		time.Sleep(2 * time.Millisecond) // ensure distinct nanosecond-based IDs
	}

	list := sm.ListSnapshots()
	if len(list) != 2 {
		t.Errorf("expected cleanup to keep only 2 snapshots, got %d", len(list))
	}
}

func TestRecoveryManagerFromWAL(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWriteAheadLog(dir+"/wal", WALConfig{MaxSegmentSize: 1 << 20, RotationTime: time.Hour})
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Stop()
	wal.Append(&WALEntry{Timestamp: 100, Operation: "set", Key: "k", Namespace: "ns", Value: []byte("v")})

	sm, err := NewSnapshotManager(dir+"/snapshots", SnapshotConfig{SnapshotInterval: time.Hour, MaxSnapshots: 5})
	if err != nil {
		t.Fatalf("failed to create snapshot manager: %v", err)
	}

	rm := NewRecoveryManager(wal, sm, RecoveryConfig{EnableRecovery: true, RecoveryTimeout: time.Minute, MaxRecoveryAttempts: 3})

	if err := rm.RecoverFromWAL(0); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	stats := rm.GetRecoveryStats()
	if stats["entries_replayed"] != 1 {
		t.Errorf("expected 1 entry replayed, got %v", stats["entries_replayed"])
	}
}

func TestRecoveryDisabled(t *testing.T) {
	dir := t.TempDir()
	wal, _ := NewWriteAheadLog(dir+"/wal", WALConfig{MaxSegmentSize: 1 << 20, RotationTime: time.Hour})
	defer wal.Stop()
	sm, _ := NewSnapshotManager(dir+"/snapshots", SnapshotConfig{SnapshotInterval: time.Hour, MaxSnapshots: 5})

	rm := NewRecoveryManager(wal, sm, RecoveryConfig{EnableRecovery: false})
	if err := rm.RecoverFromCrash(); err == nil {
		t.Error("expected error when recovery is disabled")
	}
}
