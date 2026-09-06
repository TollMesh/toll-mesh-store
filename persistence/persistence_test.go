package persistence

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLogOperationAndReplayWAL(t *testing.T) {
	dir := t.TempDir()
	pe, err := NewPersistenceEngine(dir+"/wal", dir+"/snapshots", time.Minute)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer pe.Close()

	if err := pe.LogOperation("set", "key1", []byte("value1"), "ns1", 0, "node-1", 0); err != nil {
		t.Fatalf("log operation failed: %v", err)
	}
	if err := pe.LogOperation("set", "key2", []byte("value2"), "ns1", 0, "node-1", 0); err != nil {
		t.Fatalf("log operation failed: %v", err)
	}

	entries, err := pe.ReplayWAL(0)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Key != "key1" || entries[1].Key != "key2" {
		t.Errorf("unexpected entry contents: %+v", entries)
	}
}

func TestReplayWALFiltersOldEntries(t *testing.T) {
	dir := t.TempDir()
	pe, err := NewPersistenceEngine(dir+"/wal", dir+"/snapshots", time.Minute)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer pe.Close()

	pe.LogOperation("set", "key1", []byte("value1"), "ns1", 0, "node-1", 0)
	cutoff := time.Now().UnixNano()
	time.Sleep(5 * time.Millisecond)
	pe.LogOperation("set", "key2", []byte("value2"), "ns1", 0, "node-1", 0)

	entries, err := pe.ReplayWAL(cutoff)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if len(entries) != 1 || entries[0].Key != "key2" {
		t.Fatalf("expected only key2 after cutoff, got %+v", entries)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	pe, err := NewPersistenceEngine(dir+"/wal", dir+"/snapshots", time.Minute)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer pe.Close()

	snap := &Snapshot{
		RateLimiters:     map[string]interface{}{"key": float64(5)},
		ReplayProtection: []string{"nonce1"},
		Cache:            map[string]map[string][]byte{"ns": {"k": []byte("v")}},
		CacheTTL:         map[string]map[string]int64{"ns": {"k": 12345}},
	}

	if err := pe.CreateSnapshot(snap); err != nil {
		t.Fatalf("create snapshot failed: %v", err)
	}

	loaded, err := pe.LoadLatestSnapshot()
	if err != nil {
		t.Fatalf("load snapshot failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected a snapshot, got nil")
	}
	if loaded.Cache["ns"]["k"] == nil || string(loaded.Cache["ns"]["k"]) != "v" {
		t.Errorf("cache not restored correctly: %+v", loaded.Cache)
	}
}

func TestLoadLatestSnapshotWhenNoneExist(t *testing.T) {
	dir := t.TempDir()
	pe, err := NewPersistenceEngine(dir+"/wal", dir+"/snapshots", time.Minute)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer pe.Close()

	snap, err := pe.LoadLatestSnapshot()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if snap != nil {
		t.Errorf("expected nil snapshot, got %+v", snap)
	}
}

// TestCreateSnapshotStampsFormatVersion is the regression test for a real
// gap: Snapshot had no version field at all, so a future format change
// would have no way to distinguish old-shaped data from new, risking
// silent misinterpretation instead of a clear upgrade path or error.
func TestCreateSnapshotStampsFormatVersion(t *testing.T) {
	dir := t.TempDir()
	pe, err := NewPersistenceEngine(dir+"/wal", dir+"/snapshots", time.Minute)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer pe.Close()

	snap := &Snapshot{Cache: map[string]map[string][]byte{"ns": {"k": []byte("v")}}}
	if err := pe.CreateSnapshot(snap); err != nil {
		t.Fatalf("create snapshot failed: %v", err)
	}
	if snap.FormatVersion != currentSnapshotFormatVersion {
		t.Errorf("expected FormatVersion %d after CreateSnapshot, got %d", currentSnapshotFormatVersion, snap.FormatVersion)
	}

	loaded, err := pe.LoadLatestSnapshot()
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.FormatVersion != currentSnapshotFormatVersion {
		t.Errorf("expected loaded FormatVersion %d, got %d", currentSnapshotFormatVersion, loaded.FormatVersion)
	}
}

// TestLoadLatestSnapshotTreatsMissingVersionAsVersion1 verifies a
// snapshot file written before FormatVersion existed at all (so it
// decodes with the JSON zero value, 0) loads successfully rather than
// being rejected -- backward compatibility with every snapshot taken by
// this project before this fix, not just future ones.
func TestLoadLatestSnapshotTreatsMissingVersionAsVersion1(t *testing.T) {
	dir := t.TempDir()
	pe, err := NewPersistenceEngine(dir+"/wal", dir+"/snapshots", time.Minute)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer pe.Close()

	// Hand-write a snapshot file with no format_version field at all,
	// simulating one written by code before this field existed.
	oldShaped := map[string]interface{}{
		"timestamp": int64(12345),
		"cache":     map[string]map[string][]byte{"ns": {"k": []byte("old-value")}},
	}
	data, err := json.MarshalIndent(oldShaped, "", "  ")
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if err := os.MkdirAll(dir+"/snapshots", 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(dir+"/snapshots/snapshot-12345.json", data, 0644); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	loaded, err := pe.LoadLatestSnapshot()
	if err != nil {
		t.Fatalf("expected a pre-versioning snapshot to load successfully, got error: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected a loaded snapshot, got nil")
	}
	if loaded.FormatVersion != currentSnapshotFormatVersion {
		t.Errorf("expected a versionless snapshot to be treated as the current version (%d) after upgrade, got %d", currentSnapshotFormatVersion, loaded.FormatVersion)
	}
	if string(loaded.Cache["ns"]["k"]) != "old-value" {
		t.Errorf("expected old-shaped snapshot's data to still load correctly, got %+v", loaded.Cache)
	}
}

// TestLoadLatestSnapshotRejectsFutureFormatVersion is the regression test
// for the other half of the safety property: a snapshot written by a
// NEWER build (format_version greater than this build understands) must
// be rejected with a clear error, not silently misinterpreted -- e.g.
// after a rollback to an older release following an upgrade that already
// took a snapshot in a newer shape.
func TestLoadLatestSnapshotRejectsFutureFormatVersion(t *testing.T) {
	dir := t.TempDir()
	pe, err := NewPersistenceEngine(dir+"/wal", dir+"/snapshots", time.Minute)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer pe.Close()

	futureShaped := map[string]interface{}{
		"format_version": currentSnapshotFormatVersion + 1,
		"timestamp":      int64(99999),
	}
	data, err := json.MarshalIndent(futureShaped, "", "  ")
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if err := os.MkdirAll(dir+"/snapshots", 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(dir+"/snapshots/snapshot-99999.json", data, 0644); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	_, err = pe.LoadLatestSnapshot()
	if err == nil {
		t.Fatal("expected an error loading a snapshot with a future format_version, got nil")
	}
	if !strings.Contains(err.Error(), "format_version") {
		t.Errorf("expected error to mention format_version, got: %v", err)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
