package persistence

import (
	"os"
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

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
