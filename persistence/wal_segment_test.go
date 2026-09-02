package persistence

import (
	"testing"
	"time"
)

func TestWALAppendAndReadAll(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWriteAheadLog(dir, WALConfig{MaxSegmentSize: 1 << 20, RotationTime: time.Hour})
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Stop()

	entries := []*WALEntry{
		{Timestamp: 100, Operation: "set", Key: "k1", Namespace: "ns", Value: []byte("v1")},
		{Timestamp: 200, Operation: "set", Key: "k2", Namespace: "ns", Value: []byte("v2")},
		{Timestamp: 300, Operation: "set", Key: "k3", Namespace: "ns", Value: []byte("v3")},
	}
	for _, e := range entries {
		if err := wal.Append(e); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	read, err := wal.Read(0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(read) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(read))
	}
	for i, e := range read {
		if e.Key != entries[i].Key || string(e.Value.([]byte)) != string(entries[i].Value.([]byte)) {
			t.Errorf("entry %d mismatch: got %+v", i, e)
		}
	}
}

// This is the case that was broken: reading with a fromTimestamp that skips
// some entries used to corrupt the reader for everything after the skip,
// because skipEntry read a trailing checksum field that Append never wrote.
func TestWALReadWithSkippedEntries(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWriteAheadLog(dir, WALConfig{MaxSegmentSize: 1 << 20, RotationTime: time.Hour})
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Stop()

	entries := []*WALEntry{
		{Timestamp: 100, Operation: "set", Key: "old-1", Namespace: "ns", Value: []byte("v1")},
		{Timestamp: 150, Operation: "set", Key: "old-2", Namespace: "ns", Value: []byte("v2")},
		{Timestamp: 200, Operation: "set", Key: "new-1", Namespace: "ns", Value: []byte("v3")},
		{Timestamp: 250, Operation: "set", Key: "new-2", Namespace: "ns", Value: []byte("v4")},
	}
	for _, e := range entries {
		if err := wal.Append(e); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	read, err := wal.Read(199) // should skip the first two entries
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(read) != 2 {
		t.Fatalf("expected 2 entries after skipping, got %d: %+v", len(read), read)
	}
	if read[0].Key != "new-1" || read[1].Key != "new-2" {
		t.Errorf("unexpected entries after skip: %+v", read)
	}
	if string(read[0].Value.([]byte)) != "v3" || string(read[1].Value.([]byte)) != "v4" {
		t.Errorf("entry values corrupted after skip: %+v", read)
	}
}

func TestWALSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	// Tiny max size forces rotation after a couple entries.
	wal, err := NewWriteAheadLog(dir, WALConfig{MaxSegmentSize: 50, RotationTime: time.Hour})
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Stop()

	for i := 0; i < 5; i++ {
		if err := wal.Append(&WALEntry{Timestamp: int64(i), Operation: "set", Key: "k", Namespace: "ns", Value: []byte("value")}); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	stats := wal.GetStats()
	if stats["total_segments"].(int) < 2 {
		t.Errorf("expected multiple segments after rotation, got %v", stats["total_segments"])
	}

	read, err := wal.Read(-1)
	if err != nil {
		t.Fatalf("read across segments failed: %v", err)
	}
	if len(read) != 5 {
		t.Fatalf("expected 5 entries across segments, got %d", len(read))
	}
}
