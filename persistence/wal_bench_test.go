package persistence

import (
	"fmt"
	"testing"
	"time"
)

// This file benchmarks WriteAheadLog (checksummed, length-prefixed, segment-
// rotating), which is a *different, unused* implementation from the one
// MeshStore.Set/Consume/Seen actually call: PersistenceEngine.LogOperation
// in persistence.go, a simpler JSON-line-per-entry append with no
// checksumming or segment rotation. This package holds two independent WAL
// implementations; only PersistenceEngine's is wired to MeshStore (see
// BenchmarkLogOperation below for that one's numbers) -- WriteAheadLog is
// exercised only by its own tests and this benchmark.
//
// BenchmarkWALAppend measures the write-path cost of a single WriteAheadLog
// entry append (disk write of a checksummed, length-prefixed record).
func BenchmarkWALAppend(b *testing.B) {
	dir := b.TempDir()
	wal, err := NewWriteAheadLog(dir, WALConfig{MaxSegmentSize: 64 << 20, RotationTime: time.Hour})
	if err != nil {
		b.Fatalf("NewWriteAheadLog failed: %v", err)
	}
	defer wal.Stop()

	entry := &WALEntry{
		Operation: "set",
		Key:       "bench-key",
		Value:     "bench-value-payload",
		Namespace: "bench",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		entry.Timestamp = time.Now().UnixNano()
		if err := wal.Append(entry); err != nil {
			b.Fatalf("Append failed: %v", err)
		}
	}
}

// BenchmarkWALAppendParallel measures WAL append throughput under
// concurrent writers, since a real server handles concurrent requests, not
// one at a time.
func BenchmarkWALAppendParallel(b *testing.B) {
	dir := b.TempDir()
	wal, err := NewWriteAheadLog(dir, WALConfig{MaxSegmentSize: 64 << 20, RotationTime: time.Hour})
	if err != nil {
		b.Fatalf("NewWriteAheadLog failed: %v", err)
	}
	defer wal.Stop()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			entry := &WALEntry{
				Timestamp: time.Now().UnixNano(),
				Operation: "set",
				Key:       fmt.Sprintf("bench-key-%d", i),
				Value:     "bench-value-payload",
				Namespace: "bench",
			}
			if err := wal.Append(entry); err != nil {
				b.Fatalf("Append failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkLogOperation measures PersistenceEngine.LogOperation -- the WAL
// implementation actually called from MeshStore.Set/Consume/Seen on every
// successful write. Unlike WriteAheadLog above, this is genuinely on the
// hot path today.
func BenchmarkLogOperation(b *testing.B) {
	dir := b.TempDir()
	pe, err := NewPersistenceEngine(dir+"/wal", dir+"/snapshots", time.Hour)
	if err != nil {
		b.Fatalf("NewPersistenceEngine failed: %v", err)
	}
	defer pe.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := pe.LogOperation("set", "bench-key", "bench-value-payload", "bench", 0); err != nil {
			b.Fatalf("LogOperation failed: %v", err)
		}
	}
}

// BenchmarkLogOperationParallel measures LogOperation under concurrent
// writers, matching how MeshStore actually calls it (one call per request,
// many requests in flight at once).
func BenchmarkLogOperationParallel(b *testing.B) {
	dir := b.TempDir()
	pe, err := NewPersistenceEngine(dir+"/wal", dir+"/snapshots", time.Hour)
	if err != nil {
		b.Fatalf("NewPersistenceEngine failed: %v", err)
	}
	defer pe.Close()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench-key-%d", i)
			if err := pe.LogOperation("set", key, "bench-value-payload", "bench", 0); err != nil {
				b.Fatalf("LogOperation failed: %v", err)
			}
			i++
		}
	})
}
