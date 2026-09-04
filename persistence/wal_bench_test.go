package persistence

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkWALAppend measures the write-path cost of a single WAL entry
// append (fsync-adjacent disk write of a checksummed, length-prefixed
// record). This is NOT currently on MeshStore's hot path: MeshStore.Set/
// Consume/Seen never call PersistenceEngine.LogOperation, so today this
// cost is paid only during an explicit CreateSnapshot/RestoreFromLatest
// Snapshot cycle, not on every write as the WAL's existence might suggest.
// This benchmark measures what wiring LogOperation onto the hot path would
// actually cost, since that number didn't exist anywhere before.
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
