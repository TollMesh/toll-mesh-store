package persistence

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkLogOperation measures PersistenceEngine.LogOperation -- the WAL
// implementation actually called from MeshStore.Set/Consume/Seen on every
// successful write.
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
		if err := pe.LogOperation("set", "bench-key", "bench-value-payload", "bench", 0, "bench-node", 0); err != nil {
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
			if err := pe.LogOperation("set", key, "bench-value-payload", "bench", 0, "bench-node", 0); err != nil {
				b.Fatalf("LogOperation failed: %v", err)
			}
			i++
		}
	})
}
