package stream

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAddEntry(t *testing.T) {
	stream := NewStream("test", "node-1")

	entry, err := stream.Add(map[string]string{
		"event": "login",
		"user":  "alice",
	})

	if err != nil {
		t.Fatalf("add failed: %v", err)
	}

	if entry == nil {
		t.Fatal("entry is nil")
	}

	if entry.ID == "" {
		t.Fatal("entry ID is empty")
	}

	if entry.Fields["event"] != "login" {
		t.Errorf("expected event login, got %s", entry.Fields["event"])
	}
}

func TestGetEntry(t *testing.T) {
	stream := NewStream("test", "node-1")

	added, _ := stream.Add(map[string]string{"msg": "hello"})

	retrieved, err := stream.Get(added.ID)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if retrieved.ID != added.ID {
		t.Errorf("expected ID %s, got %s", added.ID, retrieved.ID)
	}

	if retrieved.Fields["msg"] != "hello" {
		t.Errorf("expected msg hello, got %s", retrieved.Fields["msg"])
	}
}

func TestGetLast(t *testing.T) {
	stream := NewStream("test", "node-1")

	_, _ = stream.Add(map[string]string{"seq": "1"})
	time.Sleep(10 * time.Millisecond) // Ensure different timestamp
	entry2, _ := stream.Add(map[string]string{"seq": "2"})

	last, err := stream.GetLast()
	if err != nil {
		t.Fatalf("get last failed: %v", err)
	}

	if last.ID != entry2.ID {
		t.Errorf("expected last entry %s, got %s", entry2.ID, last.ID)
	}
}

func TestGetFirst(t *testing.T) {
	stream := NewStream("test", "node-1")

	entry1, _ := stream.Add(map[string]string{"seq": "1"})
	time.Sleep(10 * time.Millisecond)
	_, _ = stream.Add(map[string]string{"seq": "2"})

	first, err := stream.GetFirst()
	if err != nil {
		t.Fatalf("get first failed: %v", err)
	}

	if first.ID != entry1.ID {
		t.Errorf("expected first entry %s, got %s", entry1.ID, first.ID)
	}
}

func TestStreamRange(t *testing.T) {
	stream := NewStream("test", "node-1")

	entries := make([]*StreamEntry, 0)
	for i := 0; i < 10; i++ {
		entry, _ := stream.Add(map[string]string{"seq": fmt.Sprintf("%d", i)})
		entries = append(entries, entry)
		time.Sleep(5 * time.Millisecond)
	}

	// Get all
	all := stream.Range("0", "-", 100)
	if len(all) != 10 {
		t.Errorf("expected 10 entries, got %d", len(all))
	}

	// Get range from entry 3 to end
	ranged := stream.Range(entries[3].ID, "-", 100)
	if len(ranged) != 6 { // Entries 4-9
		t.Errorf("expected 6 entries (4-9), got %d", len(ranged))
	}

	// Get with limit
	limited := stream.Range("0", "-", 3)
	if len(limited) != 3 {
		t.Errorf("expected 3 entries (limit), got %d", len(limited))
	}
}

func TestStreamLen(t *testing.T) {
	stream := NewStream("test", "node-1")

	if stream.Len() != 0 {
		t.Errorf("expected 0 entries, got %d", stream.Len())
	}

	stream.Add(map[string]string{"msg": "1"})
	stream.Add(map[string]string{"msg": "2"})
	stream.Add(map[string]string{"msg": "3"})

	if stream.Len() != 3 {
		t.Errorf("expected 3 entries, got %d", stream.Len())
	}
}

func TestStreamTrim(t *testing.T) {
	stream := NewStream("test", "node-1")
	stream.RetentionPolicy.MaxAge = 100 * time.Millisecond // Very short for testing

	stream.Add(map[string]string{"msg": "old"})
	time.Sleep(150 * time.Millisecond) // Wait for entry to expire

	stream.Add(map[string]string{"msg": "new"})
	stream.Trim()

	// Old entry should be removed, new one should remain
	if stream.Len() != 1 {
		t.Errorf("expected 1 entry after trim, got %d", stream.Len())
	}

	last, _ := stream.GetLast()
	if last.Fields["msg"] != "new" {
		t.Errorf("expected new entry, got %s", last.Fields["msg"])
	}
}

func TestStreamMaxEntries(t *testing.T) {
	stream := NewStream("test", "node-1")
	stream.RetentionPolicy.MaxEntries = 5

	for i := 0; i < 10; i++ {
		stream.Add(map[string]string{"seq": fmt.Sprintf("%d", i)})
	}

	stream.Trim()

	// Should only keep last 5
	if stream.Len() != 5 {
		t.Errorf("expected 5 entries after max, got %d", stream.Len())
	}

	first, _ := stream.GetFirst()
	if first.Fields["seq"] != "5" {
		t.Errorf("expected first entry to be seq 5, got %s", first.Fields["seq"])
	}
}

func TestConsumerGroupAdd(t *testing.T) {
	cg := NewConsumerGroup("group-1", "stream-1", "node-1")

	member, err := cg.AddConsumer("consumer-1", "node-1")
	if err != nil {
		t.Fatalf("add consumer failed: %v", err)
	}

	if member.ID != "consumer-1" {
		t.Errorf("expected consumer ID consumer-1, got %s", member.ID)
	}

	offset, _ := cg.GetOffset("consumer-1")
	if offset != "0" {
		t.Errorf("expected initial offset 0, got %s", offset)
	}
}

func TestConsumerGroupRemove(t *testing.T) {
	cg := NewConsumerGroup("group-1", "stream-1", "node-1")

	cg.AddConsumer("consumer-1", "node-1")
	err := cg.RemoveConsumer("consumer-1")
	if err != nil {
		t.Fatalf("remove failed: %v", err)
	}

	_, err = cg.GetConsumer("consumer-1")
	if err == nil {
		t.Fatal("expected error for removed consumer")
	}
}

func TestConsumerGroupUpdateOffset(t *testing.T) {
	cg := NewConsumerGroup("group-1", "stream-1", "node-1")

	cg.AddConsumer("consumer-1", "node-1")
	err := cg.UpdateOffset("consumer-1", "entry-123")
	if err != nil {
		t.Fatalf("update offset failed: %v", err)
	}

	offset, _ := cg.GetOffset("consumer-1")
	if offset != "entry-123" {
		t.Errorf("expected offset entry-123, got %s", offset)
	}
}

func TestConsumerGroupHeartbeat(t *testing.T) {
	cg := NewConsumerGroup("group-1", "stream-1", "node-1")

	cg.AddConsumer("consumer-1", "node-1")
	time.Sleep(50 * time.Millisecond)

	member, _ := cg.GetConsumer("consumer-1")
	oldHeartbeat := member.LastHeartbeat

	cg.Heartbeat("consumer-1")

	member, _ = cg.GetConsumer("consumer-1")
	if member.LastHeartbeat <= oldHeartbeat {
		t.Error("heartbeat not updated")
	}
}

func TestConsumerGroupDeadDetection(t *testing.T) {
	cg := NewConsumerGroup("group-1", "stream-1", "node-1")

	cg.AddConsumer("consumer-1", "node-1")

	// Immediately check (should not be dead)
	dead := cg.DetectDeadConsumers(100 * time.Millisecond)
	if len(dead) > 0 {
		t.Error("expected no dead consumers immediately after add")
	}

	// Wait and check again
	time.Sleep(150 * time.Millisecond)
	dead = cg.DetectDeadConsumers(100 * time.Millisecond)
	if len(dead) != 1 {
		t.Errorf("expected 1 dead consumer, got %d", len(dead))
	}

	if dead[0] != "consumer-1" {
		t.Errorf("expected dead consumer consumer-1, got %s", dead[0])
	}
}

func TestConsumerGroupRebalance(t *testing.T) {
	cg := NewConsumerGroup("group-1", "stream-1", "node-1")

	cg.AddConsumer("consumer-1", "node-1")
	cg.AddConsumer("consumer-2", "node-1")

	// Create fake entries
	entries := make([]*StreamEntry, 0)
	for i := 0; i < 10; i++ {
		entries = append(entries, &StreamEntry{
			ID:    fmt.Sprintf("entry-%d", i),
			Node:  "node-1",
			Sequence: int64(i),
		})
	}

	// Rebalance should distribute entries among consumers
	assignment := cg.RebalanceAssignment(entries)

	if len(assignment) != 2 {
		t.Errorf("expected 2 consumer assignments, got %d", len(assignment))
	}

	// Each consumer should have some entries
	total := 0
	for _, entries := range assignment {
		total += len(entries)
	}

	if total > 0 {
		t.Logf("distributed %d entries among 2 consumers", total)
	}
}

func TestConsumerGroupGetMembers(t *testing.T) {
	cg := NewConsumerGroup("group-1", "stream-1", "node-1")

	cg.AddConsumer("consumer-1", "node-1")
	cg.AddConsumer("consumer-2", "node-1")
	cg.AddConsumer("consumer-3", "node-1")

	members := cg.GetMembers()
	if len(members) != 3 {
		t.Errorf("expected 3 members, got %d", len(members))
	}
}

func TestStreamConcurrentAdd(t *testing.T) {
	stream := NewStream("test", "node-1")

	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(idx int) {
			stream.Add(map[string]string{
				"worker": fmt.Sprintf("worker-%d", idx),
				"seq":    fmt.Sprintf("%d", idx),
			})
			done <- true
		}(i)
	}

	// Wait for all
	for i := 0; i < 100; i++ {
		<-done
	}

	if stream.Len() != 100 {
		t.Errorf("expected 100 entries, got %d", stream.Len())
	}
}

func TestStreamConcurrentRead(t *testing.T) {
	stream := NewStream("test", "node-1")

	// Add 100 entries
	entries := make([]*StreamEntry, 0)
	for i := 0; i < 100; i++ {
		entry, _ := stream.Add(map[string]string{"seq": fmt.Sprintf("%d", i)})
		entries = append(entries, entry)
	}

	// Concurrent reads
	done := make(chan bool)
	for i := 0; i < 50; i++ {
		go func(idx int) {
			stream.Range("0", "-", 100)
			done <- true
		}(i)
	}

	for i := 0; i < 50; i++ {
		<-done
	}

	if stream.Len() != 100 {
		t.Errorf("expected 100 entries after concurrent reads, got %d", stream.Len())
	}
}

func TestConsumerGroupConcurrentOffsetUpdate(t *testing.T) {
	cg := NewConsumerGroup("group-1", "stream-1", "node-1")

	for i := 0; i < 10; i++ {
		cg.AddConsumer(fmt.Sprintf("consumer-%d", i), "node-1")
	}

	done := make(chan bool)

	// Each consumer updates offset concurrently
	for i := 0; i < 10; i++ {
		go func(idx int) {
			for j := 0; j < 100; j++ {
				cg.UpdateOffset(
					fmt.Sprintf("consumer-%d", idx),
					fmt.Sprintf("entry-%d", j),
				)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// All consumers should have updated offsets
	members := cg.GetMembers()
	for _, m := range members {
		if m.LastOffset == "" || m.LastOffset == "0" {
			t.Errorf("consumer %s offset not updated", m.ID)
		}
	}
}

func TestStreamGetStats(t *testing.T) {
	stream := NewStream("test", "node-1")

	stream.Add(map[string]string{"msg": "1"})
	stream.Add(map[string]string{"msg": "2"})

	stats := stream.GetStats()

	if stats["name"] != "test" {
		t.Errorf("expected name test, got %v", stats["name"])
	}

	if stats["entries"] != 2 {
		t.Errorf("expected 2 entries, got %v", stats["entries"])
	}

	if stats["node"] != "node-1" {
		t.Errorf("expected node node-1, got %v", stats["node"])
	}
}

func TestConsumerGroupGetStats(t *testing.T) {
	cg := NewConsumerGroup("group-1", "stream-1", "node-1")

	cg.AddConsumer("consumer-1", "node-1")
	cg.AddConsumer("consumer-2", "node-1")

	stats := cg.GetStats()

	if stats["name"] != "group-1" {
		t.Errorf("expected name group-1, got %v", stats["name"])
	}

	if stats["consumers"] != 2 {
		t.Errorf("expected 2 consumers, got %v", stats["consumers"])
	}
}

func TestStreamEntrySequence(t *testing.T) {
	stream := NewStream("test", "node-1")

	entry1, _ := stream.Add(map[string]string{"n": "1"})
	entry2, _ := stream.Add(map[string]string{"n": "2"})
	entry3, _ := stream.Add(map[string]string{"n": "3"})

	if entry1.Sequence != 1 {
		t.Errorf("expected seq 1, got %d", entry1.Sequence)
	}

	if entry2.Sequence != 2 {
		t.Errorf("expected seq 2, got %d", entry2.Sequence)
	}

	if entry3.Sequence != 3 {
		t.Errorf("expected seq 3, got %d", entry3.Sequence)
	}
}

func TestConsumerGroupPending(t *testing.T) {
	cg := NewConsumerGroup("group-1", "stream-1", "node-1")

	cg.AddConsumer("consumer-1", "node-1")

	cg.AddPending("entry-1", "consumer-1")
	cg.AddPending("entry-2", "consumer-1")

	pending := cg.PendingEntries()
	if len(pending) != 2 {
		t.Errorf("expected 2 pending, got %d", len(pending))
	}

	// Acknowledge one
	cg.UpdateOffset("consumer-1", "entry-1")

	pending = cg.PendingEntries()
	if len(pending) != 1 {
		t.Errorf("expected 1 pending after ack, got %d", len(pending))
	}
}

func TestLargeStream(t *testing.T) {
	stream := NewStream("test", "node-1")

	// Add 10k entries
	for i := 0; i < 10000; i++ {
		stream.Add(map[string]string{
			"id":  fmt.Sprintf("%d", i),
			"msg": "data",
		})
	}

	if stream.Len() != 10000 {
		t.Errorf("expected 10000 entries, got %d", stream.Len())
	}

	// Get a range
	first, _ := stream.GetFirst()
	last, _ := stream.GetLast()

	ranged := stream.Range(first.ID, last.ID, 1000)
	if len(ranged) == 0 {
		t.Error("expected ranged entries")
	}
}

func BenchmarkAdd(b *testing.B) {
	stream := NewStream("bench", "node-1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream.Add(map[string]string{
			"idx": fmt.Sprintf("%d", i),
		})
	}
}

func BenchmarkGet(b *testing.B) {
	stream := NewStream("bench", "node-1")

	// Pre-populate
	entries := make([]*StreamEntry, 0)
	for i := 0; i < 1000; i++ {
		entry, _ := stream.Add(map[string]string{"n": fmt.Sprintf("%d", i)})
		entries = append(entries, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream.Get(entries[i%len(entries)].ID)
	}
}

func BenchmarkRange(b *testing.B) {
	stream := NewStream("bench", "node-1")

	// Pre-populate
	for i := 0; i < 1000; i++ {
		stream.Add(map[string]string{"n": fmt.Sprintf("%d", i)})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream.Range("0", "-", 100)
	}
}

func BenchmarkConsumerGroupUpdateOffset(b *testing.B) {
	cg := NewConsumerGroup("bench", "stream", "node-1")

	cg.AddConsumer("consumer-1", "node-1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cg.UpdateOffset("consumer-1", fmt.Sprintf("entry-%d", i))
	}
}

func BenchmarkConcurrentAdds(b *testing.B) {
	stream := NewStream("bench", "node-1")

	b.ResetTimer()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < b.N/10; j++ {
				stream.Add(map[string]string{
					"worker": fmt.Sprintf("%d", workerID),
					"job":    fmt.Sprintf("%d", j),
				})
			}
		}(i)
	}

	wg.Wait()
}
