package sortedset

import (
	"math"
	"testing"
)

func TestAddMember(t *testing.T) {
	zs := NewSortedSet("test", "node-1")

	err := zs.Add("player-1", 100.0)
	if err != nil {
		t.Fatalf("add failed: %v", err)
	}

	score, exists := zs.Get("player-1")
	if !exists {
		t.Fatal("member not found")
	}

	if score != 100.0 {
		t.Errorf("expected score 100.0, got %f", score)
	}
}

func TestUpdateScore(t *testing.T) {
	zs := NewSortedSet("test", "node-1")

	zs.Add("player-1", 100.0)
	zs.Add("player-1", 150.0) // Update

	score, _ := zs.Get("player-1")
	if score != 150.0 {
		t.Errorf("expected updated score 150.0, got %f", score)
	}
}

// TestAddCanLowerScore is a regression test: Add used to run every write
// through compareMembers, which compares (score, timestamp, node) and
// treats a lower score as a "losing" write regardless of timestamp -- so
// this same node's own perfectly ordinary sequential score decrease was
// silently dropped. Add is a local mutation with a strictly increasing
// Lamport clock, so it should always apply; conflict resolution belongs to
// Merge (reconciling a different node's concurrent write), not to a node's
// own sequential updates.
func TestAddCanLowerScore(t *testing.T) {
	zs := NewSortedSet("test", "node-1")

	zs.Add("player-1", 100.0)
	zs.Add("player-1", 10.0)

	score, exists := zs.Get("player-1")
	if !exists {
		t.Fatal("player-1 not found after lowering score")
	}
	if score != 10.0 {
		t.Errorf("expected score lowered to 10.0, got %f", score)
	}

	rank, found := zs.Rank("player-1")
	if !found || rank != 0 {
		t.Errorf("expected player-1 at rank 0 after lowering score, got rank=%d found=%v", rank, found)
	}
	if zs.Card() != 1 {
		t.Errorf("expected card=1 (no duplicate skip-list entry), got %d", zs.Card())
	}
}

func TestRank(t *testing.T) {
	zs := NewSortedSet("test", "node-1")

	zs.Add("player-1", 100.0)
	zs.Add("player-2", 200.0)
	zs.Add("player-3", 50.0)

	rank1, _ := zs.Rank("player-1")
	rank2, _ := zs.Rank("player-2")
	rank3, _ := zs.Rank("player-3")

	if rank3 != 0 {
		t.Errorf("expected rank 0 for lowest score, got %d", rank3)
	}

	if rank1 != 1 {
		t.Errorf("expected rank 1 for middle score, got %d", rank1)
	}

	if rank2 != 2 {
		t.Errorf("expected rank 2 for highest score, got %d", rank2)
	}
}

func TestRevRank(t *testing.T) {
	zs := NewSortedSet("test", "node-1")

	zs.Add("player-1", 100.0)
	zs.Add("player-2", 200.0)
	zs.Add("player-3", 50.0)

	revRank1, _ := zs.RevRank("player-1")
	revRank2, _ := zs.RevRank("player-2")
	revRank3, _ := zs.RevRank("player-3")

	if revRank2 != 0 {
		t.Errorf("expected rev rank 0 for highest score, got %d", revRank2)
	}

	if revRank1 != 1 {
		t.Errorf("expected rev rank 1 for middle score, got %d", revRank1)
	}

	if revRank3 != 2 {
		t.Errorf("expected rev rank 2 for lowest score, got %d", revRank3)
	}
}

func TestRange(t *testing.T) {
	zs := NewSortedSet("test", "node-1")

	for i := 0; i < 10; i++ {
		zs.Add("m"+string(rune(i)), float64(i*10))
	}

	members := zs.Range(20.0, 60.0, 100)

	if len(members) != 5 {
		t.Errorf("expected 5 members in range, got %d", len(members))
	}

	if members[0].Score != 20.0 {
		t.Errorf("expected first member score 20.0, got %f", members[0].Score)
	}

	if members[4].Score != 60.0 {
		t.Errorf("expected last member score 60.0, got %f", members[4].Score)
	}
}

func TestRangeByRank(t *testing.T) {
	zs := NewSortedSet("test", "node-1")

	for i := 0; i < 10; i++ {
		zs.Add("m"+string(rune('0'+byte(i))), float64(i))
	}

	members := zs.RangeByRank(2, 5)

	if len(members) != 4 {
		t.Errorf("expected 4 members, got %d", len(members))
	}

	if members[0].Score != 2.0 {
		t.Errorf("expected first score 2.0, got %f", members[0].Score)
	}
}

func TestRemove(t *testing.T) {
	zs := NewSortedSet("test", "node-1")

	zs.Add("player-1", 100.0)
	zs.Add("player-2", 200.0)

	err := zs.Remove("player-1")
	if err != nil {
		t.Fatalf("remove failed: %v", err)
	}

	_, exists := zs.Get("player-1")
	if exists {
		t.Fatal("member still exists after removal")
	}

	card := zs.Card()
	if card != 1 {
		t.Errorf("expected card 1, got %d", card)
	}
}

func TestCount(t *testing.T) {
	zs := NewSortedSet("test", "node-1")

	for i := 0; i < 100; i++ {
		zs.Add("m"+string(rune(i)), float64(i))
	}

	count := zs.Count(20.0, 50.0)
	if count != 31 {
		t.Errorf("expected count 31, got %d", count)
	}
}

func TestCRDTConflictResolution(t *testing.T) {
	// Test conflict resolution: (score, timestamp, node)
	zs := NewSortedSet("test", "node-1")

	// Add initial score
	zs.Add("player-1", 100.0)

	// Manually create older update (should be ignored). Same score as the
	// existing member so the comparison is decided by timestamp, not score
	// (compareMembers checks score first). zs.Add above already consumed
	// Lamport timestamp 1, so a genuinely older write must precede it.
	olderMember := &SortedSetMember{
		Member:    "player-1",
		Score:     100.0,
		Timestamp: 0, // Older timestamp
		Node:      "node-2",
	}

	// Manually merge older member (should not update)
	existing := zs.MemberMap["player-1"]
	cmp := zs.compareMembers(olderMember, existing)
	if cmp >= 0 {
		t.Error("older timestamp should lose conflict")
	}

	score, _ := zs.Get("player-1")
	if score != 100.0 {
		t.Errorf("score should remain 100.0, got %f", score)
	}
}

func TestCRDTMerge(t *testing.T) {
	zs1 := NewSortedSet("test", "node-1")
	zs2 := NewSortedSet("test", "node-2")

	// Add to first
	zs1.Add("player-1", 100.0)
	zs1.Add("player-2", 200.0)

	// Add to second
	zs2.Add("player-3", 300.0)
	zs2.Add("player-2", 250.0) // Conflicting member

	// Merge zs2 into zs1
	zs1.Merge(zs2)

	// Check merged state
	card := zs1.Card()
	if card != 3 {
		t.Errorf("expected card 3 after merge, got %d", card)
	}

	// player-3 should exist
	_, exists := zs1.Get("player-3")
	if !exists {
		t.Fatal("player-3 not found after merge")
	}

	// player-2 score should be from conflict resolution
	score, _ := zs1.Get("player-2")
	if score == 0 {
		t.Error("player-2 score is 0 after merge")
	}
}

func TestTombstoneMerge(t *testing.T) {
	zs1 := NewSortedSet("test", "node-1")
	zs2 := NewSortedSet("test", "node-2")

	zs1.Add("player-1", 100.0)

	// zs2 removes player-1
	zs2.Add("player-1", 100.0)
	zs2.Remove("player-1")

	// Merge - should remove player-1 due to tombstone
	zs1.Merge(zs2)

	_, exists := zs1.Get("player-1")
	if exists {
		t.Fatal("player-1 should be removed after tombstone merge")
	}
}

func TestLargeLeaderboard(t *testing.T) {
	zs := NewSortedSet("scores", "node-1")

	// Add 1k members (reduced for test speed)
	for i := 0; i < 1000; i++ {
		score := float64(i) * 1.5
		zs.Add("p"+string(rune(i)), score)
	}

	// Get top 10
	members := zs.RevRange(math.Inf(1), math.Inf(-1), 10)

	if len(members) != 10 {
		t.Errorf("expected 10 members, got %d", len(members))
	}

	// Scores should be in descending order
	for i := 1; i < len(members); i++ {
		if members[i].Score > members[i-1].Score {
			t.Error("scores not in descending order")
		}
	}
}

func TestConcurrentOperations(t *testing.T) {
	zs := NewSortedSet("test", "node-1")

	// Add members concurrently
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(idx int) {
			zs.Add("player", float64(idx))
			done <- true
		}(i)
	}

	// Wait for all to complete
	for i := 0; i < 100; i++ {
		<-done
	}

	// Check final state
	card := zs.Card()
	if card == 0 {
		t.Fatal("set is empty after concurrent adds")
	}
}

func BenchmarkAdd(b *testing.B) {
	zs := NewSortedSet("bench", "node-1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		zs.Add("member", float64(i))
	}
}

func BenchmarkRank(b *testing.B) {
	zs := NewSortedSet("bench", "node-1")

	// Pre-populate
	for i := 0; i < 10000; i++ {
		zs.Add("m"+string(rune(i)), float64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		zs.Rank("m5000")
	}
}

func BenchmarkRange(b *testing.B) {
	zs := NewSortedSet("bench", "node-1")

	// Pre-populate
	for i := 0; i < 10000; i++ {
		zs.Add("m"+string(rune(i)), float64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		zs.Range(1000.0, 5000.0, 100)
	}
}
