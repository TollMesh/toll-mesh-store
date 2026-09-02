package core

import (
	"testing"
	"time"
)

func TestGCounter_IncrementAndValue(t *testing.T) {
	counter := NewGCounter()

	if counter.Value() != 0 {
		t.Errorf("Expected initial value 0, got %d", counter.Value())
	}

	counter.Increment("node1")
	counter.Increment("node1")
	counter.Increment("node2")

	if counter.Value() != 3 {
		t.Errorf("Expected value 3 after increments, got %d", counter.Value())
	}
}

func TestGCounter_Merge(t *testing.T) {
	counter1 := NewGCounter()
	counter2 := NewGCounter()

	counter1.Increment("node1")
	counter1.Increment("node1")
	counter2.Increment("node2")
	counter2.Increment("node2")
	counter2.Increment("node2")

	counter1.Merge(counter2)

	if counter1.Value() != 5 {
		t.Errorf("Expected value 5 after merge, got %d", counter1.Value())
	}
}

func TestGSet_AddAndContains(t *testing.T) {
	set := NewGSet()

	if set.Contains("item1") {
		t.Error("Set should not contain item1 initially")
	}

	set.Add("item1")
	set.Add("item2")

	if !set.Contains("item1") {
		t.Error("Set should contain item1 after adding")
	}

	if !set.Contains("item2") {
		t.Error("Set should contain item2 after adding")
	}

	if set.Contains("item3") {
		t.Error("Set should not contain item3")
	}
}

func TestGSet_Merge(t *testing.T) {
	set1 := NewGSet()
	set2 := NewGSet()

	set1.Add("item1")
	set2.Add("item2")
	set2.Add("item3")

	set1.Merge(set2)

	if !set1.Contains("item1") || !set1.Contains("item2") || !set1.Contains("item3") {
		t.Error("All items should be present after merge")
	}
}

func TestExpiringSet_AddAndContains(t *testing.T) {
	set := NewExpiringSet()
	defer set.Stop()

	set.Add("item1", 100*time.Millisecond)

	if !set.Contains("item1") {
		t.Error("Set should contain item1 immediately after adding")
	}

	time.Sleep(150 * time.Millisecond)

	if set.Contains("item1") {
		t.Error("Set should not contain item1 after expiration")
	}
}

func TestExpiringSet_BackgroundCleanup(t *testing.T) {
	set := NewExpiringSet()

	set.Add("item1", 50*time.Millisecond)
	set.Add("item2", 100*time.Millisecond)

	time.Sleep(200 * time.Millisecond)

	if set.Contains("item1") || set.Contains("item2") {
		t.Error("Background cleanup should have removed expired items")
	}

	set.Stop()
}

func TestGCounter_SnapshotRestore(t *testing.T) {
	g := NewGCounter()
	g.Increment("node1")
	g.Increment("node1")
	g.Increment("node2")

	snap := g.Snapshot()
	restored := RestoreGCounter(snap)

	if restored.Value() != g.Value() {
		t.Errorf("expected restored value %d, got %d", g.Value(), restored.Value())
	}
}

func TestGSet_SnapshotRestore(t *testing.T) {
	g := NewGSet()
	g.Add("a")
	g.Add("b")

	snap := g.Snapshot()
	restored := RestoreGSet(snap)

	if !restored.Contains("a") || !restored.Contains("b") {
		t.Error("restored set missing items from snapshot")
	}
}
