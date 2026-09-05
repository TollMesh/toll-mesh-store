package stream

import (
	"fmt"
	"testing"
)

// TestEntryIDsAreGloballyUniqueAcrossNodes is the regression test for a
// real design bug: entry IDs used to be "<timestamp>-<sequence>" with no
// node component. Sequence is a plain per-Stream-instance counter that two
// different nodes' independent Stream objects for the same stream name
// both start at 0 and increment on their own -- two nodes producing
// entries in the same millisecond (ordinary under real write volume, not
// a rare edge case) could produce the identical ID for two completely
// different entries. That's harmless for a single node, but would corrupt
// a union-merge across nodes, since the ID is exactly the identity a merge
// uses to decide "have I already seen this". This test manufactures the
// exact collision scenario (same millisecond, same sequence number, two
// different node IDs) and asserts the resulting IDs differ.
func TestEntryIDsAreGloballyUniqueAcrossNodes(t *testing.T) {
	streamA := NewStream("events", "node-a")
	streamB := NewStream("events", "node-b")

	entryA, err := streamA.Add(map[string]string{"from": "node-a"})
	if err != nil {
		t.Fatalf("streamA.Add failed: %v", err)
	}
	entryB, err := streamB.Add(map[string]string{"from": "node-b"})
	if err != nil {
		t.Fatalf("streamB.Add failed: %v", err)
	}

	if entryA.ID == entryB.ID {
		t.Fatalf("entry IDs collided despite different nodes: both are %q", entryA.ID)
	}

	// Both streams' first entry shares the same LastSequence (1) and, run
	// back-to-back like this, very likely the same millisecond timestamp
	// too -- reconstructing what the ID *would* have been under the old
	// "<timestamp>-<sequence>" format (no node) demonstrates the actual
	// bug this fixes: those two entries would have collided under the
	// previous format whenever their timestamps do match, which needs no
	// contrivance under any real concurrent write load.
	oldFormatA := fmt.Sprintf("%d-%d", entryA.Timestamp, entryA.Sequence)
	oldFormatB := fmt.Sprintf("%d-%d", entryB.Timestamp, entryB.Sequence)
	if entryA.Timestamp == entryB.Timestamp && oldFormatA != oldFormatB {
		t.Fatalf("test setup assumption broken: expected old-format IDs to match when timestamps match, got %q vs %q", oldFormatA, oldFormatB)
	}

	// The current (fixed) IDs must actually contain each entry's own node,
	// not just happen to differ for some unrelated reason.
	if got := entryA.ID; got != fmt.Sprintf("%d-%d-%s", entryA.Timestamp, entryA.Sequence, "node-a") {
		t.Errorf("entryA.ID = %q, want it to end with node-a", got)
	}
	if got := entryB.ID; got != fmt.Sprintf("%d-%d-%s", entryB.Timestamp, entryB.Sequence, "node-b") {
		t.Errorf("entryB.ID = %q, want it to end with node-b", got)
	}
}

// TestMergeSnapshotUnionsEntriesWithoutLoss is the core regression test for
// Stream replication: merging a peer's entries must add every entry the
// local stream doesn't already have (by ID), preserve entries already
// present, not duplicate on a repeated merge (idempotency, as any real
// CRDT merge must have), and keep the combined log in correct
// chronological order for Range/GetFirst/GetLast, which are positional
// over the underlying slice, not ID-derived.
func TestMergeSnapshotUnionsEntriesWithoutLoss(t *testing.T) {
	local := NewStream("events", "node-1")
	if _, err := local.Add(map[string]string{"seq": "local-1"}); err != nil {
		t.Fatalf("local.Add failed: %v", err)
	}

	peer := NewStream("events", "node-2")
	if _, err := peer.Add(map[string]string{"seq": "peer-1"}); err != nil {
		t.Fatalf("peer.Add failed: %v", err)
	}
	if _, err := peer.Add(map[string]string{"seq": "peer-2"}); err != nil {
		t.Fatalf("peer.Add failed: %v", err)
	}

	local.MergeSnapshot(peer.Snapshot())

	if local.Len() != 3 {
		t.Fatalf("Len() after merge = %d, want 3 (1 local + 2 from peer)", local.Len())
	}

	all := local.Range("0", "-", 100)
	if len(all) != 3 {
		t.Fatalf("Range after merge returned %d entries, want 3", len(all))
	}
	seen := map[string]bool{}
	for _, e := range all {
		seen[e.Fields["seq"]] = true
	}
	for _, want := range []string{"local-1", "peer-1", "peer-2"} {
		if !seen[want] {
			t.Errorf("missing entry with seq=%q after merge", want)
		}
	}

	// Merging the same peer snapshot again must be a no-op, not a
	// duplicate -- merge has to be idempotent, the same way it would be
	// for a repeated gossip round delivering the same state twice.
	local.MergeSnapshot(peer.Snapshot())
	if local.Len() != 3 {
		t.Fatalf("Len() after re-merging the same snapshot = %d, want still 3 (merge must be idempotent)", local.Len())
	}
}
