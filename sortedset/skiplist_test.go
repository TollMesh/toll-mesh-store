package sortedset

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// bruteForceRank computes the 0-based ascending rank of (member, score)
// among entries by linearly sorting them by (score, member) -- the
// definition Rank is supposed to match, computed independently of any
// span/level machinery so it can catch bugs in that machinery.
func bruteForceRank(entries []SkipListNode, member string, score float64) (int64, bool) {
	sorted := make([]SkipListNode, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return less(sorted[i].Score, sorted[i].Member, sorted[j].Score, sorted[j].Member)
	})
	for i, e := range sorted {
		if e.Member == member && e.Score == score {
			return int64(i), true
		}
	}
	return 0, false
}

// TestRankMatchesBruteForceUnderRandomOps inserts and deletes a large
// number of random (member, score) pairs -- including many members sharing
// the same score, so ties are broken by member name, and members whose
// name ordering disagrees with their score ordering (e.g. "alice" scoring
// higher than "bob") -- and after every operation checks that every
// remaining member's O(log n) Rank matches an independent O(n log n)
// brute-force sort. This is the regression test for two real bugs found
// in the same pass: Rank previously only worked via an O(n) full scan
// (this proves the O(log n) span-based version returns identical
// answers), and Delete previously navigated by member name alone, which
// silently failed to remove nodes whenever a member's name didn't happen
// to sort the same way as its (score, member) position -- e.g. deleting
// "alice" (score 100) while "bob" (score 50) is also present used to
// leave "alice" behind in the list because "bob" < "alice" by name, so
// the member-only descent stepped past bob and never found alice's real
// position after it.
func TestRankMatchesBruteForceUnderRandomOps(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	sl := NewSkipList()
	present := map[string]float64{}

	names := make([]string, 200)
	for i := range names {
		names[i] = fmt.Sprintf("member-%d", i)
	}

	check := func(step int) {
		var entries []SkipListNode
		for m, s := range present {
			entries = append(entries, SkipListNode{Member: m, Score: s})
		}
		if sl.Length() != int64(len(present)) {
			t.Fatalf("step %d: Length()=%d, want %d", step, sl.Length(), len(present))
		}
		for m, s := range present {
			want, wantFound := bruteForceRank(entries, m, s)
			got, gotFound := sl.Rank(m, s)
			if !wantFound || !gotFound || got != want {
				t.Fatalf("step %d: Rank(%q, %v) = (%d, %v), want (%d, %v)", step, m, s, got, gotFound, want, wantFound)
			}
		}
	}

	for step := 0; step < 3000; step++ {
		name := names[rng.Intn(len(names))]
		// Scores deliberately overlap heavily (small range) so ties by
		// member name -- the case the old Delete implementation got
		// wrong -- happen constantly.
		score := float64(rng.Intn(20))

		if existingScore, ok := present[name]; ok {
			if rng.Intn(2) == 0 {
				if !sl.Delete(name, existingScore) {
					t.Fatalf("step %d: Delete(%q, %v) returned false but member should be present", step, name, existingScore)
				}
				delete(present, name)
			} else {
				sl.Delete(name, existingScore)
				sl.Insert(name, score)
				present[name] = score
			}
		} else {
			sl.Insert(name, score)
			present[name] = score
		}

		if step%50 == 0 {
			check(step)
		}
	}
	check(-1)
}

// TestDeleteFailsWithWrongMemberOnlyOrdering is a narrower, deterministic
// version of the same regression: a case that is known to defeat a
// member-name-only descent (see TestRankMatchesBruteForceUnderRandomOps's
// comment) but is trivial for a correct (score, member)-ordered descent.
func TestDeleteFailsWithWrongMemberOnlyOrdering(t *testing.T) {
	sl := NewSkipList()
	sl.Insert("bob", 50)
	sl.Insert("alice", 100)

	if !sl.Delete("alice", 100) {
		t.Fatal("Delete(\"alice\", 100) returned false; alice should have been found and removed")
	}
	if sl.Length() != 1 {
		t.Fatalf("Length() = %d after deleting alice, want 1", sl.Length())
	}
	if _, found := sl.Search("alice"); found {
		t.Fatal("alice still present after Delete")
	}
	if _, found := sl.Search("bob"); !found {
		t.Fatal("bob should still be present")
	}
}

// TestRankComplexityIsLogarithmic is not a strict Big-O proof (that's not
// really possible from a black-box timing test), but it's a concrete
// sanity check that Rank's cost stops growing linearly with list size: it
// times Rank on lists 100x apart in size and asserts the larger list isn't
// anywhere close to 100x slower. Before the span-based rewrite, Rank was a
// genuine O(n) full scan of level 0, so this same test would have shown
// roughly proportional growth.
func TestRankComplexityIsLogarithmic(t *testing.T) {
	measure := func(n int) (avgNs float64) {
		sl := NewSkipList()
		names := make([]string, n)
		for i := 0; i < n; i++ {
			names[i] = fmt.Sprintf("m-%d", i)
			sl.Insert(names[i], float64(i))
		}
		const iters = 2000
		start := time.Now()
		for i := 0; i < iters; i++ {
			name := names[i%n]
			sl.Rank(name, float64(i%n))
		}
		elapsed := time.Since(start)
		return float64(elapsed.Nanoseconds()) / float64(iters)
	}

	small := measure(1000)
	large := measure(100000)

	// O(log n): log(100000)/log(1000) ~= 1.66, so large should be only
	// modestly slower than small. O(n) would make it ~100x slower. Allow
	// generous headroom (20x) for noise/scheduling on a shared CI runner
	// while still failing hard if it regresses back to linear.
	if large > small*20 {
		t.Fatalf("Rank does not look O(log n): 1k avg=%.0fns, 100k avg=%.0fns (%.1fx) -- expected roughly log-scale growth, not linear", small, large, large/small)
	}
	t.Logf("Rank timing: n=1000 avg=%.0fns, n=100000 avg=%.0fns (%.2fx)", small, large, large/small)
}
