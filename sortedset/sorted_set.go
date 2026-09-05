package sortedset

import (
	"fmt"
	"sync"
	"time"
)

// SortedSetMember represents a member in a sorted set with CRDT metadata
type SortedSetMember struct {
	Member    string    `json:"member"`
	Score     float64   `json:"score"`
	Timestamp int64     `json:"timestamp"` // Lamport clock for conflict resolution
	Node      string    `json:"node"`      // Which node added this
	Tombstone bool      `json:"tombstone"` // Soft delete marker
	TombAt    int64     `json:"tomb_at"`   // When tombstoned
}

// SortedSet implements a CRDT-based sorted set
type SortedSet struct {
	Name       string
	Members    *SkipList                    // Primary index by score
	MemberMap  map[string]*SortedSetMember  // Fast lookup by member
	mu         sync.RWMutex
	Timestamp  int64                        // Lamport clock
	NodeID     string                       // This node's ID
	replicas   map[string]*SortedSet        // Replicas on other nodes (for testing)
}

// NewSortedSet creates a new CRDT sorted set
func NewSortedSet(name string, nodeID string) *SortedSet {
	return &SortedSet{
		Name:      name,
		Members:   NewSkipList(),
		MemberMap: make(map[string]*SortedSetMember),
		NodeID:    nodeID,
		Timestamp: 0,
		replicas:  make(map[string]*SortedSet),
	}
}

// Add inserts or updates a member using CRDT rules. This is a local
// mutation on this node -- its own Lamport clock is strictly increasing, so
// it is always causally after whatever this node last wrote for member, and
// is applied unconditionally (including lowering a score). compareMembers'
// (score, timestamp, node) conflict resolution is for Merge, where two
// different nodes' concurrent writes must be reconciled without a shared
// causal order; it previously also gated Add, which meant any score
// decrease from any caller -- even a completely ordinary sequential update
// from this same node -- was silently dropped, since score is compared
// before timestamp. Confirmed live: ZADD lowering a member's score from
// 100 to 10 did nothing.
func (zs *SortedSet) Add(member string, score float64) error {
	zs.mu.Lock()
	defer zs.mu.Unlock()

	// Increment Lamport clock
	zs.Timestamp++
	ts := zs.Timestamp

	newMember := &SortedSetMember{
		Member:    member,
		Score:     score,
		Timestamp: ts,
		Node:      zs.NodeID,
	}

	// Check if member exists
	existing, exists := zs.MemberMap[member]

	if exists {
		if existing.Score == score {
			// No-op: same score already in place, nothing to move in the
			// skip list. Still record the new timestamp/node below so
			// Merge sees this as the latest write.
		} else {
			zs.Members.Delete(member, existing.Score)
		}
	}

	// Add to skiplist and map
	zs.Members.Insert(member, score)
	zs.MemberMap[member] = newMember

	// Replicate to other nodes (for testing)
	zs.replicateAdd(newMember)

	return nil
}

// Remove marks a member as removed using tombstone (soft delete)
func (zs *SortedSet) Remove(member string) error {
	zs.mu.Lock()
	defer zs.mu.Unlock()

	existing, exists := zs.MemberMap[member]
	if !exists {
		return fmt.Errorf("member not found: %s", member)
	}

	// Increment Lamport clock
	zs.Timestamp++

	// Create tombstone
	existing.Tombstone = true
	existing.TombAt = time.Now().UnixMilli()
	existing.Timestamp = zs.Timestamp
	existing.Node = zs.NodeID

	// Remove from skiplist but keep in map (soft delete)
	zs.Members.Delete(member, existing.Score)

	// Replicate tombstone
	zs.replicateTombstone(existing)

	return nil
}

// Get retrieves a member's score
func (zs *SortedSet) Get(member string) (float64, bool) {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	m, exists := zs.MemberMap[member]
	if !exists || m.Tombstone {
		return 0, false
	}

	return m.Score, true
}

// Rank returns the rank (0-based index) of a member
func (zs *SortedSet) Rank(member string) (int64, bool) {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	m, exists := zs.MemberMap[member]
	if !exists || m.Tombstone {
		return 0, false
	}

	return zs.Members.Rank(member, m.Score)
}

// RevRank returns the reverse rank (from highest score)
func (zs *SortedSet) RevRank(member string) (int64, bool) {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	m, exists := zs.MemberMap[member]
	if !exists || m.Tombstone {
		return 0, false
	}

	rank, found := zs.Members.Rank(member, m.Score)
	if !found {
		return 0, false
	}

	return zs.Members.Length() - rank - 1, true
}

// Range returns members within score range [min, max] in ascending order
func (zs *SortedSet) Range(min, max float64, limit int64) []*SortedSetMember {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	nodes := zs.Members.Range(min, max, limit)
	var members []*SortedSetMember

	for _, node := range nodes {
		m := zs.MemberMap[node.Member]
		if m != nil && !m.Tombstone {
			members = append(members, m)
		}
	}

	return members
}

// RangeByRank returns members by rank range [start, stop] in ascending order
func (zs *SortedSet) RangeByRank(start, stop int64) []*SortedSetMember {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	nodes := zs.Members.RangeByRank(start, stop)
	var members []*SortedSetMember

	for _, node := range nodes {
		m := zs.MemberMap[node.Member]
		if m != nil && !m.Tombstone {
			members = append(members, m)
		}
	}

	return members
}

// RevRange returns members within score range [min, max], highest score
// first. Following Redis's ZREVRANGEBYSCORE convention, the upper bound
// (max) is the first parameter and the lower bound (min) is the second.
func (zs *SortedSet) RevRange(max, min float64, limit int64) []*SortedSetMember {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	nodes := zs.Members.RangeDesc(min, max, limit)
	var members []*SortedSetMember

	for _, node := range nodes {
		m := zs.MemberMap[node.Member]
		if m != nil && !m.Tombstone {
			members = append(members, m)
		}
	}

	return members
}

// Count returns number of members in score range [min, max]
func (zs *SortedSet) Count(min, max float64) int64 {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	return zs.Members.Count(min, max)
}

// Card returns total number of members
func (zs *SortedSet) Card() int64 {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	count := int64(0)
	for _, m := range zs.MemberMap {
		if !m.Tombstone {
			count++
		}
	}
	return count
}

// Snapshot returns a copy of every member, including tombstoned
// (soft-deleted) ones -- gossip replication needs tombstones too, so a
// delete actually propagates to peers instead of just being invisible
// local bookkeeping.
func (zs *SortedSet) Snapshot() []SortedSetMember {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	out := make([]SortedSetMember, 0, len(zs.MemberMap))
	for _, m := range zs.MemberMap {
		out = append(out, *m)
	}
	return out
}

// MergeSnapshot merges a peer's Snapshot output using the exact same CRDT
// rules as Merge -- it just takes the wire-friendly slice form (what
// actually travels over gossip) instead of a live *SortedSet, by wrapping
// it in one just long enough to reuse Merge's logic rather than
// duplicating it.
func (zs *SortedSet) MergeSnapshot(members []SortedSetMember) {
	memberMap := make(map[string]*SortedSetMember, len(members))
	for i := range members {
		memberMap[members[i].Member] = &members[i]
	}
	zs.Merge(&SortedSet{MemberMap: memberMap})
}

// Merge performs a CRDT merge with another sorted set
func (zs *SortedSet) Merge(other *SortedSet) {
	zs.mu.Lock()
	defer zs.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	// For each member in other
	for member, otherMember := range other.MemberMap {
		existing, exists := zs.MemberMap[member]

		if !exists {
			// New member, add it
			zs.MemberMap[member] = otherMember
			if !otherMember.Tombstone {
				zs.Members.Insert(member, otherMember.Score)
			}
		} else {
			// Member exists, apply conflict resolution
			cmp := zs.compareMembers(otherMember, existing)
			if cmp > 0 {
				// Other version wins
				zs.MemberMap[member] = otherMember
				if otherMember.Tombstone {
					zs.Members.Delete(member, existing.Score)
				} else {
					zs.Members.Delete(member, existing.Score)
					zs.Members.Insert(member, otherMember.Score)
				}
			}
		}
	}

	// Update Lamport clock
	if other.Timestamp > zs.Timestamp {
		zs.Timestamp = other.Timestamp
	}
	zs.Timestamp++
}

// GetStats returns statistics about the sorted set
func (zs *SortedSet) GetStats() map[string]interface{} {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	active := int64(0)
	tombstoned := int64(0)

	for _, m := range zs.MemberMap {
		if m.Tombstone {
			tombstoned++
		} else {
			active++
		}
	}

	return map[string]interface{}{
		"name":      zs.Name,
		"active":    active,
		"tombstoned": tombstoned,
		"total":     int64(len(zs.MemberMap)),
		"timestamp": zs.Timestamp,
		"node":      zs.NodeID,
	}
}

// Helper functions

// compareMembers implements CRDT conflict resolution: (score, timestamp, node)
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func (zs *SortedSet) compareMembers(a, b *SortedSetMember) int {
	// Compare by score first
	if a.Score < b.Score {
		return -1
	}
	if a.Score > b.Score {
		return 1
	}

	// Same score, compare by timestamp (higher timestamp wins)
	if a.Timestamp < b.Timestamp {
		return -1
	}
	if a.Timestamp > b.Timestamp {
		return 1
	}

	// Same timestamp, compare by node ID (lexicographic)
	if a.Node < b.Node {
		return -1
	}
	if a.Node > b.Node {
		return 1
	}

	return 0
}

func (zs *SortedSet) replicateAdd(member *SortedSetMember) {
	// In real implementation, this would send to other nodes via gossip
	// For testing, we replicate to registered replicas
	for _, replica := range zs.replicas {
		replica.Add(member.Member, member.Score)
	}
}

func (zs *SortedSet) replicateTombstone(member *SortedSetMember) {
	// In real implementation, gossip protocol would handle this
	for _, replica := range zs.replicas {
		replica.Remove(member.Member)
	}
}

// RegisterReplica registers another sorted set as a replica for testing
func (zs *SortedSet) RegisterReplica(replica *SortedSet) {
	zs.mu.Lock()
	defer zs.mu.Unlock()
	zs.replicas[replica.NodeID] = replica
}
