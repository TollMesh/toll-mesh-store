package sortedset

import (
	"math"
	"math/rand"
	"sync"
)

const (
	MAX_LEVEL = 16
	P         = 0.5
)

// SkipListNode represents a node in the skip list
type SkipListNode struct {
	Member   string
	Score    float64
	Forward  []*SkipListNode
	Backward *SkipListNode
}

// SkipList implements a probabilistic data structure for sorted operations
type SkipList struct {
	header *SkipListNode
	tail   *SkipListNode
	level  int
	length int64
	mu     sync.RWMutex
}

// NewSkipList creates a new skip list
func NewSkipList() *SkipList {
	header := &SkipListNode{
		Member:  "",
		Score:   math.Inf(-1),
		Forward: make([]*SkipListNode, MAX_LEVEL),
	}

	return &SkipList{
		header: header,
		level:  1,
		length: 0,
	}
}

// Insert adds or updates a member in the skip list
func (sl *SkipList) Insert(member string, score float64) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// Find update nodes at each level
	update := make([]*SkipListNode, MAX_LEVEL)
	rank := make([]int64, MAX_LEVEL)

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for x.Forward[i] != nil &&
			(x.Forward[i].Score < score ||
				(x.Forward[i].Score == score && x.Forward[i].Member < member)) {
			rank[i] += 1
			x = x.Forward[i]
		}
		update[i] = x
	}

	// Check if member already exists
	if x.Forward[0] != nil && x.Forward[0].Score == score && x.Forward[0].Member == member {
		// Member exists, just update score
		return false
	}

	// Generate random level for new node
	level := sl.randomLevel()

	// Expand header if needed
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.header
		}
		sl.level = level
	}

	// Create new node
	x = &SkipListNode{
		Member:  member,
		Score:   score,
		Forward: make([]*SkipListNode, level),
	}

	// Update backward pointers
	if update[0].Forward[0] != nil {
		x.Backward = update[0]
		update[0].Forward[0].Backward = x
	} else {
		x.Backward = update[0]
	}

	// Update forward pointers
	for i := 0; i < level; i++ {
		x.Forward[i] = update[i].Forward[i]
		update[i].Forward[i] = x
	}

	// Update tail pointer
	if x.Forward[0] == nil {
		sl.tail = x
	}

	sl.length++
	return true
}

// Search finds a member and returns its score
func (sl *SkipList) Search(member string) (float64, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.Forward[i] != nil &&
			(x.Forward[i].Score < 0 ||
				(x.Forward[i].Score == 0 && x.Forward[i].Member < member)) {
			x = x.Forward[i]
		}
	}

	x = x.Forward[0]
	if x != nil && x.Member == member {
		return x.Score, true
	}

	return 0, false
}

// Delete removes a member from the skip list
func (sl *SkipList) Delete(member string) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*SkipListNode, MAX_LEVEL)

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.Forward[i] != nil && x.Forward[i].Member < member {
			x = x.Forward[i]
		}
		update[i] = x
	}

	x = x.Forward[0]
	if x == nil || x.Member != member {
		return false
	}

	// Update backward/forward pointers
	for i := 0; i < sl.level; i++ {
		if update[i].Forward[i] == x {
			update[i].Forward[i] = x.Forward[i]
		}
	}

	if x.Forward[0] != nil {
		x.Forward[0].Backward = x.Backward
	} else {
		sl.tail = x.Backward
	}

	// Update level
	for sl.level > 1 && sl.header.Forward[sl.level-1] == nil {
		sl.level--
	}

	sl.length--
	return true
}

// Range returns members within score range [min, max]
func (sl *SkipList) Range(min, max float64, limit int64) []*SkipListNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var nodes []*SkipListNode

	// Find start node
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.Forward[i] != nil && x.Forward[i].Score < min {
			x = x.Forward[i]
		}
	}

	x = x.Forward[0]

	// Collect nodes in range
	for x != nil && x.Score <= max && int64(len(nodes)) < limit {
		if x.Score >= min {
			nodes = append(nodes, x)
		}
		x = x.Forward[0]
	}

	return nodes
}

// RangeByRank returns members by rank (index) within range [start, stop]
func (sl *SkipList) RangeByRank(start, stop int64) []*SkipListNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var nodes []*SkipListNode

	// Start from beginning
	x := sl.header.Forward[0]
	var i int64

	for x != nil && i <= stop {
		if i >= start {
			nodes = append(nodes, x)
		}
		x = x.Forward[0]
		i++
	}

	return nodes
}

// Rank returns the rank (index) of a member
func (sl *SkipList) Rank(member string) (int64, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var rank int64
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.Forward[i] != nil && x.Forward[i].Member != member &&
			(x.Forward[i].Score < 0 || x.Forward[i].Member < member) {
			rank += 1
			x = x.Forward[i]
		}
	}

	x = x.Forward[0]
	if x != nil && x.Member == member {
		return rank, true
	}

	return 0, false
}

// Count returns number of members with score in range [min, max]
func (sl *SkipList) Count(min, max float64) int64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var count int64

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.Forward[i] != nil && x.Forward[i].Score < min {
			x = x.Forward[i]
		}
	}

	x = x.Forward[0]
	for x != nil && x.Score <= max {
		if x.Score >= min {
			count++
		}
		x = x.Forward[0]
	}

	return count
}

// Length returns total number of members
func (sl *SkipList) Length() int64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.length
}

// GetAll returns all members in order
func (sl *SkipList) GetAll() []*SkipListNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var nodes []*SkipListNode
	x := sl.header.Forward[0]

	for x != nil {
		nodes = append(nodes, x)
		x = x.Forward[0]
	}

	return nodes
}

// Helper function
func (sl *SkipList) randomLevel() int {
	level := 1
	for rand.Float64() < P && level < MAX_LEVEL {
		level++
	}
	return level
}
