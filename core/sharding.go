package core

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// ShardingConfig holds configuration for consistent hashing
type ShardingConfig struct {
	VirtualNodes      int // Number of virtual nodes per physical node
	ReplicationFactor int // Number of replicas for each key
}

// Ring represents a consistent hash ring
type Ring struct {
	mu                sync.RWMutex
	nodes             map[uint64]*Node
	sortedKeys        []uint64
	virtualNodes      int
	replicationFactor int
}

// NewRing creates a new consistent hash ring
func NewRing(config ShardingConfig) *Ring {
	return &Ring{
		nodes:             make(map[uint64]*Node),
		sortedKeys:        make([]uint64, 0),
		virtualNodes:      config.VirtualNodes,
		replicationFactor: config.ReplicationFactor,
	}
}

// AddNode adds a node to the ring
func (r *Ring) AddNode(node *Node) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		hash := r.hashKey(fmt.Sprintf("%s:%d", node.ID, i))
		r.nodes[hash] = node
		r.sortedKeys = append(r.sortedKeys, hash)
	}

	sort.Slice(r.sortedKeys, func(i, j int) bool {
		return r.sortedKeys[i] < r.sortedKeys[j]
	})

	return nil
}

// RemoveNode removes a node from the ring
func (r *Ring) RemoveNode(nodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var keysToRemove []uint64
	for hash, node := range r.nodes {
		if node.ID == nodeID {
			keysToRemove = append(keysToRemove, hash)
		}
	}

	for _, hash := range keysToRemove {
		delete(r.nodes, hash)
	}

	// Rebuild sorted keys
	r.sortedKeys = make([]uint64, 0, len(r.nodes))
	for hash := range r.nodes {
		r.sortedKeys = append(r.sortedKeys, hash)
	}
	sort.Slice(r.sortedKeys, func(i, j int) bool {
		return r.sortedKeys[i] < r.sortedKeys[j]
	})

	return nil
}

// GetNode returns the node responsible for a key
func (r *Ring) GetNode(key string) (*Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedKeys) == 0 {
		return nil, fmt.Errorf("no nodes in ring")
	}

	hash := r.hashKey(key)
	idx := r.search(hash)
	return r.nodes[r.sortedKeys[idx]], nil
}

// GetNodes returns the N nodes responsible for a key (for replication)
func (r *Ring) GetNodes(key string, count int) ([]*Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedKeys) == 0 {
		return nil, fmt.Errorf("no nodes in ring")
	}

	if count > len(r.nodes)/r.virtualNodes {
		count = len(r.nodes) / r.virtualNodes
	}

	hash := r.hashKey(key)
	idx := r.search(hash)

	nodes := make([]*Node, 0, count)
	seen := make(map[string]bool)

	for i := 0; i < len(r.sortedKeys) && len(nodes) < count; i++ {
		nodeIdx := (idx + i) % len(r.sortedKeys)
		node := r.nodes[r.sortedKeys[nodeIdx]]
		if !seen[node.ID] {
			nodes = append(nodes, node)
			seen[node.ID] = true
		}
	}

	return nodes, nil
}

// GetStats returns ring statistics
func (r *Ring) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	uniqueNodes := make(map[string]bool)
	for _, node := range r.nodes {
		uniqueNodes[node.ID] = true
	}

	return map[string]interface{}{
		"total_nodes":            len(uniqueNodes),
		"total_virtual_nodes":    len(r.nodes),
		"virtual_nodes_per_node": r.virtualNodes,
		"replication_factor":     r.replicationFactor,
		"ring_size":              len(r.sortedKeys),
	}
}

// Private helper methods

func (r *Ring) hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func (r *Ring) search(hash uint64) int {
	idx := sort.Search(len(r.sortedKeys), func(i int) bool {
		return r.sortedKeys[i] >= hash
	})

	if idx == len(r.sortedKeys) {
		idx = 0
	}

	return idx
}
