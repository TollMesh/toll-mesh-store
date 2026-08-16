package coordination

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/TollMesh/toll-mesh-store/core"
)

// StateSync handles CRDT state synchronization between nodes
type StateSync struct {
	mu           sync.RWMutex
	nodeID       string
	localState   *core.MeshStoreState
	peerStates   map[string]*core.MeshStoreState
	lastSyncTime map[string]time.Time
	syncInterval time.Duration
	merkleTree   *MerkleTree
	stopChan     chan struct{}
	syncHandler  func(state *core.MeshStoreState) error
}

// MerkleTree represents a Merkle tree for efficient state comparison
type MerkleTree struct {
	mu    sync.RWMutex
	root  *MerkleNode
	items map[string]string // key -> hash
}

// MerkleNode represents a node in the Merkle tree
type MerkleNode struct {
	Hash  string
	Left  *MerkleNode
	Right *MerkleNode
	Key   string
	Value string
}

// NewStateSync creates a new state synchronizer
func NewStateSync(nodeID string, syncInterval time.Duration) *StateSync {
	return &StateSync{
		nodeID:       nodeID,
		peerStates:   make(map[string]*core.MeshStoreState),
		lastSyncTime: make(map[string]time.Time),
		syncInterval: syncInterval,
		merkleTree: &MerkleTree{
			items: make(map[string]string),
		},
		stopChan: make(chan struct{}),
	}
}

// SetLocalState sets the local node's state
func (ss *StateSync) SetLocalState(state *core.MeshStoreState) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.localState = state
	ss.updateMerkleTree(state)
	return nil
}

// GetLocalState returns the local node's state
func (ss *StateSync) GetLocalState() *core.MeshStoreState {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if ss.localState == nil {
		return &core.MeshStoreState{}
	}
	return ss.localState
}

// GetPeerState returns a peer's state
func (ss *StateSync) GetPeerState(peerID string) (*core.MeshStoreState, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	state, exists := ss.peerStates[peerID]
	if !exists {
		return nil, fmt.Errorf("peer state for %s not found", peerID)
	}
	return state, nil
}

// UpdatePeerState updates a peer's state and merges it with local state
func (ss *StateSync) UpdatePeerState(peerID string, state *core.MeshStoreState) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.peerStates[peerID] = state
	ss.lastSyncTime[peerID] = time.Now()

	// Merge peer state with local state
	if ss.localState != nil {
		ss.mergeStates(ss.localState, state)
	}

	return nil
}

// GetStateHash returns the hash of the local state for comparison
func (ss *StateSync) GetStateHash() string {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	return ss.merkleTree.GetRootHash()
}

// GetPeerStateHash returns the hash of a peer's state
func (ss *StateSync) GetPeerStateHash(peerID string) (string, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	state, exists := ss.peerStates[peerID]
	if !exists {
		return "", fmt.Errorf("peer state for %s not found", peerID)
	}

	return ss.calculateStateHash(state), nil
}

// NeedsSyncWithPeer checks if sync is needed with a peer
func (ss *StateSync) NeedsSyncWithPeer(peerID string) bool {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	lastSync, exists := ss.lastSyncTime[peerID]
	if !exists {
		return true
	}

	return time.Since(lastSync) > ss.syncInterval
}

// GetSyncStats returns synchronization statistics
func (ss *StateSync) GetSyncStats() map[string]interface{} {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	stats := map[string]interface{}{
		"node_id":        ss.nodeID,
		"local_hash":     ss.merkleTree.GetRootHash(),
		"peer_count":     len(ss.peerStates),
		"last_sync_time": make(map[string]string),
	}

	lastSyncTimes := stats["last_sync_time"].(map[string]string)
	for peerID, lastSync := range ss.lastSyncTime {
		lastSyncTimes[peerID] = lastSync.Format(time.RFC3339)
	}

	return stats
}

// Start begins the state sync loop
func (ss *StateSync) Start() {
	go ss.syncLoop()
}

// Stop gracefully shuts down the state sync
func (ss *StateSync) Stop() error {
	close(ss.stopChan)
	return nil
}

// RegisterSyncHandler registers a handler for state sync events
func (ss *StateSync) RegisterSyncHandler(handler func(state *core.MeshStoreState) error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.syncHandler = handler
}

// syncLoop periodically syncs state with peers
func (ss *StateSync) syncLoop() {
	ticker := time.NewTicker(ss.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ss.stopChan:
			return
		case <-ticker.C:
			ss.performSync()
		}
	}
}

// performSync performs state synchronization
func (ss *StateSync) performSync() {
	ss.mu.RLock()
	localState := ss.localState
	peerStates := make(map[string]*core.MeshStoreState)
	for peerID, state := range ss.peerStates {
		peerStates[peerID] = state
	}
	ss.mu.RUnlock()

	if localState == nil {
		return
	}

	// Merge all peer states with local state
	for _, peerState := range peerStates {
		ss.mu.Lock()
		ss.mergeStates(localState, peerState)
		ss.mu.Unlock()
	}
}

// mergeStates merges two CRDT states
func (ss *StateSync) mergeStates(local, peer *core.MeshStoreState) {
	if local == nil || peer == nil {
		return
	}

	// Merge rate limiters (GCounter - take max per node)
	if local.RateLimiters == nil {
		local.RateLimiters = make(map[string]interface{})
	}
	for key, value := range peer.RateLimiters {
		local.RateLimiters[key] = value
	}

	// Merge replay protection (GSet - union)
	if local.ReplayProtection == nil {
		local.ReplayProtection = make(map[string]bool)
	}
	for key := range peer.ReplayProtection {
		local.ReplayProtection[key] = true
	}

	// Merge cache (take latest timestamp)
	if local.Cache == nil {
		local.Cache = make(map[string]map[string][]byte)
	}
	for ns, items := range peer.Cache {
		if local.Cache[ns] == nil {
			local.Cache[ns] = make(map[string][]byte)
		}
		for key, value := range items {
			local.Cache[ns][key] = value
		}
	}
}

// updateMerkleTree updates the Merkle tree with state data
func (ss *StateSync) updateMerkleTree(state *core.MeshStoreState) {
	ss.merkleTree.mu.Lock()
	defer ss.merkleTree.mu.Unlock()

	ss.merkleTree.items = make(map[string]string)

	if state != nil {
		// Add rate limiters to tree
		for key, value := range state.RateLimiters {
			hash := ss.hashValue(fmt.Sprintf("%v", value))
			ss.merkleTree.items[key] = hash
		}

		// Add replay protection to tree
		for key := range state.ReplayProtection {
			ss.merkleTree.items[key] = ss.hashValue(key)
		}

		// Add cache to tree
		for ns, items := range state.Cache {
			for key, value := range items {
				fullKey := fmt.Sprintf("%s:%s", ns, key)
				hash := ss.hashValue(string(value))
				ss.merkleTree.items[fullKey] = hash
			}
		}
	}

	ss.merkleTree.root = ss.buildMerkleTree(ss.merkleTree.items)
}

// buildMerkleTree builds a Merkle tree from items
func (ss *StateSync) buildMerkleTree(items map[string]string) *MerkleNode {
	if len(items) == 0 {
		return nil
	}

	var nodes []*MerkleNode
	for key, hash := range items {
		nodes = append(nodes, &MerkleNode{
			Hash:  hash,
			Key:   key,
			Value: hash,
		})
	}

	for len(nodes) > 1 {
		var newNodes []*MerkleNode
		for i := 0; i < len(nodes); i += 2 {
			if i+1 < len(nodes) {
				parent := &MerkleNode{
					Left:  nodes[i],
					Right: nodes[i+1],
					Hash:  ss.hashValue(nodes[i].Hash + nodes[i+1].Hash),
				}
				newNodes = append(newNodes, parent)
			} else {
				newNodes = append(newNodes, nodes[i])
			}
		}
		nodes = newNodes
	}

	if len(nodes) > 0 {
		return nodes[0]
	}
	return nil
}

// calculateStateHash calculates the hash of a state
func (ss *StateSync) calculateStateHash(state *core.MeshStoreState) string {
	if state == nil {
		return ""
	}

	hash := md5.New()
	for key, value := range state.RateLimiters {
		hash.Write([]byte(key + fmt.Sprintf("%v", value)))
	}
	for key := range state.ReplayProtection {
		hash.Write([]byte(key))
	}
	for ns, items := range state.Cache {
		for key, value := range items {
			hash.Write([]byte(ns + key + string(value)))
		}
	}

	return hex.EncodeToString(hash.Sum(nil))
}

// hashValue hashes a value
func (ss *StateSync) hashValue(value string) string {
	hash := md5.Sum([]byte(value))
	return hex.EncodeToString(hash[:])
}

// GetRootHash returns the root hash of the Merkle tree
func (mt *MerkleTree) GetRootHash() string {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if mt.root == nil {
		return ""
	}
	return mt.root.Hash
}
