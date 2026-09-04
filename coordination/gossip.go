package coordination

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/toll-mesh/store/core"
)

// GossipMessage represents a state sync message between nodes
type GossipMessage struct {
	NodeID           string                 `json:"node_id"`
	Timestamp        int64                  `json:"timestamp"`
	RateLimiters     map[string]interface{} `json:"rate_limiters"`
	ReplayProtection []string               `json:"replay_protection"`
}

// GossipCoordinator manages peer-to-peer state synchronization. A peer's
// core.Node.Address/Port are that peer's HTTP API address (the same one
// SDKs talk to) -- gossip rides the HTTP API rather than a separate wire
// protocol, fetching GET <peer>/internal/state each round and handing the
// decoded state to whatever stateMerger was registered.
type GossipCoordinator struct {
	mu             sync.RWMutex
	config         *core.ClusterConfig
	peers          map[string]*core.Node
	lastSync       map[string]time.Time
	syncInterval   time.Duration
	stopChan       chan struct{}
	messageHandler func(msg *GossipMessage) error
	stateMerger    func(peer *core.MeshStoreState)
	httpClient     *http.Client
}

// NewGossipCoordinator creates a new gossip coordinator
func NewGossipCoordinator(config *core.ClusterConfig, syncInterval time.Duration) *GossipCoordinator {
	gc := &GossipCoordinator{
		config:       config,
		peers:        make(map[string]*core.Node),
		lastSync:     make(map[string]time.Time),
		syncInterval: syncInterval,
		stopChan:     make(chan struct{}),
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}

	// Initialize peers from config
	for _, node := range config.Nodes {
		if node.ID != config.NodeName {
			gc.peers[node.ID] = &node
		}
	}

	return gc
}

// RegisterStateMerger registers the function called with a peer's decoded
// state after each successful gossip round. In practice this is
// MeshStore.MergeState.
func (gc *GossipCoordinator) RegisterStateMerger(merger func(peer *core.MeshStoreState)) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.stateMerger = merger
}

// Start begins the gossip protocol
func (gc *GossipCoordinator) Start(ctx context.Context) error {
	go gc.gossipLoop(ctx)
	return nil
}

// Stop gracefully shuts down the gossip coordinator
func (gc *GossipCoordinator) Stop() error {
	close(gc.stopChan)
	return nil
}

// RegisterMessageHandler registers a handler for incoming gossip messages
func (gc *GossipCoordinator) RegisterMessageHandler(handler func(msg *GossipMessage) error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.messageHandler = handler
}

// gossipLoop runs the periodic gossip protocol
func (gc *GossipCoordinator) gossipLoop(ctx context.Context) {
	ticker := time.NewTicker(gc.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gc.stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			gc.performGossip(ctx)
		}
	}
}

// performGossip selects a random peer, fetches its current state over
// HTTP, and merges it into local state via the registered stateMerger.
func (gc *GossipCoordinator) performGossip(ctx context.Context) {
	gc.mu.RLock()
	peers := make([]*core.Node, 0, len(gc.peers))
	for _, peer := range gc.peers {
		peers = append(peers, peer)
	}
	merger := gc.stateMerger
	client := gc.httpClient
	gc.mu.RUnlock()

	if len(peers) == 0 || merger == nil {
		return
	}

	// Select random peer
	peer := peers[rand.Intn(len(peers))]

	url := fmt.Sprintf("http://%s:%d/internal/state", peer.Address, peer.Port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	var state core.MeshStoreState
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return
	}

	merger(&state)

	gc.mu.Lock()
	gc.lastSync[peer.ID] = time.Now()
	gc.mu.Unlock()
}

// HandleMessage processes an incoming gossip message
func (gc *GossipCoordinator) HandleMessage(msg *GossipMessage) error {
	gc.mu.RLock()
	handler := gc.messageHandler
	gc.mu.RUnlock()

	if handler != nil {
		return handler(msg)
	}

	return nil
}

// GetPeers returns the list of known peers
func (gc *GossipCoordinator) GetPeers() []*core.Node {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	peers := make([]*core.Node, 0, len(gc.peers))
	for _, peer := range gc.peers {
		peers = append(peers, peer)
	}
	return peers
}

// AddPeer adds a new peer to the cluster
func (gc *GossipCoordinator) AddPeer(node *core.Node) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	if node.ID == gc.config.NodeName {
		return fmt.Errorf("cannot add self as peer")
	}

	gc.peers[node.ID] = node
	gc.lastSync[node.ID] = time.Now()
	return nil
}

// RemovePeer removes a peer from the cluster
func (gc *GossipCoordinator) RemovePeer(nodeID string) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	delete(gc.peers, nodeID)
	delete(gc.lastSync, nodeID)
	return nil
}

// GetStats returns gossip statistics
func (gc *GossipCoordinator) GetStats() map[string]interface{} {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	stats := map[string]interface{}{
		"node_id":       gc.config.NodeName,
		"peer_count":    len(gc.peers),
		"sync_interval": gc.syncInterval.String(),
		"last_syncs":    make(map[string]string),
	}

	lastSyncs := stats["last_syncs"].(map[string]string)
	for nodeID, lastSync := range gc.lastSync {
		lastSyncs[nodeID] = lastSync.Format(time.RFC3339)
	}

	return stats
}
