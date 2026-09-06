package coordination

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
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
//
// peerManager tracks each peer's health (via performGossip's own results,
// plus its own independent periodic GET <peer>/health check -- see
// PeerManager's doc comment for why both matter). performGossip prefers
// healthy peers when picking who to gossip with, so a cluster with any
// live peers doesn't keep wasting rounds on ones already known to be
// down; it falls back to trying everyone if none are currently healthy,
// so recovery from a total outage isn't permanently blocked.
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
	clusterSecret  string // sent as X-Cluster-Secret on every outgoing gossip request, if set
	peerManager    *PeerManager
	useTLS         bool // if true, outgoing requests use https:// (see SetTLSConfig)
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
		// 3 consecutive failed pings before a peer is marked unhealthy,
		// checked on the same cadence as gossip rounds -- reusing
		// syncInterval rather than adding a second configurable interval,
		// since there's no reason health checks need a different cadence
		// from gossip itself.
		peerManager: NewPeerManager(3, syncInterval),
	}

	// Initialize peers from config
	for _, node := range config.Nodes {
		if node.ID != config.NodeName {
			n := node
			gc.peers[node.ID] = &n
			gc.peerManager.AddPeer(&n)
		}
	}

	return gc
}

// scheme returns "https" or "http" depending on whether SetTLSConfig has
// been called. Callers must already hold gc.mu (for reading).
func (gc *GossipCoordinator) scheme() string {
	if gc.useTLS {
		return "https"
	}
	return "http"
}

// SetTLSConfig switches this coordinator (and its PeerManager) to speak
// https:// to every peer, using tlsConfig for outgoing connections --
// typically a *tls.Config with RootCAs set to a shared cluster CA, so a
// peer's server certificate is actually verified rather than blindly
// trusted. Gossip and health-check traffic then travels encrypted
// instead of the plaintext this project previously always sent
// cluster-secret-protected but fully interceptable/tamperable state and
// pings over. Does not affect this node's own HTTP server, which is
// configured for TLS separately (see api.HTTPServer.StartTLS).
func (gc *GossipCoordinator) SetTLSConfig(tlsConfig *tls.Config) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gc.useTLS = true
	gc.httpClient = &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
	}
	gc.peerManager.SetTLSConfig(tlsConfig)
}

// SetClusterSecret sets the value sent as X-Cluster-Secret on every
// outgoing gossip request (to a peer's /internal/state). Peers that
// enforce a cluster secret of their own reject requests without a
// matching one -- see api.HTTPServer's authMiddleware.
func (gc *GossipCoordinator) SetClusterSecret(secret string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.clusterSecret = secret
}

// RegisterStateMerger registers the function called with a peer's decoded
// state after each successful gossip round. In practice this is
// MeshStore.MergeState.
func (gc *GossipCoordinator) RegisterStateMerger(merger func(peer *core.MeshStoreState)) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.stateMerger = merger
}

// Start begins the gossip protocol and peer health checking.
func (gc *GossipCoordinator) Start(ctx context.Context) error {
	gc.peerManager.Start()
	go gc.gossipLoop(ctx)
	return nil
}

// Stop gracefully shuts down the gossip coordinator and peer health
// checking.
func (gc *GossipCoordinator) Stop() error {
	gc.peerManager.Stop()
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

// performGossip selects a peer, fetches its current state over HTTP, and
// merges it into local state via the registered stateMerger. Prefers a
// peer PeerManager currently considers healthy (avoiding wasted rounds on
// a peer already known to be down); if none are healthy right now, falls
// back to trying any known peer at random, so a total outage doesn't
// permanently prevent ever attempting a peer again once it might have
// recovered.
func (gc *GossipCoordinator) performGossip(ctx context.Context) {
	gc.mu.RLock()
	peers := make([]*core.Node, 0, len(gc.peers))
	for _, peer := range gc.peers {
		peers = append(peers, peer)
	}
	merger := gc.stateMerger
	client := gc.httpClient
	clusterSecret := gc.clusterSecret
	peerManager := gc.peerManager
	scheme := gc.scheme()
	gc.mu.RUnlock()

	if len(peers) == 0 || merger == nil {
		return
	}

	peer := selectPeer(peers, peerManager)

	start := time.Now()
	url := fmt.Sprintf("%s://%s:%d/internal/state", scheme, peer.Address, peer.Port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	if clusterSecret != "" {
		req.Header.Set("X-Cluster-Secret", clusterSecret)
	}

	resp, err := client.Do(req)
	if err != nil {
		peerManager.RecordFailure(peer.ID)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		peerManager.RecordFailure(peer.ID)
		return
	}

	var state core.MeshStoreState
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		peerManager.RecordFailure(peer.ID)
		return
	}

	merger(&state)
	peerManager.RecordSuccess(peer.ID, time.Since(start))

	gc.mu.Lock()
	gc.lastSync[peer.ID] = time.Now()
	gc.mu.Unlock()
}

// selectPeer picks one candidate from peers, preferring one peerManager
// currently considers healthy (falling back to any known peer if none are
// healthy right now, so a total outage doesn't permanently block ever
// trying again). Shared by performGossip (pull) and PushState (push) so
// both use the identical peer-selection policy. peers must be non-empty.
func selectPeer(peers []*core.Node, peerManager *PeerManager) *core.Node {
	candidates := peerManager.GetHealthyPeers()
	if len(candidates) == 0 {
		candidates = peers
	}
	return candidates[rand.Intn(len(candidates))]
}

// PushState proactively sends state to one peer (the same selection
// policy performGossip's pull uses) via POST /internal/state, instead of
// waiting for that peer -- or anyone -- to pull it on the next periodic
// round. This is NOT a replacement for periodic pull-gossip, which
// remains the steady-state mechanism that eventually reaches every node;
// it's a narrow, opt-in latency reduction for specific operations whose
// cross-node visibility already has a documented race-window gap (see
// MeshStore's job-claim and transaction-commit callers). Reaching only
// one peer, not all, keeps this at the same bandwidth cost as an ordinary
// gossip round rather than fanning out to the whole cluster on every
// call; that peer's own subsequent pulls (and pushes, if it also has
// something to push) continue propagating it onward the normal way.
//
// This does not eliminate the race it's mitigating, only shrinks it: two
// nodes claiming the same job at nearly the same instant can still both
// push before either push arrives. It reduces the *typical* window from
// "up to one gossip interval" (e.g. 5s) to "one HTTP round trip"
// (milliseconds) -- a real, large reduction in likelihood, not a
// guarantee.
func (gc *GossipCoordinator) PushState(ctx context.Context, state *core.MeshStoreState) error {
	gc.mu.RLock()
	peers := make([]*core.Node, 0, len(gc.peers))
	for _, peer := range gc.peers {
		peers = append(peers, peer)
	}
	client := gc.httpClient
	clusterSecret := gc.clusterSecret
	peerManager := gc.peerManager
	scheme := gc.scheme()
	gc.mu.RUnlock()

	if len(peers) == 0 {
		return nil
	}
	peer := selectPeer(peers, peerManager)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(state); err != nil {
		gz.Close()
		return fmt.Errorf("encoding state to push: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("compressing state to push: %w", err)
	}

	url := fmt.Sprintf("%s://%s:%d/internal/state", scheme, peer.Address, peer.Port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	if clusterSecret != "" {
		req.Header.Set("X-Cluster-Secret", clusterSecret)
	}

	resp, err := client.Do(req)
	if err != nil {
		peerManager.RecordFailure(peer.ID)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		peerManager.RecordFailure(peer.ID)
		return fmt.Errorf("push to %s returned status %d", peer.ID, resp.StatusCode)
	}

	peerManager.RecordSuccess(peer.ID, 0)
	return nil
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

// AddPeer adds a new peer to the cluster. Idempotent: re-adding a peer
// already known (e.g. a duplicate join) is not an error, it just leaves
// that peer's existing health-tracking state alone rather than resetting
// it.
func (gc *GossipCoordinator) AddPeer(node *core.Node) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	if node.ID == gc.config.NodeName {
		return fmt.Errorf("cannot add self as peer")
	}

	gc.peers[node.ID] = node
	gc.lastSync[node.ID] = time.Now()
	gc.peerManager.AddPeer(node) // ignore "already exists" -- see doc comment
	return nil
}

// RemovePeer removes a peer from the cluster
func (gc *GossipCoordinator) RemovePeer(nodeID string) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	delete(gc.peers, nodeID)
	delete(gc.lastSync, nodeID)
	gc.peerManager.RemovePeer(nodeID)
	return nil
}

// GetStats returns gossip statistics, including peer health (see
// PeerManager.GetStats).
func (gc *GossipCoordinator) GetStats() map[string]interface{} {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	stats := map[string]interface{}{
		"node_id":       gc.config.NodeName,
		"peer_count":    len(gc.peers),
		"sync_interval": gc.syncInterval.String(),
		"last_syncs":    make(map[string]string),
		"peer_health":   gc.peerManager.GetStats(),
	}

	lastSyncs := stats["last_syncs"].(map[string]string)
	for nodeID, lastSync := range gc.lastSync {
		lastSyncs[nodeID] = lastSync.Format(time.RFC3339)
	}

	return stats
}

// PeerManager returns this coordinator's PeerManager, for callers (e.g.
// api.HealthChecker) that need direct access to peer health tracking.
func (gc *GossipCoordinator) PeerManager() *PeerManager {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.peerManager
}

// GetPeerHealth returns detailed per-peer health info (success/failure
// counts, response time, whether currently considered healthy).
func (gc *GossipCoordinator) GetPeerHealth() []*PeerInfo {
	gc.mu.RLock()
	peerManager := gc.peerManager
	gc.mu.RUnlock()

	all := peerManager.GetAllPeers()
	out := make([]*PeerInfo, 0, len(all))
	for _, node := range all {
		info, err := peerManager.GetPeerInfo(node.ID)
		if err == nil {
			out = append(out, info)
		}
	}
	return out
}
