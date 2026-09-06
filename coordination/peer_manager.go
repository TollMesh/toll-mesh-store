package coordination

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/toll-mesh/store/core"
)

// PeerManager handles peer discovery and health monitoring: it tracks
// each peer's recent ping outcomes and periodically checks every known
// peer directly (via a real GET <peer>/health, not just relying on
// whichever peer gossip happens to pick that round), so failure/recovery
// is detected on a fixed schedule regardless of cluster size.
type PeerManager struct {
	mu                  sync.RWMutex
	peers               map[string]*PeerInfo
	failureThreshold    int
	healthCheckInterval time.Duration
	healthCheckTicker   *time.Ticker
	stopChan            chan struct{}
	httpClient          *http.Client
	useTLS              bool // if true, health checks use https:// (see SetTLSConfig)
}

// SetTLSConfig switches health checks to use https:// with tlsConfig for
// outgoing connections. See GossipCoordinator.SetTLSConfig, which calls
// this to keep both in sync.
func (pm *PeerManager) SetTLSConfig(tlsConfig *tls.Config) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.useTLS = true
	pm.httpClient = &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
	}
}

// PeerInfo contains information about a peer node
type PeerInfo struct {
	Node            *core.Node
	LastSeen        time.Time
	FailureCount    int
	IsHealthy       bool
	LastHeartbeat   time.Time
	ResponseTime    time.Duration
	SuccessfulPings int64
	FailedPings     int64
}

// NewPeerManager creates a new peer manager
func NewPeerManager(failureThreshold int, healthCheckInterval time.Duration) *PeerManager {
	pm := &PeerManager{
		peers:               make(map[string]*PeerInfo),
		failureThreshold:    failureThreshold,
		healthCheckInterval: healthCheckInterval,
		stopChan:            make(chan struct{}),
		httpClient:          &http.Client{Timeout: 5 * time.Second},
	}
	return pm
}

// AddPeer adds a new peer to the manager
func (pm *PeerManager) AddPeer(node *core.Node) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.peers[node.ID]; exists {
		return fmt.Errorf("peer %s already exists", node.ID)
	}

	pm.peers[node.ID] = &PeerInfo{
		Node:          node,
		LastSeen:      time.Now(),
		FailureCount:  0,
		IsHealthy:     true,
		LastHeartbeat: time.Now(),
	}

	return nil
}

// RemovePeer removes a peer from the manager
func (pm *PeerManager) RemovePeer(nodeID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.peers[nodeID]; !exists {
		return fmt.Errorf("peer %s not found", nodeID)
	}

	delete(pm.peers, nodeID)
	return nil
}

// RecordSuccess records a successful ping to a peer
func (pm *PeerManager) RecordSuccess(nodeID string, responseTime time.Duration) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	peerInfo, exists := pm.peers[nodeID]
	if !exists {
		return fmt.Errorf("peer %s not found", nodeID)
	}

	peerInfo.LastSeen = time.Now()
	peerInfo.LastHeartbeat = time.Now()
	peerInfo.FailureCount = 0
	peerInfo.IsHealthy = true
	peerInfo.ResponseTime = responseTime
	peerInfo.SuccessfulPings++

	return nil
}

// RecordFailure records a failed ping to a peer
func (pm *PeerManager) RecordFailure(nodeID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	peerInfo, exists := pm.peers[nodeID]
	if !exists {
		return fmt.Errorf("peer %s not found", nodeID)
	}

	peerInfo.FailureCount++
	peerInfo.FailedPings++

	if peerInfo.FailureCount >= pm.failureThreshold {
		peerInfo.IsHealthy = false
	}

	return nil
}

// GetHealthyPeers returns all healthy peers
func (pm *PeerManager) GetHealthyPeers() []*core.Node {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var healthyPeers []*core.Node
	for _, peerInfo := range pm.peers {
		if peerInfo.IsHealthy {
			healthyPeers = append(healthyPeers, peerInfo.Node)
		}
	}
	return healthyPeers
}

// GetAllPeers returns all peers regardless of health status
func (pm *PeerManager) GetAllPeers() []*core.Node {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var allPeers []*core.Node
	for _, peerInfo := range pm.peers {
		allPeers = append(allPeers, peerInfo.Node)
	}
	return allPeers
}

// GetPeerInfo returns detailed information about a peer
func (pm *PeerManager) GetPeerInfo(nodeID string) (*PeerInfo, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	peerInfo, exists := pm.peers[nodeID]
	if !exists {
		return nil, fmt.Errorf("peer %s not found", nodeID)
	}

	// Return a copy to avoid external modifications
	copy := *peerInfo
	return &copy, nil
}

// GetStats returns peer manager statistics
func (pm *PeerManager) GetStats() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	healthyCount := 0
	unhealthyCount := 0
	totalFailures := int64(0)
	totalSuccesses := int64(0)

	for _, peerInfo := range pm.peers {
		if peerInfo.IsHealthy {
			healthyCount++
		} else {
			unhealthyCount++
		}
		totalFailures += peerInfo.FailedPings
		totalSuccesses += peerInfo.SuccessfulPings
	}

	return map[string]interface{}{
		"total_peers":       len(pm.peers),
		"healthy_peers":     healthyCount,
		"unhealthy_peers":   unhealthyCount,
		"total_successes":   totalSuccesses,
		"total_failures":    totalFailures,
		"failure_threshold": pm.failureThreshold,
	}
}

// Start begins the peer manager's health check loop
func (pm *PeerManager) Start() {
	// The ticker is created here, not in the constructor, so a
	// PeerManager built with a non-positive interval (e.g. a
	// GossipCoordinator constructed for tests that only exercise its
	// HTTP-facing methods and never calls Start) doesn't panic just from
	// being constructed -- time.NewTicker rejects a non-positive
	// duration. Such a PeerManager simply never runs its health-check
	// loop, mirroring GossipCoordinator's own gossipLoop, which has the
	// identical "only ever ticks if Start is actually called" shape.
	if pm.healthCheckInterval <= 0 {
		return
	}
	pm.healthCheckTicker = time.NewTicker(pm.healthCheckInterval)
	go pm.healthCheckLoop()
}

// Stop gracefully shuts down the peer manager
func (pm *PeerManager) Stop() error {
	close(pm.stopChan)
	if pm.healthCheckTicker != nil {
		pm.healthCheckTicker.Stop()
	}
	return nil
}

// healthCheckLoop periodically checks peer health
func (pm *PeerManager) healthCheckLoop() {
	for {
		select {
		case <-pm.stopChan:
			return
		case <-pm.healthCheckTicker.C:
			pm.performHealthCheck()
		}
	}
}

// performHealthCheck checks every known peer directly, via a real
// GET <peer>/health -- the same unauthenticated endpoint load balancers
// use, so no cluster secret or API key is needed here. This runs on its
// own fixed schedule independent of gossip's random per-round peer pick,
// which is what makes it useful for failure *and* recovery detection: a
// peer performGossip hasn't happened to select in a while still gets
// checked, and an unhealthy peer keeps getting checked (not just skipped)
// so its recovery is actually noticed.
func (pm *PeerManager) performHealthCheck() {
	pm.mu.RLock()
	peers := make([]*PeerInfo, 0, len(pm.peers))
	for _, peerInfo := range pm.peers {
		peers = append(peers, peerInfo)
	}
	client := pm.httpClient
	scheme := "http"
	if pm.useTLS {
		scheme = "https"
	}
	pm.mu.RUnlock()

	for _, peerInfo := range peers {
		start := time.Now()
		url := fmt.Sprintf("%s://%s:%d/health", scheme, peerInfo.Node.Address, peerInfo.Node.Port)
		resp, err := client.Get(url)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			pm.RecordFailure(peerInfo.Node.ID)
			continue
		}
		resp.Body.Close()
		pm.RecordSuccess(peerInfo.Node.ID, time.Since(start))
	}
}
