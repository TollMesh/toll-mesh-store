package coordination

import (
	"fmt"
	"sync"
	"time"

	"github.com/TollMesh/toll-mesh-store/core"
)

// PeerManager handles peer discovery and health monitoring
type PeerManager struct {
	mu                sync.RWMutex
	peers             map[string]*PeerInfo
	failureThreshold  int
	healthCheckTicker *time.Ticker
	stopChan          chan struct{}
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
		peers:             make(map[string]*PeerInfo),
		failureThreshold:  failureThreshold,
		healthCheckTicker: time.NewTicker(healthCheckInterval),
		stopChan:          make(chan struct{}),
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
	go pm.healthCheckLoop()
}

// Stop gracefully shuts down the peer manager
func (pm *PeerManager) Stop() error {
	close(pm.stopChan)
	pm.healthCheckTicker.Stop()
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

// performHealthCheck checks the health of all peers
func (pm *PeerManager) performHealthCheck() {
	pm.mu.RLock()
	peers := make([]*PeerInfo, 0, len(pm.peers))
	for _, peerInfo := range pm.peers {
		peers = append(peers, peerInfo)
	}
	pm.mu.RUnlock()

	// Check each peer (in real implementation, this would be actual health checks)
	for _, peerInfo := range peers {
		// Simulate health check - in production, this would be actual HTTP/gRPC calls
		if time.Since(peerInfo.LastHeartbeat) > 30*time.Second {
			pm.RecordFailure(peerInfo.Node.ID)
		}
	}
}
