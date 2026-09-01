package api

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/toll-mesh/store/core"
)

// LoadBalancerConfig holds configuration for load balancing
type LoadBalancerConfig struct {
	Strategy            string        // Strategy: round-robin, least-connections, random, health-aware
	HealthCheckInterval time.Duration // Interval for health checks
	MaxRetries          int           // Maximum retry attempts
}

// LoadBalancer distributes requests across nodes
type LoadBalancer struct {
	mu                sync.RWMutex
	config            LoadBalancerConfig
	nodes             []*core.Node
	currentIndex      int64
	nodeHealth        map[string]bool
	nodeConnections   map[string]int64
	healthCheckTicker *time.Ticker
	stopChan          chan struct{}
}

// NewLoadBalancer creates a new load balancer
func NewLoadBalancer(config LoadBalancerConfig) *LoadBalancer {
	return &LoadBalancer{
		config:          config,
		nodes:           make([]*core.Node, 0),
		nodeHealth:      make(map[string]bool),
		nodeConnections: make(map[string]int64),
		stopChan:        make(chan struct{}),
	}
}

// AddNode adds a node to the load balancer
func (lb *LoadBalancer) AddNode(node *core.Node) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.nodes = append(lb.nodes, node)
	lb.nodeHealth[node.ID] = true
	lb.nodeConnections[node.ID] = 0

	return nil
}

// RemoveNode removes a node from the load balancer
func (lb *LoadBalancer) RemoveNode(nodeID string) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	for i, node := range lb.nodes {
		if node.ID == nodeID {
			lb.nodes = append(lb.nodes[:i], lb.nodes[i+1:]...)
			delete(lb.nodeHealth, nodeID)
			delete(lb.nodeConnections, nodeID)
			return nil
		}
	}

	return fmt.Errorf("node %s not found", nodeID)
}

// SelectNode selects a node based on the configured strategy
func (lb *LoadBalancer) SelectNode() (*core.Node, error) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	if len(lb.nodes) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}

	switch lb.config.Strategy {
	case "round-robin":
		return lb.selectRoundRobin()
	case "least-connections":
		return lb.selectLeastConnections()
	case "health-aware":
		return lb.selectHealthAware()
	default:
		return lb.selectRoundRobin()
	}
}

// RecordConnection records a connection to a node
func (lb *LoadBalancer) RecordConnection(nodeID string) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if _, exists := lb.nodeConnections[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	lb.nodeConnections[nodeID]++
	return nil
}

// ReleaseConnection releases a connection from a node
func (lb *LoadBalancer) ReleaseConnection(nodeID string) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if _, exists := lb.nodeConnections[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if lb.nodeConnections[nodeID] > 0 {
		lb.nodeConnections[nodeID]--
	}

	return nil
}

// MarkNodeHealthy marks a node as healthy
func (lb *LoadBalancer) MarkNodeHealthy(nodeID string) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if _, exists := lb.nodeHealth[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	lb.nodeHealth[nodeID] = true
	return nil
}

// MarkNodeUnhealthy marks a node as unhealthy
func (lb *LoadBalancer) MarkNodeUnhealthy(nodeID string) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if _, exists := lb.nodeHealth[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	lb.nodeHealth[nodeID] = false
	return nil
}

// GetStats returns load balancer statistics
func (lb *LoadBalancer) GetStats() map[string]interface{} {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	healthyCount := 0
	totalConnections := int64(0)

	nodeStats := make(map[string]map[string]interface{})
	for _, node := range lb.nodes {
		isHealthy := lb.nodeHealth[node.ID]
		connections := lb.nodeConnections[node.ID]

		if isHealthy {
			healthyCount++
		}
		totalConnections += connections

		nodeStats[node.ID] = map[string]interface{}{
			"healthy":     isHealthy,
			"connections": connections,
			"address":     fmt.Sprintf("%s:%d", node.Address, node.Port),
		}
	}

	return map[string]interface{}{
		"total_nodes":       len(lb.nodes),
		"healthy_nodes":     healthyCount,
		"total_connections": totalConnections,
		"strategy":          lb.config.Strategy,
		"node_stats":        nodeStats,
	}
}

// Start begins the load balancer
func (lb *LoadBalancer) Start() {
	lb.healthCheckTicker = time.NewTicker(lb.config.HealthCheckInterval)
	go lb.healthCheckLoop()
}

// Stop gracefully shuts down the load balancer
func (lb *LoadBalancer) Stop() error {
	close(lb.stopChan)
	if lb.healthCheckTicker != nil {
		lb.healthCheckTicker.Stop()
	}
	return nil
}

// Private helper methods

func (lb *LoadBalancer) selectRoundRobin() (*core.Node, error) {
	// Get next healthy node in round-robin fashion
	for i := 0; i < len(lb.nodes); i++ {
		idx := (atomic.AddInt64(&lb.currentIndex, 1) - 1) % int64(len(lb.nodes))
		node := lb.nodes[idx]

		if lb.nodeHealth[node.ID] {
			return node, nil
		}
	}

	return nil, fmt.Errorf("no healthy nodes available")
}

func (lb *LoadBalancer) selectLeastConnections() (*core.Node, error) {
	var selectedNode *core.Node
	minConnections := int64(^uint64(0) >> 1) // Max int64

	for _, node := range lb.nodes {
		if !lb.nodeHealth[node.ID] {
			continue
		}

		connections := lb.nodeConnections[node.ID]
		if connections < minConnections {
			minConnections = connections
			selectedNode = node
		}
	}

	if selectedNode == nil {
		return nil, fmt.Errorf("no healthy nodes available")
	}

	return selectedNode, nil
}

func (lb *LoadBalancer) selectHealthAware() (*core.Node, error) {
	// Prefer healthy nodes with fewer connections
	var candidates []*core.Node

	for _, node := range lb.nodes {
		if lb.nodeHealth[node.ID] {
			candidates = append(candidates, node)
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no healthy nodes available")
	}

	// Select node with least connections from healthy candidates
	var selectedNode *core.Node
	minConnections := int64(^uint64(0) >> 1)

	for _, node := range candidates {
		connections := lb.nodeConnections[node.ID]
		if connections < minConnections {
			minConnections = connections
			selectedNode = node
		}
	}

	return selectedNode, nil
}

func (lb *LoadBalancer) healthCheckLoop() {
	for {
		select {
		case <-lb.stopChan:
			return
		case <-lb.healthCheckTicker.C:
			// Health checks are performed externally
		}
	}
}
