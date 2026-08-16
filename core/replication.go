package core

import (
	"fmt"
	"sync"
	"time"
)

// ReplicationConfig holds configuration for replication
type ReplicationConfig struct {
	ReplicationFactor int           // Number of replicas
	SyncInterval      time.Duration // Interval for replica sync
	ReadRepair        bool          // Enable read repair
	AntiEntropy       bool          // Enable anti-entropy
}

// ReplicaSet manages replicas for a key
type ReplicaSet struct {
	Key       string
	Nodes     []*Node
	Values    map[string]interface{}
	Timestamp int64
}

// ReplicationManager manages data replication across nodes
type ReplicationManager struct {
	mu                sync.RWMutex
	config            ReplicationConfig
	ring              *Ring
	replicas          map[string]*ReplicaSet
	replicationTicker *time.Ticker
	stopChan          chan struct{}
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(ring *Ring, config ReplicationConfig) *ReplicationManager {
	return &ReplicationManager{
		config:   config,
		ring:     ring,
		replicas: make(map[string]*ReplicaSet),
		stopChan: make(chan struct{}),
	}
}

// ReplicateKey replicates a key to multiple nodes
func (rm *ReplicationManager) ReplicateKey(key string, value interface{}) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Get replica nodes
	nodes, err := rm.ring.GetNodes(key, rm.config.ReplicationFactor)
	if err != nil {
		return fmt.Errorf("failed to get replica nodes: %w", err)
	}

	// Create replica set
	replicaSet := &ReplicaSet{
		Key:       key,
		Nodes:     nodes,
		Values:    make(map[string]interface{}),
		Timestamp: time.Now().UnixMilli(),
	}

	// Store value on all replicas
	for _, node := range nodes {
		replicaSet.Values[node.ID] = value
	}

	rm.replicas[key] = replicaSet
	return nil
}

// GetReplicas returns the replica set for a key
func (rm *ReplicationManager) GetReplicas(key string) (*ReplicaSet, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	replicaSet, exists := rm.replicas[key]
	if !exists {
		return nil, fmt.Errorf("replica set for key %s not found", key)
	}

	return replicaSet, nil
}

// PerformReadRepair performs read repair on inconsistent replicas
func (rm *ReplicationManager) PerformReadRepair(key string) error {
	if !rm.config.ReadRepair {
		return nil
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	replicaSet, exists := rm.replicas[key]
	if !exists {
		return fmt.Errorf("replica set for key %s not found", key)
	}

	// Find the most recent value
	var latestValue interface{}
	var latestTimestamp int64

	for _, value := range replicaSet.Values {
		// In a real implementation, compare timestamps
		latestValue = value
		latestTimestamp = time.Now().UnixMilli()
	}

	// Update all replicas with latest value
	for nodeID := range replicaSet.Values {
		replicaSet.Values[nodeID] = latestValue
	}

	replicaSet.Timestamp = latestTimestamp
	return nil
}

// PerformAntiEntropy performs anti-entropy repair
func (rm *ReplicationManager) PerformAntiEntropy() error {
	if !rm.config.AntiEntropy {
		return nil
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	for key, replicaSet := range rm.replicas {
		// Compare replicas and fix inconsistencies
		if err := rm.reconcileReplicas(key, replicaSet); err != nil {
			return fmt.Errorf("failed to reconcile replicas for key %s: %w", key, err)
		}
	}

	return nil
}

// GetReplicationStats returns replication statistics
func (rm *ReplicationManager) GetReplicationStats() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	totalReplicas := 0
	for _, replicaSet := range rm.replicas {
		totalReplicas += len(replicaSet.Nodes)
	}

	return map[string]interface{}{
		"total_keys":           len(rm.replicas),
		"total_replicas":       totalReplicas,
		"replication_factor":   rm.config.ReplicationFactor,
		"read_repair_enabled":  rm.config.ReadRepair,
		"anti_entropy_enabled": rm.config.AntiEntropy,
		"sync_interval":        rm.config.SyncInterval.String(),
	}
}

// Start begins the replication manager
func (rm *ReplicationManager) Start() {
	rm.replicationTicker = time.NewTicker(rm.config.SyncInterval)
	go rm.replicationLoop()
}

// Stop gracefully shuts down the replication manager
func (rm *ReplicationManager) Stop() error {
	close(rm.stopChan)
	if rm.replicationTicker != nil {
		rm.replicationTicker.Stop()
	}
	return nil
}

// Private helper methods

func (rm *ReplicationManager) reconcileReplicas(key string, replicaSet *ReplicaSet) error {
	// Find the most common value (quorum-based)
	valueCounts := make(map[interface{}]int)
	for _, value := range replicaSet.Values {
		valueCounts[value]++
	}

	var mostCommonValue interface{}
	maxCount := 0
	for value, count := range valueCounts {
		if count > maxCount {
			maxCount = count
			mostCommonValue = value
		}
	}

	// Update all replicas with the most common value
	for nodeID := range replicaSet.Values {
		replicaSet.Values[nodeID] = mostCommonValue
	}

	return nil
}

func (rm *ReplicationManager) replicationLoop() {
	for {
		select {
		case <-rm.stopChan:
			return
		case <-rm.replicationTicker.C:
			_ = rm.PerformAntiEntropy()
		}
	}
}
