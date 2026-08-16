package coordination

import (
	"fmt"
	"sync"
	"time"
)

// FailureDetector detects and handles node failures in the cluster
type FailureDetector struct {
	mu                   sync.RWMutex
	nodeID               string
	suspectedNodes       map[string]*SuspicionInfo
	heartbeatTimeout     time.Duration
	suspicionThreshold   int
	recoveryTimeout      time.Duration
	failureDetectionTick time.Duration
	stopChan             chan struct{}
	failureHandler       func(nodeID string) error
	recoveryHandler      func(nodeID string) error
}

// SuspicionInfo tracks suspicion information about a node
type SuspicionInfo struct {
	NodeID              string
	LastHeartbeat       time.Time
	SuspicionCount      int
	IsSuspected         bool
	SuspectedAt         time.Time
	RecoveryAttempts    int
	LastRecoveryAttempt time.Time
}

// NewFailureDetector creates a new failure detector
func NewFailureDetector(
	nodeID string,
	heartbeatTimeout time.Duration,
	suspicionThreshold int,
	recoveryTimeout time.Duration,
	failureDetectionTick time.Duration,
) *FailureDetector {
	return &FailureDetector{
		nodeID:               nodeID,
		suspectedNodes:       make(map[string]*SuspicionInfo),
		heartbeatTimeout:     heartbeatTimeout,
		suspicionThreshold:   suspicionThreshold,
		recoveryTimeout:      recoveryTimeout,
		failureDetectionTick: failureDetectionTick,
		stopChan:             make(chan struct{}),
	}
}

// RecordHeartbeat records a heartbeat from a node
func (fd *FailureDetector) RecordHeartbeat(nodeID string) error {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	suspicion, exists := fd.suspectedNodes[nodeID]
	if !exists {
		suspicion = &SuspicionInfo{
			NodeID:         nodeID,
			LastHeartbeat:  time.Now(),
			SuspicionCount: 0,
			IsSuspected:    false,
		}
		fd.suspectedNodes[nodeID] = suspicion
	} else {
		suspicion.LastHeartbeat = time.Now()
		suspicion.SuspicionCount = 0
		suspicion.IsSuspected = false
	}

	return nil
}

// SuspectNode marks a node as suspected
func (fd *FailureDetector) SuspectNode(nodeID string) error {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	suspicion, exists := fd.suspectedNodes[nodeID]
	if !exists {
		suspicion = &SuspicionInfo{
			NodeID:         nodeID,
			LastHeartbeat:  time.Now(),
			SuspicionCount: 1,
			IsSuspected:    true,
			SuspectedAt:    time.Now(),
		}
		fd.suspectedNodes[nodeID] = suspicion
	} else {
		suspicion.SuspicionCount++
		if suspicion.SuspicionCount >= fd.suspicionThreshold {
			suspicion.IsSuspected = true
			suspicion.SuspectedAt = time.Now()
		}
	}

	return nil
}

// IsNodeSuspected checks if a node is suspected
func (fd *FailureDetector) IsNodeSuspected(nodeID string) bool {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	suspicion, exists := fd.suspectedNodes[nodeID]
	if !exists {
		return false
	}

	return suspicion.IsSuspected
}

// GetSuspicionInfo returns suspicion information about a node
func (fd *FailureDetector) GetSuspicionInfo(nodeID string) (*SuspicionInfo, error) {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	suspicion, exists := fd.suspectedNodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// Return a copy
	copy := *suspicion
	return &copy, nil
}

// GetSuspectedNodes returns all suspected nodes
func (fd *FailureDetector) GetSuspectedNodes() []string {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	var suspected []string
	for nodeID, suspicion := range fd.suspectedNodes {
		if suspicion.IsSuspected {
			suspected = append(suspected, nodeID)
		}
	}
	return suspected
}

// GetHealthyNodes returns all healthy nodes
func (fd *FailureDetector) GetHealthyNodes() []string {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	var healthy []string
	for nodeID, suspicion := range fd.suspectedNodes {
		if !suspicion.IsSuspected {
			healthy = append(healthy, nodeID)
		}
	}
	return healthy
}

// RegisterFailureHandler registers a handler for node failures
func (fd *FailureDetector) RegisterFailureHandler(handler func(nodeID string) error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.failureHandler = handler
}

// RegisterRecoveryHandler registers a handler for node recovery
func (fd *FailureDetector) RegisterRecoveryHandler(handler func(nodeID string) error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.recoveryHandler = handler
}

// GetStats returns failure detector statistics
func (fd *FailureDetector) GetStats() map[string]interface{} {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	suspectedCount := 0
	healthyCount := 0
	totalRecoveryAttempts := 0

	for _, suspicion := range fd.suspectedNodes {
		if suspicion.IsSuspected {
			suspectedCount++
		} else {
			healthyCount++
		}
		totalRecoveryAttempts += suspicion.RecoveryAttempts
	}

	return map[string]interface{}{
		"node_id":                 fd.nodeID,
		"total_nodes":             len(fd.suspectedNodes),
		"suspected_nodes":         suspectedCount,
		"healthy_nodes":           healthyCount,
		"total_recovery_attempts": totalRecoveryAttempts,
		"heartbeat_timeout":       fd.heartbeatTimeout.String(),
		"suspicion_threshold":     fd.suspicionThreshold,
		"recovery_timeout":        fd.recoveryTimeout.String(),
	}
}

// Start begins the failure detection loop
func (fd *FailureDetector) Start() {
	go fd.detectionLoop()
}

// Stop gracefully shuts down the failure detector
func (fd *FailureDetector) Stop() error {
	close(fd.stopChan)
	return nil
}

// detectionLoop periodically checks for failures and attempts recovery
func (fd *FailureDetector) detectionLoop() {
	ticker := time.NewTicker(fd.failureDetectionTick)
	defer ticker.Stop()

	for {
		select {
		case <-fd.stopChan:
			return
		case <-ticker.C:
			fd.performDetection()
			fd.attemptRecovery()
		}
	}
}

// performDetection checks for node failures
func (fd *FailureDetector) performDetection() {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	now := time.Now()

	for nodeID, suspicion := range fd.suspectedNodes {
		timeSinceHeartbeat := now.Sub(suspicion.LastHeartbeat)

		// If no heartbeat for heartbeat timeout, suspect the node
		if timeSinceHeartbeat > fd.heartbeatTimeout && !suspicion.IsSuspected {
			suspicion.SuspicionCount++
			if suspicion.SuspicionCount >= fd.suspicionThreshold {
				suspicion.IsSuspected = true
				suspicion.SuspectedAt = now

				// Call failure handler
				if fd.failureHandler != nil {
					go fd.failureHandler(nodeID)
				}
			}
		}
	}
}

// attemptRecovery attempts to recover suspected nodes
func (fd *FailureDetector) attemptRecovery() {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	now := time.Now()

	for nodeID, suspicion := range fd.suspectedNodes {
		if !suspicion.IsSuspected {
			continue
		}

		timeSinceSuspicion := now.Sub(suspicion.SuspectedAt)

		// If suspected for recovery timeout, attempt recovery
		if timeSinceSuspicion > fd.recoveryTimeout {
			suspicion.RecoveryAttempts++
			suspicion.LastRecoveryAttempt = now

			// Reset suspicion for recovery attempt
			suspicion.SuspicionCount = 0

			// Call recovery handler
			if fd.recoveryHandler != nil {
				go fd.recoveryHandler(nodeID)
			}
		}
	}
}

// MarkNodeRecovered marks a node as recovered
func (fd *FailureDetector) MarkNodeRecovered(nodeID string) error {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	suspicion, exists := fd.suspectedNodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	suspicion.IsSuspected = false
	suspicion.SuspicionCount = 0
	suspicion.LastHeartbeat = time.Now()

	return nil
}

// ClearNode removes a node from tracking
func (fd *FailureDetector) ClearNode(nodeID string) error {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if _, exists := fd.suspectedNodes[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	delete(fd.suspectedNodes, nodeID)
	return nil
}
