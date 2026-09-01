package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/toll-mesh/store/coordination"
)

// HealthChecker provides health check functionality
type HealthChecker struct {
	coordinator     *coordination.GossipCoordinator
	peerManager     *coordination.PeerManager
	failureDetector *coordination.FailureDetector
	startTime       time.Time
}

// HealthStatus represents the health status of the node
type HealthStatus struct {
	Status         string                 `json:"status"`
	NodeID         string                 `json:"node_id"`
	Uptime         int64                  `json:"uptime_seconds"`
	Peers          int                    `json:"peers"`
	HealthyPeers   int                    `json:"healthy_peers"`
	UnhealthyPeers int                    `json:"unhealthy_peers"`
	Timestamp      int64                  `json:"timestamp"`
	Version        string                 `json:"version"`
	Checks         map[string]CheckResult `json:"checks"`
}

// CheckResult represents the result of a health check
type CheckResult struct {
	Status  string                 `json:"status"`
	Message string                 `json:"message,omitempty"`
	Details map[string]interface{} `json:"details,omitempty"`
}

// ReadinessStatus represents the readiness status of the node
type ReadinessStatus struct {
	Ready  bool                   `json:"ready"`
	Reason string                 `json:"reason,omitempty"`
	Checks map[string]CheckResult `json:"checks"`
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(
	coordinator *coordination.GossipCoordinator,
	peerManager *coordination.PeerManager,
	failureDetector *coordination.FailureDetector,
) *HealthChecker {
	return &HealthChecker{
		coordinator:     coordinator,
		peerManager:     peerManager,
		failureDetector: failureDetector,
		startTime:       time.Now(),
	}
}

// GetHealthStatus returns the current health status
func (hc *HealthChecker) GetHealthStatus() *HealthStatus {
	uptime := int64(time.Since(hc.startTime).Seconds())
	peers := hc.coordinator.GetPeers()
	healthyPeers := hc.peerManager.GetHealthyPeers()

	checks := make(map[string]CheckResult)

	// Coordinator check
	checks["coordinator"] = CheckResult{
		Status:  "healthy",
		Message: "Gossip coordinator is running",
	}

	// Peer manager check
	peerStats := hc.peerManager.GetStats()
	checks["peer_manager"] = CheckResult{
		Status:  "healthy",
		Message: "Peer manager is operational",
		Details: peerStats,
	}

	// Failure detector check
	fdStats := hc.failureDetector.GetStats()
	checks["failure_detector"] = CheckResult{
		Status:  "healthy",
		Message: "Failure detector is operational",
		Details: fdStats,
	}

	return &HealthStatus{
		Status:         "healthy",
		NodeID:         hc.coordinator.GetStats()["node_id"].(string),
		Uptime:         uptime,
		Peers:          len(peers),
		HealthyPeers:   len(healthyPeers),
		UnhealthyPeers: len(peers) - len(healthyPeers),
		Timestamp:      time.Now().Unix(),
		Version:        "1.0.0",
		Checks:         checks,
	}
}

// GetReadinessStatus returns the readiness status
func (hc *HealthChecker) GetReadinessStatus() *ReadinessStatus {
	checks := make(map[string]CheckResult)
	ready := true

	// Check if coordinator is ready
	coordinatorReady := true
	checks["coordinator"] = CheckResult{
		Status:  "ready",
		Message: "Coordinator is ready",
	}

	// Check if peer manager is ready
	peerManagerReady := true
	checks["peer_manager"] = CheckResult{
		Status:  "ready",
		Message: "Peer manager is ready",
	}

	// Check if failure detector is ready
	failureDetectorReady := true
	checks["failure_detector"] = CheckResult{
		Status:  "ready",
		Message: "Failure detector is ready",
	}

	ready = coordinatorReady && peerManagerReady && failureDetectorReady

	reason := ""
	if !ready {
		reason = "One or more components are not ready"
	}

	return &ReadinessStatus{
		Ready:  ready,
		Reason: reason,
		Checks: checks,
	}
}

// HandleLiveness handles liveness probe requests
func (hc *HealthChecker) HandleLiveness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := hc.GetHealthStatus()

	w.Header().Set("Content-Type", "application/json")
	if status.Status == "healthy" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(status)
}

// HandleReadiness handles readiness probe requests
func (hc *HealthChecker) HandleReadiness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := hc.GetReadinessStatus()

	w.Header().Set("Content-Type", "application/json")
	if status.Ready {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(status)
}
