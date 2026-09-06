package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/toll-mesh/store/coordination"
)

// HealthChecker backs the /livez and /readyz probe endpoints, distinct
// from the simple, unconditional /health used by SDKs and load balancers.
// Liveness answers "is this process itself broken and should be
// restarted" -- true as long as the process is up and responding at all,
// since a distributed node isolated from its peers (a network partition)
// is not the same thing as a broken process, and having Kubernetes (or
// any orchestrator) kill and restart every partitioned node would turn a
// partition into a much worse, cascading outage. Readiness answers "is
// this node currently able to do useful cluster work" -- real logic
// derived from PeerManager's actual health tracking (see peer_manager.go),
// not a hardcoded true.
type HealthChecker struct {
	coordinator *coordination.GossipCoordinator
	startTime   time.Time
}

// HealthStatus represents the current liveness status of the node.
type HealthStatus struct {
	Status    string `json:"status"`
	NodeID    string `json:"node_id"`
	Uptime    int64  `json:"uptime_seconds"`
	Timestamp int64  `json:"timestamp"`
}

// ReadinessStatus represents the current readiness status of the node.
type ReadinessStatus struct {
	Ready          bool   `json:"ready"`
	Reason         string `json:"reason,omitempty"`
	Peers          int    `json:"peers"`
	HealthyPeers   int    `json:"healthy_peers"`
	UnhealthyPeers int    `json:"unhealthy_peers"`
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker(coordinator *coordination.GossipCoordinator) *HealthChecker {
	return &HealthChecker{
		coordinator: coordinator,
		startTime:   time.Now(),
	}
}

// GetHealthStatus returns this node's liveness status -- always healthy
// if this code is running at all (see doc comment above for why liveness
// deliberately doesn't factor in peer connectivity).
func (hc *HealthChecker) GetHealthStatus() *HealthStatus {
	nodeID, _ := hc.coordinator.GetStats()["node_id"].(string)
	return &HealthStatus{
		Status:    "healthy",
		NodeID:    nodeID,
		Uptime:    int64(time.Since(hc.startTime).Seconds()),
		Timestamp: time.Now().Unix(),
	}
}

// GetReadinessStatus returns this node's readiness status. A standalone
// node with no configured peers is always ready (there's no cluster
// connectivity to lose). A node with configured peers is ready as long as
// at least one is currently healthy (per PeerManager's real health
// tracking, fed by both performGossip's own results and the independent
// periodic /health check -- see gossip.go and peer_manager.go); it is not
// ready if every known peer is currently unreachable, since this node is
// then fully isolated from the cluster and can only serve local,
// unreplicated state.
func (hc *HealthChecker) GetReadinessStatus() *ReadinessStatus {
	peers := hc.coordinator.GetPeers()
	healthy := hc.coordinator.PeerManager().GetHealthyPeers()

	status := &ReadinessStatus{
		Peers:          len(peers),
		HealthyPeers:   len(healthy),
		UnhealthyPeers: len(peers) - len(healthy),
	}

	if len(peers) == 0 || len(healthy) > 0 {
		status.Ready = true
		return status
	}

	status.Ready = false
	status.Reason = "isolated from cluster: no configured peer is currently reachable"
	return status
}

// HandleLiveness handles liveness probe requests (e.g. Kubernetes livez).
func (hc *HealthChecker) HandleLiveness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := hc.GetHealthStatus()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(status)
}

// HandleReadiness handles readiness probe requests (e.g. Kubernetes
// readyz).
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
