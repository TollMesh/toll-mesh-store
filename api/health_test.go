package api

import (
	"testing"
	"time"

	"github.com/toll-mesh/store/coordination"
	"github.com/toll-mesh/store/core"
)

// TestReadinessStatus_StandaloneNodeIsAlwaysReady verifies a node with no
// configured peers is ready -- there's no cluster connectivity to lose.
func TestReadinessStatus_StandaloneNodeIsAlwaysReady(t *testing.T) {
	config := &core.ClusterConfig{NodeName: "node1"}
	coordinator := coordination.NewGossipCoordinator(config, time.Second)
	hc := NewHealthChecker(coordinator)

	status := hc.GetReadinessStatus()
	if !status.Ready {
		t.Fatalf("expected a standalone node with no peers to be ready, got %+v", status)
	}
}

// TestReadinessStatus_NotReadyWhenFullyIsolated is the regression test
// for real readiness logic: a node with configured peers, all of which
// PeerManager currently considers unhealthy, must report not-ready --
// unlike the previous hardcoded-true implementation, which never reported
// anything but "ready" regardless of actual peer state.
func TestReadinessStatus_NotReadyWhenFullyIsolated(t *testing.T) {
	config := &core.ClusterConfig{NodeName: "node1"}
	coordinator := coordination.NewGossipCoordinator(config, time.Second)
	if err := coordinator.AddPeer(&core.Node{ID: "peer1", Address: "127.0.0.1", Port: 1}); err != nil {
		t.Fatalf("AddPeer failed: %v", err)
	}
	for i := 0; i < 5; i++ {
		coordinator.PeerManager().RecordFailure("peer1")
	}

	hc := NewHealthChecker(coordinator)
	status := hc.GetReadinessStatus()
	if status.Ready {
		t.Fatalf("expected a node fully isolated from its only peer to be not-ready, got %+v", status)
	}
	if status.Peers != 1 || status.HealthyPeers != 0 || status.UnhealthyPeers != 1 {
		t.Errorf("unexpected peer counts in readiness status: %+v", status)
	}
}

// TestReadinessStatus_ReadyWithAtLeastOneHealthyPeer verifies a node with
// multiple peers, only some unhealthy, is still ready.
func TestReadinessStatus_ReadyWithAtLeastOneHealthyPeer(t *testing.T) {
	config := &core.ClusterConfig{NodeName: "node1"}
	coordinator := coordination.NewGossipCoordinator(config, time.Second)
	coordinator.AddPeer(&core.Node{ID: "peer1", Address: "127.0.0.1", Port: 1})
	coordinator.AddPeer(&core.Node{ID: "peer2", Address: "127.0.0.1", Port: 2})
	coordinator.PeerManager().RecordSuccess("peer1", time.Millisecond)
	for i := 0; i < 5; i++ {
		coordinator.PeerManager().RecordFailure("peer2")
	}

	hc := NewHealthChecker(coordinator)
	status := hc.GetReadinessStatus()
	if !status.Ready {
		t.Fatalf("expected a node with at least one healthy peer to be ready, got %+v", status)
	}
}

// TestLivenessAlwaysHealthy verifies liveness deliberately does not
// factor in peer connectivity (see HealthChecker's doc comment for why):
// even a fully isolated node is alive, just not ready.
func TestLivenessAlwaysHealthy(t *testing.T) {
	config := &core.ClusterConfig{NodeName: "node1"}
	coordinator := coordination.NewGossipCoordinator(config, time.Second)
	coordinator.AddPeer(&core.Node{ID: "peer1", Address: "127.0.0.1", Port: 1})
	for i := 0; i < 5; i++ {
		coordinator.PeerManager().RecordFailure("peer1")
	}

	hc := NewHealthChecker(coordinator)
	status := hc.GetHealthStatus()
	if status.Status != "healthy" {
		t.Fatalf("expected liveness to stay healthy regardless of peer isolation, got %+v", status)
	}
}
