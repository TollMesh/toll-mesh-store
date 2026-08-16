package coordination

import (
	"testing"
	"time"

	"github.com/TollMesh/toll-mesh-store/core"
)

// TestPeerManager tests the peer manager functionality
func TestPeerManager(t *testing.T) {
	pm := NewPeerManager(3, 1*time.Second)

	// Test adding a peer
	node := &core.Node{
		ID:      "node2",
		Address: "localhost",
		Port:    8001,
	}

	err := pm.AddPeer(node)
	if err != nil {
		t.Fatalf("Failed to add peer: %v", err)
	}

	// Test getting all peers
	peers := pm.GetAllPeers()
	if len(peers) != 1 {
		t.Fatalf("Expected 1 peer, got %d", len(peers))
	}

	// Test recording success
	err = pm.RecordSuccess("node2", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to record success: %v", err)
	}

	// Test getting healthy peers
	healthyPeers := pm.GetHealthyPeers()
	if len(healthyPeers) != 1 {
		t.Fatalf("Expected 1 healthy peer, got %d", len(healthyPeers))
	}

	// Test recording failures
	for i := 0; i < 3; i++ {
		pm.RecordFailure("node2")
	}

	// Test that peer is now unhealthy
	healthyPeers = pm.GetHealthyPeers()
	if len(healthyPeers) != 0 {
		t.Fatalf("Expected 0 healthy peers, got %d", len(healthyPeers))
	}

	// Test removing a peer
	err = pm.RemovePeer("node2")
	if err != nil {
		t.Fatalf("Failed to remove peer: %v", err)
	}

	peers = pm.GetAllPeers()
	if len(peers) != 0 {
		t.Fatalf("Expected 0 peers, got %d", len(peers))
	}
}

// TestStateSync tests the state synchronization functionality
func TestStateSync(t *testing.T) {
	ss := NewStateSync("node1", 1*time.Second)

	// Test setting local state
	localState := &core.MeshStoreState{
		RateLimiters:     make(map[string]interface{}),
		ReplayProtection: make(map[string]bool),
		Cache:            make(map[string]map[string][]byte),
	}

	localState.RateLimiters["key1"] = 10
	localState.ReplayProtection["nonce1"] = true

	err := ss.SetLocalState(localState)
	if err != nil {
		t.Fatalf("Failed to set local state: %v", err)
	}

	// Test getting local state
	retrieved := ss.GetLocalState()
	if retrieved == nil {
		t.Fatalf("Failed to retrieve local state")
	}

	// Test state hash
	hash := ss.GetStateHash()
	if hash == "" {
		t.Fatalf("Expected non-empty hash")
	}

	// Test updating peer state
	peerState := &core.MeshStoreState{
		RateLimiters:     make(map[string]interface{}),
		ReplayProtection: make(map[string]bool),
		Cache:            make(map[string]map[string][]byte),
	}

	peerState.RateLimiters["key2"] = 20
	peerState.ReplayProtection["nonce2"] = true

	err = ss.UpdatePeerState("node2", peerState)
	if err != nil {
		t.Fatalf("Failed to update peer state: %v", err)
	}

	// Test getting peer state
	retrieved, err = ss.GetPeerState("node2")
	if err != nil {
		t.Fatalf("Failed to get peer state: %v", err)
	}

	if retrieved == nil {
		t.Fatalf("Expected non-nil peer state")
	}

	// Test needs sync
	needsSync := ss.NeedsSyncWithPeer("node3")
	if !needsSync {
		t.Fatalf("Expected needs sync to be true for unknown peer")
	}
}

// TestFailureDetector tests the failure detector functionality
func TestFailureDetector(t *testing.T) {
	fd := NewFailureDetector("node1", 1*time.Second, 2, 2*time.Second, 500*time.Millisecond)

	// Test recording heartbeat
	err := fd.RecordHeartbeat("node2")
	if err != nil {
		t.Fatalf("Failed to record heartbeat: %v", err)
	}

	// Test that node is not suspected
	if fd.IsNodeSuspected("node2") {
		t.Fatalf("Expected node to not be suspected")
	}

	// Test suspecting a node (need to suspect twice to reach threshold of 2)
	err = fd.SuspectNode("node2")
	if err != nil {
		t.Fatalf("Failed to suspect node: %v", err)
	}

	err = fd.SuspectNode("node2")
	if err != nil {
		t.Fatalf("Failed to suspect node: %v", err)
	}

	// Test getting healthy nodes
	healthy := fd.GetHealthyNodes()
	if len(healthy) != 0 {
		t.Fatalf("Expected 0 healthy nodes, got %d", len(healthy))
	}

	// Test getting suspected nodes
	suspected := fd.GetSuspectedNodes()
	if len(suspected) != 1 {
		t.Fatalf("Expected 1 suspected node, got %d", len(suspected))
	}

	// Test marking node as recovered
	err = fd.MarkNodeRecovered("node2")
	if err != nil {
		t.Fatalf("Failed to mark node as recovered: %v", err)
	}

	if fd.IsNodeSuspected("node2") {
		t.Fatalf("Expected node to not be suspected after recovery")
	}

	// Test clearing a node
	err = fd.ClearNode("node2")
	if err != nil {
		t.Fatalf("Failed to clear node: %v", err)
	}

	// Test that node is no longer tracked
	_, err = fd.GetSuspicionInfo("node2")
	if err == nil {
		t.Fatalf("Expected error when getting info for cleared node")
	}
}

// TestGossipCoordinator tests the gossip coordinator functionality
func TestGossipCoordinator(t *testing.T) {
	config := &core.ClusterConfig{
		NodeName: "node1",
		BindAddr: "localhost",
		BindPort: 8000,
		Nodes: []core.Node{
			{ID: "node1", Address: "localhost", Port: 8000},
			{ID: "node2", Address: "localhost", Port: 8001},
		},
	}

	gc := NewGossipCoordinator(config, 1*time.Second)

	// Test getting peers
	peers := gc.GetPeers()
	if len(peers) != 1 {
		t.Fatalf("Expected 1 peer, got %d", len(peers))
	}

	// Test adding a peer
	newNode := &core.Node{
		ID:      "node3",
		Address: "localhost",
		Port:    8002,
	}

	err := gc.AddPeer(newNode)
	if err != nil {
		t.Fatalf("Failed to add peer: %v", err)
	}

	peers = gc.GetPeers()
	if len(peers) != 2 {
		t.Fatalf("Expected 2 peers, got %d", len(peers))
	}

	// Test removing a peer
	err = gc.RemovePeer("node3")
	if err != nil {
		t.Fatalf("Failed to remove peer: %v", err)
	}

	peers = gc.GetPeers()
	if len(peers) != 1 {
		t.Fatalf("Expected 1 peer, got %d", len(peers))
	}

	// Test getting stats
	stats := gc.GetStats()
	if stats == nil {
		t.Fatalf("Expected non-nil stats")
	}

	if stats["peer_count"] != 1 {
		t.Fatalf("Expected peer_count to be 1, got %v", stats["peer_count"])
	}
}

// TestConcurrentPeerManager tests concurrent operations on peer manager
func TestConcurrentPeerManager(t *testing.T) {
	pm := NewPeerManager(3, 1*time.Second)

	// Add multiple peers concurrently
	for i := 0; i < 10; i++ {
		go func(id int) {
			node := &core.Node{
				ID:      string(rune(id)),
				Address: "localhost",
				Port:    8000 + id,
			}
			pm.AddPeer(node)
		}(i)
	}

	time.Sleep(100 * time.Millisecond)

	peers := pm.GetAllPeers()
	if len(peers) != 10 {
		t.Fatalf("Expected 10 peers, got %d", len(peers))
	}
}

// TestConcurrentFailureDetector tests concurrent operations on failure detector
func TestConcurrentFailureDetector(t *testing.T) {
	fd := NewFailureDetector("node1", 1*time.Second, 2, 2*time.Second, 500*time.Millisecond)

	// Record heartbeats concurrently
	for i := 0; i < 10; i++ {
		go func(id int) {
			nodeID := string(rune(id))
			for j := 0; j < 5; j++ {
				fd.RecordHeartbeat(nodeID)
				time.Sleep(100 * time.Millisecond)
			}
		}(i)
	}

	time.Sleep(600 * time.Millisecond)

	stats := fd.GetStats()
	if stats == nil {
		t.Fatalf("Expected non-nil stats")
	}
}
