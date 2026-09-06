package coordination

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toll-mesh/store/core"
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

// TestPeerManagerRealHealthCheckDetectsFailureAndRecovery is the
// regression test for a real bug: performHealthCheck's previous
// implementation didn't check anything real ("Simulate health check" was
// its own comment) -- it just marked a peer failed if enough wall-clock
// time had passed since its last *gossip-round* success, regardless of
// whether the peer was actually reachable. This verifies the fixed
// version performs a real GET <peer>/health and reacts to its actual
// result: unreachable marks failure, reachable-again marks success.
func TestPeerManagerRealHealthCheckDetectsFailureAndRecovery(t *testing.T) {
	up := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host, portStr, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("failed to parse port: %v", err)
	}

	pm := NewPeerManager(2, time.Hour) // interval irrelevant, we call performHealthCheck directly
	if err := pm.AddPeer(&core.Node{ID: "peer1", Address: host, Port: port}); err != nil {
		t.Fatalf("AddPeer failed: %v", err)
	}

	// Peer is up: one real check should record success.
	pm.performHealthCheck()
	if len(pm.GetHealthyPeers()) != 1 {
		t.Fatalf("expected peer to be healthy after a successful real health check")
	}

	// Peer goes down: enough real checks (matching the failure threshold)
	// must mark it unhealthy.
	up = false
	pm.performHealthCheck()
	pm.performHealthCheck()
	if len(pm.GetHealthyPeers()) != 0 {
		t.Fatalf("expected peer to be unhealthy after real health checks against a failing endpoint")
	}

	// Peer recovers: the very next real check must mark it healthy again
	// (RecordSuccess resets FailureCount/IsHealthy immediately).
	up = true
	pm.performHealthCheck()
	if len(pm.GetHealthyPeers()) != 1 {
		t.Fatalf("expected peer to be healthy again after recovering")
	}
}

// TestGossipCoordinatorPrefersHealthyPeers verifies performGossip skips a
// peer PeerManager has marked unhealthy in favor of a healthy one, so a
// cluster with any live peers doesn't keep wasting gossip rounds on a
// peer already known to be down.
func TestGossipCoordinatorPrefersHealthyPeers(t *testing.T) {
	var mergedFrom []string
	var mu sync.Mutex

	healthyServer := newStateServer(t, func() { mu.Lock(); mergedFrom = append(mergedFrom, "healthy"); mu.Unlock() })
	defer healthyServer.Close()

	config := &core.ClusterConfig{NodeName: "node1"}
	gc := NewGossipCoordinator(config, 20*time.Millisecond)
	gc.RegisterStateMerger(func(peer *core.MeshStoreState) {})

	healthyNode := mustParseTestServerNode(t, "healthy", healthyServer)
	deadNode := &core.Node{ID: "dead", Address: "127.0.0.1", Port: 1} // nothing listens on port 1
	gc.AddPeer(healthyNode)
	gc.AddPeer(deadNode)

	// Mark "dead" unhealthy directly, simulating prior failed rounds/
	// health checks, without waiting on the real threshold to be crossed.
	for i := 0; i < 3; i++ {
		gc.peerManager.RecordFailure("dead")
	}
	if len(gc.peerManager.GetHealthyPeers()) != 1 {
		t.Fatalf("expected exactly 1 healthy peer (the test server) after marking 'dead' unhealthy")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 10; i++ {
		gc.performGossip(ctx)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(mergedFrom) == 0 {
		t.Fatal("expected at least one successful gossip round against the healthy peer")
	}
	for _, from := range mergedFrom {
		if from != "healthy" {
			t.Fatalf("performGossip picked the unhealthy peer despite a healthy one being available: %v", mergedFrom)
		}
	}
}

// TestGossipCoordinatorFallsBackToAllPeersWhenNoneHealthy verifies
// performGossip still attempts a peer even when PeerManager considers
// every known peer unhealthy, so a total outage doesn't permanently
// prevent ever trying again once a peer might have recovered.
func TestGossipCoordinatorFallsBackToAllPeersWhenNoneHealthy(t *testing.T) {
	var merged bool
	var mu sync.Mutex

	server := newStateServer(t, func() { mu.Lock(); merged = true; mu.Unlock() })
	defer server.Close()

	config := &core.ClusterConfig{NodeName: "node1"}
	gc := NewGossipCoordinator(config, 20*time.Millisecond)
	gc.RegisterStateMerger(func(peer *core.MeshStoreState) {})

	node := mustParseTestServerNode(t, "peer1", server)
	gc.AddPeer(node)

	// Mark it unhealthy directly, without ever having a healthy peer to
	// prefer -- performGossip must still try it via the fallback path.
	for i := 0; i < 3; i++ {
		gc.peerManager.RecordFailure("peer1")
	}
	if len(gc.peerManager.GetHealthyPeers()) != 0 {
		t.Fatalf("expected 0 healthy peers after marking the only peer unhealthy")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gc.performGossip(ctx)

	mu.Lock()
	defer mu.Unlock()
	if !merged {
		t.Fatal("expected performGossip to fall back to the only (unhealthy) known peer")
	}
}

// newStateServer returns a test HTTP server serving a minimal, valid
// /internal/state response, calling onMerge whenever it's hit.
func newStateServer(t *testing.T, onMerge func()) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		onMerge()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
}

func mustParseTestServerNode(t *testing.T, id string, server *httptest.Server) *core.Node {
	t.Helper()
	host, portStr, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("failed to parse port: %v", err)
	}
	return &core.Node{ID: id, Address: host, Port: port}
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

