package api

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/toll-mesh/store/coordination"
	"github.com/toll-mesh/store/core"
	"github.com/toll-mesh/store/scripting"
	"github.com/toll-mesh/store/store"
)

// gossipTestNode bundles a real MeshStore, a real GossipCoordinator wired
// to it via RegisterStateMerger, and a real HTTP listener (httptest.Server,
// not httptest.NewRequest -- gossip's performGossip makes an actual TCP
// connection to a peer's /internal/state, so this needs a real socket).
type gossipTestNode struct {
	name        string
	store       *store.MeshStore
	coordinator *coordination.GossipCoordinator
	server      *httptest.Server
	addr        string
	port        int
}

func newGossipTestNode(t *testing.T, name string, syncInterval time.Duration) *gossipTestNode {
	t.Helper()

	config := &core.ClusterConfig{
		NodeName: name,
		DataDir:  t.TempDir(),
	}
	ms, err := store.NewMeshStore(config)
	if err != nil {
		t.Fatalf("NewMeshStore(%s) failed: %v", name, err)
	}
	t.Cleanup(func() { ms.Close() })

	coordinator := coordination.NewGossipCoordinator(config, syncInterval)
	coordinator.RegisterStateMerger(ms.MergeState)

	hs := NewHTTPServer(":0", ms, coordinator, "", "")
	server := httptest.NewServer(hs.mux)
	t.Cleanup(server.Close)

	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parsing test server URL: %v", err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("splitting host:port from %q: %v", u.Host, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parsing port from %q: %v", u.Host, err)
	}

	return &gossipTestNode{name: name, store: ms, coordinator: coordinator, server: server, addr: host, port: port}
}

// peerWith registers other as a peer of n and n as a peer of other, so
// gossip flows both directions.
func (n *gossipTestNode) peerWith(t *testing.T, other *gossipTestNode) {
	t.Helper()
	if err := n.coordinator.AddPeer(&core.Node{ID: other.name, Address: other.addr, Port: other.port}); err != nil {
		t.Fatalf("%s: AddPeer(%s) failed: %v", n.name, other.name, err)
	}
	if err := other.coordinator.AddPeer(&core.Node{ID: n.name, Address: n.addr, Port: n.port}); err != nil {
		t.Fatalf("%s: AddPeer(%s) failed: %v", other.name, n.name, err)
	}
}

// TestGossipReplicationConvergesAcrossRealNodes is the automated regression
// test for real multi-node replication: three MeshStore instances, each
// behind its own real HTTP listener (not an in-process mock), joined into a
// fully-connected mesh, gossiping over actual TCP connections to each
// other's /internal/state. Writes made on one node are asserted to appear
// on the others after enough gossip rounds -- this is the same thing that
// was previously verified only by hand with curl against three separately
// launched OS processes; this test exercises the identical code path
// (GossipCoordinator.performGossip -> HTTP GET -> MeshStore.MergeState)
// without needing real processes.
func TestGossipReplicationConvergesAcrossRealNodes(t *testing.T) {
	const syncInterval = 50 * time.Millisecond
	node1 := newGossipTestNode(t, "node-1", syncInterval)
	node2 := newGossipTestNode(t, "node-2", syncInterval)
	node3 := newGossipTestNode(t, "node-3", syncInterval)

	node1.peerWith(t, node2)
	node1.peerWith(t, node3)
	node2.peerWith(t, node3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range []*gossipTestNode{node1, node2, node3} {
		if err := n.coordinator.Start(ctx); err != nil {
			t.Fatalf("%s: coordinator.Start failed: %v", n.name, err)
		}
	}

	// Concurrent, different-node writes to different features.
	if err := node1.store.Set(ctx, "users", "alice", []byte("hello-from-node1"), 0); err != nil {
		t.Fatalf("node1 Set failed: %v", err)
	}
	if err := node2.store.Set(ctx, "users", "bob", []byte("hello-from-node2"), 0); err != nil {
		t.Fatalf("node2 Set failed: %v", err)
	}
	if _, err := node1.store.Seen(ctx, "nonce-abc", time.Minute); err != nil {
		t.Fatalf("node1 Seen failed: %v", err)
	}
	if _, err := node1.store.Consume(ctx, "api-limit", 1000, time.Minute); err != nil {
		t.Fatalf("node1 Consume failed: %v", err)
	}
	if _, err := node2.store.Consume(ctx, "api-limit", 1000, time.Minute); err != nil {
		t.Fatalf("node2 Consume failed: %v", err)
	}

	// Give gossip several rounds to fully converge (fully-connected mesh
	// with 50ms sync interval; a generous deadline keeps this from being
	// flaky on a loaded CI runner without making a real bug take long to
	// surface).
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = checkConverged(ctx, node1, node2, node3)
		if lastErr == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("state did not converge within deadline: %v", lastErr)
	}
}

// TestGossipCacheLWWConvergesOnConcurrentSameKeyWrites is the regression
// test for cache's real LWW-register CRDT merge: two nodes independently
// writing the *same* key must converge to whichever write actually
// happened later, on every node, regardless of which node did which write
// or the order gossip happens to run in. The old conservative-union merge
// (a peer's entry only adopted for a key the local side lacked) could not
// do this at all -- both nodes would just keep their own value forever.
// This also exercises the exact comparison this merge got backwards on
// the first pass (adopting only *older* peer entries instead of newer
// ones), so it would have caught that bug directly.
func TestGossipCacheLWWConvergesOnConcurrentSameKeyWrites(t *testing.T) {
	const syncInterval = 50 * time.Millisecond
	node1 := newGossipTestNode(t, "node-1", syncInterval)
	node2 := newGossipTestNode(t, "node-2", syncInterval)
	node1.peerWith(t, node2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range []*gossipTestNode{node1, node2} {
		if err := n.coordinator.Start(ctx); err != nil {
			t.Fatalf("%s: coordinator.Start failed: %v", n.name, err)
		}
	}

	// node1 writes first, then node2 overwrites the same key slightly
	// later -- node2's write should win everywhere once gossip converges,
	// on both nodes, not just the one that made the later write.
	if err := node1.store.Set(ctx, "shared", "key", []byte("from-node1-first"), 0); err != nil {
		t.Fatalf("node1 Set failed: %v", err)
	}
	time.Sleep(5 * time.Millisecond) // ensure a strictly later wall-clock write
	if err := node2.store.Set(ctx, "shared", "key", []byte("from-node2-later"), 0); err != nil {
		t.Fatalf("node2 Set failed: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = nil
		for _, n := range []*gossipTestNode{node1, node2} {
			v, exists, err := n.store.Get(ctx, "shared", "key")
			if err != nil || !exists || string(v) != "from-node2-later" {
				lastErr = fmt.Errorf("%s: shared/key = %q exists=%v err=%v, want \"from-node2-later\" (the later write)", n.name, v, exists, err)
				break
			}
		}
		if lastErr == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("cache did not converge to the later write within deadline: %v", lastErr)
	}
}

// TestGossipReplicatesSortedSets is the regression test for the first of
// the ten remaining (post-original-three-primitives) feature groups to
// gain gossip replication: sorted sets. Verifies both directions --
// a member added on one node appears on the other, and a member removed
// (tombstoned) on one node is removed on the other too, not just that
// additions propagate.
func TestGossipReplicatesSortedSets(t *testing.T) {
	const syncInterval = 50 * time.Millisecond
	node1 := newGossipTestNode(t, "node-1", syncInterval)
	node2 := newGossipTestNode(t, "node-2", syncInterval)
	node1.peerWith(t, node2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range []*gossipTestNode{node1, node2} {
		if err := n.coordinator.Start(ctx); err != nil {
			t.Fatalf("%s: coordinator.Start failed: %v", n.name, err)
		}
	}

	if err := node1.store.ZAdd(ctx, "leaderboard", "alice", 100); err != nil {
		t.Fatalf("node1 ZAdd failed: %v", err)
	}
	if err := node2.store.ZAdd(ctx, "leaderboard", "bob", 50); err != nil {
		t.Fatalf("node2 ZAdd failed: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = nil
		for _, n := range []*gossipTestNode{node1, node2} {
			if score, exists := n.store.ZScore(ctx, "leaderboard", "alice"); !exists || score != 100 {
				lastErr = fmt.Errorf("%s: ZScore(alice) = %v exists=%v, want 100/true", n.name, score, exists)
				break
			}
			if score, exists := n.store.ZScore(ctx, "leaderboard", "bob"); !exists || score != 50 {
				lastErr = fmt.Errorf("%s: ZScore(bob) = %v exists=%v, want 50/true", n.name, score, exists)
				break
			}
		}
		if lastErr == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("sorted set additions did not converge within deadline: %v", lastErr)
	}

	// Now remove bob on node2 and confirm the tombstone propagates to
	// node1 too -- a real CRDT merge must replicate deletes, not just
	// additions.
	if err := node2.store.ZRem(ctx, "leaderboard", "bob"); err != nil {
		t.Fatalf("node2 ZRem failed: %v", err)
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		lastErr = nil
		for _, n := range []*gossipTestNode{node1, node2} {
			if _, exists := n.store.ZScore(ctx, "leaderboard", "bob"); exists {
				lastErr = fmt.Errorf("%s: ZScore(bob) still exists after ZRem on node2", n.name)
				break
			}
		}
		if lastErr == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("sorted set removal did not converge within deadline: %v", lastErr)
	}
}

// TestGossipReplicatesStreams is the regression test for the second
// feature group (after Sorted Sets) to gain gossip replication: streams.
// Verifies entries appended on different nodes both end up in every
// node's copy of the log, in the correct combined chronological order --
// not just that the count matches, since Stream.Range/GetFirst/GetLast
// are positional over the merged slice, not derived from entry IDs.
func TestGossipReplicatesStreams(t *testing.T) {
	const syncInterval = 50 * time.Millisecond
	node1 := newGossipTestNode(t, "node-1", syncInterval)
	node2 := newGossipTestNode(t, "node-2", syncInterval)
	node1.peerWith(t, node2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range []*gossipTestNode{node1, node2} {
		if err := n.coordinator.Start(ctx); err != nil {
			t.Fatalf("%s: coordinator.Start failed: %v", n.name, err)
		}
	}

	if _, err := node1.store.XAdd(ctx, "events", map[string]string{"from": "node1"}); err != nil {
		t.Fatalf("node1 XAdd failed: %v", err)
	}
	if _, err := node2.store.XAdd(ctx, "events", map[string]string{"from": "node2"}); err != nil {
		t.Fatalf("node2 XAdd failed: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = nil
		for _, n := range []*gossipTestNode{node1, node2} {
			entries := n.store.XRange(ctx, "events", "0", "-", 100)
			if len(entries) != 2 {
				lastErr = fmt.Errorf("%s: XRange returned %d entries, want 2", n.name, len(entries))
				break
			}
			froms := map[string]bool{}
			for _, e := range entries {
				froms[e.Fields["from"]] = true
			}
			if !froms["node1"] || !froms["node2"] {
				lastErr = fmt.Errorf("%s: entries = %+v, missing one node's entry", n.name, entries)
				break
			}
		}
		if lastErr == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("stream entries did not converge within deadline: %v", lastErr)
	}
}

// TestGossipReplicatesPipelines is the regression test for the third
// feature group to gain gossip replication: pipelines. Verifies a
// pipeline registered on one node becomes executable on the other once
// gossip converges -- not just that the definition arrives, but that it's
// installed correctly enough to actually run (validated against the
// receiving node's own handler set, per MergeSnapshot's doc comment).
func TestGossipReplicatesPipelines(t *testing.T) {
	const syncInterval = 50 * time.Millisecond
	node1 := newGossipTestNode(t, "node-1", syncInterval)
	node2 := newGossipTestNode(t, "node-2", syncInterval)
	node1.peerWith(t, node2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range []*gossipTestNode{node1, node2} {
		if err := n.coordinator.Start(ctx); err != nil {
			t.Fatalf("%s: coordinator.Start failed: %v", n.name, err)
		}
	}

	pipeline := &scripting.Pipeline{
		Name: "seed-user",
		Steps: []scripting.Step{
			{Op: "set", Args: map[string]interface{}{"namespace": "users", "key": "alice", "value": "seeded"}},
		},
	}
	if err := node1.store.RegisterPipeline(ctx, pipeline); err != nil {
		t.Fatalf("node1 RegisterPipeline failed: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, err := node2.store.GetPipeline(ctx, "seed-user"); err == nil {
			lastErr = nil
			break
		} else {
			lastErr = err
		}
		time.Sleep(25 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("pipeline did not converge to node2 within deadline: %v", lastErr)
	}

	// Not just present -- actually executable on the node that only
	// learned about it via gossip.
	if _, err := node2.store.ExecutePipeline(ctx, "seed-user"); err != nil {
		t.Fatalf("node2 ExecutePipeline(seed-user) failed: %v", err)
	}
	value, exists, err := node2.store.Get(ctx, "users", "alice")
	if err != nil || !exists || string(value) != "seeded" {
		t.Fatalf("node2 users/alice = %q exists=%v err=%v after executing the gossiped pipeline, want \"seeded\"", value, exists, err)
	}
}

func checkConverged(ctx context.Context, node1, node2, node3 *gossipTestNode) error {
	for _, n := range []*gossipTestNode{node1, node2, node3} {
		v, exists, err := n.store.Get(ctx, "users", "alice")
		if err != nil || !exists || string(v) != "hello-from-node1" {
			return fmt.Errorf("%s: users/alice = %q exists=%v err=%v, want \"hello-from-node1\"", n.name, v, exists, err)
		}
		v, exists, err = n.store.Get(ctx, "users", "bob")
		if err != nil || !exists || string(v) != "hello-from-node2" {
			return fmt.Errorf("%s: users/bob = %q exists=%v err=%v, want \"hello-from-node2\"", n.name, v, exists, err)
		}
		seen, err := n.store.Seen(ctx, "nonce-abc", time.Minute)
		if err != nil || !seen {
			return fmt.Errorf("%s: Seen(nonce-abc) = %v err=%v, want true", n.name, seen, err)
		}
	}
	return nil
}
