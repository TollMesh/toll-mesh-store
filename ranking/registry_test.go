package ranking

import "testing"

func TestRegisterAndRankWithConfig(t *testing.T) {
	r := NewRegistry()

	err := r.Register(&RankingConfig{
		Name:     "boost-b",
		Strategy: "context",
		Boosts:   map[string]float32{"b": 10},
		Node:     "node-1",
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	items := []RankedItem{{ID: "a", Score: 1}, {ID: "b", Score: 1}}
	result, err := r.RankWithConfig("boost-b", items)
	if err != nil {
		t.Fatalf("rank with config failed: %v", err)
	}
	if result[0].ID != "b" {
		t.Errorf("expected boosted item 'b' to rank first, got %s", result[0].ID)
	}
}

func TestRegisterRequiresName(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(&RankingConfig{Strategy: "bm25"}); err == nil {
		t.Error("expected error for empty config name")
	}
}

func TestGetDeleteUnknownConfig(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Get("ghost"); err == nil {
		t.Error("expected error getting unknown config")
	}
	if err := r.Delete("ghost"); err == nil {
		t.Error("expected error deleting unknown config")
	}
}

func TestListReturnsAllConfigs(t *testing.T) {
	r := NewRegistry()
	r.Register(&RankingConfig{Name: "a", Strategy: "bm25"})
	r.Register(&RankingConfig{Name: "b", Strategy: "vector"})

	configs := r.List()
	if len(configs) != 2 {
		t.Fatalf("expected 2 configs, got %d", len(configs))
	}
}

func TestRankWithConfigDefaultsToBM25(t *testing.T) {
	r := NewRegistry()
	r.Register(&RankingConfig{Name: "default", Strategy: ""})

	items := []RankedItem{{ID: "a", Score: 1}, {ID: "b", Score: 3}}
	result, err := r.RankWithConfig("default", items)
	if err != nil {
		t.Fatalf("rank failed: %v", err)
	}
	if result[0].ID != "b" {
		t.Errorf("expected highest-score item first, got %s", result[0].ID)
	}
}

// TestMergeSnapshotAdoptsNewerPeerConfig is the regression test for
// ranking config gossip replication: MergeSnapshot must adopt a peer's
// config only when it's strictly newer (by Created, then Node), the same
// LWW-register rule as Pipelines.
func TestMergeSnapshotAdoptsNewerPeerConfig(t *testing.T) {
	r := NewRegistry()
	r.Register(&RankingConfig{Name: "cfg1", Strategy: "bm25", Node: "node-1"})

	local, _ := r.Get("cfg1")

	// A stale peer version must not overwrite the newer local one.
	stalePeer := RankingConfig{Name: "cfg1", Strategy: "vector", Created: local.Created - 1000, Node: "node-2"}
	r.MergeSnapshot([]RankingConfig{stalePeer})

	current, _ := r.Get("cfg1")
	if current.Strategy != "bm25" {
		t.Fatalf("stale peer config incorrectly adopted, strategy = %s", current.Strategy)
	}

	// A newer peer version must be adopted.
	newerPeer := RankingConfig{Name: "cfg1", Strategy: "vector", Created: local.Created + 1000, Node: "node-2"}
	r.MergeSnapshot([]RankingConfig{newerPeer})

	current, _ = r.Get("cfg1")
	if current.Strategy != "vector" || current.Node != "node-2" {
		t.Fatalf("newer peer config not adopted correctly: %+v", current)
	}
}

// TestMergeSnapshotInsertsUnknownPeerConfig verifies a config registered
// only on a peer (never seen locally) is inserted outright.
func TestMergeSnapshotInsertsUnknownPeerConfig(t *testing.T) {
	r := NewRegistry()

	peer := RankingConfig{Name: "peer-cfg", Strategy: "llm", Created: 1000, Node: "node-2"}
	r.MergeSnapshot([]RankingConfig{peer})

	cfg, err := r.Get("peer-cfg")
	if err != nil {
		t.Fatalf("peer config not found after merge: %v", err)
	}
	if cfg.Strategy != "llm" {
		t.Fatalf("unexpected merged config: %+v", cfg)
	}
}
