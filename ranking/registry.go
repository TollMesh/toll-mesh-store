package ranking

import (
	"fmt"
	"sync"
	"time"
)

// RankingConfig is a named, reusable ranking configuration: a strategy
// (which Ranker to build) plus optional boosts, so a client registers it
// once and references it by name on every later rank call instead of
// resending strategy/boosts each time -- the same shape Pipelines gives
// operation sequences. Created doubles as this config's LWW-register
// version for gossip replication (Register re-stamps it on every
// registration, not just the first) -- Node is the registering node's ID,
// breaking ties the same way Pipeline/cache do.
type RankingConfig struct {
	Name     string             `json:"name"`
	Strategy string             `json:"strategy"`
	Boosts   map[string]float32 `json:"boosts,omitempty"`
	Created  int64              `json:"created"`
	Node     string             `json:"node,omitempty"`
}

// Registry stores named RankingConfigs and builds/runs the Ranker each one
// names.
type Registry struct {
	mu      sync.RWMutex
	configs map[string]*RankingConfig
}

// NewRegistry creates an empty ranking config registry.
func NewRegistry() *Registry {
	return &Registry{configs: make(map[string]*RankingConfig)}
}

// Register saves a named ranking config, replacing any existing one under
// the same name. Strategy follows MeshStore.Rank's existing convention:
// "vector", "llm", "context", or anything else (including empty) defaults
// to BM25.
func (r *Registry) Register(cfg *RankingConfig) error {
	if cfg.Name == "" {
		return fmt.Errorf("ranking config name is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	cfg.Created = time.Now().UnixMilli()
	r.configs[cfg.Name] = cfg
	return nil
}

// Get retrieves a registered ranking config by name.
func (r *Registry) Get(name string) (*RankingConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cfg, exists := r.configs[name]
	if !exists {
		return nil, fmt.Errorf("ranking config not found: %s", name)
	}
	return cfg, nil
}

// List returns every registered ranking config.
func (r *Registry) List() []*RankingConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*RankingConfig, 0, len(r.configs))
	for _, cfg := range r.configs {
		out = append(out, cfg)
	}
	return out
}

// Delete removes a registered ranking config.
func (r *Registry) Delete(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.configs[name]; !exists {
		return fmt.Errorf("ranking config not found: %s", name)
	}
	delete(r.configs, name)
	return nil
}

// BuildRanker constructs the Ranker a (strategy, boosts) pair names -- the
// same selection logic MeshStore.Rank already used inline for its ad-hoc
// (unregistered) path, factored out here so RankWithConfig can reuse it
// exactly rather than duplicating the switch.
func BuildRanker(strategy string, boosts map[string]float32) Ranker {
	switch strategy {
	case "vector":
		return NewVectorRanker()
	case "llm":
		return NewLLMRanker()
	case "context":
		ctxMap := map[string]interface{}{}
		if boosts != nil {
			ctxMap["boosts"] = boosts
		}
		return NewContextRanker(ctxMap)
	default:
		return NewBM25Ranker()
	}
}

// RankWithConfig looks up a registered config by name and ranks items
// using the strategy/boosts it names.
func (r *Registry) RankWithConfig(name string, items []RankedItem) ([]RankedItem, error) {
	cfg, err := r.Get(name)
	if err != nil {
		return nil, err
	}
	return BuildRanker(cfg.Strategy, cfg.Boosts).Rank(items), nil
}

// Snapshot returns a copy of every registered config, for gossip
// replication.
func (r *Registry) Snapshot() []RankingConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]RankingConfig, 0, len(r.configs))
	for _, cfg := range r.configs {
		out = append(out, *cfg)
	}
	return out
}

// MergeSnapshot merges a peer's Snapshot output: a (Created, Node)
// LWW-register comparison per config name, the same pattern as Pipelines
// -- a peer's config is adopted only when it's strictly newer.
//
// Known limitation, the same as Pipelines: Delete is a hard local delete
// with no tombstone, so a deleted config will be silently re-introduced by
// the next gossip round from any peer that still has it.
func (r *Registry) MergeSnapshot(configs []RankingConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := range configs {
		peer := &configs[i]

		local, exists := r.configs[peer.Name]
		if exists && !configLess(local.Created, local.Node, peer.Created, peer.Node) {
			continue
		}

		peerCopy := *peer
		r.configs[peer.Name] = &peerCopy
	}
}

// configLess reports whether (createdA, nodeA) sorts strictly before
// (createdB, nodeB) in the ranking config LWW-register's version order.
func configLess(createdA int64, nodeA string, createdB int64, nodeB string) bool {
	if createdA != createdB {
		return createdA < createdB
	}
	return nodeA < nodeB
}
