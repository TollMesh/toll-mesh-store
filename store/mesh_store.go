package store

import (
	"context"
	"sync"
	"time"

	"github.com/toll-mesh/store/core"
)

// MeshStore implements the Store interface using CRDTs and gossip protocol.
type MeshStore struct {
	mu               sync.RWMutex
	config           *core.ClusterConfig
	rateLimiters     map[string]*core.GCounter
	replayProtection *core.GSet
	cache            map[string]map[string][]byte
	cacheTTL         map[string]map[string]time.Time
	stopChan         chan struct{}
}

// NewMeshStore creates a new MeshStore instance.
func NewMeshStore(config *core.ClusterConfig) (*MeshStore, error) {
	ms := &MeshStore{
		config:           config,
		rateLimiters:     make(map[string]*core.GCounter),
		replayProtection: core.NewGSet(),
		cache:            make(map[string]map[string][]byte),
		cacheTTL:         make(map[string]map[string]time.Time),
		stopChan:         make(chan struct{}),
	}
	go ms.backgroundCleanup()
	return ms, nil
}

// Consume implements rate limiting using GCounter CRDT.
func (ms *MeshStore) Consume(ctx context.Context, key string, limit int, window time.Duration) (core.ConsumeResult, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	counter, exists := ms.rateLimiters[key]
	if !exists {
		counter = core.NewGCounter()
		ms.rateLimiters[key] = counter
	}

	current := counter.Value()
	if current < limit {
		counter.Increment(ms.config.NodeName)
		return core.ConsumeResult{
			OK:        true,
			Remaining: limit - current - 1,
			ResetAt:   time.Now().Add(window).UnixMilli(),
		}, nil
	}

	return core.ConsumeResult{
		OK:        false,
		Remaining: 0,
		ResetAt:   time.Now().Add(window).UnixMilli(),
	}, nil
}

// Seen implements replay protection using GSet CRDT.
func (ms *MeshStore) Seen(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.replayProtection.Contains(key) {
		return true, nil
	}

	ms.replayProtection.Add(key)
	return false, nil
}

// Get retrieves a cached value.
func (ms *MeshStore) Get(ctx context.Context, ns, key string) ([]byte, bool, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	nsCache, exists := ms.cache[ns]
	if !exists {
		return nil, false, nil
	}

	value, exists := nsCache[key]
	if !exists {
		return nil, false, nil
	}

	ttlMap, ttlExists := ms.cacheTTL[ns]
	if ttlExists {
		if expiry, ok := ttlMap[key]; ok {
			if time.Now().After(expiry) {
				return nil, false, nil
			}
		}
	}

	return value, true, nil
}

// Set stores a value with TTL.
func (ms *MeshStore) Set(ctx context.Context, ns, key string, value []byte, ttl time.Duration) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.cache[ns]; !exists {
		ms.cache[ns] = make(map[string][]byte)
		ms.cacheTTL[ns] = make(map[string]time.Time)
	}

	ms.cache[ns][key] = value
	ms.cacheTTL[ns][key] = time.Now().Add(ttl)
	return nil
}

// Close gracefully shuts down the store.
func (ms *MeshStore) Close() error {
	close(ms.stopChan)
	return nil
}

// backgroundCleanup removes expired cache entries.
func (ms *MeshStore) backgroundCleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ms.stopChan:
			return
		case <-ticker.C:
			ms.mu.Lock()
			now := time.Now()
			for ns, ttlMap := range ms.cacheTTL {
				for key, expiry := range ttlMap {
					if now.After(expiry) {
						delete(ms.cache[ns], key)
						delete(ttlMap, key)
					}
				}
			}
			ms.mu.Unlock()
		}
	}
}
