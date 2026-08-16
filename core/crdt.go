package core

import (
	"sync"
	"time"
)

// GCounter is a grow-only counter CRDT for rate limiting.
type GCounter struct {
	mu     sync.RWMutex
	counts map[string]int
}

func NewGCounter() *GCounter {
	return &GCounter{
		counts: make(map[string]int),
	}
}

func (g *GCounter) Increment(nodeID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.counts[nodeID]++
}

func (g *GCounter) Value() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	total := 0
	for _, count := range g.counts {
		total += count
	}
	return total
}

func (g *GCounter) Merge(other *GCounter) {
	g.mu.Lock()
	defer g.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	for nodeID, count := range other.counts {
		if current, exists := g.counts[nodeID]; !exists || count > current {
			g.counts[nodeID] = count
		}
	}
}

// GSet is a grow-only set CRDT for replay protection.
type GSet struct {
	mu    sync.RWMutex
	items map[string]struct{}
}

func NewGSet() *GSet {
	return &GSet{
		items: make(map[string]struct{}),
	}
}

func (g *GSet) Add(item string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.items[item] = struct{}{}
}

func (g *GSet) Contains(item string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, exists := g.items[item]
	return exists
}

func (g *GSet) Merge(other *GSet) {
	g.mu.Lock()
	defer g.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	for item := range other.items {
		g.items[item] = struct{}{}
	}
}

// ExpiringSet is a set with time-based expiration.
type ExpiringSet struct {
	mu       sync.RWMutex
	items    map[string]time.Time
	stopChan chan struct{}
}

func NewExpiringSet() *ExpiringSet {
	es := &ExpiringSet{
		items:    make(map[string]time.Time),
		stopChan: make(chan struct{}),
	}
	go es.cleanup()
	return es
}

func (es *ExpiringSet) Add(item string, ttl time.Duration) {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.items[item] = time.Now().Add(ttl)
}

func (es *ExpiringSet) Contains(item string) bool {
	es.mu.RLock()
	defer es.mu.RUnlock()
	expiry, exists := es.items[item]
	return exists && time.Now().Before(expiry)
}

func (es *ExpiringSet) cleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-es.stopChan:
			return
		case <-ticker.C:
			es.mu.Lock()
			now := time.Now()
			for item, expiry := range es.items {
				if now.After(expiry) {
					delete(es.items, item)
				}
			}
			es.mu.Unlock()
		}
	}
}

func (es *ExpiringSet) Stop() {
	close(es.stopChan)
}
