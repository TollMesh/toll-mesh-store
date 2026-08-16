package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks operational statistics for MeshStore
type Metrics struct {
	mu sync.RWMutex

	// Counter metrics
	consumeTotal   int64
	consumeAllowed int64
	consumeDenied  int64
	seenTotal      int64
	seenReplays    int64
	getTotal       int64
	getHits        int64
	getMisses      int64
	setTotal       int64
	cacheEvictions int64

	// Latency metrics (in microseconds)
	consumeLatencies []int64
	seenLatencies    []int64
	getLatencies     []int64
	setLatencies     []int64

	// Gossip metrics
	gossipMessagesIn  int64
	gossipMessagesOut int64
	gossipErrors      int64

	// Timestamps
	startTime time.Time
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{
		startTime:        time.Now(),
		consumeLatencies: make([]int64, 0, 1000),
		seenLatencies:    make([]int64, 0, 1000),
		getLatencies:     make([]int64, 0, 1000),
		setLatencies:     make([]int64, 0, 1000),
	}
}

// RecordConsume records a consume operation
func (m *Metrics) RecordConsume(allowed bool, latencyMicros int64) {
	atomic.AddInt64(&m.consumeTotal, 1)
	if allowed {
		atomic.AddInt64(&m.consumeAllowed, 1)
	} else {
		atomic.AddInt64(&m.consumeDenied, 1)
	}

	m.mu.Lock()
	m.consumeLatencies = append(m.consumeLatencies, latencyMicros)
	m.mu.Unlock()
}

// RecordSeen records a seen operation
func (m *Metrics) RecordSeen(replay bool, latencyMicros int64) {
	atomic.AddInt64(&m.seenTotal, 1)
	if replay {
		atomic.AddInt64(&m.seenReplays, 1)
	}

	m.mu.Lock()
	m.seenLatencies = append(m.seenLatencies, latencyMicros)
	m.mu.Unlock()
}

// RecordGet records a get operation
func (m *Metrics) RecordGet(hit bool, latencyMicros int64) {
	atomic.AddInt64(&m.getTotal, 1)
	if hit {
		atomic.AddInt64(&m.getHits, 1)
	} else {
		atomic.AddInt64(&m.getMisses, 1)
	}

	m.mu.Lock()
	m.getLatencies = append(m.getLatencies, latencyMicros)
	m.mu.Unlock()
}

// RecordSet records a set operation
func (m *Metrics) RecordSet(latencyMicros int64) {
	atomic.AddInt64(&m.setTotal, 1)

	m.mu.Lock()
	m.setLatencies = append(m.setLatencies, latencyMicros)
	m.mu.Unlock()
}

// RecordCacheEviction records a cache eviction
func (m *Metrics) RecordCacheEviction() {
	atomic.AddInt64(&m.cacheEvictions, 1)
}

// RecordGossipMessageIn records an incoming gossip message
func (m *Metrics) RecordGossipMessageIn() {
	atomic.AddInt64(&m.gossipMessagesIn, 1)
}

// RecordGossipMessageOut records an outgoing gossip message
func (m *Metrics) RecordGossipMessageOut() {
	atomic.AddInt64(&m.gossipMessagesOut, 1)
}

// RecordGossipError records a gossip error
func (m *Metrics) RecordGossipError() {
	atomic.AddInt64(&m.gossipErrors, 1)
}

// GetStats returns current metrics as a map
func (m *Metrics) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	uptime := time.Since(m.startTime)

	stats := map[string]interface{}{
		"uptime_seconds": uptime.Seconds(),

		// Consume metrics
		"consume_total":   atomic.LoadInt64(&m.consumeTotal),
		"consume_allowed": atomic.LoadInt64(&m.consumeAllowed),
		"consume_denied":  atomic.LoadInt64(&m.consumeDenied),
		"consume_latency": m.calculateLatencyStats(m.consumeLatencies),

		// Seen metrics
		"seen_total":   atomic.LoadInt64(&m.seenTotal),
		"seen_replays": atomic.LoadInt64(&m.seenReplays),
		"seen_latency": m.calculateLatencyStats(m.seenLatencies),

		// Get metrics
		"get_total":   atomic.LoadInt64(&m.getTotal),
		"get_hits":    atomic.LoadInt64(&m.getHits),
		"get_misses":  atomic.LoadInt64(&m.getMisses),
		"get_latency": m.calculateLatencyStats(m.getLatencies),

		// Set metrics
		"set_total":   atomic.LoadInt64(&m.setTotal),
		"set_latency": m.calculateLatencyStats(m.setLatencies),

		// Cache metrics
		"cache_evictions": atomic.LoadInt64(&m.cacheEvictions),

		// Gossip metrics
		"gossip_messages_in":  atomic.LoadInt64(&m.gossipMessagesIn),
		"gossip_messages_out": atomic.LoadInt64(&m.gossipMessagesOut),
		"gossip_errors":       atomic.LoadInt64(&m.gossipErrors),
	}

	return stats
}

// calculateLatencyStats calculates latency statistics
func (m *Metrics) calculateLatencyStats(latencies []int64) map[string]interface{} {
	if len(latencies) == 0 {
		return map[string]interface{}{
			"count": 0,
			"min":   0,
			"max":   0,
			"avg":   0,
			"p50":   0,
			"p99":   0,
		}
	}

	// Calculate min, max, avg
	var min, max, sum int64 = latencies[0], latencies[0], 0
	for _, lat := range latencies {
		if lat < min {
			min = lat
		}
		if lat > max {
			max = lat
		}
		sum += lat
	}
	avg := sum / int64(len(latencies))

	// Calculate percentiles (simplified)
	p50 := latencies[len(latencies)/2]
	p99 := latencies[(len(latencies)*99)/100]

	return map[string]interface{}{
		"count": len(latencies),
		"min":   min,
		"max":   max,
		"avg":   avg,
		"p50":   p50,
		"p99":   p99,
	}
}

// PrometheusMetrics formats metrics for Prometheus export
func (m *Metrics) PrometheusMetrics() string {
	stats := m.GetStats()

	output := "# HELP toll_mesh_store_operations Total operations\n"
	output += "# TYPE toll_mesh_store_operations counter\n"
	output += fmt.Sprintf("toll_mesh_store_consume_total %d\n", stats["consume_total"])
	output += fmt.Sprintf("toll_mesh_store_consume_allowed %d\n", stats["consume_allowed"])
	output += fmt.Sprintf("toll_mesh_store_consume_denied %d\n", stats["consume_denied"])
	output += fmt.Sprintf("toll_mesh_store_seen_total %d\n", stats["seen_total"])
	output += fmt.Sprintf("toll_mesh_store_seen_replays %d\n", stats["seen_replays"])
	output += fmt.Sprintf("toll_mesh_store_get_total %d\n", stats["get_total"])
	output += fmt.Sprintf("toll_mesh_store_get_hits %d\n", stats["get_hits"])
	output += fmt.Sprintf("toll_mesh_store_get_misses %d\n", stats["get_misses"])
	output += fmt.Sprintf("toll_mesh_store_set_total %d\n", stats["set_total"])
	output += fmt.Sprintf("toll_mesh_store_cache_evictions %d\n", stats["cache_evictions"])

	output += "\n# HELP toll_mesh_store_latency_microseconds Operation latency\n"
	output += "# TYPE toll_mesh_store_latency_microseconds gauge\n"
	consumeLatency := stats["consume_latency"].(map[string]interface{})
	output += fmt.Sprintf("toll_mesh_store_consume_latency_avg %d\n", consumeLatency["avg"])
	output += fmt.Sprintf("toll_mesh_store_consume_latency_p99 %d\n", consumeLatency["p99"])

	output += "\n# HELP toll_mesh_store_gossip Gossip protocol metrics\n"
	output += "# TYPE toll_mesh_store_gossip counter\n"
	output += fmt.Sprintf("toll_mesh_store_gossip_messages_in %d\n", stats["gossip_messages_in"])
	output += fmt.Sprintf("toll_mesh_store_gossip_messages_out %d\n", stats["gossip_messages_out"])
	output += fmt.Sprintf("toll_mesh_store_gossip_errors %d\n", stats["gossip_errors"])

	output += "\n# HELP toll_mesh_store_uptime_seconds Uptime in seconds\n"
	output += "# TYPE toll_mesh_store_uptime_seconds gauge\n"
	output += fmt.Sprintf("toll_mesh_store_uptime_seconds %v\n", stats["uptime_seconds"])

	return output
}
