package metrics

import "testing"

func TestPercentilesAreComputedFromSortedLatencies(t *testing.T) {
	m := NewMetrics()

	// Record in an order where insertion order != magnitude order.
	// Values: 100, 1, 50, 2, 99, 3, 98, 4, 97, 5 -- if p99/p50 index into
	// this unsorted slice by position, they'll return whatever value
	// happens to sit at that array index, not the actual 99th/50th
	// percentile by value.
	values := []int64{100, 1, 50, 2, 99, 3, 98, 4, 97, 5}
	for _, v := range values {
		m.RecordConsume(true, v)
	}

	stats := m.GetStats()
	latency := stats["consume_latency"].(map[string]interface{})

	// Sorted: [1,2,3,4,5,50,97,98,99,100]. True p50 (index 5) = 50,
	// true max = 100.
	if latency["max"] != int64(100) {
		t.Errorf("expected max 100, got %v", latency["max"])
	}
	p50 := latency["p50"].(int64)
	if p50 != 50 {
		t.Errorf("expected p50 to be the true median (50) of sorted values, got %v (looks like it indexed into unsorted insertion order)", p50)
	}
}

func TestRecordConsumeCounters(t *testing.T) {
	m := NewMetrics()
	m.RecordConsume(true, 10)
	m.RecordConsume(false, 20)
	m.RecordConsume(true, 30)

	stats := m.GetStats()
	if stats["consume_total"] != int64(3) {
		t.Errorf("expected 3 total, got %v", stats["consume_total"])
	}
	if stats["consume_allowed"] != int64(2) {
		t.Errorf("expected 2 allowed, got %v", stats["consume_allowed"])
	}
	if stats["consume_denied"] != int64(1) {
		t.Errorf("expected 1 denied, got %v", stats["consume_denied"])
	}
}

func TestLatencySamplesAreBounded(t *testing.T) {
	m := NewMetrics()
	// Record far more samples than any reasonable in-memory cap.
	for i := 0; i < 200000; i++ {
		m.RecordConsume(true, int64(i))
	}

	m.mu.RLock()
	n := m.consumeLatencies.count
	m.mu.RUnlock()

	if n > 50000 {
		t.Errorf("latency samples grew unbounded: %d entries retained for 200000 recorded operations (memory leak for a long-running process)", n)
	}
}

func TestPrometheusMetricsFormat(t *testing.T) {
	m := NewMetrics()
	m.RecordConsume(true, 10)
	m.RecordGet(true, 5)

	output := m.PrometheusMetrics()
	if len(output) == 0 {
		t.Error("expected non-empty Prometheus output")
	}
}
