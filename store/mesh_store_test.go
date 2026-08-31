package store

import (
	"context"
	"testing"
	"time"

	"github.com/toll-mesh/store/core"
)

func TestMeshStore_Consume_RateLimiting(t *testing.T) {
	config := &core.ClusterConfig{
		NodeName: "node1",
		BindAddr: "127.0.0.1",
		BindPort: 8000,
	}
	store, err := NewMeshStore(config)
	if err != nil {
		t.Fatalf("Failed to create mesh store: %v", err)
	}
	defer store.Close()

	result, err := store.Consume(context.Background(), "test-key", 5, 1000*time.Millisecond)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	if !result.OK {
		t.Error("First consume should be allowed")
	}

	if result.Remaining != 4 {
		t.Errorf("Expected 4 remaining, got %d", result.Remaining)
	}

	for i := 0; i < 4; i++ {
		result, err = store.Consume(context.Background(), "test-key", 5, 1000*time.Millisecond)
		if err != nil {
			t.Fatalf("Consume %d failed: %v", i+2, err)
		}
		if !result.OK {
			t.Errorf("Consume %d should be allowed", i+2)
		}
	}

	result, err = store.Consume(context.Background(), "test-key", 5, 1000*time.Millisecond)
	if err != nil {
		t.Fatalf("Final consume failed: %v", err)
	}

	if result.OK {
		t.Error("Final consume should be rate limited")
	}

	if result.Remaining != 0 {
		t.Errorf("Expected 0 remaining, got %d", result.Remaining)
	}
}

func TestMeshStore_Seen_ReplayProtection(t *testing.T) {
	config := &core.ClusterConfig{
		NodeName: "node1",
		BindAddr: "127.0.0.1",
		BindPort: 8000,
	}
	store, err := NewMeshStore(config)
	if err != nil {
		t.Fatalf("Failed to create mesh store: %v", err)
	}
	defer store.Close()

	seen, err := store.Seen(context.Background(), "unique-nonce", 5000*time.Millisecond)
	if err != nil {
		t.Fatalf("Seen check failed: %v", err)
	}

	if seen {
		t.Error("First seen check should return false")
	}

	seen, err = store.Seen(context.Background(), "unique-nonce", 5000*time.Millisecond)
	if err != nil {
		t.Fatalf("Second seen check failed: %v", err)
	}

	if !seen {
		t.Error("Second seen check should return true (replay detected)")
	}
}

func TestMeshStore_GetSet_CacheOperations(t *testing.T) {
	config := &core.ClusterConfig{
		NodeName: "node1",
		BindAddr: "127.0.0.1",
		BindPort: 8000,
	}
	store, err := NewMeshStore(config)
	if err != nil {
		t.Fatalf("Failed to create mesh store: %v", err)
	}
	defer store.Close()

	testValue := []byte("test-value")
	err = store.Set(context.Background(), "test-ns", "test-key", testValue, 5000*time.Millisecond)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, exists, err := store.Get(context.Background(), "test-ns", "test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !exists {
		t.Error("Value should exist after being set")
	}

	if string(retrieved) != string(testValue) {
		t.Errorf("Expected %s, got %s", string(testValue), string(retrieved))
	}

	_, exists, err = store.Get(context.Background(), "test-ns", "non-existent")
	if err != nil {
		t.Fatalf("Get for non-existent key failed: %v", err)
	}

	if exists {
		t.Error("Non-existent key should not exist")
	}
}

func TestMeshStore_ConcurrentAccess(t *testing.T) {
	config := &core.ClusterConfig{
		NodeName: "node1",
		BindAddr: "127.0.0.1",
		BindPort: 8000,
	}
	store, err := NewMeshStore(config)
	if err != nil {
		t.Fatalf("Failed to create mesh store: %v", err)
	}
	defer store.Close()

	const concurrentRequests = 10
	const limit = 5

	results := make(chan core.ConsumeResult, concurrentRequests)
	errors := make(chan error, concurrentRequests)

	for i := 0; i < concurrentRequests; i++ {
		go func() {
			result, err := store.Consume(context.Background(), "concurrent-key", limit, 1000*time.Millisecond)
			if err != nil {
				errors <- err
				return
			}
			results <- result
		}()
	}

	var successfulConsumes int
	for i := 0; i < concurrentRequests; i++ {
		select {
		case result := <-results:
			if result.OK {
				successfulConsumes++
			}
		case err := <-errors:
			t.Fatalf("Concurrent consume failed: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for concurrent results")
		}
	}

	if successfulConsumes != limit {
		t.Errorf("Expected %d successful consumes, got %d", limit, successfulConsumes)
	}
}
