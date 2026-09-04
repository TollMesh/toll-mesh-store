---
layout: default
title: Go Backend
nav_order: 2
---

# Go Backend

## Overview

TollMeshCache core is built in Go with CRDTs (Conflict-Free Replicated Data Types) for distributed coordination.

## Core Components

### Conflict-Free Replicated Data Types (CRDTs)

#### GCounter (Grow-only Counter)
Used for distributed rate limiting. Each node maintains its own count; the total is the sum of all node counts.

```go
import "github.com/toll-mesh/store/core"

counter := core.NewGCounter("node-1")
counter.Increment(5)
total := counter.Value()
```

#### GSet (Grow-only Set)
Used for replay protection. Distributed set of seen nonces that automatically converges across nodes.

```go
set := core.NewGSet()
set.Add("nonce-123")
exists := set.Contains("nonce-123")
```

#### ExpiringSet (TTL-based Set)
TTL-based set with automatic cleanup for caching.

```go
expiring := core.NewExpiringSet(time.Minute)
expiring.Add("key", time.Now().Add(10*time.Minute))
exists := expiring.Contains("key")
```

### MeshStore

Core distributed store implementation:

```go
package main

import (
	"context"
	"time"
	"github.com/toll-mesh/store/store"
	"github.com/toll-mesh/store/core"
)

func main() {
	config := &core.ClusterConfig{
		NodeName: "node-1",
		BindAddr: "127.0.0.1",
		BindPort: 8000,
	}
	
	meshStore, _ := store.NewMeshStore(config)
	defer meshStore.Close()
	
	// Rate limiting
	result, _ := meshStore.Consume(
		context.Background(),
		"api-key",
		100,
		time.Minute,
	)
	if result.OK {
		fmt.Println("Request allowed, remaining:", result.Remaining)
	}
	
	// Replay protection
	seen, _ := meshStore.Seen(
		context.Background(),
		"nonce-123",
		5*time.Minute,
	)
	if seen {
		fmt.Println("Replay detected!")
	}
	
	// Caching
	meshStore.Set(
		context.Background(),
		"cache",
		"key-1",
		[]byte("value"),
		10*time.Minute,
	)
	value, exists, _ := meshStore.Get(context.Background(), "cache", "key-1")
	if exists {
		fmt.Println("Cached value:", string(value))
	}
}
```

## API Operations

### Consume (Rate Limiting)
```go
result, err := meshStore.Consume(ctx, key, limit, window)
// ConsumeResult: {OK bool, Remaining int64, ResetAt int64}
```

### Seen (Replay Protection)
```go
seen, err := meshStore.Seen(ctx, key, ttl)
// Returns: true if already seen (replay), false if new
```

### Get (Cache Retrieval)
```go
value, exists, err := meshStore.Get(ctx, namespace, key)
// Returns: value bytes, exists flag, error
```

### Set (Cache Storage)
```go
err := meshStore.Set(ctx, namespace, key, value, ttl)
// Stores value with automatic TTL expiration
```

The Go backend also implements Job Queues, Sorted Sets, Streams, Pub/Sub, Transactions, Persistence (WAL + snapshot), Pipelines, WASM Scripting (TinyGo-compiled Go, executed sandboxed via wazero), Search, Ranking, and Metrics, each exposed over HTTP and wired into every SDK — see the [API Reference](api-reference.md) for the HTTP-level contract of each.

## Testing

```bash
# Run all tests
go test ./... -v

# Run specific package
go test ./core -v
go test ./store -v

# With coverage
go test ./... -cover
```

## Performance

- **Rate Limiting**: O(1) per operation
- **Replay Protection**: O(1) per operation
- **Caching**: O(1) per operation
- **Memory**: O(n) where n = unique keys
- **Automatic Cleanup**: Background goroutine every 1 minute

## Configuration

```go
config := &core.ClusterConfig{
	NodeName:    "node-1",
	BindAddr:    "127.0.0.1",
	BindPort:    8000,
	ClusterName: "default",
}
```

## Architecture

The Go backend implements:
- CRDT-based coordination for automatic state convergence
- Gossip protocol for peer-to-peer synchronization (planned)
- Thread-safe operations with RWMutex
- Graceful shutdown and cleanup
- Health monitoring and status reporting

## License

Apache License 2.0
