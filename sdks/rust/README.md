# TollMeshCache - Rust SDK

Distributed CRDT-based caching and coordination for Rust applications.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
tollmeshcache = "1.0"
```

## Quick Start

```rust
use std::collections::HashMap;
use tollmeshcache::{Client, ClientConfig};

#[tokio::main]
async fn main() {
    let config = ClientConfig::new().host("localhost").port(8080);
    let client = Client::new(config).unwrap();

    // Job Queues - distributed task processing
    let job = client.enqueue("tasks", "my-job", 5, 3, None).await.unwrap();
    let claimed = client.claim("tasks", "worker-1").await.unwrap();
    client.complete("tasks", &claimed.id, "done").await.unwrap();

    // Sorted Sets - O(log n) leaderboards
    client.zadd("scores", "player-1", 100.0).await.unwrap();
    client.zadd("scores", "player-2", 150.0).await.unwrap();
    let top_scores = client.zrevrange("scores", f64::INFINITY, f64::NEG_INFINITY, 10).await.unwrap(); // highest first

    // Streams - append-only event logs
    let mut fields = HashMap::new();
    fields.insert("event".to_string(), "login".to_string());
    let entry = client.xadd("events", fields).await.unwrap();
    client.xgroup_create("events", "analytics").await.unwrap();
    for e in client.xreadgroup("analytics", "worker-1", "events", 100).await.unwrap() {
        client.xack("events", "analytics", "worker-1", &e.id).await.unwrap();
    }
}
```

## Features

- **Job Queues**: Distributed task processing with exactly-once semantics
- **Sorted Sets**: O(log n) leaderboards and rankings
- **Streams**: Append-only event logs with consumer groups
- **CRDT-based**: Eventual consistency without central coordinator
- **Async/Await**: Full async support with Tokio
- **Type-safe**: Strong type system for correctness

## Documentation

See https://github.com/TollMesh/toll-mesh-store for complete documentation.

## License

Apache License 2.0
