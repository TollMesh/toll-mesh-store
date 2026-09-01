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
use tollmeshcache::Client;

#[tokio::main]
async fn main() {
    let client = Client::new("localhost:8080").await.unwrap();

    // Job Queues
    client.enqueue("tasks", "my-job", 5).await.unwrap();
    let job = client.claim("tasks").await.unwrap();
    client.complete("tasks", &job.id).await.unwrap();

    // Sorted Sets
    client.zadd("scores", 100.0, "player-1").await.unwrap();
    client.zadd("scores", 150.0, "player-2").await.unwrap();
    let scores = client.zrange("scores", 0, -1).await.unwrap();

    // Streams
    client.xadd("events", json!({"event": "login", "user": "alice"})).await.unwrap();
    let events = client.xrange("events", "-", "+").await.unwrap();
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

See https://github.com/toll-mesh/store for complete documentation.

## License

Apache License 2.0
