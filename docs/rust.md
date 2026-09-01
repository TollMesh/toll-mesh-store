---
layout: default
title: Rust SDK
nav_order: 6
---

# Rust SDK

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
tollmeshcache = "1.0"
tokio = { version = "1", features = ["full"] }
```

## Quick Start

```rust
use tollmeshcache::{Client, ClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let config = ClientConfig::new()
    .with_host("localhost")
    .with_port(8080);

  let client = Client::new(config)?;

  // Rate limiting
  let result = client.consume("user-123", 100, Duration::from_secs(60)).await?;
  if result.ok {
    println!("Request allowed");
  }

  Ok(())
}
```

## Features

- ✅ Full async/await with tokio
- ✅ Type-safe error handling
- ✅ Zero-cost abstractions
- ✅ Connection pooling with reqwest
- ✅ Exponential backoff retry logic

## API Reference

### Rate Limiting
```rust
let result = client.consume(key, limit, window).await?;
// Fields: ok, remaining, reset_at
```

### Replay Protection
```rust
let result = client.seen(key, ttl).await?;
// Field: seen
```

### Caching
```rust
client.cache_set(namespace, key, value, ttl).await?;
let (value, exists) = client.cache_get(namespace, key).await?;
```

### Health
```rust
let health = client.health().await?;
let peers = client.get_peers().await?;
```

## Error Handling

```rust
use tollmeshcache::{TollMeshError, ErrorCode};

match client.consume("key", 100, Duration::from_secs(60)).await {
  Ok(result) => println!("Success: {:?}", result),
  Err(error) => {
    match error {
      TollMeshError::RateLimited => println!("Rate limited"),
      TollMeshError::NotFound => println!("Not found"),
      _ => eprintln!("Error: {}", error),
    }
  }
}
```

## Configuration

```rust
let config = ClientConfig::new()
  .with_host("localhost")
  .with_port(8080)
  .with_timeout(Duration::from_secs(5))
  .with_api_key("optional-key")
  .with_verify_ssl(true)
  .with_max_retries(3)
  .with_connection_pool_size(10);

let client = Client::new(config)?;
```

## Retry Logic

```rust
use tollmeshcache::with_retry;

let result = with_retry(
  || async { client.consume("key", 100, Duration::from_secs(60)).await },
  Default::default()
).await?;
```

## Testing

```bash
cargo test
```

## Examples

See `examples/` for:
- `rate_limiting.rs` - Rate limiting patterns

Run examples:

```bash
cargo run --example rate_limiting
```
