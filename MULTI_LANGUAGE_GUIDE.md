# TollMeshCache Multi-Language SDK Guide

Complete guide to using TollMeshCache SDKs across all supported languages.

## Supported Languages

- **Python** - `pip install tollmeshcache`
- **Node.js/TypeScript** - `npm install tollmeshcache`
- **Java** - Maven Central: `com.tollmesh:tollmeshcache`
- **Rust** - `cargo add tollmeshcache`
- **Ruby** - `gem install tollmeshcache`
- **C# / .NET** - NuGet: `Install-Package TollMeshCache`
- **PHP** - Composer: `composer require toll-mesh/cache`
- **Go** - Go Modules: `github.com/toll-mesh/store/go`

## Core Features (All Languages)

### 1. Rate Limiting
Distributed rate limiting with automatic convergence across cluster nodes.

```
consume(key: string, limit: int, window: Duration) -> {ok, remaining, reset_at}
```

**Example Use Cases:**
- API rate limiting per user/IP
- Query throttling for database connections
- Request queueing for backend services

### 2. Replay Protection
Prevent replay attacks by tracking seen nonces across the cluster.

```
seen(key: string, ttl: Duration) -> {seen}
```

**Example Use Cases:**
- Prevent duplicate transaction submissions
- Block repeated payment requests
- Detect replay attacks in authentication

### 3. Distributed Caching
Store and retrieve values with automatic expiration.

```
get(namespace: string, key: string) -> {value, exists}
set(namespace: string, key: string, value: bytes, ttl?: Duration) -> void
```

**Example Use Cases:**
- Cache user profiles
- Store session tokens
- Cache computed results
- Distributed session state

### 4. Health & Monitoring
Check node health and cluster status.

```
health() -> {status, node, peers, stats}
getPeers() -> [{id, address, port, latency_ms}]
```

## Language-Specific Guides

### Python SDK

```python
from tollmeshcache import Client, ClientConfig
from datetime import timedelta

# Initialize
config = ClientConfig(host="localhost", port=8080)
client = Client(config)

# Rate limiting
result = client.consume("user-123", limit=100, window=timedelta(minutes=1))
if result["ok"]:
    process_request()
else:
    handle_rate_limit(result["reset_at"])

# Replay protection
if client.seen("nonce-123", ttl=timedelta(minutes=5))["seen"]:
    raise ReplayAttackError()

# Caching
value, exists = client.cache_get("users", "user-123")
if not exists:
    value = fetch_user_data("user-123")
    client.cache_set("users", "user-123", value, ttl=timedelta(hours=1))

# Health
health = client.health()
print(f"Status: {health['status']}, Peers: {health['peers']}")

client.close()
```

**Installation:**
```bash
pip install tollmeshcache
```

**File Structure:**
```
sdks/python/
├── setup.py
├── tollmeshcache/
│   ├── __init__.py
│   ├── client.py
│   ├── errors.py
│   └── config.py
├── examples/
│   ├── rate_limiting.py
│   ├── replay_protection.py
│   └── caching.py
└── tests/
    └── test_client.py
```

### Node.js / TypeScript SDK

```typescript
import { Client, ClientConfig, TollMeshError } from 'tollmeshcache';

const config: ClientConfig = {
  host: 'localhost',
  port: 8080,
};

const client = new Client(config);

// Rate limiting
try {
  const result = await client.consume('user-123', 100, 60000);
  if (result.ok) {
    await processRequest();
  } else {
    handleRateLimit(result.reset_at);
  }
} catch (error) {
  if (error instanceof TollMeshError && error.isRateLimited()) {
    console.log('Rate limited!');
  }
}

// Replay protection
const seenResult = await client.seen('nonce-123', 300000);
if (seenResult.seen) {
  throw new Error('Replay attack detected!');
}

// Caching
const { value, exists } = await client.cacheGet('users', 'user-123');
if (!exists) {
  const userData = await fetchUserData('user-123');
  await client.cacheSet('users', 'user-123', JSON.stringify(userData), 3600000);
}

client.close();
```

**Installation:**
```bash
npm install tollmeshcache
# or with yarn
yarn add tollmeshcache
```

**TypeScript Support:** Full type definitions included (`dist/index.d.ts`)

### Java SDK

```java
import com.tollmesh.store.*;
import java.time.Duration;

ClientConfig config = new ClientConfig()
    .setHost("localhost")
    .setPort(8080);

try (Client client = new Client(config)) {
    // Rate limiting
    ConsumeResult result = client.consume("user-123", 100, Duration.ofMinutes(1));
    if (result.isOk()) {
        processRequest();
    } else {
        handleRateLimit(result.getResetAt());
    }

    // Replay protection
    SeenResult seenResult = client.seen("nonce-123", Duration.ofMinutes(5));
    if (seenResult.isSeen()) {
        throw new ReplayAttackException();
    }

    // Caching
    CacheValue cached = client.cacheGet("users", "user-123");
    if (!cached.isExists()) {
        String userData = fetchUserData("user-123");
        client.cacheSet("users", "user-123", userData, Duration.ofHours(1));
    }

    // Health
    HealthResponse health = client.health();
    System.out.println("Status: " + health.getStatus());
}
```

**Installation:**
```xml
<dependency>
    <groupId>com.tollmesh</groupId>
    <artifactId>tollmeshcache</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Rust SDK

```rust
use tollmeshcache::{Client, ClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::default()
        .with_host("localhost")
        .with_port(8080);

    let client = Client::new(config)?;

    // Rate limiting
    let result = client.consume(
        "user-123",
        100,
        Duration::from_secs(60)
    ).await?;

    if result.ok {
        process_request().await?;
    } else {
        handle_rate_limit(result.reset_at);
    }

    // Replay protection
    let seen_result = client.seen(
        "nonce-123",
        Duration::from_secs(300)
    ).await?;

    if seen_result.seen {
        return Err("Replay attack detected".into());
    }

    // Caching
    match client.cache_get("users", "user-123").await? {
        Some(value) => println!("Cached: {}", value),
        None => {
            let data = fetch_user_data("user-123").await?;
            client.cache_set(
                "users",
                "user-123",
                &data,
                Some(Duration::from_secs(3600))
            ).await?;
        }
    }

    Ok(())
}
```

**Installation:**
```toml
[dependencies]
tollmeshcache = "1.0"
tokio = { version = "1", features = ["full"] }
```

### Ruby SDK

```ruby
require 'tollmeshcache'

config = TollMeshCache::ClientConfig.new(
  host: 'localhost',
  port: 8080
)

client = TollMeshCache::Client.new(config)

begin
  # Rate limiting
  result = client.consume('user-123', limit: 100, window: 60)
  if result.ok?
    process_request()
  else
    handle_rate_limit(result.reset_at)
  end

  # Replay protection
  seen_result = client.seen('nonce-123', ttl: 300)
  raise 'Replay attack detected!' if seen_result.seen?

  # Caching
  value, exists = client.cache_get('users', 'user-123')
  unless exists
    data = fetch_user_data('user-123')
    client.cache_set('users', 'user-123', data, ttl: 3600)
  end

  # Health
  health = client.health
  puts "Status: #{health.status}, Peers: #{health.peers}"
ensure
  client.close
end
```

**Installation:**
```bash
gem install tollmeshcache
# or in Gemfile
gem 'tollmeshcache'
```

### C# / .NET SDK

```csharp
using TollMeshCache;
using System;
using System.Threading.Tasks;

var config = new ClientConfig
{
    Host = "localhost",
    Port = 8080,
    Timeout = TimeSpan.FromSeconds(5)
};

using (var client = new Client(config))
{
    // Rate limiting
    var result = await client.ConsumeAsync(
        "user-123",
        limit: 100,
        window: TimeSpan.FromMinutes(1)
    );

    if (result.Ok)
    {
        await ProcessRequest();
    }
    else
    {
        HandleRateLimit(result.ResetAt);
    }

    // Replay protection
    var seenResult = await client.SeenAsync(
        "nonce-123",
        ttl: TimeSpan.FromMinutes(5)
    );

    if (seenResult.Seen)
    {
        throw new InvalidOperationException("Replay attack detected!");
    }

    // Caching
    var cached = await client.CacheGetAsync("users", "user-123");
    if (!cached.Exists)
    {
        var userData = await FetchUserData("user-123");
        await client.CacheSetAsync(
            "users",
            "user-123",
            userData,
            ttl: TimeSpan.FromHours(1)
        );
    }
}
```

**Installation:**
```bash
dotnet add package TollMeshCache
```

### PHP SDK

```php
<?php

require 'vendor/autoload.php';

use TollMesh\Cache\Client;
use TollMesh\Cache\ClientConfig;

$config = new ClientConfig();
$config->setHost('localhost');
$config->setPort(8080);

$client = new Client($config);

try {
    // Rate limiting
    $result = $client->consume('user-123', 100, 60000);
    if ($result['ok']) {
        processRequest();
    } else {
        handleRateLimit($result['reset_at']);
    }

    // Replay protection
    $seen = $client->seen('nonce-123', 300000);
    if ($seen['seen']) {
        throw new Exception('Replay attack detected!');
    }

    // Caching
    [$value, $exists] = $client->cacheGet('users', 'user-123');
    if (!$exists) {
        $userData = fetchUserData('user-123');
        $client->cacheSet('users', 'user-123', $userData, 3600000);
    }

    // Health
    $health = $client->health();
    echo "Status: {$health['status']}, Peers: {$health['peers']}";
} finally {
    $client->close();
}
```

**Installation:**
```bash
composer require toll-mesh/cache
```

## Error Handling

All SDKs use consistent error codes for cross-language compatibility:

| Code | Constant | Meaning |
|------|----------|---------|
| 400 | INVALID_REQUEST | Bad request parameters |
| 404 | NOT_FOUND | Resource not found |
| 429 | RATE_LIMITED | Rate limit exceeded |
| 500 | INTERNAL | Internal server error |
| 503 | UNAVAILABLE | Service unavailable |
| 1001 | REPLAY_DETECTED | Replay attack detected |
| 1002 | CACHE_MISS | Cache miss |
| 1003-1012 | Various | TollMesh-specific errors |

**Error Handling Pattern:**

```python
# Python
try:
    result = client.consume("key", 100, timedelta(minutes=1))
except TollMeshError as e:
    if e.is_rate_limited():
        # Handle rate limit
        print(f"Reset at: {e.details['reset_at']}")
    elif e.is_retryable():
        # Retry the operation
        pass
    else:
        # Log and propagate
        raise
```

## Configuration

All SDKs support these configuration options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| host | string | "localhost" | Server hostname |
| port | int | 8080 | Server port |
| scheme | string | "http" | "http" or "https" |
| timeout | float | 5.0 | Request timeout in seconds |
| verify_ssl | bool | true | Verify SSL certificates |
| api_key | string | null | Optional API key for auth |

## Best Practices

1. **Reuse Clients**: Create client once, reuse across requests
2. **Handle Errors**: Always handle rate limit and replay errors
3. **Cache TTLs**: Set appropriate TTLs to prevent stale data
4. **Monitor Health**: Periodically check cluster health
5. **Connection Pooling**: All SDKs use HTTP connection pooling
6. **Metrics**: Track consume/seen/cache operations

## Examples

See `/examples` directory in each SDK:
- `rate_limiting.{py,ts,java,rs,rb,cs,php}`
- `replay_protection.{py,ts,java,rs,rb,cs,php}`
- `caching.{py,ts,java,rs,rb,cs,php}`
- `advanced.{py,ts,java,rs,rb,cs,php}` (clustering, monitoring)

## Contributing

All SDKs follow the same patterns and error handling. When contributing:

1. Keep API consistent across languages
2. Use idiomatic patterns for each language
3. Maintain 100% test coverage
4. Document with examples
5. Keep error codes synchronized

## Support

- **Documentation**: https://docs.tollmesh.io
- **Issues**: https://github.com/toll-mesh/store/issues
- **Discussions**: https://github.com/toll-mesh/store/discussions
