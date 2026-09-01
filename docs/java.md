---
layout: default
title: Java SDK
nav_order: 5
---

# Java SDK

## Installation

Add to `pom.xml`:

```xml
<dependency>
  <groupId>com.tollmesh</groupId>
  <artifactId>tollmeshcache</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Quick Start

```java
import com.tollmesh.store.*;
import java.time.Duration;

ClientConfig config = new ClientConfig()
  .setHost("localhost")
  .setPort(8080);

Client client = new Client(config);

// Rate limiting
ConsumeResult result = client.consume("user-123", 100, Duration.ofMinutes(1));
if (result.ok) {
  System.out.println("Request allowed");
}

client.close();
```

## Features

- ✅ Sync & async clients
- ✅ CompletableFuture async support
- ✅ OkHttp3 connection pooling
- ✅ Strong type safety
- ✅ Builder pattern configuration

## Async Usage

```java
AsyncClient asyncClient = new AsyncClient(config);

asyncClient.consume("user-123", 100, Duration.ofMinutes(1))
  .thenAccept(result -> {
    if (result.ok) {
      System.out.println("Request allowed");
    }
  })
  .exceptionally(error -> {
    System.err.println("Error: " + error.getMessage());
    return null;
  });
```

## API Reference

### Rate Limiting
```java
ConsumeResult result = client.consume(key, limit, window);
// Fields: ok, remaining, reset_at
```

### Replay Protection
```java
SeenResult result = client.seen(key, ttl);
// Field: seen
```

### Caching
```java
client.cacheSet(namespace, key, value, ttl);
CacheValue cached = client.cacheGet(namespace, key);
// Fields: value, exists
```

### Health
```java
HealthResponse health = client.health();
// Methods: isHealthy(), isDegraded(), getUptimeSeconds()
```

## Error Handling

```java
import com.tollmesh.store.TollMeshException;

try {
  result = client.consume("key", 100, Duration.ofMinutes(1));
} catch (TollMeshException e) {
  System.err.println("Error " + e.getErrorCode() + ": " + e.getMessage());
  if (e.isRateLimited()) {
    // Handle rate limit
  }
}
```

## Configuration

```java
ClientConfig config = new ClientConfig()
  .setHost("localhost")
  .setPort(8080)
  .setTimeout(5000)
  .setVerifySsl(true)
  .setApiKey("optional-key")
  .setHttpScheme("http")
  .setMaxRetries(3)
  .setRetryBackoff(1.0)
  .setConnectionPoolSize(10);
```

## Testing

```bash
mvn test
```

## Examples

See `examples/` for:
- `RateLimitingExample.java` - Rate limiting patterns
