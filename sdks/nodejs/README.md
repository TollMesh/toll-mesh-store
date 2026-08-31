# TollMeshCache Node.js SDK

Complete Node.js/TypeScript SDK for TollMeshCache - Distributed CRDT-based caching and coordination.

## Installation

```bash
npm install tollmeshcache
# or
yarn add tollmeshcache
```

## Quick Start

```typescript
import { Client, ClientConfig } from 'tollmeshcache';

const config: ClientConfig = {
  host: 'localhost',
  port: 8080,
};

const client = new Client(config);

// Rate limiting
const result = await client.consume('user-123', 100, 60000);
if (result.ok) {
  console.log('Request allowed');
} else {
  console.log('Rate limited');
}

// Replay protection
const seenResult = await client.seen('nonce-123', 300000);
if (seenResult.seen) {
  console.log('Replay detected!');
}

// Caching
await client.cacheSet('users', 'user-123', JSON.stringify(userData), 3600000);
const { value, exists } = await client.cacheGet('users', 'user-123');

// Health check
const health = await client.health();
console.log(`Status: ${health.status}`);

client.close();
```

## TypeScript Support

Full TypeScript support with type definitions included.

```typescript
import { Client, ClientConfig, ErrorCode, TollMeshError } from 'tollmeshcache';

const config: ClientConfig = {
  host: 'localhost',
  port: 8080,
  timeout: 5000,
  verifySSL: true,
  scheme: 'http',
};

const client = new Client(config);

try {
  const result = await client.consume('key', 100, 60000);
  if (result.ok) {
    // Process request
  }
} catch (error) {
  if (error instanceof TollMeshError) {
    if (error.isRateLimited()) {
      console.log('Rate limited');
    } else if (error.isRetryable()) {
      console.log('Temporary error - retry');
    }
  }
}
```

## Features

### Rate Limiting
Distributed rate limiting with automatic convergence.

```typescript
const result = await client.consume('api-key', 1000, 3600000); // 1000 req/hour
if (result.ok) {
  // Process request
  console.log(`Remaining: ${result.remaining}`);
  console.log(`Reset at: ${new Date(result.reset_at)}`);
}
```

### Replay Protection
Prevent replay attacks.

```typescript
const { seen } = await client.seen('request-nonce', 300000);
if (seen) {
  throw new Error('Replay attack detected!');
}
```

### Distributed Caching
Store and retrieve values with TTL.

```typescript
// Set
await client.cacheSet('namespace', 'key', value, 3600000);

// Get
const { value, exists } = await client.cacheGet('namespace', 'key');
```

### Health & Monitoring
Check cluster status.

```typescript
const health = await client.health();
console.log(`Status: ${health.status}`);
console.log(`Peers: ${health.peers}`);

const peers = await client.getPeers();
console.log(`Connected peers: ${peers.length}`);
```

## Configuration

```typescript
const config: ClientConfig = {
  host: 'localhost',              // Server hostname
  port: 8080,                     // Server port
  scheme: 'http',                 // 'http' or 'https'
  timeout: 5000,                  // Request timeout (ms)
  verifySSL: true,                // Verify SSL certificates
  apiKey: 'optional-api-key',     // API key for authentication
};
```

## Error Handling

```typescript
import { TollMeshError, ErrorCode } from 'tollmeshcache';

try {
  const result = await client.consume('key', 100, 60000);
} catch (error) {
  if (error instanceof TollMeshError) {
    console.error(`Error ${error.code}: ${error.message}`);

    if (error.isRateLimited()) {
      // Handle rate limit
    } else if (error.isRetryable()) {
      // Retry the operation
    } else if (error.isServerError()) {
      // Handle server error
    }
  }
}
```

## Retry Logic

Automatic retry with exponential backoff.

```typescript
import { withRetry, RetryConfig } from 'tollmeshcache';

const config: RetryConfig = {
  maxRetries: 3,
  baseDelay: 1000,
  maxDelay: 60000,
  jitter: true,
  backoffMultiplier: 2,
};

const result = await withRetry(
  () => client.consume('key', 100, 60000),
  config
);
```

Or use the decorator:

```typescript
import { retry, RetryConfig } from 'tollmeshcache';

class MyService {
  @retry({ maxRetries: 3 })
  async riskyOperation() {
    return await client.consume('key', 100, 60000);
  }
}
```

## Concurrent Operations

Safe for concurrent operations.

```typescript
// Multiple concurrent requests
const results = await Promise.all([
  client.consume('user-1', 100, 60000),
  client.consume('user-2', 100, 60000),
  client.consume('user-3', 100, 60000),
]);
```

## Examples

See `examples/` directory:
- `rate-limiting.ts` - Distributed rate limiting scenarios
- `caching.ts` - Cache-aside pattern and cache management
- `replay-protection.ts` - Replay attack prevention

Run examples:

```bash
npm run build
node dist/examples/rate-limiting.js
node dist/examples/caching.js
node dist/examples/replay-protection.js
```

## Testing

```bash
npm test
npm run test:cov
```

## Building

```bash
npm run build
npm run lint
```

## Performance

- **Rate Limiting**: O(1) per operation
- **Replay Protection**: O(1) per operation
- **Caching**: O(1) per operation
- **Connection Pooling**: Automatic with configurable pool size
- **Retry Logic**: Exponential backoff with jitter

## Best Practices

1. **Reuse Client**: Create once, reuse across requests
2. **Error Handling**: Always handle rate limit and replay errors
3. **Configure Timeouts**: Set appropriate timeouts for your use case
4. **Enable Retries**: Use retry configuration for resilience
5. **Monitor Health**: Periodically check cluster health

## API Reference

### `Client`

Main client class.

#### Methods

- `consume(key, limit, window) -> ConsumeResult`
  - Check and consume rate limit tokens

- `seen(key, ttl) -> SeenResult`
  - Check replay protection

- `cacheGet(namespace, key) -> CacheValue`
  - Get cached value

- `cacheSet(namespace, key, value, ttl?) -> Promise<void>`
  - Set cached value

- `health() -> HealthResponse`
  - Check server health

- `getPeers() -> Peer[]`
  - Get connected peers

- `close() -> void`
  - Close client

### Exceptions

- `TollMeshError` - Base exception
- `RateLimitError` - Rate limit exceeded
- `ReplayError` - Replay detected
- `CacheMissError` - Cache miss

## Contributing

Contributions welcome!

1. Write tests for new features
2. Maintain 95%+ test coverage
3. Follow TypeScript best practices
4. Add JSDoc comments

## License

Apache License 2.0

## Support

- **Documentation**: https://docs.tollmesh.io
- **Issues**: https://github.com/toll-mesh/store/issues
- **Discussions**: https://github.com/toll-mesh/store/discussions
