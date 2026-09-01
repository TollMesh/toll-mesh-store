---
layout: default
title: Node.js SDK
nav_order: 4
---

# Node.js/TypeScript SDK

## Installation

```bash
npm install tollmeshcache
# or
yarn add tollmeshcache
```

## Quick Start

```typescript
import { Client, ClientConfig } from 'tollmeshcache';

const config = new ClientConfig({
  host: 'localhost',
  port: 8080,
});

const client = new Client(config);

// Rate limiting
const result = await client.consume('user-123', 100, 60000);
if (result.ok) {
  console.log('Request allowed');
}

// Async/await
const value = await client.cacheGet('namespace', 'key');
```

## Features

- ✅ Full TypeScript support
- ✅ Async/await patterns
- ✅ Streaming for large values
- ✅ Type-safe error handling
- ✅ Connection pooling

## API Reference

### Rate Limiting
```typescript
const result = await client.consume(key, limit, windowMs);
// Returns: { ok: boolean, remaining: number, reset_at: number }
```

### Replay Protection
```typescript
const result = await client.seen(key, ttlMs);
// Returns: { seen: boolean }
```

### Caching
```typescript
await client.cacheSet(namespace, key, value, ttlMs);
const [value, exists] = await client.cacheGet(namespace, key);
```

### Health
```typescript
const health = await client.health();
const peers = await client.getPeers();
```

## Examples

See `examples/` for:
- `rate-limiting.ts` - Rate limiting scenarios
- `caching.ts` - Cache-aside patterns
- `replay-protection.ts` - Replay detection

## Error Handling

```typescript
import { TollMeshError, RateLimitError, isRetryable } from 'tollmeshcache';

try {
  const result = await client.consume('key', 100, 60000);
} catch (error) {
  if (error instanceof RateLimitError) {
    console.log('Rate limited');
  } else if (error instanceof TollMeshError) {
    console.log(`Error ${error.code}: ${error.message}`);
  }
}
```

## Configuration

```typescript
const config = new ClientConfig({
  host: 'localhost',
  port: 8080,
  timeout: 5000,
  verifySsl: true,
  apiKey: 'optional-key',
  httpScheme: 'http',
  maxRetries: 3,
  retryBackoff: 1.0,
  connectionPoolSize: 10,
});
```

## Streaming

For large cache values:

```typescript
import { streamToBuffer, bufferToStream } from 'tollmeshcache';

// Read streaming response
const buffer = await streamToBuffer(response);

// Write streaming request
const stream = bufferToStream(largeBuffer);
```

## Testing

```bash
npm install --save-dev jest ts-jest @types/jest
npm test
```
