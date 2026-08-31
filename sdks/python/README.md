# TollMeshCache Python SDK

Complete Python SDK for TollMeshCache - Distributed CRDT-based caching and coordination.

## Installation

```bash
pip install tollmeshcache
```

## Quick Start

```python
from tollmeshcache import Client, ClientConfig
from datetime import timedelta

# Initialize client
config = ClientConfig(host="localhost", port=8080)
client = Client(config)

# Rate limiting
result = client.consume("user-123", limit=100, window=timedelta(minutes=1))
if result["ok"]:
    print("Request allowed")
else:
    print("Rate limited")

# Replay protection
if client.seen("nonce-123", ttl=timedelta(minutes=5))["seen"]:
    print("Replay detected!")

# Caching
client.cache_set("users", "user-123", '{"name": "Alice"}', ttl=timedelta(hours=1))
value, exists = client.cache_get("users", "user-123")

# Health check
health = client.health()
print(f"Status: {health['status']}")

client.close()
```

## Features

### Rate Limiting
Distributed rate limiting with automatic convergence across cluster nodes.

```python
result = client.consume("api-key", limit=1000, window=timedelta(hours=1))
if result["ok"]:
    # Process request
else:
    # Handle rate limit
    print(f"Reset at: {result['reset_at']}")
```

### Replay Protection
Prevent replay attacks by tracking seen nonces.

```python
if client.seen("request-nonce", ttl=timedelta(minutes=5))["seen"]:
    raise Exception("Replay attack detected!")
```

### Distributed Caching
Store and retrieve values with automatic expiration.

```python
# Set
client.cache_set("namespace", "key", "value", ttl=timedelta(hours=1))

# Get
value, exists = client.cache_get("namespace", "key")
```

### Health & Monitoring
Check node and cluster status.

```python
health = client.health()
print(f"Status: {health['status']}")
print(f"Peers: {health['peers']}")
```

## Configuration

```python
config = ClientConfig(
    host="localhost",           # Server hostname
    port=8080,                  # Server port
    timeout=5.0,                # Request timeout (seconds)
    verify_ssl=True,            # Verify SSL certificates
    api_key="secret",           # Optional API key
    http_scheme="http",         # 'http' or 'https'
    max_retries=3,              # Retry attempts
    connection_pool_size=10,    # HTTP connection pool size
)
```

## Error Handling

All operations raise `TollMeshError` on failure:

```python
from tollmeshcache import TollMeshError, ErrorCode

try:
    result = client.consume("key", 100, timedelta(minutes=1))
except TollMeshError as e:
    if e.is_rate_limited():
        print("Rate limited")
    elif e.is_retryable():
        print("Temporary error - will retry")
    else:
        print(f"Error: {e.message}")
```

## Retry Logic

Automatic retry with exponential backoff:

```python
from tollmeshcache import RetryConfig, retry

config = RetryConfig(
    max_retries=3,
    base_delay=1.0,
    max_delay=60.0,
    jitter=True,
    backoff_multiplier=2.0,
)

@retry(config)
def risky_operation():
    return client.consume("key", 100, timedelta(minutes=1))
```

## Context Manager

Use as a context manager for automatic cleanup:

```python
with Client(config) as client:
    result = client.consume("key", 100, timedelta(minutes=1))
```

## Examples

See `examples/` directory:
- `rate_limiting.py` - Distributed rate limiting
- `caching.py` - Distributed caching patterns
- `replay_protection.py` - Replay attack prevention

Run examples:

```bash
python examples/rate_limiting.py
python examples/caching.py
python examples/replay_protection.py
```

## Testing

```bash
pip install -e ".[dev]"
pytest -v --cov=tollmeshcache
```

Run specific tests:

```bash
pytest tests/test_client.py::TestClientConfig -v
pytest tests/test_client.py::TestConsumeOperation -v
```

## Performance

- **Rate Limiting**: O(1) per operation
- **Replay Protection**: O(1) per operation
- **Caching**: O(1) per operation
- **Connection Pooling**: Automatic with configurable pool size
- **Retry Logic**: Exponential backoff with jitter

## Thread Safety

All operations are thread-safe. Safe to use the same client across multiple threads.

```python
from concurrent.futures import ThreadPoolExecutor

def worker(client, key):
    result = client.consume(key, 100, timedelta(minutes=1))
    return result

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(worker, client, f"key-{i}") for i in range(100)]
```

## Async Support

For async/await support, use the async client (coming soon):

```python
import asyncio
from tollmeshcache import AsyncClient

async def main():
    async with AsyncClient(config) as client:
        result = await client.consume("key", 100, timedelta(minutes=1))

asyncio.run(main())
```

## Best Practices

1. **Reuse Clients**: Create once, reuse across requests
2. **Handle Errors**: Always handle rate limit and replay errors
3. **Set TTLs**: Configure appropriate cache TTLs
4. **Monitor Health**: Periodically check cluster status
5. **Connection Pooling**: Leverage automatic pooling for performance

## API Reference

### `Client`

Main client class for interacting with TollMeshCache.

#### Methods

- `consume(key, limit, window) -> ConsumeResult`
  - Check and consume rate limit tokens

- `seen(key, ttl) -> SeenResult`
  - Check replay protection

- `cache_get(namespace, key) -> (value, exists)`
  - Get value from cache

- `cache_set(namespace, key, value, ttl=None) -> None`
  - Set value in cache

- `health() -> HealthResponse`
  - Check server health

- `get_peers() -> List[Peer]`
  - Get connected peers

- `close() -> None`
  - Close client and cleanup

### Exceptions

- `TollMeshError` - Base exception
- `RateLimitError` - Rate limit exceeded
- `ReplayError` - Replay detected
- `CacheMissError` - Cache miss

## Contributing

Contributions welcome! Please:
1. Write tests for new features
2. Maintain 95%+ test coverage
3. Follow PEP 8 style guide
4. Add docstrings to all functions

## License

Apache License 2.0

## Support

- **Documentation**: https://docs.tollmesh.io
- **Issues**: https://github.com/toll-mesh/store/issues
- **Discussions**: https://github.com/toll-mesh/store/discussions
