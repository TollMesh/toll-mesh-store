---
layout: default
title: Python SDK
nav_order: 3
---

# Python SDK

## Installation

```bash
pip install tollmeshcache
pip install tollmeshcache[async]  # For async support
```

## Quick Start

```python
from tollmeshcache import Client, AsyncClient, ClientConfig
from datetime import timedelta

# Sync client
config = ClientConfig(host='localhost', port=8080)
client = Client(config)

result = client.consume('user-123', 100, timedelta(minutes=1))
if result['ok']:
    print('Request allowed')

client.close()

# Async client
async with AsyncClient(config) as client:
    result = await client.consume('user-123', 100, timedelta(minutes=1))
```

## Features

- ✅ Sync & async clients
- ✅ Type hints
- ✅ Retry logic with exponential backoff
- ✅ Connection pooling
- ✅ Comprehensive error handling

## API Reference

### Rate Limiting
```python
result = client.consume(key, limit, window)
# Returns: {'ok': bool, 'remaining': int, 'reset_at': int}
```

### Replay Protection
```python
result = client.seen(key, ttl)
# Returns: {'seen': bool}
```

### Caching
```python
client.cache_set(namespace, key, value, ttl)
value, exists = client.cache_get(namespace, key)
```

### Health
```python
health = client.health()
peers = client.get_peers()
```

## Examples

See `examples/` for:
- `rate_limiting.py` - Rate limiting scenarios
- `caching.py` - Cache-aside patterns
- `replay_protection.py` - Replay detection
- `async_example.py` - Async operations

## Error Handling

```python
from tollmeshcache import TollMeshError, RateLimitError, ReplayError

try:
    result = client.consume('key', 100, timedelta(minutes=1))
except RateLimitError as e:
    print(f"Rate limited: {e.message}")
except TollMeshError as e:
    print(f"Error {e.code}: {e.message}")
```

## Configuration

```python
config = ClientConfig(
    host='localhost',
    port=8080,
    timeout=5.0,
    verify_ssl=True,
    api_key='optional-key',
    http_scheme='http',
    max_retries=3,
    retry_backoff=1.0,
    connection_pool_size=10,
)
```

## Testing

```bash
pip install -e ".[dev]"
pytest -v --cov=tollmeshcache
```
