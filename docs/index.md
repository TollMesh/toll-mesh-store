---
layout: default
title: Home
nav_order: 1
description: "Production-ready distributed caching with 7-language SDK support"
---

# TollMeshCache Documentation

**Production-ready distributed caching and coordination** with built-in rate limiting, replay protection, and CRDT-based convergence. **7 language SDKs** for every platform.

---

## 🚀 Get Started in 5 Minutes

### 1. Choose Your Language

| Go | Python | Node.js | Java | Rust | Ruby | C# | PHP |
|----|--------|--------|------|------|------|----|----|
| [Backend](go/) | [Async/Sync](python/) | [TypeScript](nodejs/) | [CompletableFuture](java/) | [Tokio](rust/) | [Idiomatic](ruby/) | [Async/Await](csharp/) | [Composer](php/) |

### 2. Install

```bash
pip install tollmeshcache          # Python
npm install tollmeshcache          # Node.js
cargo add tollmeshcache             # Rust
dotnet add package TollMeshCache    # C#
gem install tollmeshcache           # Ruby
composer require toll-mesh/cache    # PHP
mvn dependency:get ...              # Java
```

### 3. Use

```python
# Python example
from tollmeshcache import Client
from datetime import timedelta

client = Client()
result = client.consume('user-123', limit=100, window=timedelta(minutes=1))
if result['ok']:
    print('Request allowed')
```

---

## 📚 Documentation Map

### Quick References
- **[API Reference](api-reference.md)** - All endpoints, error codes, configuration
- **[vs Redis](vs-redis.md)** - Feature comparison & when to use what

### Language Guides
- **[Go Backend](go/)** - CRDT implementations, MeshStore
- **[Python SDK](python/)** - Async/sync, type hints, retry logic
- **[Node.js SDK](nodejs/)** - TypeScript, streaming, promises
- **[Java SDK](java/)** - OkHttp3, CompletableFuture async
- **[Rust SDK](rust/)** - Tokio async, zero-cost abstractions
- **[Ruby SDK](ruby/)** - HTTPClient, idiomatic patterns
- **[C# SDK](csharp/)** - Native async/await, multi-target
- **[PHP SDK](php/)** - Guzzle, PSR-compliant

### Use Cases
1. **[Rate Limiting](api-reference.md#rate-limiting-strategy)** - Distributed token bucket
2. **[Replay Protection](api-reference.md#replay-protection-strategy)** - Nonce tracking
3. **[Distributed Caching](api-reference.md#caching-strategy)** - Cache-aside pattern
4. **[Health Monitoring](api-reference.md#5-health---health-check)** - Cluster status

---

## 🎯 Core Features

### ✅ Distributed Rate Limiting
- CRDT-based token bucket across cluster nodes
- No single point of failure
- Automatic convergence
- Perfect for API throttling

### ✅ Replay Protection
- Built-in nonce/request ID tracking
- Automatic cleanup with TTL
- Prevents duplicate payments, double-clicks, etc.

### ✅ Distributed Caching
- TTL-based cache with automatic expiration
- Cache-aside pattern support
- No central cache server needed

### ✅ Multi-Language Support
- **7 language SDKs** with identical APIs
- Async/await in all languages
- Type-safe implementations
- Production-ready code

### ✅ Zero Configuration
- Automatic peer discovery
- Self-healing cluster
- No coordinator needed
- Drop-in replacement for Redis rate limiting

---

## 📊 Comparison

### vs Redis
TollMeshCache is **complementary to Redis**, not a replacement:

| Feature | TollMeshCache | Redis |
|---------|---------------|-------|
| Rate Limiting | ✅ **Built-in** | ⚠️ Custom logic |
| Replay Protection | ✅ **Built-in** | ⚠️ Custom logic |
| No central server | ✅ **Yes** | ❌ Single master |
| Auto-recovery | ✅ **Self-healing** | ⚠️ Needs Sentinel |
| High throughput | ⚠️ 50k/node | ✅ 100k+ ops |
| Complex data types | ⚠️ Limited | ✅ Sorted sets, streams |

**[See full comparison →](vs-redis.md)**

---

## 💡 Common Patterns

### Rate Limiting an API

```python
# Check rate limit
result = client.consume(user_id, limit=100, window=timedelta(minutes=1))
if not result['ok']:
    return HttpResponse(status=429)  # Too Many Requests

# Process request
```

### Preventing Duplicate Payments

```python
# Check if request already processed
if client.seen(transaction_id, ttl=timedelta(hours=24))['seen']:
    return "Duplicate transaction"

# Process payment
charge_card(user, amount)
```

### User Data Caching

```python
# Try cache first
data, exists = client.cache_get('users', user_id)
if exists:
    return json.loads(data)

# Cache miss - fetch and store
user = database.get_user(user_id)
client.cache_set('users', user_id, json.dumps(user), ttl=timedelta(hours=1))
return user
```

---

## 🔧 Configuration

Every SDK supports:
- Custom host/port
- Timeout configuration
- SSL verification
- API key authentication
- Connection pooling
- Retry logic

**[See configuration details →](api-reference.md#configuration-options)**

---

## 📈 Performance

All operations are **O(1)**:
- **Rate Limiting**: ~50k operations/sec per node
- **Replay Protection**: ~50k operations/sec per node
- **Caching**: ~50k operations/sec per node

Scales horizontally with added cluster nodes.

---

## 🏗️ Architecture

TollMeshCache uses **CRDTs (Conflict-Free Replicated Data Types)** for automatic state convergence:

```
Node 1: Rate Limit = 42, Replays = {nonce-1, nonce-2}
Node 2: Rate Limit = 58, Replays = {nonce-3}
        ↓ Gossip Protocol ↓
Result: Rate Limit = 100, Replays = {nonce-1, nonce-2, nonce-3}
```

No coordinator, no consensus, automatic conflict resolution.

---

## 🤝 Contributing

We welcome contributions to any SDK or documentation.

[See CONTRIBUTING.md →](../CONTRIBUTING.md)

---

## 📄 License

Apache License 2.0 - [See LICENSE →](../LICENSE)

---

## 🆘 Support

- **[GitHub Issues](https://github.com/TollMesh/toll-mesh-store/issues)** - Bug reports & features
- **[GitHub Discussions](https://github.com/TollMesh/toll-mesh-store/discussions)** - Questions & ideas
- **[Examples](../examples/)** - Language-specific usage examples

## 📦 Installation

```bash
# Python
pip install tollmeshcache

# Node.js
npm install tollmeshcache

# Java
mvn dependency:get -Dartifact=com.tollmesh:tollmeshcache:1.0.0

# Rust
cargo add tollmeshcache

# Ruby
gem install tollmeshcache

# C#
dotnet add package TollMeshCache

# PHP
composer require toll-mesh/cache
```

## 🚀 Basic Usage

```python
from tollmeshcache import Client
from datetime import timedelta

client = Client()

# Rate limiting
result = client.consume("user-123", limit=100, window=timedelta(minutes=1))
if result["ok"]:
    # Process request
    pass

# Replay protection
if client.seen("nonce-123", ttl=timedelta(minutes=5))["seen"]:
    raise Exception("Replay detected!")

# Caching
client.cache_set("users", "user-123", json.dumps(data), ttl=timedelta(hours=1))
value, exists = client.cache_get("users", "user-123")
```

## 📚 Documentation

- **[Architecture](../ARCHITECTURE.md)** - Design principles and CRDT concepts
- **[API Reference](api/)** - Full API documentation
- **[Examples](examples/)** - Language-specific examples
- **[Contributing](../CONTRIBUTING.md)** - How to contribute

## 🏗️ Architecture

```
┌─────────────────────────────────────┐
│      TollMeshCache Node             │
├─────────────────────────────────────┤
│  Rate Limiting (GCounter)           │
│  Replay Protection (GSet)           │
│  Distributed Cache (TTL)            │
│  Health & Monitoring                │
└─────────────────────────────────────┘
         ↓
    Gossip Protocol
    (Peer-to-peer sync)
```

## 📊 Performance

- **Rate Limiting**: O(1) per operation
- **Replay Protection**: O(1) per operation
- **Caching**: O(1) per operation
- **Automatic convergence** across cluster nodes

## 🔧 Configuration

All SDKs support:
- Custom host/port
- Timeout configuration
- SSL verification
- API key authentication
- Connection pooling

## 📄 License

Apache License 2.0

## 🤝 Support

- **Issues**: [GitHub Issues](https://github.com/toll-mesh/store/issues)
- **Discussions**: [GitHub Discussions](https://github.com/toll-mesh/store/discussions)
- **Docs**: [Full Documentation](../README.md)
