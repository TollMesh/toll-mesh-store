# TollMeshCache

**Distributed CRDT-based caching and coordination layer** for high-performance, Redis-free distributed systems.

## ⚡ Quick Start

Choose your language:

- **[Python](python/)** - Async/sync, type hints, retry logic
- **[Node.js](nodejs/)** - TypeScript, promises, streaming
- **[Java](java/)** - Async with CompletableFuture
- **[Rust](rust/)** - Tokio async, type-safe
- **[Ruby](ruby/)** - HTTPClient-based, idiomatic
- **[C#](csharp/)** - Async/await native
- **[PHP](php/)** - Composer, PSR compliant

## 🎯 Features

- **Rate Limiting** - Distributed token bucket across cluster
- **Replay Protection** - CRDT-based nonce tracking
- **Distributed Caching** - TTL-based cache with automatic expiration
- **Health Checks** - Monitor cluster status
- **Error Handling** - Standardized error codes across all languages

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
