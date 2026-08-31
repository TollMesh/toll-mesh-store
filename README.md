# TollMeshCache - Multi-Language SDK Ecosystem

Production-ready distributed caching and coordination SDK with **7 language support**: Python, Node.js, Java, Rust, Ruby, C#, and PHP. Built on CRDT (Conflict-Free Replicated Data Types) technology for Redis-free distributed systems.

## 🚀 Features

- **7 Language SDKs**: Python, Node.js, Java, Rust, Ruby, C#, PHP
- **Distributed Rate Limiting**: CRDT-based token bucket across cluster
- **Replay Protection**: Secure nonce tracking with automatic convergence
- **Distributed Caching**: TTL-based cache with automatic expiration
- **Async/Await**: Full async support in all SDKs
- **Type-Safe**: Native type hints and strong typing
- **Production-Ready**: Comprehensive error handling and health checks

## 📦 Installation

Choose your language and install:

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

## 📚 Documentation

Full documentation available on [GitHub Pages](https://toll-mesh.github.io/toll-mesh-store/):

- **[Python SDK](docs/python.md)** - Async/sync with httpx
- **[Node.js SDK](docs/nodejs.md)** - TypeScript, streaming support
- **[Java SDK](docs/java.md)** - OkHttp3, CompletableFuture async
- **[Rust SDK](docs/rust.md)** - Tokio async, type-safe
- **[Ruby SDK](docs/ruby.md)** - HTTPClient, idiomatic
- **[C# SDK](docs/csharp.md)** - Native async/await
- **[PHP SDK](docs/php.md)** - Guzzle, PSR-compliant

## 💻 Quick Start

### Python

```python
from tollmeshcache import Client
from datetime import timedelta

client = Client(host='localhost', port=8080)

# Rate limiting
result = client.consume('user-123', limit=100, window=timedelta(minutes=1))
if result['ok']:
    print('Request allowed')

# Replay protection
if client.seen('nonce-123', ttl=timedelta(minutes=5))['seen']:
    raise Exception('Replay detected!')

# Caching
client.cache_set('users', 'user-123', data, ttl=timedelta(hours=1))
value, exists = client.cache_get('users', 'user-123')
```

### Node.js/TypeScript

```typescript
import { Client } from 'tollmeshcache';

const client = new Client({ host: 'localhost', port: 8080 });

// Rate limiting
const result = await client.consume('user-123', 100, 60000);
if (result.ok) console.log('Request allowed');

// Replay protection
if ((await client.seen('nonce-123', 300000)).seen) {
  throw new Error('Replay detected!');
}

// Caching
await client.cacheSet('users', 'user-123', data, 3600000);
const [value, exists] = await client.cacheGet('users', 'user-123');
```

### Java

```java
Client client = new Client(config);

// Rate limiting
ConsumeResult result = client.consume("user-123", 100, Duration.ofMinutes(1));
if (result.ok) System.out.println("Request allowed");

// Replay protection
if (client.seen("nonce-123", Duration.ofMinutes(5)).seen)
  throw new Exception("Replay detected!");

// Caching
client.cacheSet("users", "user-123", data, Duration.ofHours(1));
CacheValue cached = client.cacheGet("users", "user-123");
```

More examples in [docs/](docs/) directory.

## 🧪 Testing

Each SDK includes comprehensive test coverage:

```bash
# Python
pytest sdks/python/tests/

# Node.js
npm test --workspace=sdks/nodejs

# Java
mvn test -f sdks/java/pom.xml

# Rust
cargo test -p tollmeshcache

# Ruby
rspec sdks/ruby/spec/

# C#
dotnet test sdks/csharp/

# PHP
vendor/bin/phpunit sdks/php/tests/
```

## ⚡ Performance

All operations are O(1):

- **Rate Limiting**: Constant-time token consumption
- **Replay Protection**: Distributed set membership test
- **Caching**: Direct cache lookup with TTL validation
- **Memory**: O(n) where n = number of unique keys
- **Automatic Convergence**: CRDT-based state synchronization

## 🏗️ Architecture

```
┌─────────────────────────────────────────┐
│      TollMeshCache Cluster              │
├─────────────────────────────────────────┤
│                                         │
│  Node 1: Rate Limiting (GCounter)       │
│  Node 2: Replay Protection (GSet)       │
│  Node 3: Distributed Cache (TTL)        │
│                                         │
│  ↔️ Automatic State Convergence via CRDT
│                                         │
└─────────────────────────────────────────┘
         ↕️
    Multi-Language SDKs
    (7 languages, 1 API)
```

## 🔄 API Specifications

- **OpenAPI 3.0**: `api/openapi.yaml` - REST/HTTP endpoints
- **gRPC Proto**: `proto/store.proto` - Type definitions

## 📋 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

Apache License 2.0 - See LICENSE file for details