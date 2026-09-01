---
layout: default
title: TollMeshCache
nav_order: 1
---

# TollMeshCache

Production-ready distributed coordination and caching system with 7-language SDK support. No central coordinator required.

{% include theme-toggle.html %}
{% include language-switcher.html %}

---

## Production Status

All 7 SDKs published and production-ready on official package managers:

| Language | Package Manager | Version | Status |
|----------|-----------------|---------|--------|
| Python | PyPI | 1.0.0 | Live |
| Node.js/TypeScript | npm | 1.0.0 | Live |
| Rust | crates.io | 1.0.0 | Live |
| Ruby | RubyGems | 1.0.0 | Live |
| C#/.NET | NuGet | 1.0.0 | Live |
| PHP | Packagist | 1.0.0 | Live |
| Java | Maven Central | 1.0.0 | Live |

---

## Core Features (Complete)

### Job Queues
Distributed task processing with exactly-once semantics, priority levels, and automatic retries.

**Key Capabilities:**
- FIFO and priority-based processing
- Exactly-once delivery guarantees
- Automatic retry with exponential backoff
- Dead-letter queue for failed jobs
- Distributed coordination without central broker

**Available in:** All 7 languages with identical APIs

---

### Sorted Sets
O(log n) leaderboards and rankings using skip list data structures. Perfect for scoring systems and range queries.

**Key Capabilities:**
- O(log n) insert, update, and range queries
- CRDT-based conflict resolution
- Automatic score aggregation
- Range queries by score or rank
- TTL-based expiration

**Available in:** All 7 languages with identical APIs

---

### Streams
Append-only event logs with consumer groups for reliable event processing and replay.

**Key Capabilities:**
- Append-only immutable event log
- Consumer group coordination
- Offset tracking and liveness detection
- Event replay and rebalancing
- TTL retention policies

**Available in:** All 7 languages with identical APIs

---

## Installation

### Python
```bash
pip install tollmeshcache
```
[Python Documentation](python.md)

### Node.js / TypeScript
```bash
npm install @tollmesh/tollmeshcache
```
[Node.js Documentation](nodejs.md)

### Rust
```bash
cargo add tollmeshcache
```
[Rust Documentation](rust.md)

### Ruby
```bash
gem install tollmeshcache
```
[Ruby Documentation](ruby.md)

### C# / .NET
```bash
dotnet add package TollMeshCache
```
[C# Documentation](csharp.md)

### PHP
```bash
composer require toll-mesh/cache
```
[PHP Documentation](php.md)

### Java
```xml
<dependency>
    <groupId>io.github.prakhar998</groupId>
    <artifactId>tollmeshcache</artifactId>
    <version>1.0.0</version>
</dependency>
```
[Java Documentation](java.md)

---

## Architecture

**Key Design Principles:**
- CRDT-based consistency: Eventual consistency without central coordinator
- Lamport clocks: Distributed ordering and causality tracking
- Skip lists: O(log n) sorted operations
- Append-only logs: Immutable event history

**No Single Point of Failure**
Every node operates independently with automatic convergence.

---

## Use Cases

**Rate Limiting** - Distributed token bucket for API throttling across microservices

**Gaming Leaderboards** - Real-time player rankings with O(log n) updates

**Job Processing** - Background task queues with exactly-once semantics

**Event Sourcing** - Immutable event logs for audit and replay

**Replay Protection** - Built-in nonce tracking prevents duplicate operations

---

## Documentation

- [API Reference](api-reference.md) - Complete endpoint documentation
- [Architecture Guide](architecture.md) - Design and consistency model
- [vs Redis](vs-redis.md) - Feature comparison and selection criteria
- [Publishing Guide](publishing-guide.md) - CI/CD automation for 7 languages

---

## Quick Examples

### Rate Limiting (Python)
```python
from tollmeshcache import Client
from datetime import timedelta

client = Client('localhost:8080')
result = client.consume('api-limit', limit=100, window=timedelta(minutes=1))

if result.ok:
    print(f"Allowed. Remaining: {result.remaining}")
```

### Leaderboard (Node.js)
```typescript
import { Client } from '@tollmesh/tollmeshcache';

const client = new Client({ host: 'localhost' });

await client.zadd('leaderboard', 100, 'alice');
await client.zadd('leaderboard', 150, 'bob');

const top10 = await client.zrange('leaderboard', 0, 10);
```

### Event Processing (Rust)
```rust
let client = Client::new("localhost:8080").await?;

client.xadd("events", json!({"event": "order", "user": "alice"})).await?;
let events = client.xrange("events", "-", "+").await?;
```

---

## Contributing

Report issues or contribute improvements:
- [GitHub Repository](https://github.com/TollMesh/toll-mesh-store)
- [Issue Tracker](https://github.com/TollMesh/toll-mesh-store/issues)

---

## License

Apache License 2.0 - Free for commercial and personal use.

<!-- Build: Tue Sep  1 17:43:44 IST 2026 -->
