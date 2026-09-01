---
layout: default
title: Home
nav_order: 1
description: "Production-ready 7-language SDK for distributed coordination"
---

# TollMeshCache
## Distributed Coordination & Caching for Every Platform

**Production-ready distributed coordination system** with built-in job queues, sorted sets, and streams. **7 language SDKs** with identical APIs, async support, and enterprise features.

---

## ✨ Status: Production Ready

**All 7 SDKs published and live on official package managers:**

| Language | Package Manager | Status | Install |
|----------|-----------------|--------|---------|
| **Python** | PyPI | ✅ Published | `pip install tollmeshcache` |
| **Node.js/TypeScript** | npm | ✅ Published | `npm install @tollmesh/tollmeshcache` |
| **Rust** | crates.io | ✅ Published | `cargo add tollmeshcache` |
| **Ruby** | RubyGems | ✅ Published | `gem install tollmeshcache` |
| **C#/.NET** | NuGet | ✅ Published | `dotnet add package TollMeshCache` |
| **PHP** | Packagist | ✅ Published | `composer require toll-mesh/cache` |
| **Java** | Maven Central | ✅ Published | Add dependency to pom.xml |

---

## 🎯 Three Core Features

### 1️⃣ Job Queues
Distributed task processing with **exactly-once semantics**, priority levels, and automatic retries.

```python
# Enqueue a job
client.enqueue('tasks', 'job-id', priority=5)

# Claim and process
job = client.claim('tasks')
client.complete('tasks', job.id)
```

**Use Cases:** Background jobs, async task processing, worker coordination

---

### 2️⃣ Sorted Sets
**O(log n) leaderboards and rankings** using skip lists. Perfect for scoring systems, rankings, and time-series data.

```python
# Add members with scores
client.zadd('leaderboard', 100, 'alice')
client.zadd('leaderboard', 150, 'bob')

# Get rankings
top_10 = client.zrange('leaderboard', 0, 10)
```

**Use Cases:** Game leaderboards, top-N queries, scoring systems, rate limits

---

### 3️⃣ Streams
**Append-only event logs** with consumer groups for reliable event processing and replay.

```python
# Publish events
client.xadd('events', {'event': 'order', 'user': 'alice'})

# Process with consumer groups
messages = client.xreadgroup('analytics', 'worker-1', 'events')
```

**Use Cases:** Event sourcing, audit logs, message queues, activity feeds

---

## 🚀 Why TollMeshCache?

### ✅ **No Central Coordinator**
CRDT-based design means no single point of failure. Every node has full capability.

### ✅ **Async/Await in All Languages**
Native async support: Python asyncio, Node.js Promises, Rust Tokio, Java CompletableFuture, C# Tasks, PHP Fibers, Ruby Async.

### ✅ **Identical APIs**
Learn once, use everywhere. Same patterns across all 7 languages.

### ✅ **Production Ready**
- Comprehensive error handling
- Automatic retries with exponential backoff
- Type-safe implementations
- Full test coverage
- GPG-signed releases

### ✅ **Enterprise Features**
- Rate limiting (distributed token bucket)
- Replay protection (nonce tracking)
- TTL-based expiration
- Consumer group coordination
- Health monitoring

---

## 📚 Documentation

### Getting Started
- **[Quick Start Guides](guides/)** - 5-minute setup for each language
- **[API Reference](api-reference.md)** - Complete endpoint documentation

### Language Guides
- **[Python SDK](python.md)** - Full async/sync support
- **[Node.js SDK](nodejs.md)** - TypeScript native
- **[Rust SDK](rust.md)** - Zero-cost abstractions with Tokio
- **[Ruby SDK](ruby.md)** - Idiomatic Ruby patterns
- **[C# SDK](csharp.md)** - .NET 6+ with native async/await
- **[PHP SDK](php.md)** - Composer, PSR-4 compliant
- **[Java SDK](java.md)** - Maven Central, Spring Boot ready

### Architecture & Comparison
- **[vs Redis](vs-redis.md)** - Feature comparison and when to use what
- **[Architecture](architecture.md)** - CRDT design, consistency model
- **[Publishing Guide](publishing-guide.md)** - CI/CD automation across 7 languages

---

## 💡 Use Cases

### Real-time Analytics
Process millions of events with streams and consumer groups.

### Gaming Leaderboards
Sorted sets provide O(log n) leaderboard updates with strong consistency.

### Rate Limiting
Distributed token bucket prevents abuse across microservices without central coordination.

### Job Processing
Exactly-once semantics guarantee no duplicate processing of tasks.

### Replay Protection
Built-in nonce tracking prevents duplicate payments and double-clicks.

---

## 🛠️ Installation by Language

### Python
```bash
pip install tollmeshcache
```
📖 [Python Docs](python.md)

### Node.js / TypeScript
```bash
npm install @tollmesh/tollmeshcache
```
📖 [Node.js Docs](nodejs.md)

### Rust
```bash
cargo add tollmeshcache
```
📖 [Rust Docs](rust.md)

### Ruby
```bash
gem install tollmeshcache
```
📖 [Ruby Docs](ruby.md)

### C# / .NET
```bash
dotnet add package TollMeshCache
```
📖 [C# Docs](csharp.md)

### PHP
```bash
composer require toll-mesh/cache
```
📖 [PHP Docs](php.md)

### Java
```xml
<dependency>
    <groupId>io.github.prakhar998</groupId>
    <artifactId>tollmeshcache</artifactId>
    <version>1.0.0</version>
</dependency>
```
📖 [Java Docs](java.md)

---

## 🤝 Contributing

Have ideas? Found a bug? 
- **Report Issues:** [GitHub Issues](https://github.com/TollMesh/toll-mesh-store/issues)
- **Documentation:** Help improve docs in `/docs`
- **SDKs:** Add features, fix bugs, improve performance

---

## 📄 License

All SDKs are released under **Apache License 2.0**. Free for commercial and personal use.

---

## 🔗 Links

- **GitHub:** [TollMesh/toll-mesh-store](https://github.com/TollMesh/toll-mesh-store)
- **Package Status:** See table above
- **API Docs:** [Complete Reference](api-reference.md)

---

**Ready to build distributed systems without a coordinator?** Pick your language above and get started! 🚀
