# TollMeshCache

**Production-ready distributed coordination and caching** with 7-language SDK support.

[📖 Full Documentation](https://tollmesh.github.io/toll-mesh-store/) | [GitHub](https://github.com/TollMesh/toll-mesh-store) | [Issues](https://github.com/TollMesh/toll-mesh-store/issues)

---

## Status: Production Ready

All 3 core features and 7 language SDKs complete and published.

### Core Features (All Complete ✅)

| Feature | Status | Details |
|---------|--------|---------|
| **Job Queues** | ✅ Complete | Distributed task processing with exactly-once semantics |
| **Sorted Sets** | ✅ Complete | O(log n) leaderboards using skip lists |
| **Streams** | ✅ Complete | Append-only event logs with consumer groups |

### Language SDKs (All Published ✅)

| Language | Package Manager | Install Command |
|----------|-----------------|-----------------|
| Python | PyPI | `pip install tollmeshcache` |
| Node.js | npm | `npm install @tollmesh/tollmeshcache` |
| Rust | crates.io | `cargo add tollmeshcache` |
| Ruby | RubyGems | `gem install tollmeshcache` |
| C# | NuGet | `dotnet add package TollMeshCache` |
| PHP | Packagist | `composer require toll-mesh/cache` |
| Java | Maven Central | Add to pom.xml |

---

## Quick Start

```python
from tollmeshcache import Client

client = Client('localhost:8080')

# Job Queues
client.enqueue('tasks', 'job-id', priority=5)
job = client.claim('tasks')
client.complete('tasks', job.id)

# Sorted Sets
client.zadd('leaderboard', 100, 'alice')
top_10 = client.zrange('leaderboard', 0, 10)

# Streams
client.xadd('events', {'event': 'login', 'user': 'alice'})
events = client.xrange('events', '-', '+')
```

---

## Documentation

Visit **[GitHub Pages](https://tollmesh.github.io/toll-mesh-store/)** for:
- Language-specific guides
- API reference
- Architecture documentation
- Use cases and examples
- Dark/Light mode support
- English/Chinese language support

---

## Features

- **No Central Coordinator** - CRDT-based design
- **Async/Await in All Languages** - Native async support
- **Identical APIs** - Learn once, use everywhere
- **Production Ready** - Comprehensive testing, error handling, retries
- **Enterprise Features** - Rate limiting, replay protection, health monitoring

---

## License

Apache License 2.0

---

**[View Full Docs](https://tollmesh.github.io/toll-mesh-store/)**
