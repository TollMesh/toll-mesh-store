# TollMeshCache - Python SDK

Distributed CRDT-based caching and coordination for Python applications.

## Installation

```bash
pip install tollmeshcache
```

## Quick Start

```python
from tollmeshcache import Client, ClientConfig

client = Client(ClientConfig(host='localhost', port=8080))

# Job Queues - distributed task processing
job = client.enqueue('tasks', 'my-job', priority=5)
claimed = client.claim('tasks', 'worker-1')
client.complete('tasks', claimed['id'])

# Sorted Sets - O(log n) leaderboards
client.zadd('scores', 100, 'player-1')
client.zadd('scores', 150, 'player-2')
top_scores = client.zrevrange('scores', limit=10)  # highest first

# Streams - append-only event logs
entry = client.xadd('events', {'event': 'login', 'user': 'alice'})
client.xgroup_create('events', 'analytics')
for e in client.xreadgroup('analytics', 'worker-1', 'events'):
    client.xack('events', 'analytics', 'worker-1', e['id'])
```

## Features

- **Job Queues**: Distributed task processing with exactly-once semantics
- **Sorted Sets**: O(log n) leaderboards and rankings
- **Streams**: Append-only event logs with consumer groups
- **CRDT-based**: Eventual consistency without central coordinator
- **Async Support**: Full async/await support
- **Distributed**: Multi-node coordination with Lamport clocks

## Documentation

See https://github.com/TollMesh/toll-mesh-store for complete documentation.

## License

Apache License 2.0
