# TollMeshCache - Python SDK

Distributed CRDT-based caching and coordination for Python applications.

## Installation

```bash
pip install tollmeshcache
```

## Quick Start

```python
from tollmeshcache import Client

# Create a client
client = Client('localhost:8080')

# Job Queues - Distributed task processing
client.enqueue('tasks', 'my-job', priority=5)
job = client.claim('tasks')
client.complete('tasks', job.id)

# Sorted Sets - O(log n) leaderboards
client.zadd('scores', 100, 'player-1')
client.zadd('scores', 150, 'player-2')
scores = client.zrange('scores', 0, -1)

# Streams - Append-only event logs
client.xadd('events', {'event': 'login', 'user': 'alice'})
events = client.xrange('events', '-', '+')
```

## Features

- **Job Queues**: Distributed task processing with exactly-once semantics
- **Sorted Sets**: O(log n) leaderboards and rankings
- **Streams**: Append-only event logs with consumer groups
- **CRDT-based**: Eventual consistency without central coordinator
- **Async Support**: Full async/await support
- **Distributed**: Multi-node coordination with Lamport clocks

## Documentation

See https://github.com/toll-mesh/store for complete documentation.

## License

Apache License 2.0
