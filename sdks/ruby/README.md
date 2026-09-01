# TollMeshCache - Ruby SDK

Distributed CRDT-based caching and coordination for Ruby applications.

## Installation

```bash
gem install tollmeshcache
```

## Quick Start

```ruby
require 'tollmeshcache'

# Create a cache client
cache = TollMeshCache::Client.new('localhost:8080')

# Job Queues
cache.enqueue('tasks', 'my-job', priority: 5)
job = cache.claim('tasks')
cache.complete('tasks', job.id)

# Sorted Sets (Leaderboards)
cache.zadd('scores', 100, 'player-1')
cache.zrange('scores', 0, -1)

# Streams (Event Logs)
cache.xadd('events', { 'event' => 'login', 'user' => 'alice' })
cache.xrange('events', '-', '+')
```

## Features

- **Job Queues**: Distributed task processing with exactly-once semantics
- **Sorted Sets**: O(log n) leaderboards and rankings
- **Streams**: Append-only event logs with consumer groups
- **CRDT-based**: Eventual consistency without central coordinator
- **Async/Await**: Full async support with Fiber

## Documentation

See https://github.com/toll-mesh/store for complete documentation.

## License

Apache License 2.0
