# TollMeshCache - Ruby SDK

Distributed CRDT-based caching and coordination for Ruby applications.

## Installation

```bash
gem install tollmeshcache
```

## Quick Start

```ruby
require 'tollmeshcache'

config = TollMeshCache::ClientConfig.new(host: 'localhost', port: 8080)
client = TollMeshCache::Client.new(config)

# Job Queues - distributed task processing
job = client.enqueue('tasks', 'my-job', priority: 5)
claimed = client.claim('tasks', 'worker-1')
client.complete('tasks', claimed['id'])

# Sorted Sets - O(log n) leaderboards
client.zadd('scores', 100, 'player-1')
client.zadd('scores', 150, 'player-2')
top_scores = client.zrevrange('scores', limit: 10) # highest first

# Streams - append-only event logs
entry = client.xadd('events', { 'event' => 'login', 'user' => 'alice' })
client.xgroup_create('events', 'analytics')
client.xreadgroup('analytics', 'worker-1', 'events').each do |e|
  client.xack('events', 'analytics', 'worker-1', e['id'])
end
```

## Features

- **Job Queues**: Distributed task processing with exactly-once semantics
- **Sorted Sets**: O(log n) leaderboards and rankings
- **Streams**: Append-only event logs with consumer groups
- **CRDT-based**: Eventual consistency without central coordinator

## Documentation

See https://github.com/TollMesh/toll-mesh-store for complete documentation.

## License

Apache License 2.0
