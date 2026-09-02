# TollMeshCache - Node.js/TypeScript SDK

Distributed CRDT-based caching and coordination for Node.js and TypeScript applications.

## Installation

```bash
npm install @tollmesh/tollmeshcache
```

## Quick Start

```typescript
import { Client } from '@tollmesh/tollmeshcache';

const client = new Client({ host: 'localhost', port: 8080 });

// Job Queues - distributed task processing
const job = await client.enqueue('tasks', 'my-job', { priority: 5 });
const claimed = await client.claim('tasks', 'worker-1');
await client.complete('tasks', claimed.id);

// Sorted Sets - O(log n) leaderboards
await client.zadd('scores', 100, 'player-1');
await client.zadd('scores', 150, 'player-2');
const topScores = await client.zrevrange('scores', undefined, undefined, 10); // highest first

// Streams - append-only event logs
const entry = await client.xadd('events', { event: 'login', user: 'alice' });
await client.xgroupCreate('events', 'analytics');
for (const e of await client.xreadgroup('analytics', 'worker-1', 'events')) {
  await client.xack('events', 'analytics', 'worker-1', e.id);
}
```

## Features

- **Job Queues**: Distributed task processing with exactly-once semantics
- **Sorted Sets**: O(log n) leaderboards and rankings
- **Streams**: Append-only event logs with consumer groups
- **CRDT-based**: Eventual consistency without central coordinator
- **Async/Await**: Full async support with Promises
- **TypeScript**: Fully typed for TypeScript projects

## Documentation

See https://github.com/TollMesh/toll-mesh-store for complete documentation.

## License

Apache License 2.0
