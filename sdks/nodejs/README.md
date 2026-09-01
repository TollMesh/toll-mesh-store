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

// Job Queues
await client.enqueue('tasks', 'my-job', { priority: 5 });
const job = await client.claim('tasks');
await client.complete('tasks', job.id);

// Sorted Sets
await client.zadd('scores', 100, 'player-1');
await client.zadd('scores', 150, 'player-2');
const scores = await client.zrange('scores', 0, -1);

// Streams
await client.xadd('events', { event: 'login', user: 'alice' });
const events = await client.xrange('events', '-', '+');
```

## Features

- **Job Queues**: Distributed task processing with exactly-once semantics
- **Sorted Sets**: O(log n) leaderboards and rankings
- **Streams**: Append-only event logs with consumer groups
- **CRDT-based**: Eventual consistency without central coordinator
- **Async/Await**: Full async support with Promises
- **TypeScript**: Fully typed for TypeScript projects

## Documentation

See https://github.com/toll-mesh/store for complete documentation.

## License

Apache License 2.0
