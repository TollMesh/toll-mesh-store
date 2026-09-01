# TollMeshCache - PHP SDK

Distributed CRDT-based caching and coordination for PHP applications.

## Installation

```bash
composer require toll-mesh/cache
```

## Quick Start

```php
use TollMesh\Cache\Client;

$client = new Client('localhost:8080');

// Job Queues
$client->enqueue('tasks', 'my-job', ['priority' => 5]);
$job = $client->claim('tasks');
$client->complete('tasks', $job['id']);

// Sorted Sets
$client->zadd('scores', 100, 'player-1');
$client->zadd('scores', 150, 'player-2');
$scores = $client->zrange('scores', 0, -1);

// Streams
$client->xadd('events', ['event' => 'login', 'user' => 'alice']);
$events = $client->xrange('events', '-', '+');
```

## Features

- **Job Queues**: Distributed task processing with exactly-once semantics
- **Sorted Sets**: O(log n) leaderboards and rankings
- **Streams**: Append-only event logs with consumer groups
- **CRDT-based**: Eventual consistency without central coordinator
- **Async Support**: Full async support with PHP 8.1+
- **PSR-4**: Follows PHP Standards Recommendation

## Documentation

See https://github.com/toll-mesh/store for complete documentation.

## License

Apache License 2.0
