# TollMeshCache - C# SDK

Distributed CRDT-based caching and coordination for .NET applications.

## Installation

```bash
dotnet add package TollMeshCache
```

## Quick Start

```csharp
using TollMesh.Cache;

var config = new ClientConfig { Host = "localhost", Port = 8080 };
using (var client = new Client(config))
{
    // Job Queues - distributed task processing
    var job = await client.EnqueueAsync("tasks", "my-job", priority: 5);
    var claimed = await client.ClaimAsync("tasks", "worker-1");
    await client.CompleteAsync("tasks", claimed.Id);

    // Sorted Sets - O(log n) leaderboards
    await client.ZAddAsync("scores", "player-1", 100);
    await client.ZAddAsync("scores", "player-2", 150);
    var topScores = await client.ZRevRangeAsync("scores", limit: 10); // highest first

    // Streams - append-only event logs
    var entry = await client.XAddAsync("events", new() { ["event"] = "login", ["user"] = "alice" });
    await client.XGroupCreateAsync("events", "analytics");
    foreach (var e in await client.XReadGroupAsync("analytics", "worker-1", "events"))
    {
        await client.XAckAsync("events", "analytics", "worker-1", e.Id);
    }

    // Rate Limiting / Replay Protection / Cache
    await client.ConsumeAsync("api-limit", 10, TimeSpan.FromSeconds(60));
    var seen = await client.SeenAsync("request-nonce", TimeSpan.FromMinutes(5));
    await client.CacheSetAsync("session", "user-123", "session-data");
    var value = await client.CacheGetAsync("session", "user-123");
}
```

## Features

- **Job Queues**: Distributed task processing with exactly-once semantics
- **Sorted Sets**: O(log n) leaderboards and rankings
- **Streams**: Append-only event logs with consumer groups
- **CRDT-based**: Eventual consistency without central coordinator
- **Async/Await**: Full async support for .NET
- **Multi-framework**: Works with .NET 6.0+, .NET 7.0+, .NET 8.0+

## Documentation

See https://github.com/TollMesh/toll-mesh-store for complete documentation.

## License

Apache License 2.0
