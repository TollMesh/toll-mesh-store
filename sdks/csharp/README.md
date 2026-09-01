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
    // Job Queues
    await client.Consume("api-limit", 10, TimeSpan.FromSeconds(60));
    var seen = await client.Seen("request-nonce", TimeSpan.FromMinutes(5));
    
    // Cache Operations
    await client.CacheSet("session", "user-123", "session-data");
    var value = await client.CacheGet("session", "user-123");
    
    // Health Check
    var health = await client.Health();
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

See https://github.com/toll-mesh/store for complete documentation.

## License

Apache License 2.0
