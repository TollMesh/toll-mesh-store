---
layout: default
title: C# SDK
nav_order: 8
---

# C# SDK

## Installation

Add via NuGet:

```bash
dotnet add package TollMeshCache
```

Or in `csproj`:

```xml
<ItemGroup>
  <PackageReference Include="TollMeshCache" Version="1.0.0" />
</ItemGroup>
```

## Quick Start

```csharp
using TollMeshCache;
using System;
using System.Threading.Tasks;

class Program {
  static async Task Main() {
    var config = new ClientConfig {
      Host = "localhost",
      Port = 8080
    };

    using var client = new Client(config);

    // Rate limiting
    var result = await client.ConsumeAsync(
      "user-123",
      100,
      TimeSpan.FromMinutes(1)
    );

    if (result.Ok) {
      Console.WriteLine("Request allowed");
    }
  }
}
```

## Features

- ✅ Native async/await support
- ✅ Multi-target (.NET 6.0, 7.0, 8.0, Standard 2.1)
- ✅ Strong typing with generics
- ✅ HttpClient connection pooling
- ✅ Comprehensive error handling

## API Reference

### Rate Limiting
```csharp
var result = await client.ConsumeAsync(key, limit, window);
// Fields: Ok, Remaining, ResetAt
```

### Replay Protection
```csharp
var result = await client.SeenAsync(key, ttl);
// Field: Seen
```

### Caching
```csharp
await client.CacheSetAsync(namespace, key, value, ttl);
var result = await client.CacheGetAsync(namespace, key);
// Fields: Value, Exists
```

### Health
```csharp
var health = await client.HealthAsync();
var peers = await client.GetPeersAsync();
```

## Error Handling

```csharp
using TollMeshCache;

try {
  var result = await client.ConsumeAsync("key", 100, TimeSpan.FromMinutes(1));
} catch (RateLimitException e) {
  Console.WriteLine($"Rate limited: {e.Message}");
} catch (TollMeshException e) {
  Console.WriteLine($"Error {e.ErrorCode}: {e.Message}");
}
```

## Configuration

```csharp
var config = new ClientConfig {
  Host = "localhost",
  Port = 8080,
  Timeout = TimeSpan.FromSeconds(5),
  VerifySsl = true,
  ApiKey = "optional-key",
  HttpScheme = "http",
  MaxRetries = 3,
  RetryBackoff = 1.0,
  ConnectionPoolSize = 10
};

using var client = new Client(config);
```

## Testing

```bash
dotnet test
```

## Examples

See `examples/` for:
- `RateLimitingExample.cs` - Rate limiting patterns

Run example:

```bash
dotnet run --project sdks/csharp/examples
```

## Multi-targeting

The SDK targets multiple .NET versions for compatibility:

- **net8.0** - Latest .NET 8
- **net7.0** - .NET 7
- **net6.0** - .NET 6
- **netstandard2.1** - .NET Standard 2.1 (for .NET Framework)
