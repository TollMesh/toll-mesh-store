# TollMeshCache - Java SDK

Distributed CRDT-based caching and coordination for Java applications.

## Installation

Add to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.prakhar998</groupId>
    <artifactId>tollmeshcache</artifactId>
    <version>1.0.0</version>
</dependency>
```

Or with Gradle:

```gradle
implementation 'io.github.prakhar998:tollmeshcache:1.0.0'
```

## Quick Start

```java
import com.tollmesh.store.Client;
import com.tollmesh.store.ClientConfig;

ClientConfig config = new ClientConfig();
config.setHost("localhost");
config.setPort(8080);

try (Client client = new Client(config)) {
    // Job Queues
    ConsumeResult result = client.consume("api-limit", 10, Duration.ofSeconds(60));
    SeenResult seen = client.seen("request-nonce", Duration.ofMinutes(5));
    
    // Cache Operations
    client.cacheSet("session", "user-123", "session-data", null);
    CacheValue value = client.cacheGet("session", "user-123");
    
    // Health Check
    HealthResponse health = client.health();
}
```

## Features

- **Job Queues**: Distributed task processing with exactly-once semantics
- **Sorted Sets**: O(log n) leaderboards and rankings
- **Streams**: Append-only event logs with consumer groups
- **CRDT-based**: Eventual consistency without central coordinator
- **Spring Boot**: Easy integration with Spring Boot applications
- **Maven/Gradle**: Full build tool support

## Spring Boot Integration

```java
@Configuration
public class TollMeshConfig {
    @Bean
    public Client tollMeshClient() {
        ClientConfig config = new ClientConfig();
        config.setHost("${tollmesh.host:localhost}");
        config.setPort(8080);
        return new Client(config);
    }
}
```

## Documentation

See https://github.com/TollMesh/toll-mesh-store for complete documentation.

## License

Apache License 2.0
