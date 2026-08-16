# Getting Started with TollMesh Java Client

## Prerequisites

- Java 11 or higher
- Maven 3.6+ or Gradle 6.0+
- gRPC and Protocol Buffers libraries

## Installation

### Maven

Add to your `pom.xml`:

```xml
<dependencies>
  <!-- gRPC -->
  <dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-netty-shaded</artifactId>
    <version>1.56.0</version>
  </dependency>
  <dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-protobuf</artifactId>
    <version>1.56.0</version>
  </dependency>
  <dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-stub</artifactId>
    <version>1.56.0</version>
  </dependency>
  
  <!-- Protocol Buffers -->
  <dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>3.23.0</version>
  </dependency>
</dependencies>

<build>
  <plugins>
    <plugin>
      <groupId>org.xolstice.maven.plugins</groupId>
      <artifactId>protobuf-maven-plugin</artifactId>
      <version>0.6.1</version>
      <configuration>
        <protocArtifact>com.google.protobuf:protoc:3.23.0:exe:${os.detected.classifier}</protocArtifact>
        <pluginId>grpc-java</pluginId>
        <pluginArtifact>io.grpc:protoc-gen-grpc-java:1.56.0:exe:${os.detected.classifier}</pluginArtifact>
      </configuration>
      <executions>
        <execution>
          <goals>
            <goal>compile</goal>
            <goal>compile-custom</goal>
          </goals>
        </execution>
      </executions>
    </plugin>
  </plugins>
</build>
```

### Gradle

Add to your `build.gradle`:

```gradle
dependencies {
  implementation 'io.grpc:grpc-netty-shaded:1.56.0'
  implementation 'io.grpc:grpc-protobuf:1.56.0'
  implementation 'io.grpc:grpc-stub:1.56.0'
  implementation 'com.google.protobuf:protobuf-java:3.23.0'
}

plugins {
  id 'com.google.protobuf' version '0.9.2'
}

protobuf {
  protoc {
    artifact = 'com.google.protobuf:protoc:3.23.0'
  }
  plugins {
    grpc {
      artifact = 'io.grpc:protoc-gen-grpc-java:1.56.0'
    }
  }
  generateProtoTasks {
    all()*.plugins {
      grpc {}
    }
  }
}
```

## Basic Usage

### Connect to TollMesh

```java
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.tollmesh.TollMeshGrpc;
import com.tollmesh.TollMeshGrpc.TollMeshBlockingStub;

public class TollMeshClient {
  private final ManagedChannel channel;
  private final TollMeshBlockingStub blockingStub;

  public TollMeshClient(String host, int port) {
    this.channel = ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext()
        .build();
    this.blockingStub = TollMeshGrpc.newBlockingStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
  }
}
```

### Rate Limiting

```java
import com.tollmesh.ConsumeRequest;
import com.tollmesh.ConsumeResponse;

public void rateLimit() {
  ConsumeRequest request = ConsumeRequest.newBuilder()
      .setKey("user:123")
      .setLimit(100)
      .setWindowMs(60000)
      .build();

  ConsumeResponse response = blockingStub.consume(request);
  
  if (response.getOk()) {
    System.out.println("Request allowed. Remaining: " + response.getRemaining());
  } else {
    System.out.println("Rate limit exceeded. Reset at: " + response.getResetAt());
  }
}
```

### Cache Operations

```java
import com.tollmesh.GetRequest;
import com.tollmesh.GetResponse;
import com.tollmesh.SetRequest;
import com.tollmesh.SetResponse;
import com.google.protobuf.ByteString;

public void cacheOperations() {
  // Set value
  SetRequest setRequest = SetRequest.newBuilder()
      .setKey("mykey")
      .setValue(ByteString.copyFromUtf8("myvalue"))
      .build();
  
  SetResponse setResponse = blockingStub.set(setRequest);
  System.out.println("Set success: " + setResponse.getSuccess());

  // Get value
  GetRequest getRequest = GetRequest.newBuilder()
      .setKey("mykey")
      .build();
  
  GetResponse getResponse = blockingStub.get(getRequest);
  if (getResponse.getFound()) {
    String value = getResponse.getValue().toStringUtf8();
    System.out.println("Value: " + value);
  }
}
```

### Search Operations

```java
import com.tollmesh.SearchRequest;
import com.tollmesh.SearchResponse;

public void search() {
  SearchRequest request = SearchRequest.newBuilder()
      .setQuery("rate limit bypass")
      .setTopK(5)
      .build();

  SearchResponse response = blockingStub.search(request);
  
  for (var result : response.getResultsList()) {
    System.out.println("ID: " + result.getId());
    System.out.println("Score: " + result.getScore());
    System.out.println("Content: " + result.getContent());
  }
}
```

### Agent Operations

```java
import com.tollmesh.RegisterAgentRequest;
import com.tollmesh.RegisterAgentResponse;

public void registerAgent() {
  RegisterAgentRequest request = RegisterAgentRequest.newBuilder()
      .setId("agent-1")
      .setName("Browser Bot 1")
      .addCapabilities("javascript")
      .addCapabilities("cookies")
      .setReputation(0.8f)
      .build();

  RegisterAgentResponse response = blockingStub.registerAgent(request);
  
  if (response.getSuccess()) {
    System.out.println("Agent registered successfully");
  } else {
    System.out.println("Error: " + response.getError());
  }
}
```

### Pub/Sub Operations

```java
import com.tollmesh.PublishRequest;
import com.tollmesh.PublishResponse;
import com.google.protobuf.ByteString;

public void publish() {
  PublishRequest request = PublishRequest.newBuilder()
      .setTopic("threats:evasion")
      .setData(ByteString.copyFromUtf8("{\"type\":\"evasion_attempt\"}"))
      .build();

  PublishResponse response = blockingStub.publish(request);
  System.out.println("Published to " + response.getSubscribers() + " subscribers");
}
```

## Async Operations

```java
import io.grpc.stub.StreamObserver;
import com.tollmesh.TollMeshGrpc.TollMeshStub;

public void asyncOperations() {
  TollMeshStub asyncStub = TollMeshGrpc.newStub(channel);
  
  ConsumeRequest request = ConsumeRequest.newBuilder()
      .setKey("user:123")
      .setLimit(100)
      .setWindowMs(60000)
      .build();

  asyncStub.consume(request, new StreamObserver<ConsumeResponse>() {
    @Override
    public void onNext(ConsumeResponse response) {
      System.out.println("Response: " + response);
    }

    @Override
    public void onError(Throwable t) {
      System.err.println("Error: " + t.getMessage());
    }

    @Override
    public void onCompleted() {
      System.out.println("Completed");
    }
  });
}
```

## Error Handling

```java
import io.grpc.StatusRuntimeException;

public void handleErrors() {
  try {
    ConsumeRequest request = ConsumeRequest.newBuilder()
        .setKey("user:123")
        .setLimit(100)
        .setWindowMs(60000)
        .build();
    
    ConsumeResponse response = blockingStub.consume(request);
  } catch (StatusRuntimeException e) {
    System.err.println("RPC failed: " + e.getStatus());
    System.err.println("Description: " + e.getDescription());
  }
}
```

## Complete Example

```java
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.tollmesh.*;

public class TollMeshExample {
  public static void main(String[] args) throws InterruptedException {
    // Create channel
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
        .usePlaintext()
        .build();
    
    TollMeshGrpc.TollMeshBlockingStub stub = TollMeshGrpc.newBlockingStub(channel);

    try {
      // Rate limiting
      ConsumeRequest consumeReq = ConsumeRequest.newBuilder()
          .setKey("user:123")
          .setLimit(100)
          .setWindowMs(60000)
          .build();
      
      ConsumeResponse consumeResp = stub.consume(consumeReq);
      System.out.println("Rate limit OK: " + consumeResp.getOk());

      // Cache operations
      SetRequest setReq = SetRequest.newBuilder()
          .setKey("mykey")
          .setValue(com.google.protobuf.ByteString.copyFromUtf8("myvalue"))
          .build();
      
      stub.set(setReq);
      System.out.println("Value cached");

      // Search
      SearchRequest searchReq = SearchRequest.newBuilder()
          .setQuery("rate limit")
          .setTopK(5)
          .build();
      
      SearchResponse searchResp = stub.search(searchReq);
      System.out.println("Found " + searchResp.getResultsCount() + " results");

    } finally {
      channel.shutdown();
    }
  }
}
```

## Best Practices

1. **Connection Pooling**: Reuse channels for multiple requests
2. **Timeouts**: Set appropriate deadlines for requests
3. **Error Handling**: Always handle StatusRuntimeException
4. **Resource Cleanup**: Always shutdown channels properly
5. **Async Operations**: Use async stubs for non-blocking calls

## Testing

```java
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TollMeshClientTest {
  @Test
  public void testRateLimit() {
    // Test implementation
  }
}
```

## Documentation

- [gRPC Java Documentation](https://grpc.io/docs/languages/java/)
- [Protocol Buffers Java Guide](https://developers.google.com/protocol-buffers/docs/javatutorial)
- [TollMesh Protocol Documentation](PROTOCOL.md)

## Support

For issues or questions:
- Open an issue on GitHub
- Check [DEVELOPMENT.md](DEVELOPMENT.md)
- Review [PROTOCOL.md](PROTOCOL.md)