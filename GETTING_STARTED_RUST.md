# Getting Started with TollMesh Rust Client

## Prerequisites

- Rust 1.56+ (install from https://rustup.rs/)
- Cargo package manager

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
tonic = "0.10"
prost = "0.12"
tokio = { version = "1", features = ["full"] }
tokio-stream = "0.1"

[build-dependencies]
tonic-build = "0.10"
```

## Build Configuration

Create `build.rs` in your project root:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/tollmesh.proto")?;
    Ok(())
}
```

## Basic Usage

### Connect to TollMesh

```rust
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    Ok(())
}
```

### Rate Limiting

```rust
use tollmesh::ConsumeRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    let request = ConsumeRequest {
        key: "user:123".to_string(),
        limit: 100,
        window_ms: 60000,
    };
    
    let response = client.consume(request).await?;
    
    if response.get_ref().ok {
        println!("Request allowed. Remaining: {}", response.get_ref().remaining);
    } else {
        println!("Rate limit exceeded. Reset at: {}", response.get_ref().reset_at);
    }
    
    Ok(())
}
```

### Cache Operations

```rust
use tollmesh::{GetRequest, SetRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    // Set value
    let set_request = SetRequest {
        key: "mykey".to_string(),
        value: b"myvalue".to_vec(),
    };
    
    client.set(set_request).await?;
    println!("Value cached");
    
    // Get value
    let get_request = GetRequest {
        key: "mykey".to_string(),
    };
    
    let response = client.get(get_request).await?;
    
    if response.get_ref().found {
        let value = String::from_utf8(response.get_ref().value.clone())?;
        println!("Value: {}", value);
    }
    
    Ok(())
}
```

### Search Operations

```rust
use tollmesh::SearchRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    let request = SearchRequest {
        query: "rate limit bypass".to_string(),
        top_k: 5,
    };
    
    let response = client.search(request).await?;
    
    for (i, result) in response.get_ref().results.iter().enumerate() {
        println!("Result {}:", i + 1);
        println!("  ID: {}", result.id);
        println!("  Score: {}", result.score);
        println!("  Content: {}", result.content);
    }
    
    Ok(())
}
```

### Agent Operations

```rust
use tollmesh::RegisterAgentRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    let request = RegisterAgentRequest {
        id: "agent-1".to_string(),
        name: "Browser Bot 1".to_string(),
        capabilities: vec!["javascript".to_string(), "cookies".to_string()],
        reputation: 0.8,
    };
    
    let response = client.register_agent(request).await?;
    
    if response.get_ref().success {
        println!("Agent registered successfully");
    } else {
        println!("Error: {}", response.get_ref().error);
    }
    
    Ok(())
}
```

### Pub/Sub Operations

```rust
use tollmesh::PublishRequest;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    // Publish
    let publish_request = PublishRequest {
        topic: "threats:evasion".to_string(),
        data: br#"{"type":"evasion_attempt"}"#.to_vec(),
    };
    
    let response = client.publish(publish_request).await?;
    println!("Published to {} subscribers", response.get_ref().subscribers);
    
    // Subscribe
    let subscribe_request = tollmesh::SubscribeRequest {
        topic: "threats:*".to_string(),
    };
    
    let mut stream = client.subscribe(subscribe_request).await?.into_inner();
    
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                println!("Received message:");
                println!("  Topic: {}", msg.topic);
                println!("  Data: {:?}", msg.data);
                println!("  Timestamp: {}", msg.timestamp);
            }
            Err(e) => eprintln!("Stream error: {}", e),
        }
    }
    
    Ok(())
}
```

### Graph Operations

```rust
use tollmesh::{AddNodeRequest, AddEdgeRequest};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    // Add node
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), "Bot 1".to_string());
    properties.insert("type".to_string(), "browser".to_string());
    
    let node_request = AddNodeRequest {
        id: "agent-bot-1".to_string(),
        r#type: "agent".to_string(),
        properties,
    };
    
    let response = client.add_node(node_request).await?;
    
    if response.get_ref().success {
        println!("Node added successfully");
    } else {
        println!("Error: {}", response.get_ref().error);
    }
    
    // Add edge
    let edge_request = AddEdgeRequest {
        source: "agent-bot-1".to_string(),
        target: "threat-evasion-1".to_string(),
        r#type: "detected_by".to_string(),
        weight: 0.9,
    };
    
    let response = client.add_edge(edge_request).await?;
    
    if response.get_ref().success {
        println!("Edge added successfully");
    } else {
        println!("Error: {}", response.get_ref().error);
    }
    
    Ok(())
}
```

### Metrics

```rust
use tollmesh::GetStatsRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    let request = GetStatsRequest {
        component: "all".to_string(),
    };
    
    let response = client.get_stats(request).await?;
    
    println!("Statistics:");
    for (key, value) in &response.get_ref().stats {
        println!("  {}: {}", key, value);
    }
    
    Ok(())
}
```

## Complete Example

```rust
use tonic::transport::Channel;
use tollmesh::{ConsumeRequest, SetRequest, GetRequest, SearchRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    // Rate limiting
    let consume_request = ConsumeRequest {
        key: "user:123".to_string(),
        limit: 100,
        window_ms: 60000,
    };
    
    let response = client.consume(consume_request).await?;
    println!("Rate limit OK: {}", response.get_ref().ok);
    
    // Cache operations
    let set_request = SetRequest {
        key: "mykey".to_string(),
        value: b"myvalue".to_vec(),
    };
    
    client.set(set_request).await?;
    println!("Value cached");
    
    // Search
    let search_request = SearchRequest {
        query: "rate limit".to_string(),
        top_k: 5,
    };
    
    let response = client.search(search_request).await?;
    println!("Found {} results", response.get_ref().results.len());
    
    Ok(())
}
```

## Error Handling

```rust
use tonic::Status;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    let request = ConsumeRequest {
        key: "user:123".to_string(),
        limit: 100,
        window_ms: 60000,
    };
    
    match client.consume(request).await {
        Ok(response) => {
            println!("Success: {:?}", response.get_ref());
        }
        Err(status) => {
            eprintln!("Error code: {}", status.code());
            eprintln!("Error message: {}", status.message());
        }
    }
    
    Ok(())
}
```

## Connection Options

```rust
use tonic::transport::Channel;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    Ok(())
}
```

## Async Patterns

```rust
use futures::future::join_all;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    
    let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
    
    // Concurrent requests
    let mut futures = vec![];
    
    for i in 0..10 {
        let mut c = client.clone();
        let future = tokio::spawn(async move {
            let request = ConsumeRequest {
                key: format!("user:{}", i),
                limit: 100,
                window_ms: 60000,
            };
            c.consume(request).await
        });
        futures.push(future);
    }
    
    let results = join_all(futures).await;
    
    for result in results {
        match result {
            Ok(Ok(response)) => println!("Success: {}", response.get_ref().ok),
            Ok(Err(e)) => eprintln!("Error: {}", e),
            Err(e) => eprintln!("Task error: {}", e),
        }
    }
    
    Ok(())
}
```

## Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tonic::transport::Channel;
    
    #[tokio::test]
    async fn test_rate_limit() {
        let channel = Channel::from_static("http://localhost:50051")
            .connect()
            .await
            .unwrap();
        
        let mut client = tollmesh::toll_mesh_client::TollMeshClient::new(channel);
        
        let request = ConsumeRequest {
            key: "test:123".to_string(),
            limit: 10,
            window_ms: 60000,
        };
        
        let response = client.consume(request).await.unwrap();
        assert!(response.get_ref().ok);
    }
}
```

## Best Practices

1. **Connection Reuse**: Reuse client instances (they implement Clone)
2. **Error Handling**: Use Result types and proper error propagation
3. **Async/Await**: Leverage Tokio for concurrent operations
4. **Timeouts**: Set appropriate connection and request timeouts
5. **Streaming**: Use tokio_stream for handling streams

## Documentation

- [Tonic Documentation](https://docs.rs/tonic/)
- [Protocol Buffers Rust Guide](https://developers.google.com/protocol-buffers/docs/reference/rust-generated)
- [Tokio Documentation](https://tokio.rs/)
- [TollMesh Protocol Documentation](PROTOCOL.md)

## Support

For issues or questions:
- Open an issue on GitHub
- Check [DEVELOPMENT.md](DEVELOPMENT.md)
- Review [PROTOCOL.md](PROTOCOL.md)