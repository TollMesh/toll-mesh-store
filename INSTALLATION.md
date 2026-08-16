# TollMesh Installation Guide

Easy installation and setup for all programming languages.

## Quick Start

Choose your language and follow the simple installation steps below.

---

## 🐹 Go

### Installation

```bash
go get github.com/toll-mesh/store
```

### Quick Example

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/toll-mesh/store/core"
    "github.com/toll-mesh/store/store"
)

func main() {
    config := &core.ClusterConfig{
        NodeName: "node1",
        BindAddr: "127.0.0.1",
        BindPort: 8000,
    }
    
    meshStore, err := store.NewMeshStore(config)
    if err != nil {
        log.Fatal(err)
    }
    defer meshStore.Close()
    
    // Rate limiting
    result, err := meshStore.Consume(context.Background(), "user:123", 100, 60*time.Second)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Rate limit OK: %v, Remaining: %d", result.OK, result.Remaining)
}
```

### Documentation
- [GETTING_STARTED_GO.md](DEVELOPMENT.md) - Full Go guide
- [PROTOCOL.md](PROTOCOL.md) - Protocol documentation

---

## ☕ Java

### Installation

#### Maven

```xml
<dependency>
    <groupId>com.toll-mesh</groupId>
    <artifactId>tollmesh-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

#### Gradle

```gradle
implementation 'com.toll-mesh:tollmesh-client:1.0.0'
```

### Quick Example

```java
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.tollmesh.TollMeshGrpc;
import com.tollmesh.ConsumeRequest;
import com.tollmesh.ConsumeResponse;

public class TollMeshExample {
    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder
            .forAddress("localhost", 50051)
            .usePlaintext()
            .build();
        
        TollMeshGrpc.TollMeshBlockingStub stub = 
            TollMeshGrpc.newBlockingStub(channel);
        
        ConsumeRequest request = ConsumeRequest.newBuilder()
            .setKey("user:123")
            .setLimit(100)
            .setWindowMs(60000)
            .build();
        
        ConsumeResponse response = stub.consume(request);
        System.out.println("Rate limit OK: " + response.getOk());
        
        channel.shutdown();
    }
}
```

### Documentation
- [GETTING_STARTED_JAVA.md](GETTING_STARTED_JAVA.md) - Full Java guide
- [PROTOCOL.md](PROTOCOL.md) - Protocol documentation

---

## 🚀 JavaScript/Node.js

### Installation

```bash
npm install @toll-mesh/client
```

or

```bash
yarn add @toll-mesh/client
```

### Quick Example

```javascript
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const util = require('util');

async function main() {
    const packageDefinition = protoLoader.loadSync('tollmesh.proto');
    const tollmesh = grpc.loadPackageDefinition(packageDefinition).tollmesh;
    
    const client = new tollmesh.TollMesh(
        'localhost:50051',
        grpc.credentials.createInsecure()
    );
    
    const consume = util.promisify(client.consume.bind(client));
    
    const response = await consume({
        key: 'user:123',
        limit: 100,
        window_ms: 60000
    });
    
    console.log('Rate limit OK:', response.ok);
}

main().catch(console.error);
```

### Browser Usage

```html
<script src="tollmesh_pb.js"></script>
<script src="tollmesh_grpc_web_pb.js"></script>
<script>
    const client = new tollmesh.TollMeshClient('http://localhost:8080');
    // Use client...
</script>
```

### Documentation
- [GETTING_STARTED_JS.md](GETTING_STARTED_JS.md) - Full JavaScript guide
- [PROTOCOL.md](PROTOCOL.md) - Protocol documentation

---

## 🦀 Rust

### Installation

Add to `Cargo.toml`:

```toml
[dependencies]
tonic = "0.10"
prost = "0.12"
tokio = { version = "1", features = ["full"] }
tokio-stream = "0.1"

[build-dependencies]
tonic-build = "0.10"
```

### Quick Example

```rust
use tonic::transport::Channel;
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
    println!("Rate limit OK: {}", response.get_ref().ok);
    
    Ok(())
}
```

### Documentation
- [GETTING_STARTED_RUST.md](GETTING_STARTED_RUST.md) - Full Rust guide
- [PROTOCOL.md](PROTOCOL.md) - Protocol documentation

---

## 🐍 Python

### Installation

```bash
pip install tollmesh-client
```

### Quick Example

```python
import grpc
from tollmesh import tollmesh_pb2, tollmesh_pb2_grpc

def main():
    channel = grpc.insecure_channel('localhost:50051')
    stub = tollmesh_pb2_grpc.TollMeshStub(channel)
    
    request = tollmesh_pb2.ConsumeRequest(
        key='user:123',
        limit=100,
        window_ms=60000
    )
    
    response = stub.Consume(request)
    print(f'Rate limit OK: {response.ok}')

if __name__ == '__main__':
    main()
```

### Documentation
- [PROTOCOL.md](PROTOCOL.md) - Protocol documentation

---

## 🔷 C# / .NET

### Installation

```bash
dotnet add package TollMesh.Client
```

### Quick Example

```csharp
using Grpc.Net.Client;
using Tollmesh;

class Program {
    static async Task Main(string[] args) {
        var channel = GrpcChannel.ForAddress("http://localhost:50051");
        var client = new TollMesh.TollMeshClient(channel);
        
        var request = new ConsumeRequest {
            Key = "user:123",
            Limit = 100,
            WindowMs = 60000
        };
        
        var response = await client.ConsumeAsync(request);
        Console.WriteLine($"Rate limit OK: {response.Ok}");
    }
}
```

### Documentation
- [PROTOCOL.md](PROTOCOL.md) - Protocol documentation

---

## 💎 Ruby

### Installation

```bash
gem install tollmesh-client
```

### Quick Example

```ruby
require 'tollmesh'

channel = GRPC::Core::Channel.new('localhost:50051', {})
stub = Tollmesh::TollMesh::Stub.new(channel)

request = Tollmesh::ConsumeRequest.new(
  key: 'user:123',
  limit: 100,
  window_ms: 60000
)

response = stub.consume(request)
puts "Rate limit OK: #{response.ok}"
```

### Documentation
- [PROTOCOL.md](PROTOCOL.md) - Protocol documentation

---

## 🐳 Docker

### Run TollMesh Server

```bash
docker run -d \
  -p 50051:50051 \
  -p 8080:8080 \
  --name tollmesh \
  toll-mesh/store:latest
```

### Docker Compose

```yaml
version: '3.8'

services:
  tollmesh:
    image: toll-mesh/store:latest
    ports:
      - "50051:50051"
      - "8080:8080"
    environment:
      - NODE_NAME=node1
      - BIND_ADDR=0.0.0.0
      - BIND_PORT=50051
```

---

## 🔧 Configuration

### Environment Variables

```bash
# Server configuration
export TOLLMESH_NODE_NAME=node1
export TOLLMESH_BIND_ADDR=0.0.0.0
export TOLLMESH_BIND_PORT=50051
export TOLLMESH_GRPC_PORT=50051
export TOLLMESH_HTTP_PORT=8080

# Client configuration
export TOLLMESH_SERVER=localhost:50051
export TOLLMESH_TIMEOUT=30s
export TOLLMESH_TLS=false
```

### Configuration File

Create `tollmesh.yaml`:

```yaml
server:
  node_name: node1
  bind_addr: 0.0.0.0
  bind_port: 50051
  grpc_port: 50051
  http_port: 8080

client:
  timeout: 30s
  tls: false
  max_retries: 3

features:
  persistence: true
  pubsub: true
  transactions: true
  scripting: true
  search: true
  graph: true
  ranking: true
  agents: true
```

---

## ✅ Verification

### Test Connection

```bash
# Using grpcurl
grpcurl -plaintext localhost:50051 list

# Using curl (HTTP API)
curl http://localhost:8080/health
```

### Run Tests

```bash
# Go
go test ./...

# Java
mvn test

# JavaScript
npm test

# Rust
cargo test

# Python
pytest
```

---

## 🚀 Next Steps

1. **Choose your language** from the options above
2. **Install the client** using the provided commands
3. **Run the quick example** to verify installation
4. **Read the full guide** for your language
5. **Check the protocol documentation** for all available operations

---

## 📚 Documentation

- [README.md](README.md) - Project overview
- [PROTOCOL.md](PROTOCOL.md) - Complete API reference
- [DEVELOPMENT.md](DEVELOPMENT.md) - Development setup
- [ARCHITECTURE.md](ARCHITECTURE.md) - System architecture
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines

---

## 🆘 Troubleshooting

### Connection Issues

```bash
# Check if server is running
netstat -an | grep 50051

# Test connection
telnet localhost 50051

# Check logs
docker logs tollmesh
```

### Import Issues

**Go:**
```bash
go mod tidy
go mod download
```

**Java:**
```bash
mvn clean install
```

**JavaScript:**
```bash
npm install
npm update
```

**Rust:**
```bash
cargo update
cargo build
```

### Performance Issues

- Increase connection pool size
- Enable compression
- Use batch operations
- Monitor metrics with `GetStats()`

---

## 📞 Support

- **Issues**: Open an issue on GitHub
- **Discussions**: Use GitHub Discussions
- **Documentation**: Check [PROTOCOL.md](PROTOCOL.md)
- **Examples**: See language-specific guides

---

## 📄 License

TollMesh is released under the MIT License. See [LICENSE](LICENSE) for details.

---

**Created by**: Prakhar Tripathi & Mayaplus (Family Company)

**Version**: 1.0.0

**Last Updated**: August 2026