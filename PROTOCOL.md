# TollMesh Protocol Documentation

## Overview

TollMesh uses gRPC for efficient multi-language communication. This document describes the protocol, message formats, and API endpoints.

## Protocol Specification

### Transport
- **Protocol**: gRPC over HTTP/2
- **Serialization**: Protocol Buffers (protobuf3)
- **Port**: 50051 (default)
- **TLS**: Supported (recommended for production)

### Service Definition

The TollMesh service is defined in `api/tollmesh.proto` and provides the following RPC methods:

## API Reference

### Rate Limiting Operations

#### Consume
Consumes tokens from a rate limit bucket.

**Request:**
```protobuf
message ConsumeRequest {
  string key = 1;           // Rate limit key
  int32 limit = 2;          // Token limit
  int64 window_ms = 3;      // Time window in milliseconds
}
```

**Response:**
```protobuf
message ConsumeResponse {
  bool ok = 1;              // Whether consumption was allowed
  int32 remaining = 2;      // Remaining tokens
  int64 reset_at = 3;       // Unix timestamp when limit resets
}
```

**Example:**
```
Request: key="user:123", limit=100, window_ms=60000
Response: ok=true, remaining=99, reset_at=1692115200000
```

#### GetRateLimit
Gets current rate limit status.

**Request:**
```protobuf
message GetRateLimitRequest {
  string key = 1;           // Rate limit key
}
```

**Response:**
```protobuf
message GetRateLimitResponse {
  int32 remaining = 1;      // Remaining tokens
  int64 reset_at = 2;       // Unix timestamp when limit resets
}
```

### Cache Operations

#### Get
Retrieves a value from cache.

**Request:**
```protobuf
message GetRequest {
  string key = 1;           // Cache key
}
```

**Response:**
```protobuf
message GetResponse {
  bool found = 1;           // Whether key exists
  bytes value = 2;          // Cached value
}
```

#### Set
Sets a value in cache.

**Request:**
```protobuf
message SetRequest {
  string key = 1;           // Cache key
  bytes value = 2;          // Value to cache
}
```

**Response:**
```protobuf
message SetResponse {
  bool success = 1;         // Whether operation succeeded
}
```

#### Delete
Deletes a value from cache.

**Request:**
```protobuf
message DeleteRequest {
  string key = 1;           // Cache key
}
```

**Response:**
```protobuf
message DeleteResponse {
  bool success = 1;         // Whether operation succeeded
}
```

### Replay Protection

#### Seen
Checks if a request has been seen before (replay protection).

**Request:**
```protobuf
message SeenRequest {
  string key = 1;           // Request identifier
}
```

**Response:**
```protobuf
message SeenResponse {
  bool seen = 1;            // Whether request was seen before
}
```

### Search Operations

#### Search
Performs full-text search using BM25.

**Request:**
```protobuf
message SearchRequest {
  string query = 1;         // Search query
  int32 top_k = 2;          // Number of results to return
}
```

**Response:**
```protobuf
message SearchResponse {
  repeated SearchResult results = 1;
}

message SearchResult {
  string id = 1;            // Result ID
  string content = 2;       // Result content
  float score = 3;          // Relevance score
  int32 rank = 4;           // Result rank
}
```

#### SearchVector
Performs vector similarity search.

**Request:**
```protobuf
message SearchVectorRequest {
  repeated float vector = 1; // Query vector
  int32 top_k = 2;          // Number of results to return
}
```

**Response:**
```protobuf
message SearchVectorResponse {
  repeated SearchResult results = 1;
}
```

### Graph Operations

#### AddNode
Adds a node to the knowledge graph.

**Request:**
```protobuf
message AddNodeRequest {
  string id = 1;            // Node ID
  string type = 2;          // Node type (agent, threat, policy, etc.)
  map<string, string> properties = 3; // Node properties
}
```

**Response:**
```protobuf
message AddNodeResponse {
  bool success = 1;         // Whether operation succeeded
  string error = 2;         // Error message if failed
}
```

#### AddEdge
Adds an edge between two nodes.

**Request:**
```protobuf
message AddEdgeRequest {
  string source = 1;        // Source node ID
  string target = 2;        // Target node ID
  string type = 3;          // Edge type (related_to, detected_by, etc.)
  float weight = 4;         // Edge weight
}
```

**Response:**
```protobuf
message AddEdgeResponse {
  bool success = 1;         // Whether operation succeeded
  string error = 2;         // Error message if failed
}
```

#### Reason
Performs reasoning over the knowledge graph.

**Request:**
```protobuf
message ReasonRequest {
  string query = 1;         // Reasoning query
  map<string, string> context = 2; // Context information
}
```

**Response:**
```protobuf
message ReasonResponse {
  string reasoning = 1;     // Reasoning result
  repeated string nodes = 2; // Involved nodes
  repeated string edges = 3; // Involved edges
}
```

### Agent Operations

#### RegisterAgent
Registers a new agent.

**Request:**
```protobuf
message RegisterAgentRequest {
  string id = 1;            // Agent ID
  string name = 2;          // Agent name
  repeated string capabilities = 3; // Agent capabilities
  float reputation = 4;     // Initial reputation
}
```

**Response:**
```protobuf
message RegisterAgentResponse {
  bool success = 1;         // Whether operation succeeded
  string error = 2;         // Error message if failed
}
```

#### FindSimilarAgents
Finds agents with similar capabilities.

**Request:**
```protobuf
message FindSimilarAgentsRequest {
  string agent_id = 1;      // Reference agent ID
  int32 top_k = 2;          // Number of results to return
}
```

**Response:**
```protobuf
message FindSimilarAgentsResponse {
  repeated Agent agents = 1;
}

message Agent {
  string id = 1;            // Agent ID
  string name = 2;          // Agent name
  repeated string capabilities = 3; // Agent capabilities
  float reputation = 4;     // Agent reputation
  int64 last_seen = 5;      // Last seen timestamp
}
```

#### CoordinateAgents
Establishes coordination between two agents.

**Request:**
```protobuf
message CoordinateAgentsRequest {
  string agent_id_1 = 1;    // First agent ID
  string agent_id_2 = 2;    // Second agent ID
}
```

**Response:**
```protobuf
message CoordinateAgentsResponse {
  bool success = 1;         // Whether operation succeeded
  string error = 2;         // Error message if failed
}
```

### Pub/Sub Operations

#### Subscribe
Subscribes to a topic (streaming).

**Request:**
```protobuf
message SubscribeRequest {
  string topic = 1;         // Topic to subscribe to
}
```

**Response (streaming):**
```protobuf
message Message {
  string topic = 1;         // Message topic
  bytes data = 2;           // Message data
  int64 timestamp = 3;      // Message timestamp
}
```

#### Publish
Publishes a message to a topic.

**Request:**
```protobuf
message PublishRequest {
  string topic = 1;         // Topic to publish to
  bytes data = 2;           // Message data
}
```

**Response:**
```protobuf
message PublishResponse {
  bool success = 1;         // Whether operation succeeded
  int32 subscribers = 2;    // Number of subscribers notified
}
```

### Metrics

#### GetStats
Gets system statistics.

**Request:**
```protobuf
message GetStatsRequest {
  string component = 1;     // Component: "all", "store", "search", "graph", "agents"
}
```

**Response:**
```protobuf
message GetStatsResponse {
  map<string, string> stats = 1; // Statistics map
  int64 timestamp = 2;      // Response timestamp
}
```

## Error Handling

All RPC methods return gRPC status codes:

- `OK (0)`: Success
- `CANCELLED (1)`: Operation cancelled
- `UNKNOWN (2)`: Unknown error
- `INVALID_ARGUMENT (3)`: Invalid argument
- `DEADLINE_EXCEEDED (4)`: Deadline exceeded
- `NOT_FOUND (5)`: Resource not found
- `ALREADY_EXISTS (6)`: Resource already exists
- `PERMISSION_DENIED (7)`: Permission denied
- `RESOURCE_EXHAUSTED (8)`: Resource exhausted
- `FAILED_PRECONDITION (9)`: Failed precondition
- `ABORTED (10)`: Operation aborted
- `OUT_OF_RANGE (11)`: Out of range
- `UNIMPLEMENTED (12)`: Not implemented
- `INTERNAL (13)`: Internal error
- `UNAVAILABLE (14)`: Service unavailable
- `DATA_LOSS (15)`: Data loss
- `UNAUTHENTICATED (16)`: Unauthenticated

## Connection Examples

### Go Client
```go
import "google.golang.org/grpc"

conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
if err != nil {
    log.Fatalf("Failed to connect: %v", err)
}
defer conn.Close()

client := tollmesh.NewTollMeshClient(conn)
```

### Python Client
```python
import grpc
from tollmesh import tollmesh_pb2, tollmesh_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = tollmesh_pb2_grpc.TollMeshStub(channel)
```

### JavaScript Client
```javascript
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const packageDefinition = protoLoader.loadSync('tollmesh.proto');
const tollmesh = grpc.loadPackageDefinition(packageDefinition).tollmesh;

const client = new tollmesh.TollMesh('localhost:50051', grpc.credentials.createInsecure());
```

## Best Practices

1. **Connection Pooling**: Reuse connections for multiple requests
2. **Timeouts**: Always set appropriate timeouts for requests
3. **Error Handling**: Implement proper error handling for all RPC calls
4. **Compression**: Enable gRPC compression for large payloads
5. **TLS**: Use TLS in production environments
6. **Monitoring**: Monitor gRPC metrics (latency, error rates, etc.)

## Performance Considerations

- **Batch Operations**: Group multiple operations when possible
- **Streaming**: Use streaming for large result sets
- **Caching**: Cache frequently accessed data
- **Connection Reuse**: Maintain persistent connections

## Version History

- **v1.0** (August 2026): Initial release

## Support

For issues or questions about the protocol:
- Open an issue on GitHub
- Check the documentation in DEVELOPMENT.md
- Review examples in the clients directory