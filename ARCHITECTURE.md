# TollMeshStore Architecture

## Overview

TollMeshStore is a distributed storage system designed to replace Redis in Toll's infrastructure. It uses Conflict-Free Replicated Data Types (CRDTs) and gossip protocols to provide automatic convergence without requiring a central coordinator.

## Design Principles

1. **Decentralized**: No single point of failure
2. **Convergent**: All nodes eventually reach the same state
3. **Scalable**: Linear scaling with number of nodes
4. **Simple**: Minimal dependencies and complexity
5. **Production-Ready**: Comprehensive error handling and testing

## Core Concepts

### CRDTs (Conflict-Free Replicated Data Types)

CRDTs are data structures that can be replicated across multiple computers in a network, where the replicas can be updated independently and concurrently without coordination between replicas, and where it is mathematically guaranteed that all replicas will eventually converge to the same state.

#### GCounter (Grow-only Counter)

**Purpose**: Distributed rate limiting

**Design**:
```
GCounter = {
  counts: Map<NodeID, Int>
}

Value = Sum(counts.values())
```

**Operations**:
- `Increment(nodeID)`: Increments the counter for a specific node
- `Value()`: Returns the total count across all nodes
- `Merge(other)`: Merges another counter, taking max value per node

**Properties**:
- Monotonically increasing
- Commutative: order of operations doesn't matter
- Idempotent: applying same operation multiple times has same effect
- Convergent: all nodes eventually reach same value

**Example**:
```
Node1: {node1: 3, node2: 2} = 5
Node2: {node1: 2, node2: 4} = 6

After merge:
Both: {node1: 3, node2: 4} = 7
```

#### GSet (Grow-only Set)

**Purpose**: Distributed replay protection

**Design**:
```
GSet = {
  items: Set<String>
}
```

**Operations**:
- `Add(item)`: Adds item to the set
- `Contains(item)`: Checks if item exists
- `Merge(other)`: Merges another set (union)

**Properties**:
- Only supports add operations
- Commutative: order doesn't matter
- Idempotent: adding same item multiple times is same as adding once
- Convergent: all nodes eventually have same items

**Example**:
```
Node1: {nonce1, nonce2}
Node2: {nonce2, nonce3}

After merge:
Both: {nonce1, nonce2, nonce3}
```

#### ExpiringSet

**Purpose**: TTL-based caching with automatic cleanup

**Design**:
```
ExpiringSet = {
  items: Map<String, Timestamp>
}
```

**Operations**:
- `Add(item, ttl)`: Adds item with expiration time
- `Contains(item)`: Checks if item exists and not expired
- `cleanup()`: Background goroutine removes expired items

**Properties**:
- Items automatically expire after TTL
- Background cleanup runs every 30 seconds
- Thread-safe with RWMutex

### MeshStore

**Purpose**: Unified interface for distributed coordination

**Components**:
1. **Rate Limiters**: Map of GCounter per key
2. **Replay Protection**: Single GSet for all nonces
3. **Cache**: Nested map with TTL tracking
4. **Background Cleanup**: Goroutine for cache expiration

**Thread Safety**:
- All operations protected with RWMutex
- Read operations use RLock
- Write operations use Lock

## Data Flow

### Rate Limiting Flow

```
Client Request
    ↓
Consume(key, limit, window)
    ↓
Get or Create GCounter for key
    ↓
Check current value < limit?
    ↓
Yes: Increment counter, return OK
No: Return rate limited
    ↓
Gossip protocol syncs with peers
    ↓
All nodes converge to same count
```

### Replay Protection Flow

```
Client Request with Nonce
    ↓
Seen(nonce, ttl)
    ↓
Check if nonce in GSet?
    ↓
Yes: Return true (replay detected)
No: Add to GSet, return false
    ↓
Gossip protocol syncs with peers
    ↓
All nodes know about nonce
```

### Caching Flow

```
Set(namespace, key, value, ttl)
    ↓
Store in cache[namespace][key]
    ↓
Record expiration time
    ↓
Background cleanup removes expired entries
    ↓
Get(namespace, key)
    ↓
Check if exists and not expired
    ↓
Return value or not found
```

## Gossip Protocol (Future)

The current implementation is single-node ready. The gossip protocol will enable:

1. **Peer Discovery**: Automatic discovery of cluster members
2. **State Sync**: Periodic exchange of CRDT state
3. **Conflict Resolution**: Automatic convergence via CRDT properties
4. **Failure Handling**: Graceful handling of node failures

### Gossip Algorithm

```
Every T seconds:
  1. Select random peer
  2. Send current CRDT state
  3. Receive peer's CRDT state
  4. Merge states (CRDT handles conflicts)
  5. Update local state
```

## Performance Analysis

### Time Complexity

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Consume | O(1) | Hash map lookup + counter increment |
| Seen | O(1) | Hash set lookup + add |
| Get | O(1) | Nested hash map lookup |
| Set | O(1) | Nested hash map insert |
| Merge | O(n) | n = number of unique keys |

### Space Complexity

| Component | Complexity | Notes |
|-----------|-----------|-------|
| Rate Limiters | O(k*n) | k = keys, n = nodes |
| Replay Protection | O(m) | m = unique nonces |
| Cache | O(c) | c = cached items |
| Total | O(k*n + m + c) | Linear in data size |

### Scalability

- **Horizontal**: Add nodes without central coordinator
- **Vertical**: Each node can handle independent operations
- **Network**: Gossip protocol uses O(log n) messages per node

## Consistency Model

### Eventual Consistency

- **Strong Eventual Consistency (SEC)**: All nodes eventually converge to same state
- **Conflict-free**: CRDTs guarantee no conflicts
- **Deterministic**: Same operations always produce same result

### Guarantees

1. **Availability**: Always available for reads/writes
2. **Partition Tolerance**: Works across network partitions
3. **Eventual Consistency**: All nodes converge

(Trades off immediate consistency for availability)

## Security Considerations

1. **Replay Protection**: GSet prevents replay attacks
2. **Rate Limiting**: GCounter prevents abuse
3. **TTL Expiration**: Automatic cleanup prevents stale data
4. **Thread Safety**: RWMutex prevents race conditions

## Integration Points

### With Toll

```go
// Toll's Store interface
type Store interface {
    Consume(ctx context.Context, key string, limit int, window time.Duration) (ConsumeResult, error)
    Seen(ctx context.Context, key string, ttl time.Duration) (bool, error)
    Get(ctx context.Context, ns, key string) ([]byte, bool, error)
    Set(ctx context.Context, ns, key string, value []byte, ttl time.Duration) error
    Close() error
}

// MeshStore implements this interface
var store Store = meshStore
```

## Testing Strategy

### Unit Tests
- CRDT operations (increment, merge, add, contains)
- Individual MeshStore methods
- Edge cases and error conditions

### Integration Tests
- Multiple concurrent operations
- TTL expiration
- Background cleanup

### E2E Tests
- Full workflow with rate limiting, replay protection, caching
- Concurrent access patterns
- Stress testing

## Future Enhancements

### Phase 1: Gossip Protocol
- Implement peer-to-peer state synchronization
- Add cluster membership management
- Handle node failures

### Phase 2: HTTP API
- REST endpoints for inter-node communication
- Metrics export
- Health checks

### Phase 3: Persistence
- Optional disk-based persistence
- Crash recovery
- Snapshot/restore

### Phase 4: Advanced Features
- Sharding for horizontal scaling
- Replication factor configuration
- Custom conflict resolution

## References

- [CRDTs: Consistency without concurrency control](https://arxiv.org/abs/0907.0929)
- [A comprehensive study of CRDT](https://arxiv.org/abs/1805.06358)
- [Gossip Algorithms](https://en.wikipedia.org/wiki/Gossip_protocol)