---
layout: default
title: vs Redis
nav_order: 11
---

# TollMeshCache vs Redis

A detailed comparison for choosing the right solution.

## Feature Comparison

| Feature | TollMeshCache | Redis |
|---------|---------------|-------|
| **Language Support** | 7 languages (SDKs) | CLI-based, client libraries |
| **Rate Limiting** | ✅ Built-in CRDT | ⚠️ Requires custom logic |
| **Replay Protection** | ✅ Built-in GSet | ⚠️ Requires custom logic |
| **Distributed Caching** | ✅ TTL-based | ✅ Full caching support |
| **No Central Server** | ✅ Peer-to-peer | ❌ Single point of failure |
| **CRDT Convergence** | ✅ Automatic | ⚠️ Requires Sentinel/Cluster |
| **Type Safety** | ✅ Native types | ⚠️ String-based keys/values |
| **Async Support** | ✅ All SDKs | ⚠️ Requires client impl |
| **Zero Config** | ✅ Auto-discovery | ❌ Requires setup |
| **Memory Efficient** | ✅ Optimized | ✅ Highly optimized |

---

## Architecture Comparison

### TollMeshCache (Peer-to-Peer)

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Node 1  │────→│ Node 2  │────→│ Node 3  │
│ (Cache) │←────│(Cache)  │←────│(Cache)  │
└─────────┘     └─────────┘     └─────────┘
     ↑                              ↑
     └──────────────────────────────┘
    Automatic State Convergence (CRDT)
```

**Advantages:**
- No single point of failure
- Self-healing cluster
- Automatic conflict resolution
- Scales horizontally
- Works in edge/distributed scenarios

### Redis (Master-Slave/Cluster)

```
         ┌──────────────┐
         │ Redis Master │
         └──────────────┘
              ↓ ↓ ↓
    ┌──────────┴──────────┐
    ↓          ↓          ↓
┌────────┐ ┌────────┐ ┌────────┐
│ Slave1 │ │ Slave2 │ │ Slave3 │
└────────┘ └────────┘ └────────┘
```

**Advantages:**
- Battle-tested, mature
- High throughput
- Rich data structures
- Pub/Sub support
- Large ecosystem

---

## Use Case Comparison

### Use TollMeshCache When:

✅ **Distributed Rate Limiting**
- No central coordinator needed
- Automatic convergence across regions
- Works without synchronous calls

✅ **Replay Protection**
- Built-in nonce tracking
- Automatic cleanup
- CRDT-based deduplication

✅ **Edge Computing**
- Distributed cache at edge nodes
- No cloud latency
- Automatic sync between edges

✅ **Microservices**
- Each service has own cache
- No shared Redis dependency
- Reduces deployment complexity

✅ **Zero-Config Caching**
- Drop-in replacement
- Auto-discovery
- Self-healing

### Use Redis When:

✅ **High Throughput**
- 100k+ operations/second
- Need maximum performance
- In-memory sorted sets, hashes

✅ **Complex Data Structures**
- Sorted sets, streams, graphs
- Pub/Sub messaging
- Transaction support (MULTI/EXEC)

✅ **Session Management**
- Large session objects
- Frequent updates
- TTL expiration (similar to TollMeshCache)

✅ **Real-Time Analytics**
- High-frequency stats
- Counters, HyperLogLog
- Sliding windows

✅ **Existing Redis Ecosystem**
- Team expertise
- Monitoring/alerting setup
- Operational knowledge

---

## Performance Comparison

### Throughput

| Operation | TollMeshCache | Redis |
|-----------|---------------|-------|
| Rate Limit Check | 50k/sec/node | 100k+/sec |
| Replay Check | 50k/sec/node | 100k+/sec |
| Cache Get | 50k/sec/node | 100k+/sec |
| Cache Set | 50k/sec/node | 100k+/sec |

**Note:** TollMeshCache scales horizontally with added nodes. Redis requires sharding.

### Latency

| Operation | TollMeshCache | Redis |
|-----------|---------------|-------|
| P50 | 1-2ms | <1ms |
| P99 | 5-10ms | 2-5ms |
| P99.9 | 20-50ms | 10-20ms |

**Note:** TollMeshCache includes network roundtrip; Redis is in-process.

### Memory per Node

| Scenario | TollMeshCache | Redis |
|----------|---------------|-------|
| 1M keys | ~100MB | ~50MB |
| 10M keys | ~1GB | ~500MB |
| 100M keys | ~10GB | ~5GB |

**Note:** TollMeshCache includes CRDT metadata; both are O(n).

---

## Operational Complexity

### TollMeshCache

**Setup:** Low
- Create client instance
- Point to cluster
- Done

**Monitoring:** Medium
- Health checks per SDK
- Peer discovery status
- CRDT convergence metrics

**Scaling:** Easy
- Add new node
- Auto-discover peers
- Automatic state sync

**Failure Recovery:** Automatic
- Nodes rejoin cluster
- State automatically restored
- No manual intervention

### Redis

**Setup:** Medium
- Install & configure
- Setup master/sentinel/cluster
- Configure persistence
- Setup replication

**Monitoring:** High
- Memory monitoring
- Replication lag
- Master failover
- Cluster rebalancing

**Scaling:** Complex
- Manual shard key planning
- Data migration
- Rebalancing downtime
- Key distribution issues

**Failure Recovery:** Manual
- Master failover requires action
- Sync data before promotion
- Monitoring & alerting required

---

## Cost Analysis

### TollMeshCache

**Compute Cost:** O(1) per node
- Each node runs independent computation
- No central coordinator
- Cost scales with cluster size

**Network Cost:** Minimal gossip
- P2P state sync (minimal bandwidth)
- No central hub traffic
- Efficient CRDT messages

**Operational Cost:** Low
- Auto-healing
- Self-managed cluster
- Fewer operational tasks

### Redis

**Compute Cost:** Single master
- Master is bottleneck
- Reads scale with replicas
- Cluster adds complexity

**Network Cost:** Moderate
- Replication traffic
- Cluster gossip protocol
- More network overhead

**Operational Cost:** High
- Dedicated ops team
- Monitoring & alerting
- Planned maintenance
- Failover management

---

## Decision Matrix

Choose **TollMeshCache** if:
- [ ] Need distributed coordination without central server
- [ ] Automatic conflict resolution important
- [ ] Want to avoid single point of failure
- [ ] Need built-in rate limiting + replay protection
- [ ] Running edge/distributed infrastructure
- [ ] Want zero-config deployment

Choose **Redis** if:
- [ ] Need absolute highest throughput (100k+ ops/sec)
- [ ] Require complex data structures (sorted sets, streams)
- [ ] Team is Redis-expert
- [ ] Need existing Redis ecosystem (Sentinel, Cluster, Streams)
- [ ] Want battle-tested, mature solution
- [ ] Building real-time pub/sub system

---

## Migration Guide

### From Redis to TollMeshCache

1. **Identify what Redis is used for:**
   - Rate limiting → Use `consume()`
   - Replay protection → Use `seen()`
   - General caching → Use `cache_get/set()`
   - Other features → Keep Redis for now

2. **Parallel run:** Use both simultaneously
   - Install TollMeshCache SDK
   - Route rate limiting & replay checks to TollMeshCache
   - Keep Redis for other operations

3. **Migrate gradually:**
   - Move one rate limit bucket at a time
   - Monitor convergence
   - Remove from Redis after validation

4. **Deprecate Redis:**
   - Once all use cases migrated
   - Remove Redis from infrastructure
   - Update documentation

### From TollMeshCache to Redis

1. **Identify scaling issues:** Is rate limiting the bottleneck?
2. **Parallel run:** Add Redis alongside TollMeshCache
3. **Migrate traffic:** Gradually move operations to Redis
4. **Monitor:** Ensure no regression
5. **Deprecate:** Remove TollMeshCache once validated

---

## Conclusion

- **TollMeshCache**: Best for distributed coordination, replicated caching, zero-config
- **Redis**: Best for high throughput, complex data structures, real-time systems

**Best Practice:** Use both!
- TollMeshCache for distributed rate limiting & replay protection
- Redis for high-frequency caching & real-time features
- Complementary, not competitive

Each solves different problems. Choose based on your actual requirements, not hype.
