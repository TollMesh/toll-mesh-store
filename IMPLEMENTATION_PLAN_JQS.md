# Implementation Plan: Job Queues, Sorted Sets, Streams

## Philosophy: CRDT-Based, Distributed, Zero-Config

All three features must maintain TollMeshCache's core principles:
- **No central coordinator** - Peer-to-peer operation
- **CRDT-based** - Automatic convergence
- **Zero-config** - Self-discovery, self-healing
- **High performance** - O(1) or O(log n) operations
- **Type-safe** - Native types across all languages

---

## 1. JOB QUEUES (Priority: Critical)

### Architecture: Replicated Task Log with Vector Clocks

**Core Insight:** Jobs are append-only log entries with distributed acknowledgment tracking.

### Data Structure

```
type Job struct {
    ID              string                    // UUID
    Queue           string                    // Queue name
    Payload         []byte                    // Job data
    Status          JobStatus                 // pending, processing, done, failed
    Priority        int                       // 0-10 (higher = urgent)
    Timestamp       int64                     // Creation time (lamport clock)
    VectorClock     map[string]int64          // Distributed ordering
    RetryCount      int                       // Failed attempts
    MaxRetries      int                       // Retry limit
    ProcessedBy     string                    // Node that claimed it
    Result          []byte                    // Job result
    Error           string                    // Error message if failed
    CreatedAt       int64
    DeadlineAt      int64                     // When job expires
}

type JobQueue struct {
    Name            string
    Jobs            []Job                     // Append-only log
    Subscribers     map[string]*Subscriber    // Workers
    mu              sync.RWMutex
    vectorClock     map[string]int64
}

type JobManager struct {
    mu              sync.RWMutex
    queues          map[string]*JobQueue
    acknowledgments map[string]int64          // Track processed jobs per node
    deadLetterQueue []Job                     // Failed jobs
    maxDLQSize      int
}
```

### Algorithm: Distributed Job Claiming

**Goal:** Ensure each job is processed exactly once without central coordinator.

**Process:**
1. Job enqueued → Added to local log with vector clock increment
2. Gossip protocol spreads job to all nodes
3. Workers compete to claim job using CAS (Compare-And-Swap)
4. First to update status to "processing" wins
5. On completion, update status and acknowledge
6. Gossip propagates acknowledgment
7. After all nodes acknowledge → Job considered complete
8. Replicate complete status across cluster

**Failure Handling:**
- If node crashes during processing: Job status reverts to "pending" after timeout
- Dead-letter queue after max retries
- Automatic retry with exponential backoff

### Implementation Order

**Phase 1: Core Job Log**
```go
// job_queue.go
- NewJobQueue()
- Enqueue(job) -> error
- GetNextJob(workerID) -> (job, error)  // Attempts to claim
- MarkComplete(jobID, result) -> error
- GetStatus(jobID) -> (status, error)
```

**Phase 2: Distributed Coordination**
```go
// job_manager.go
- MultiNodeEnqueue()
- DistributedClaim()   // Vector clock ordering
- AckProcessed()       // Cross-node acknowledgment
- RetryExpired()       // Cleanup loop
```

**Phase 3: Dead-Letter Queue & Observability**
```go
// job_dlq.go
- MoveToDeadLetter(jobID, reason)
- GetDeadLetterQueue() -> []Job
- ReplayDeadLetter(jobID) -> error
```

### SDK Implementation (Python Example)

```python
# High-level API
client.enqueue('email-queue', {
    'to': 'user@example.com',
    'subject': 'Welcome'
}, priority=5, deadline=timedelta(hours=1))

# Worker
@client.worker('email-queue')
def process_email(job):
    send_email(job['to'], job['subject'])
    return {'sent_at': time.time()}

# Run worker
client.start_worker('email-queue', num_workers=4)
```

### Performance Targets
- **Enqueue:** O(1), ~0.1ms
- **Claim:** O(log n), ~1-5ms
- **Throughput:** 10k jobs/sec per node
- **Latency:** P99 < 100ms

---

## 2. SORTED SETS (Priority: High)

### Architecture: Replicated Score-Based Skip Lists

**Challenge:** Maintaining sorted order in distributed system without coordination.

**Solution:** Combine skip lists with vector clocks for distributed ordering.

### Data Structure

```
type SortedSetMember struct {
    Member    string        // Value
    Score     float64       // Sort key
    Timestamp int64         // Lamport clock for conflict resolution
    Node      string        // Who added it
}

type SortedSet struct {
    Name      string
    Members   []SortedSetMember         // Score-sorted
    Index     map[string]SortedSetMember // Quick lookup
    Levels    [][]SortedSetMember        // Skip list levels
    mu        sync.RWMutex
}

type SortedSetManager struct {
    sets      map[string]*SortedSet
    replicas  []Node                    // For replication
}
```

### Algorithm: Conflict-Free Sorted Sets

**Key Principle:** Use (score, timestamp, node) as composite key for total ordering.

```
Comparison(a, b):
    if a.score != b.score:
        return a.score < b.score
    elif a.timestamp != b.timestamp:
        return a.timestamp < b.timestamp  // Lamport clock
    else:
        return a.node < b.node            // Lexicographic tie-break
```

**Operations:**

1. **Add(member, score)**
   - Find position using skip list search
   - Add with current timestamp + node ID
   - No coordination needed
   - Gossip propagates addition

2. **Range(min, max, limit)**
   - Skip list binary search for range
   - Return top-k in score order
   - Consistent across all nodes due to deterministic ordering

3. **RemoveByRank(rank)**
   - Find element at rank position
   - Mark with tombstone (not immediate delete)
   - Tombstone eventually removed after TTL

### Use Cases

```
# Leaderboards
client.zadd('game-scores', player_id, score)
client.zrange('game-scores', 0, 9)  # Top 10
client.zrank('game-scores', player_id)

# Real-time analytics
client.zadd('latencies-1m', request_id, latency_ms)
client.zcount('latencies-1m', 0, 100)  # < 100ms

# Priority queues (alternative to job queues)
client.zadd('tasks', task_id, priority)
```

### Implementation Order

**Phase 1: Skip List Core**
```go
// skiplist.go
- NewSkipList()
- Insert(member, score)
- Search(member) -> (score, found)
- Range(min, max) -> []Member
- Rank(member) -> int
```

**Phase 2: CRDT Coordination**
```go
// sorted_set_crdt.go
- Add(member, score) with vector clock
- Remove(member) with tombstone
- Merge(remoteSet)  // Convergence
- ConflictResolve(local, remote) using composite key
```

**Phase 3: Indexes & Optimization**
```go
// sorted_set_indexes.go
- CountByScore(min, max)
- RangeByRank(start, stop)
- RevRange(max, min)  // Descending
```

### Performance Targets
- **Add:** O(log n), ~0.5ms
- **Range:** O(log n + k), ~1-2ms
- **Rank:** O(log n), ~0.5ms
- **Memory:** O(n) with skip list overhead ~20%

---

## 3. STREAMS (Priority: Medium-High)

### Architecture: Replicated Append-Only Event Log

**Design:** Similar to Apache Kafka but with CRDT-based consumer groups.

### Data Structure

```
type StreamEntry struct {
    ID              string            // Timestamp-ID: "1723456789-0"
    Timestamp       int64             // Milliseconds
    Fields          map[string]string // Key-value pairs
    Node            string            // Producer node
    VectorClock     map[string]int64
}

type StreamConsumerGroup struct {
    Name            string
    Stream          string
    Consumers       map[string]*Consumer  // Consumer -> last offset
    LastEntry       string                // Highest ID processed
}

type Consumer struct {
    ID              string
    Stream          string
    Group           string
    LastOffset      string            // Last processed entry ID
    LastHeartbeat   int64
}

type StreamManager struct {
    mu              sync.RWMutex
    streams         map[string][]StreamEntry       // Append-only logs
    consumerGroups  map[string]*StreamConsumerGroup
    retentionPolicy RetentionPolicy
}

type RetentionPolicy struct {
    MaxAge     time.Duration  // Keep last 24 hours
    MaxSize    int64          // Keep last 1GB
    MaxEntries int64          // Keep last 1M entries
}
```

### Algorithm: Distributed Consumer Groups

**Challenge:** Tracking which entries have been consumed without central coordinator.

**Solution:** 
1. Each consumer maintains local offset
2. Consumer group stores offsets for all consumers
3. Gossip protocol syncs offsets
4. Entry considered "consumed" when all consumers in group have processed it

```
ConsumeEntry(groupID, consumerID):
    1. Read entry at consumer's last offset
    2. Process entry
    3. Update consumer.lastOffset locally
    4. Gossip new offset to all nodes
    5. Auto-balance: If consumer dies, others take over
```

### Operations

```go
type Stream interface {
    // Producer side
    Add(entry) (entryID string, error)
    
    // Consumer side
    Read(groupID, consumerID, startID, count) ([]Entry, error)
    Ack(groupID, consumerID, entryID) error  // Mark as processed
    
    // Group management
    CreateGroup(groupID, startID) error
    GetGroupStatus(groupID) (status, error)
}
```

### Use Cases

```
# Event sourcing
stream = client.stream('user-events')
stream.add({'event': 'login', 'user_id': '123', 'ip': '1.2.3.4'})

# Real-time processing
group = stream.consumer_group('event-processor')
while True:
    events = group.read(count=100)
    process(events)
    group.ack(events[-1].id)

# Audit logs
stream.add({'action': 'delete_user', 'user_id': '456', 'admin': 'alice'})
audit_entries = stream.range('-', '0')  # All entries
```

### Implementation Order

**Phase 1: Append-Only Log**
```go
// stream.go
- NewStream(name)
- Add(entry) -> (id, error)
- Get(id) -> (entry, error)
- Range(start, end, limit) -> []Entry
```

**Phase 2: Consumer Groups**
```go
// stream_consumer_group.go
- CreateConsumerGroup(name, startID)
- Read(groupID, consumerID, count)
- Ack(groupID, consumerID, entryID)
- GetGroupStats() -> offsets per consumer
```

**Phase 3: Retention & Cleanup**
```go
// stream_retention.go
- Trim(maxAge, maxSize)
- CleanupExpiredEntries()
- Backfill(entries)
```

### Performance Targets
- **Add:** O(1), ~0.2ms
- **Read:** O(k), ~1-2ms for k entries
- **Ack:** O(log n), ~0.5ms
- **Group rebalance:** < 5 seconds
- **Memory:** O(n) with retention policy

---

## Implementation Timeline & Dependencies

### Week 1-2: Job Queues
```
Day 1-2:   Job struct + core append-only log
Day 3-4:   Vector clock distributed ordering
Day 5:     Claiming algorithm + acknowledgment
Day 6-7:   Retry logic + dead-letter queue
Day 8-10:  SDK implementations (7 languages)
Day 11-14: Testing + benchmarking
```

### Week 3: Sorted Sets
```
Day 1-2:   Skip list implementation
Day 3-4:   CRDT-based conflict resolution
Day 5-6:   Replication + gossip
Day 7-8:   SDK implementations
Day 9-10:  Testing + benchmarking
```

### Week 4: Streams
```
Day 1-2:   Append-only log + entry ID generation
Day 3-4:   Consumer group tracking
Day 5-6:   Acknowledgment protocol
Day 7-8:   Retention policies
Day 9-10:  SDK implementations + testing
```

---

## Quality Standards (Non-Negotiable)

### Code Quality
- ✅ 95%+ test coverage
- ✅ Benchmark tests for performance
- ✅ Integration tests (multi-node scenarios)
- ✅ Zero panics in production paths
- ✅ Proper error types and handling

### Performance
- ✅ All operations meet latency targets
- ✅ Memory usage monitored
- ✅ No memory leaks under sustained load
- ✅ Graceful degradation under failures

### Correctness
- ✅ Exactly-once semantics for job processing
- ✅ Total ordering for sorted sets
- ✅ Eventually consistent consumer groups
- ✅ No data loss on node failures

### Documentation
- ✅ API docs for all operations
- ✅ Examples in 7 languages
- ✅ Architecture explanations
- ✅ Troubleshooting guides

---

## Risk Mitigation

### Potential Issues & Solutions

**Issue 1: Distributed Job Claiming Race Condition**
- Solution: Use atomic CAS on vector clock
- Backup: Idempotent processing (process twice, same result)

**Issue 2: Sorted Sets Conflict Resolution**
- Solution: Composite key (score, timestamp, node)
- Backup: User-provided conflict resolution function

**Issue 3: Stream Consumer Group Coordination**
- Solution: Offset stored in replicated state
- Backup: Read-all-since-offset (inefficient but safe)

---

## Success Criteria

✅ All 3 features implemented in Go backend  
✅ SDKs for 7 languages with identical APIs  
✅ 95%+ test coverage across all code  
✅ Benchmark shows performance targets met  
✅ Documentation complete with examples  
✅ Zero regressions in existing features  
✅ Production-ready code quality  

---

## Next Steps

1. **Approve architecture** - Any changes?
2. **Start Job Queues** - Most critical, can be used immediately
3. **Parallel SDK work** - Language teams work on SDKs while Go is being written
4. **Continuous testing** - Integration tests as features ship

**Ready to start? 🚀**
