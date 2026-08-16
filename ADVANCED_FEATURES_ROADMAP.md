# TollMeshStore Advanced Features Roadmap

## Overview

This document outlines advanced features that would transform TollMeshStore from a specialized coordination store into a comprehensive AI-agent-friendly knowledge and coordination platform.

---

## Phase 1: Data Persistence (Q4 2026)

### 1.1 Disk-Based Persistence

**Goal**: Enable crash recovery and long-term state storage

```go
// Snapshot-based persistence
type PersistenceEngine struct {
    snapshotInterval time.Duration
    walPath          string  // Write-Ahead Log
    snapshotPath     string
}

// Snapshot format: JSON + compression
type Snapshot struct {
    Timestamp        int64
    RateLimiters     map[string]interface{}
    ReplayProtection []string
    Cache            map[string]map[string][]byte
    CacheTTL         map[string]map[string]int64
}
```

**Implementation:**
- ✅ Write-Ahead Log (WAL) for durability
- ✅ Periodic snapshots (configurable interval)
- ✅ Crash recovery on startup
- ✅ Incremental backups
- ✅ Point-in-time recovery

**Benefits:**
- Survives node failures
- Enables long-term state storage
- Supports audit trails
- Enables disaster recovery

**Estimated Effort**: 2-3 weeks

---

## Phase 2: Pub/Sub Messaging (Q4 2026)

### 2.1 Topic-Based Pub/Sub

**Goal**: Enable event-driven coordination between agents

```go
// Pub/Sub system
type PubSubBroker struct {
    topics map[string]*Topic
    mu     sync.RWMutex
}

type Topic struct {
    subscribers map[string]chan Message
    mu          sync.RWMutex
}

type Message struct {
    Topic     string
    Payload   []byte
    Timestamp int64
    Publisher string
}

// API endpoints
POST   /pubsub/subscribe/{topic}
POST   /pubsub/publish/{topic}
GET    /pubsub/topics
DELETE /pubsub/unsubscribe/{topic}
```

**Features:**
- ✅ Topic-based subscriptions
- ✅ Pattern matching (e.g., `agent.*`)
- ✅ Message persistence (optional)
- ✅ Subscriber groups
- ✅ Dead-letter queues

**Use Cases for Toll:**
- Agent coordination events
- Rate limit threshold alerts
- Challenge completion notifications
- Replay attack warnings

**Estimated Effort**: 2 weeks

---

## Phase 3: Transactions (Q1 2027)

### 3.1 ACID Transactions

**Goal**: Enable atomic multi-operation coordination

```go
// Transaction support
type Transaction struct {
    ID        string
    Operations []Operation
    Status    TransactionStatus
    Timestamp int64
}

type Operation struct {
    Type   OperationType  // Consume, Seen, Get, Set
    Key    string
    Value  interface{}
    Result interface{}
}

// API endpoints
POST   /transactions/begin
POST   /transactions/{id}/execute
POST   /transactions/{id}/commit
POST   /transactions/{id}/rollback
```

**ACID Properties:**
- ✅ **Atomicity**: All-or-nothing execution
- ✅ **Consistency**: CRDT-based convergence
- ✅ **Isolation**: Snapshot isolation
- ✅ **Durability**: WAL-based persistence

**Example Use Case:**
```go
// Atomic rate limit + challenge creation
tx := store.BeginTransaction()
tx.Consume("user123", 100, 1*time.Minute)
tx.Set("challenges", "user123", challengeData, 5*time.Minute)
tx.Commit()  // Both succeed or both fail
```

**Estimated Effort**: 3-4 weeks

---

## Phase 4: Lua Scripting (Q1 2027)

### 4.1 Embedded Lua Runtime

**Goal**: Enable custom business logic execution

```go
// Lua scripting engine
type LuaEngine struct {
    vm *lua.LState
}

// API endpoints
POST /scripts/register/{name}
POST /scripts/execute/{name}
GET  /scripts/list

// Example script: Complex rate limiting
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = redis.call('GET', key)
if current == false then
    redis.call('SET', key, 1, 'EX', window)
    return {1, limit - 1}
else
    local count = tonumber(current)
    if count < limit then
        redis.call('INCR', key)
        return {count + 1, limit - count - 1}
    else
        return {0, 0}
    end
end
```

**Features:**
- ✅ Sandboxed execution
- ✅ Access to store operations
- ✅ Script caching
- ✅ Timeout protection
- ✅ Error handling

**Use Cases:**
- Complex rate limiting rules
- Conditional challenge creation
- Multi-step verification flows
- Custom agent logic

**Estimated Effort**: 2-3 weeks

---

## Phase 5: Hybrid Search (BM25 + Dense Vectors) (Q2 2027)

### 5.1 Full-Text + Semantic Search

**Goal**: Enable intelligent agent discovery and retrieval

```go
// Hybrid search engine
type SearchEngine struct {
    bm25Index    *BM25Index      // Full-text search
    vectorIndex  *VectorIndex    // Semantic search
    embeddings   *EmbeddingModel // Dense vectors
}

// Data structures
type Document struct {
    ID       string
    Content  string
    Metadata map[string]interface{}
    Vector   []float32  // Dense embedding
}

// API endpoints
POST   /search/index
POST   /search/query
GET    /search/documents/{id}
DELETE /search/documents/{id}

// Query types
type SearchQuery struct {
    Text      string    // BM25 query
    Vector    []float32 // Dense vector
    Hybrid    bool      // Combine both
    TopK      int
    Threshold float32
}
```

**Implementation:**
- ✅ BM25 full-text indexing
- ✅ Dense vector embeddings (using ONNX models)
- ✅ Hybrid ranking (combine BM25 + cosine similarity)
- ✅ Approximate nearest neighbor search (HNSW)
- ✅ Real-time index updates

**Use Cases for Toll:**
- Agent capability discovery
- Challenge template matching
- Policy rule retrieval
- Threat pattern matching

**Example:**
```go
// Find similar agents based on behavior
query := SearchQuery{
    Text: "browser automation detection",
    Vector: embedModel.Embed("automated browser"),
    Hybrid: true,
    TopK: 10,
}
results := searchEngine.Query(query)
// Returns: agents with similar capabilities/patterns
```

**Estimated Effort**: 4-5 weeks

---

## Phase 6: Graph RAG (Retrieval-Augmented Generation) (Q2 2027)

### 6.1 Knowledge Graph + LLM Integration

**Goal**: Enable intelligent agent reasoning and decision-making

```go
// Knowledge graph
type KnowledgeGraph struct {
    nodes map[string]*Node
    edges map[string]*Edge
    mu    sync.RWMutex
}

type Node struct {
    ID         string
    Type       string  // agent, threat, policy, challenge
    Properties map[string]interface{}
    Embeddings []float32
}

type Edge struct {
    Source string
    Target string
    Type   string  // related_to, detected_by, mitigated_by
    Weight float32
}

// Graph RAG pipeline
type GraphRAG struct {
    kg          *KnowledgeGraph
    llm         *LLMClient  // OpenAI, Claude, etc.
    retriever   *Retriever
    reasoner    *Reasoner
}

// API endpoints
POST   /graph/nodes
POST   /graph/edges
POST   /graph/query
POST   /graph/reason
GET    /graph/neighbors/{nodeID}
```

**Features:**
- ✅ Knowledge graph construction
- ✅ Entity extraction from logs
- ✅ Relationship inference
- ✅ LLM-powered reasoning
- ✅ Multi-hop retrieval
- ✅ Explainable decisions

**Use Cases for Toll:**
```go
// Example: Intelligent threat analysis
query := "What agents are similar to this bot and how should we respond?"

// Graph RAG pipeline:
// 1. Extract entities from logs
// 2. Build knowledge graph
// 3. Retrieve relevant nodes/edges
// 4. Use LLM to reason about response
// 5. Generate explanation

response := graphRAG.Reason(query, context)
// Returns: {decision, reasoning, confidence, similar_agents}
```

**Benefits:**
- Explainable AI decisions
- Multi-hop reasoning
- Pattern discovery
- Automated threat analysis

**Estimated Effort**: 5-6 weeks

---

## Phase 7: Ranking & Reranking (Q3 2027)

### 7.1 Multi-Stage Ranking Pipeline

**Goal**: Improve search and retrieval quality

```go
// Ranking pipeline
type RankingPipeline struct {
    retrievers []Retriever      // Multiple retrieval strategies
    rankers    []Ranker         // Multiple ranking models
    fusion     *RankFusion      // Combine rankings
}

// Ranker types
type Ranker interface {
    Rank(query string, documents []Document) []RankedResult
}

type BM25Ranker struct{}
type VectorRanker struct{}
type LLMRanker struct{}  // Cross-encoder
type ContextRanker struct{}  // Domain-specific

// API endpoints
POST /ranking/rerank
GET  /ranking/models
POST /ranking/evaluate
```

**Multi-Stage Pipeline:**
```
Stage 1: Retrieval (BM25 + Vector)
  ↓
Stage 2: Initial Ranking (BM25 + Vector scores)
  ↓
Stage 3: Reranking (LLM cross-encoder)
  ↓
Stage 4: Context Ranking (Domain-specific)
  ↓
Final Results (Top-K)
```

**Use Cases:**
- Improved agent matching
- Better policy retrieval
- Challenge template ranking
- Threat pattern prioritization

**Estimated Effort**: 3-4 weeks

---

## Phase 8: Agent Enhancement Features (Q3 2027)

### 8.1 Agent-Specific Capabilities

**Goal**: Make TollMeshStore a comprehensive agent coordination platform

```go
// Agent registry
type AgentRegistry struct {
    agents map[string]*Agent
    mu     sync.RWMutex
}

type Agent struct {
    ID           string
    Name         string
    Capabilities []string
    Reputation   float32
    LastSeen     int64
    Metadata     map[string]interface{}
}

// Agent coordination
type AgentCoordinator struct {
    registry    *AgentRegistry
    searchEngine *SearchEngine
    graphRAG    *GraphRAG
    pubsub      *PubSubBroker
}

// API endpoints
POST   /agents/register
GET    /agents/{id}
POST   /agents/{id}/capabilities
GET    /agents/discover
POST   /agents/{id}/coordinate
```

**Features:**
- ✅ Agent discovery (via hybrid search)
- ✅ Capability matching
- ✅ Reputation tracking
- ✅ Coordination protocols
- ✅ Event-driven communication
- ✅ Reasoning about agent behavior

**Example Workflow:**
```go
// Agent discovers similar agents and coordinates
agent := coordinator.GetAgent("agent123")
similar := searchEngine.FindSimilar(agent)
for _, other := range similar {
    coordinator.Coordinate(agent, other)
}
```

**Estimated Effort**: 2-3 weeks

---

## Implementation Priority Matrix

| Feature | Complexity | Impact | Priority | Timeline |
|---------|-----------|--------|----------|----------|
| Data Persistence | Medium | High | 1 | Q4 2026 |
| Pub/Sub | Medium | High | 2 | Q4 2026 |
| Transactions | High | Medium | 3 | Q1 2027 |
| Lua Scripting | Medium | Medium | 4 | Q1 2027 |
| Hybrid Search | High | High | 5 | Q2 2027 |
| Graph RAG | Very High | Very High | 6 | Q2 2027 |
| Ranking | High | Medium | 7 | Q3 2027 |
| Agent Features | Medium | High | 8 | Q3 2027 |

---

## Architecture Evolution

### Current (Phase 0)
```
TollMeshStore
├── Core (CRDTs)
├── Store (Rate limiting, Replay, Cache)
├── Coordination (Gossip)
├── API (HTTP)
└── Metrics
```

### After Phase 8
```
TollMeshStore Enterprise
├── Core (CRDTs)
├── Store (Rate limiting, Replay, Cache)
├── Coordination (Gossip)
├── Persistence (WAL + Snapshots)
├── Pub/Sub (Event broker)
├── Transactions (ACID)
├── Scripting (Lua)
├── Search (Hybrid BM25 + Vector)
├── Knowledge Graph (RAG)
├── Ranking (Multi-stage)
├── Agent Coordination
├── API (HTTP + WebSocket)
└── Metrics
```

---

## Benefits for Toll & Agents

### For Toll:
1. **Comprehensive coordination**: All agent interactions in one system
2. **Intelligent decisions**: Graph RAG for threat analysis
3. **Better performance**: Hybrid search for policy retrieval
4. **Flexibility**: Lua scripting for custom rules
5. **Reliability**: Transactions for critical operations
6. **Observability**: Pub/Sub for event tracking

### For Agents:
1. **Discovery**: Find similar agents via hybrid search
2. **Coordination**: Pub/Sub for communication
3. **Intelligence**: Graph RAG for reasoning
4. **Flexibility**: Lua scripting for custom logic
5. **Reliability**: Transactions for atomic operations
6. **Persistence**: Survive failures and restarts

---

## Estimated Total Effort

- **Phase 1-4** (Core features): 9-11 weeks
- **Phase 5-6** (Advanced search): 9-11 weeks
- **Phase 7-8** (Agent features): 5-7 weeks
- **Total**: 23-29 weeks (~6 months)

---

## Conclusion

These advanced features would transform TollMeshStore from a specialized coordination store into a comprehensive AI-agent-friendly platform that rivals Redis in capability while maintaining its advantages in distributed coordination and zero-dependency deployment.

The phased approach allows for incremental value delivery while managing complexity and risk.