package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/toll-mesh/store/core"
	"github.com/toll-mesh/store/metrics"
	"github.com/toll-mesh/store/persistence"
	"github.com/toll-mesh/store/pubsub"
	"github.com/toll-mesh/store/queue"
	"github.com/toll-mesh/store/ranking"
	"github.com/toll-mesh/store/scripting"
	"github.com/toll-mesh/store/search"
	"github.com/toll-mesh/store/sortedset"
	"github.com/toll-mesh/store/stream"
	"github.com/toll-mesh/store/transactions"
)

// cacheEntry is a cache value plus what's needed to merge it as a real
// LWW-register CRDT across nodes: Timestamp (wall-clock nanoseconds at
// write time) and Node (the writer's node ID, used to deterministically
// break ties between two writes with an identical Timestamp). Two nodes
// concurrently writing the same key converge to whichever write has the
// later Timestamp, with Node breaking exact ties -- the same outcome on
// every node, regardless of gossip order.
type cacheEntry struct {
	Value     []byte
	ExpiresAt time.Time // zero means no TTL
	Timestamp int64
	Node      string
}

// MeshStore implements the Store interface using CRDTs and gossip protocol.
type MeshStore struct {
	mu               sync.RWMutex
	config           *core.ClusterConfig
	rateLimiters     map[string]*core.GCounter
	replayProtection *core.GSet
	cache            map[string]map[string]*cacheEntry
	stopChan         chan struct{}

	jobManager *queue.JobManager

	zsetsMu sync.RWMutex
	zsets   map[string]*sortedset.SortedSet

	streamsMu sync.RWMutex
	streams   map[string]*stream.Stream

	groupsMu sync.RWMutex
	// keyed by "streamName:groupName"
	groups map[string]*stream.ConsumerGroup

	pubsubBroker    *pubsub.PubSubBroker
	txnManager      *transactions.TransactionManager
	persistence     *persistence.PersistenceEngine
	pipelines       *scripting.Engine
	wasmEngine      *scripting.WasmEngine // nil if TinyGo wasn't found at startup
	wasmUnavailable error                 // reason wasmEngine is nil, if it is
	searchEngine    *search.HybridSearchEngine
	metricsColl     *metrics.Metrics
}

// NewMeshStore creates a new MeshStore instance.
func NewMeshStore(config *core.ClusterConfig) (*MeshStore, error) {
	dataDir := config.DataDir
	if dataDir == "" {
		dataDir = filepath.Join("data", config.NodeName)
	}

	pe, err := persistence.NewPersistenceEngine(
		filepath.Join(dataDir, "wal"),
		filepath.Join(dataDir, "snapshots"),
		5*time.Minute,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create persistence engine: %w", err)
	}

	ms := &MeshStore{
		config:           config,
		rateLimiters:     make(map[string]*core.GCounter),
		replayProtection: core.NewGSet(),
		cache:            make(map[string]map[string]*cacheEntry),
		stopChan:         make(chan struct{}),
		jobManager:       queue.NewJobManager(config.NodeName),
		zsets:            make(map[string]*sortedset.SortedSet),
		streams:          make(map[string]*stream.Stream),
		groups:           make(map[string]*stream.ConsumerGroup),
		pubsubBroker:     pubsub.NewPubSubBroker(1000),
		txnManager:       transactions.NewTransactionManager(1000, 5*time.Minute),
		persistence:      pe,
		pipelines:        scripting.NewEngine(50, 30*time.Second),
		searchEngine:     search.NewHybridSearchEngine(),
		metricsColl:      metrics.NewMetrics(),
	}

	// WASM scripting requires the external TinyGo toolchain, which is a
	// much heavier dependency than anything else this server needs (a
	// separate compiler installation, not a Go module). A machine without
	// it should still be able to run every other feature, so this degrades
	// to "unavailable" (wasmEngine stays nil, handled explicitly by every
	// WASM method below) rather than failing the whole server's startup.
	if wasmEngine, err := scripting.NewWasmEngine("", 10*time.Second); err == nil {
		ms.wasmEngine = wasmEngine
	} else {
		ms.wasmUnavailable = err
	}

	ms.registerPipelineHandlers()

	if err := ms.recoverFromDisk(); err != nil {
		return nil, fmt.Errorf("failed to recover persisted state: %w", err)
	}

	go ms.backgroundCleanup()
	return ms, nil
}

// recoverFromDisk restores state left by a previous run, if any: the most
// recent snapshot (if one exists), then every WAL entry written after that
// snapshot was taken (or, if there's no snapshot at all, every WAL entry
// ever written). This runs once at startup before the server accepts any
// traffic, so replaying the WAL from a blank starting point and reapplying
// every logged operation in order is exactly correct -- there is no
// "already applied" state to double-apply on top of.
func (ms *MeshStore) recoverFromDisk() error {
	snap, err := ms.persistence.LoadLatestSnapshot()
	if err != nil {
		return fmt.Errorf("loading latest snapshot: %w", err)
	}

	var afterTimestamp int64
	if snap != nil {
		ms.mu.Lock()
		ms.applySnapshotLocked(snap)
		ms.mu.Unlock()
		afterTimestamp = snap.Timestamp
	}

	entries, err := ms.persistence.ReplayWAL(afterTimestamp)
	if err != nil {
		return fmt.Errorf("replaying WAL: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, entry := range entries {
		ms.applyWALEntryLocked(entry)
	}
	return nil
}

// applyWALEntryLocked applies one recovered WAL entry to live state.
// Callers must hold ms.mu.
func (ms *MeshStore) applyWALEntryLocked(entry persistence.WALEntry) {
	switch entry.Operation {
	case "consume":
		counter, exists := ms.rateLimiters[entry.Key]
		if !exists {
			counter = core.NewGCounter()
			ms.rateLimiters[entry.Key] = counter
		}
		// This WAL is this node's own history being replayed on itself
		// after a restart, so the increment belongs to this node's own
		// per-node count in the GCounter.
		counter.Increment(ms.config.NodeName)

	case "seen":
		ms.replayProtection.Add(entry.Key)

	case "set":
		valueStr, _ := entry.Value.(string)
		if _, exists := ms.cache[entry.Namespace]; !exists {
			ms.cache[entry.Namespace] = make(map[string]*cacheEntry)
		}
		ce := &cacheEntry{Value: []byte(valueStr), Timestamp: entry.Version, Node: entry.Node}
		if entry.ExpiresAt > 0 {
			ce.ExpiresAt = time.UnixMilli(entry.ExpiresAt)
		}
		ms.cache[entry.Namespace][entry.Key] = ce
	}
}

// registerPipelineHandlers exposes the store's own operations as pipeline
// step handlers, so a Pipeline can only ever do what the store already does
// through its normal API -- there is no separate execution surface.
func (ms *MeshStore) registerPipelineHandlers() {
	ctx := context.Background()

	ms.pipelines.RegisterHandler("get", func(args map[string]interface{}) (interface{}, error) {
		ns, _ := args["namespace"].(string)
		key, _ := args["key"].(string)
		value, exists, err := ms.Get(ctx, ns, key)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"value": string(value), "exists": exists}, nil
	})

	ms.pipelines.RegisterHandler("set", func(args map[string]interface{}) (interface{}, error) {
		ns, _ := args["namespace"].(string)
		key, _ := args["key"].(string)
		value, _ := args["value"].(string)
		ttlMs, _ := args["ttl"].(float64)
		return nil, ms.Set(ctx, ns, key, []byte(value), time.Duration(ttlMs)*time.Millisecond)
	})

	ms.pipelines.RegisterHandler("zadd", func(args map[string]interface{}) (interface{}, error) {
		key, _ := args["key"].(string)
		member, _ := args["member"].(string)
		score, _ := args["score"].(float64)
		return nil, ms.ZAdd(ctx, key, member, score)
	})

	ms.pipelines.RegisterHandler("zscore", func(args map[string]interface{}) (interface{}, error) {
		key, _ := args["key"].(string)
		member, _ := args["member"].(string)
		score, exists := ms.ZScore(ctx, key, member)
		return map[string]interface{}{"score": score, "exists": exists}, nil
	})

	ms.pipelines.RegisterHandler("enqueue", func(args map[string]interface{}) (interface{}, error) {
		queueName, _ := args["queue"].(string)
		payload, _ := args["payload"].(string)
		priority, _ := args["priority"].(float64)
		return ms.Enqueue(ctx, queueName, []byte(payload), int(priority), 3, 24*time.Hour)
	})

	ms.pipelines.RegisterHandler("xadd", func(args map[string]interface{}) (interface{}, error) {
		streamName, _ := args["stream"].(string)
		fields := map[string]string{}
		if raw, ok := args["fields"].(map[string]interface{}); ok {
			for k, v := range raw {
				fields[k] = fmt.Sprintf("%v", v)
			}
		}
		return ms.XAdd(ctx, streamName, fields)
	})
}

func (ms *MeshStore) getOrCreateZSet(key string) *sortedset.SortedSet {
	ms.zsetsMu.Lock()
	defer ms.zsetsMu.Unlock()

	zs, exists := ms.zsets[key]
	if !exists {
		zs = sortedset.NewSortedSet(key, ms.config.NodeName)
		ms.zsets[key] = zs
	}
	return zs
}

func (ms *MeshStore) getZSet(key string) (*sortedset.SortedSet, bool) {
	ms.zsetsMu.RLock()
	defer ms.zsetsMu.RUnlock()
	zs, exists := ms.zsets[key]
	return zs, exists
}

func (ms *MeshStore) getOrCreateStream(name string) *stream.Stream {
	ms.streamsMu.Lock()
	defer ms.streamsMu.Unlock()

	s, exists := ms.streams[name]
	if !exists {
		s = stream.NewStream(name, ms.config.NodeName)
		ms.streams[name] = s
	}
	return s
}

func (ms *MeshStore) getStream(name string) (*stream.Stream, bool) {
	ms.streamsMu.RLock()
	defer ms.streamsMu.RUnlock()
	s, exists := ms.streams[name]
	return s, exists
}

func groupKey(streamName, groupName string) string {
	return streamName + ":" + groupName
}

func (ms *MeshStore) getGroup(streamName, groupName string) (*stream.ConsumerGroup, bool) {
	ms.groupsMu.RLock()
	defer ms.groupsMu.RUnlock()
	g, exists := ms.groups[groupKey(streamName, groupName)]
	return g, exists
}

// Job Queues

// Enqueue adds a job to the named queue.
func (ms *MeshStore) Enqueue(ctx context.Context, queueName string, payload []byte, priority, maxRetries int, deadline time.Duration) (*queue.Job, error) {
	return ms.jobManager.Enqueue(queueName, payload, queue.JobOptions{
		Priority:   priority,
		MaxRetries: maxRetries,
		Deadline:   deadline,
	})
}

// ClaimJob claims the next available job from the named queue.
func (ms *MeshStore) ClaimJob(ctx context.Context, queueName, workerID string) (*queue.Job, error) {
	return ms.jobManager.ClaimJob(queueName, workerID)
}

// CompleteJob marks a claimed job as completed.
func (ms *MeshStore) CompleteJob(ctx context.Context, queueName, jobID string, result []byte) error {
	return ms.jobManager.CompleteJob(queueName, jobID, result)
}

// FailJob marks a claimed job as failed, triggering retry or dead-lettering.
func (ms *MeshStore) FailJob(ctx context.Context, queueName, jobID, errMsg string) error {
	return ms.jobManager.FailJob(queueName, jobID, errMsg)
}

// GetJobStatus returns the current status of a job.
func (ms *MeshStore) GetJobStatus(ctx context.Context, queueName, jobID string) (*queue.Job, error) {
	return ms.jobManager.GetJobStatus(queueName, jobID)
}

// GetQueueStats returns statistics for the named queue.
func (ms *MeshStore) GetQueueStats(ctx context.Context, queueName string) (map[string]interface{}, error) {
	return ms.jobManager.GetQueueStats(queueName)
}

// Sorted Sets

// ZAdd inserts or updates a member's score in the named sorted set.
func (ms *MeshStore) ZAdd(ctx context.Context, key, member string, score float64) error {
	return ms.getOrCreateZSet(key).Add(member, score)
}

// ZRem removes a member from the named sorted set.
func (ms *MeshStore) ZRem(ctx context.Context, key, member string) error {
	zs, exists := ms.getZSet(key)
	if !exists {
		return fmt.Errorf("sorted set not found: %s", key)
	}
	return zs.Remove(member)
}

// ZScore returns a member's score.
func (ms *MeshStore) ZScore(ctx context.Context, key, member string) (float64, bool) {
	zs, exists := ms.getZSet(key)
	if !exists {
		return 0, false
	}
	return zs.Get(member)
}

// ZRank returns a member's ascending-order rank.
func (ms *MeshStore) ZRank(ctx context.Context, key, member string) (int64, bool) {
	zs, exists := ms.getZSet(key)
	if !exists {
		return 0, false
	}
	return zs.Rank(member)
}

// ZRevRank returns a member's descending-order rank.
func (ms *MeshStore) ZRevRank(ctx context.Context, key, member string) (int64, bool) {
	zs, exists := ms.getZSet(key)
	if !exists {
		return 0, false
	}
	return zs.RevRank(member)
}

// ZRange returns members with scores in [min, max], ascending, up to limit.
func (ms *MeshStore) ZRange(ctx context.Context, key string, min, max float64, limit int64) []*sortedset.SortedSetMember {
	zs, exists := ms.getZSet(key)
	if !exists {
		return nil
	}
	return zs.Range(min, max, limit)
}

// ZRevRange returns members with scores in [min, max], descending, up to limit.
func (ms *MeshStore) ZRevRange(ctx context.Context, key string, max, min float64, limit int64) []*sortedset.SortedSetMember {
	zs, exists := ms.getZSet(key)
	if !exists {
		return nil
	}
	return zs.RevRange(max, min, limit)
}

// ZRangeByRank returns members by rank range [start, stop], ascending.
func (ms *MeshStore) ZRangeByRank(ctx context.Context, key string, start, stop int64) []*sortedset.SortedSetMember {
	zs, exists := ms.getZSet(key)
	if !exists {
		return nil
	}
	return zs.RangeByRank(start, stop)
}

// ZCard returns the number of members in the named sorted set.
func (ms *MeshStore) ZCard(ctx context.Context, key string) int64 {
	zs, exists := ms.getZSet(key)
	if !exists {
		return 0
	}
	return zs.Card()
}

// Streams

// XAdd appends a new entry to the named stream.
func (ms *MeshStore) XAdd(ctx context.Context, streamName string, fields map[string]string) (*stream.StreamEntry, error) {
	return ms.getOrCreateStream(streamName).Add(fields)
}

// XRange returns entries between startID and endID, up to limit.
func (ms *MeshStore) XRange(ctx context.Context, streamName, startID, endID string, limit int64) []*stream.StreamEntry {
	s, exists := ms.getStream(streamName)
	if !exists {
		return nil
	}
	return s.Range(startID, endID, limit)
}

// XLen returns the number of entries in the named stream.
func (ms *MeshStore) XLen(ctx context.Context, streamName string) int64 {
	s, exists := ms.getStream(streamName)
	if !exists {
		return 0
	}
	return s.Len()
}

// XGroupCreate creates a consumer group for the named stream.
func (ms *MeshStore) XGroupCreate(ctx context.Context, streamName, groupName string) error {
	ms.getOrCreateStream(streamName) // ensure the stream exists

	ms.groupsMu.Lock()
	defer ms.groupsMu.Unlock()

	key := groupKey(streamName, groupName)
	if _, exists := ms.groups[key]; exists {
		return fmt.Errorf("consumer group already exists: %s on stream %s", groupName, streamName)
	}
	ms.groups[key] = stream.NewConsumerGroup(groupName, streamName, ms.config.NodeName)
	return nil
}

// XReadGroup reads up to limit unacknowledged entries for a consumer,
// registering the consumer in the group on first read.
func (ms *MeshStore) XReadGroup(ctx context.Context, streamName, groupName, consumerID string, limit int64) ([]*stream.StreamEntry, error) {
	s, exists := ms.getStream(streamName)
	if !exists {
		return nil, fmt.Errorf("stream not found: %s", streamName)
	}

	group, exists := ms.getGroup(streamName, groupName)
	if !exists {
		return nil, fmt.Errorf("consumer group not found: %s on stream %s", groupName, streamName)
	}

	if _, err := group.GetConsumer(consumerID); err != nil {
		if _, err := group.AddConsumer(consumerID, ms.config.NodeName); err != nil {
			return nil, err
		}
	}

	offset, err := group.GetOffset(consumerID)
	if err != nil {
		return nil, err
	}

	entries := s.Range(offset, "-", limit)
	for _, e := range entries {
		group.AddPending(e.ID, consumerID)
	}

	return entries, nil
}

// XAck acknowledges that a consumer has processed up to entryID, advancing
// its offset and clearing matching pending entries.
func (ms *MeshStore) XAck(ctx context.Context, streamName, groupName, consumerID, entryID string) error {
	group, exists := ms.getGroup(streamName, groupName)
	if !exists {
		return fmt.Errorf("consumer group not found: %s on stream %s", groupName, streamName)
	}
	return group.UpdateOffset(consumerID, entryID)
}

// Consume implements rate limiting using GCounter CRDT.
func (ms *MeshStore) Consume(ctx context.Context, key string, limit int, window time.Duration) (core.ConsumeResult, error) {
	start := time.Now()
	var allowed bool
	defer func() { ms.metricsColl.RecordConsume(allowed, time.Since(start).Microseconds()) }()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	counter, exists := ms.rateLimiters[key]
	if !exists {
		counter = core.NewGCounter()
		ms.rateLimiters[key] = counter
	}

	current := counter.Value()
	if current < limit {
		counter.Increment(ms.config.NodeName)
		allowed = true
		ms.persistence.LogOperation("consume", key, nil, "", 0, "", 0)
		return core.ConsumeResult{
			OK:        true,
			Remaining: limit - current - 1,
			ResetAt:   time.Now().Add(window).UnixMilli(),
		}, nil
	}

	return core.ConsumeResult{
		OK:        false,
		Remaining: 0,
		ResetAt:   time.Now().Add(window).UnixMilli(),
	}, nil
}

// Seen implements replay protection using GSet CRDT.
func (ms *MeshStore) Seen(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	start := time.Now()
	var replay bool
	defer func() { ms.metricsColl.RecordSeen(replay, time.Since(start).Microseconds()) }()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.replayProtection.Contains(key) {
		replay = true
		return true, nil
	}

	ms.replayProtection.Add(key)
	ms.persistence.LogOperation("seen", key, nil, "", 0, "", 0)
	return false, nil
}

// Get retrieves a cached value.
func (ms *MeshStore) Get(ctx context.Context, ns, key string) ([]byte, bool, error) {
	start := time.Now()
	var hit bool
	defer func() { ms.metricsColl.RecordGet(hit, time.Since(start).Microseconds()) }()

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	nsCache, exists := ms.cache[ns]
	if !exists {
		return nil, false, nil
	}

	entry, exists := nsCache[key]
	if !exists {
		return nil, false, nil
	}

	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		return nil, false, nil
	}

	hit = true
	return entry.Value, true, nil
}

// Set stores a value with TTL.
func (ms *MeshStore) Set(ctx context.Context, ns, key string, value []byte, ttl time.Duration) error {
	start := time.Now()
	defer func() { ms.metricsColl.RecordSet(time.Since(start).Microseconds()) }()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.cache[ns]; !exists {
		ms.cache[ns] = make(map[string]*cacheEntry)
	}

	// Set is a local mutation on this node, same reasoning as SortedSet.Add:
	// its own wall-clock write time is always causally after whatever this
	// node last wrote for this key, so it applies unconditionally. The
	// Timestamp/Node recorded here are what let a peer's MergeState later
	// decide, for a key written on two different nodes, which write is
	// newer -- see cacheEntry's doc comment.
	ts := time.Now().UnixNano()
	ce := &cacheEntry{Value: value, Timestamp: ts, Node: ms.config.NodeName}

	// ttl <= 0 means "no expiration" (this is the documented contract every
	// SDK exposes, e.g. Python's cache_set(..., ttl=None)). Get() treats a
	// zero ExpiresAt as never expiring, so simply don't set it -- previously
	// this always wrote time.Now().Add(ttl), which for ttl=0 set the expiry
	// to right now, making every "no TTL" cache_set dead on arrival.
	var expiresAtMs int64
	if ttl > 0 {
		ce.ExpiresAt = time.Now().Add(ttl)
		expiresAtMs = ce.ExpiresAt.UnixMilli()
	}

	ms.cache[ns][key] = ce
	ms.persistence.LogOperation("set", key, string(value), ns, expiresAtMs, ms.config.NodeName, 0)
	return nil
}

// Close gracefully shuts down the store.
func (ms *MeshStore) Close() error {
	ms.jobManager.Stop()
	ms.persistence.Close()
	close(ms.stopChan)
	return nil
}

// ===== Pub/Sub =====

// Subscribe subscribes to a topic with optional regex pattern matching.
func (ms *MeshStore) Subscribe(ctx context.Context, subscriberID, topic, pattern string) error {
	_, err := ms.pubsubBroker.Subscribe(subscriberID, topic, pattern)
	return err
}

// Unsubscribe removes a subscription.
func (ms *MeshStore) Unsubscribe(ctx context.Context, subscriberID, topic string) error {
	return ms.pubsubBroker.Unsubscribe(subscriberID, topic)
}

// Publish publishes a message to a topic, returning the number of
// subscribers it was delivered to.
func (ms *MeshStore) Publish(ctx context.Context, topic, publisher string, payload []byte) (int, error) {
	return ms.pubsubBroker.Publish(topic, publisher, payload)
}

// PollMessages retrieves up to limit currently-available messages for a
// subscriber, waiting up to timeout if none are immediately available.
func (ms *MeshStore) PollMessages(ctx context.Context, subscriberID string, limit int, timeout time.Duration) ([]pubsub.Message, error) {
	return ms.pubsubBroker.Poll(subscriberID, limit, timeout)
}

// GetTopics returns all known pub/sub topics.
func (ms *MeshStore) GetTopics(ctx context.Context) []string {
	return ms.pubsubBroker.GetTopics()
}

// GetTopicSubscribers returns subscriber IDs for a topic.
func (ms *MeshStore) GetTopicSubscribers(ctx context.Context, topic string) []string {
	return ms.pubsubBroker.GetSubscribers(topic)
}

// GetPubSubStats returns pub/sub statistics.
func (ms *MeshStore) GetPubSubStats(ctx context.Context) map[string]interface{} {
	return ms.pubsubBroker.GetStats()
}

// ===== Transactions =====

// BeginTransaction starts a new transaction.
func (ms *MeshStore) BeginTransaction(ctx context.Context, txnID string) (*transactions.Transaction, error) {
	return ms.txnManager.BeginTransaction(txnID)
}

// AddTransactionOperation queues an operation within a pending transaction.
// Only OpSet operations are actually applied on commit; other operation
// types are recorded for audit purposes but do not affect store state --
// Consume/Seen have side effects (incrementing shared counters) that don't
// have well-defined "deferred apply" semantics the way a plain key/value
// write does.
func (ms *MeshStore) AddTransactionOperation(ctx context.Context, txnID string, op transactions.Operation) error {
	return ms.txnManager.AddOperation(txnID, op)
}

// CommitTransaction validates and commits a transaction, then applies all
// of its queued Set operations to the real cache atomically (under the
// store's own lock, so no other Set/Get can interleave mid-apply).
func (ms *MeshStore) CommitTransaction(ctx context.Context, txnID string) error {
	ops, err := ms.txnManager.GetTransactionOperations(txnID)
	if err != nil {
		return err
	}

	if err := ms.txnManager.CommitTransaction(txnID); err != nil {
		return err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, op := range ops {
		if op.Type != transactions.OpSet {
			continue
		}
		valueStr, _ := op.Value.(string)
		if _, exists := ms.cache[op.Namespace]; !exists {
			ms.cache[op.Namespace] = make(map[string]*cacheEntry)
		}
		ms.cache[op.Namespace][op.Key] = &cacheEntry{
			Value:     []byte(valueStr),
			Timestamp: time.Now().UnixNano(),
			Node:      ms.config.NodeName,
		}
	}

	return nil
}

// RollbackTransaction rolls back a pending transaction. Since queued
// operations are never applied to real state until commit, rollback is
// simply discarding the queue -- there is nothing to undo.
func (ms *MeshStore) RollbackTransaction(ctx context.Context, txnID string) error {
	return ms.txnManager.RollbackTransaction(txnID)
}

// GetTransactionStatus returns the status of a transaction.
func (ms *MeshStore) GetTransactionStatus(ctx context.Context, txnID string) (transactions.TransactionStatus, error) {
	return ms.txnManager.GetTransactionStatus(txnID)
}

// ===== Persistence =====

// copyCacheLocked flattens the live cache into the parallel-map shape used
// by both persistence.Snapshot and core.MeshStoreState on the wire.
// Callers must hold ms.mu (for reading).
func (ms *MeshStore) copyCacheLocked() (
	cache map[string]map[string][]byte,
	ttl map[string]map[string]int64,
	timestamp map[string]map[string]int64,
	node map[string]map[string]string,
) {
	cache = make(map[string]map[string][]byte, len(ms.cache))
	ttl = make(map[string]map[string]int64, len(ms.cache))
	timestamp = make(map[string]map[string]int64, len(ms.cache))
	node = make(map[string]map[string]string, len(ms.cache))

	for ns, kv := range ms.cache {
		cacheNs := make(map[string][]byte, len(kv))
		ttlNs := make(map[string]int64, len(kv))
		tsNs := make(map[string]int64, len(kv))
		nodeNs := make(map[string]string, len(kv))
		for k, v := range kv {
			cacheNs[k] = v.Value
			if !v.ExpiresAt.IsZero() {
				ttlNs[k] = v.ExpiresAt.UnixMilli()
			}
			tsNs[k] = v.Timestamp
			nodeNs[k] = v.Node
		}
		cache[ns] = cacheNs
		ttl[ns] = ttlNs
		timestamp[ns] = tsNs
		node[ns] = nodeNs
	}
	return cache, ttl, timestamp, node
}

// CreateSnapshot captures the current live store state to disk.
func (ms *MeshStore) CreateSnapshot(ctx context.Context) error {
	ms.mu.RLock()
	rateLimiters := make(map[string]interface{}, len(ms.rateLimiters))
	for k, v := range ms.rateLimiters {
		rateLimiters[k] = v.Snapshot()
	}
	replayProtection := ms.replayProtection.Snapshot()
	cacheCopy, cacheTTLCopy, cacheTimestampCopy, cacheNodeCopy := ms.copyCacheLocked()
	ms.mu.RUnlock()

	snap := &persistence.Snapshot{
		RateLimiters:     rateLimiters,
		ReplayProtection: replayProtection,
		Cache:            cacheCopy,
		CacheTTL:         cacheTTLCopy,
		CacheTimestamp:   cacheTimestampCopy,
		CacheNode:        cacheNodeCopy,
	}

	if err := ms.persistence.CreateSnapshot(snap); err != nil {
		return err
	}

	// Everything up to this snapshot is now captured on disk in the
	// snapshot file itself, so the WAL entries logged before it are
	// redundant -- rotating (archiving the current WAL and starting a
	// fresh one) keeps disk usage from growing forever. Recovery only
	// ever replays WAL entries after the latest snapshot's timestamp
	// anyway, so this is purely a cleanup step, not a correctness one.
	//
	// Known narrow race, not fully closed here: the snapshot's state was
	// copied (above, under RLock) slightly before persistence.CreateSnapshot
	// stamps its Timestamp with its own time.Now(). A write landing in
	// that gap could be logged to the WAL with a timestamp <= the
	// snapshot's, making recovery treat it as already covered by the
	// snapshot even if it arrived just after the state was copied. This
	// is a narrow window (microseconds) rather than a structural gap.
	return ms.persistence.RotateWAL()
}

// GetLatestSnapshot returns the most recent snapshot, or nil if none exist.
func (ms *MeshStore) GetLatestSnapshot(ctx context.Context) (*persistence.Snapshot, error) {
	return ms.persistence.LoadLatestSnapshot()
}

// RestoreFromLatestSnapshot loads the most recent snapshot and applies it
// to live store state, replacing current rate limiters, replay-protection
// records, and cache contents.
func (ms *MeshStore) RestoreFromLatestSnapshot(ctx context.Context) error {
	snap, err := ms.persistence.LoadLatestSnapshot()
	if err != nil {
		return err
	}
	if snap == nil {
		return fmt.Errorf("no snapshot available")
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.applySnapshotLocked(snap)
	return nil
}

// applySnapshotLocked replaces live rate limiter/replay-protection/cache
// state with a loaded snapshot's contents. Callers must hold ms.mu.
func (ms *MeshStore) applySnapshotLocked(snap *persistence.Snapshot) {
	ms.rateLimiters = make(map[string]*core.GCounter, len(snap.RateLimiters))
	for k, v := range snap.RateLimiters {
		counts := map[string]int{}
		if raw, ok := v.(map[string]interface{}); ok {
			for node, c := range raw {
				if f, ok := c.(float64); ok {
					counts[node] = int(f)
				}
			}
		}
		ms.rateLimiters[k] = core.RestoreGCounter(counts)
	}

	ms.replayProtection = core.RestoreGSet(snap.ReplayProtection)

	ms.cache = restoreCacheLocked(snap.Cache, snap.CacheTTL, snap.CacheTimestamp, snap.CacheNode)
}

// restoreCacheLocked is the inverse of copyCacheLocked: reassembles the
// live cache representation from the wire/disk parallel-map shape. ttl,
// timestamp, and node may be nil (e.g. an older snapshot written before
// this cache versioning existed) -- entries just come back with a zero
// Timestamp/Node in that case, which MergeState treats as always losing
// to anything with a real version.
func restoreCacheLocked(
	cache map[string]map[string][]byte,
	ttl map[string]map[string]int64,
	timestamp map[string]map[string]int64,
	node map[string]map[string]string,
) map[string]map[string]*cacheEntry {
	out := make(map[string]map[string]*cacheEntry, len(cache))
	for ns, kv := range cache {
		nsOut := make(map[string]*cacheEntry, len(kv))
		for k, v := range kv {
			ce := &cacheEntry{Value: v}
			if ttl != nil {
				if ms, ok := ttl[ns][k]; ok && ms > 0 {
					ce.ExpiresAt = time.UnixMilli(ms)
				}
			}
			if timestamp != nil {
				ce.Timestamp = timestamp[ns][k]
			}
			if node != nil {
				ce.Node = node[ns][k]
			}
			nsOut[k] = ce
		}
		out[ns] = nsOut
	}
	return out
}

// GetPersistenceStats returns persistence statistics.
func (ms *MeshStore) GetPersistenceStats(ctx context.Context) map[string]interface{} {
	return ms.persistence.GetStats()
}

// ===== Gossip replication =====
//
// GetState and MergeState are the two halves of this node's multi-node
// replication: a peer periodically fetches this node's GetState() over
// HTTP (see api/http.go's /internal/state) and feeds the result into its
// own MergeState. Only the original three CRDT-backed primitives (rate
// limiting, replay protection, cache) are covered -- the eight feature
// groups added later (Pub/Sub, Transactions, Persistence, Pipelines, WASM
// Scripting, Search, Ranking, Metrics) are not part of this state and
// remain single-node only.

// GetState returns a snapshot of this node's replicated CRDT state, for a
// peer to merge into its own via MergeState.
func (ms *MeshStore) GetState() *core.MeshStoreState {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	rateLimiters := make(map[string]interface{}, len(ms.rateLimiters))
	for k, v := range ms.rateLimiters {
		rateLimiters[k] = v.Snapshot()
	}

	cacheCopy, cacheTTLCopy, cacheTimestampCopy, cacheNodeCopy := ms.copyCacheLocked()

	ms.zsetsMu.RLock()
	sortedSets := make(map[string][]sortedset.SortedSetMember, len(ms.zsets))
	for name, zs := range ms.zsets {
		sortedSets[name] = zs.Snapshot()
	}
	ms.zsetsMu.RUnlock()

	ms.streamsMu.RLock()
	streams := make(map[string][]stream.StreamEntry, len(ms.streams))
	for name, s := range ms.streams {
		streams[name] = s.Snapshot()
	}
	ms.streamsMu.RUnlock()

	return &core.MeshStoreState{
		RateLimiters:     rateLimiters,
		ReplayProtection: boolMap(ms.replayProtection.Snapshot()),
		Cache:            cacheCopy,
		CacheTTL:         cacheTTLCopy,
		CacheTimestamp:   cacheTimestampCopy,
		CacheNode:        cacheNodeCopy,
		SortedSets:       sortedSets,
		Streams:          streams,
	}
}

// MergeState merges a peer's state into this node's live state using each
// primitive's real CRDT merge. GCounter and GSet merge is commutative,
// associative, and idempotent, so it is safe to apply the same peer state
// more than once or in any order. Cache is a real LWW-register CRDT: for a
// key present on both sides, the entry with the later Timestamp wins, with
// Node breaking an exact tie (using the same node ID on both sides of a
// comparison always breaks the tie the same way, so this is deterministic
// and order-independent) -- this converges two nodes' concurrent writes to
// the *same* key to a single answer, the same way GCounter/GSet already do,
// which the previous conservative-union merge (peer only filled in keys
// the local side lacked) did not.
func (ms *MeshStore) MergeState(peer *core.MeshStoreState) {
	if peer == nil {
		return
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	for key, raw := range peer.RateLimiters {
		counts := decodeGCounterSnapshot(raw)
		if len(counts) == 0 {
			continue
		}
		local, exists := ms.rateLimiters[key]
		if !exists {
			local = core.NewGCounter()
			ms.rateLimiters[key] = local
		}
		local.Merge(core.RestoreGCounter(counts))
	}

	if len(peer.ReplayProtection) > 0 {
		items := make([]string, 0, len(peer.ReplayProtection))
		for item, seen := range peer.ReplayProtection {
			if seen {
				items = append(items, item)
			}
		}
		ms.replayProtection.Merge(core.RestoreGSet(items))
	}

	for ns, kv := range peer.Cache {
		if _, exists := ms.cache[ns]; !exists {
			ms.cache[ns] = make(map[string]*cacheEntry)
		}
		for k, v := range kv {
			var peerTimestamp int64
			var peerNode string
			if peer.CacheTimestamp != nil {
				peerTimestamp = peer.CacheTimestamp[ns][k]
			}
			if peer.CacheNode != nil {
				peerNode = peer.CacheNode[ns][k]
			}

			local, exists := ms.cache[ns][k]
			if exists && !cacheEntryLess(local.Timestamp, local.Node, peerTimestamp, peerNode) {
				// !(local < peer), i.e. local is newer than or exactly
				// equal to peer's version -- nothing to change. Only
				// replace local when peer is strictly newer (local < peer).
				continue
			}

			peerEntry := &cacheEntry{Value: v, Timestamp: peerTimestamp, Node: peerNode}
			var expiresAtMs int64
			if peer.CacheTTL != nil {
				if ttlMs, ok := peer.CacheTTL[ns][k]; ok && ttlMs > 0 {
					peerEntry.ExpiresAt = time.UnixMilli(ttlMs)
					expiresAtMs = ttlMs
				}
			}
			ms.cache[ns][k] = peerEntry

			// Persist the adopted value to this node's own WAL too, under
			// its original version (peerTimestamp), not a merge-time
			// stamp -- otherwise it only lives in memory, and this node's
			// own next restart would recover its own older local write
			// instead of what gossip already converged it to. Confirmed
			// live: without this, a crash on the node that merely
			// *received* a merge (not the one that made the winning
			// write) reverted to its own stale value on restart.
			ms.persistence.LogOperation("set", k, string(v), ns, expiresAtMs, peerNode, peerTimestamp)
		}
	}

	// Sorted sets: the first of the ten remaining feature groups (beyond
	// the original three primitives) to get gossip replication. Each
	// set's own Merge already implements a real (score, timestamp, node)
	// CRDT conflict resolution -- MergeSnapshot just feeds it the
	// wire-format member list gossip actually carries.
	for name, members := range peer.SortedSets {
		ms.getOrCreateZSet(name).MergeSnapshot(members)
	}

	for name, entries := range peer.Streams {
		ms.getOrCreateStream(name).MergeSnapshot(entries)
	}
}

// cacheEntryLess reports whether (tsA, nodeA) sorts strictly before (tsB,
// nodeB) in the cache LWW-register's version order: primarily by
// Timestamp, Node breaking an exact tie. Used to decide, during a merge,
// whether an incoming entry is newer than what's already there.
func cacheEntryLess(tsA int64, nodeA string, tsB int64, nodeB string) bool {
	if tsA != tsB {
		return tsA < tsB
	}
	return nodeA < nodeB
}

// boolMap converts a GSet snapshot ([]string) into the map[string]bool
// shape core.MeshStoreState.ReplayProtection uses on the wire.
func boolMap(items []string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, item := range items {
		out[item] = true
	}
	return out
}

// decodeGCounterSnapshot recovers a GCounter's per-node counts from the
// interface{} produced by json.Unmarshal of GetState's output -- numbers
// decode as float64, not int, once they've round-tripped through JSON.
func decodeGCounterSnapshot(raw interface{}) map[string]int {
	counts := map[string]int{}
	switch v := raw.(type) {
	case map[string]int:
		for k, c := range v {
			counts[k] = c
		}
	case map[string]interface{}:
		for k, c := range v {
			if f, ok := c.(float64); ok {
				counts[k] = int(f)
			}
		}
	}
	return counts
}

// ===== Scripting (Pipelines) =====

// RegisterPipeline registers a named pipeline for later execution by name.
func (ms *MeshStore) RegisterPipeline(ctx context.Context, p *scripting.Pipeline) error {
	return ms.pipelines.RegisterPipeline(p)
}

// ExecutePipeline runs a registered pipeline by name.
func (ms *MeshStore) ExecutePipeline(ctx context.Context, name string) (*scripting.ExecutionResult, error) {
	return ms.pipelines.Execute(name)
}

// ExecuteInlinePipeline runs an ad-hoc list of steps without registering them.
func (ms *MeshStore) ExecuteInlinePipeline(ctx context.Context, steps []scripting.Step) (*scripting.ExecutionResult, error) {
	return ms.pipelines.ExecuteInline(steps)
}

// GetPipeline retrieves a registered pipeline by name.
func (ms *MeshStore) GetPipeline(ctx context.Context, name string) (*scripting.Pipeline, error) {
	return ms.pipelines.GetPipeline(name)
}

// ListPipelines returns all registered pipelines.
func (ms *MeshStore) ListPipelines(ctx context.Context) []*scripting.Pipeline {
	return ms.pipelines.ListPipelines()
}

// DeletePipeline removes a registered pipeline.
func (ms *MeshStore) DeletePipeline(ctx context.Context, name string) error {
	return ms.pipelines.DeletePipeline(name)
}

// ===== Scripting (WASM) =====

var errWasmUnavailable = fmt.Errorf("WASM scripting unavailable: TinyGo toolchain not found at server startup")

// CompileScript compiles Go source to a sandboxed WASM module via TinyGo
// and registers it under name.
func (ms *MeshStore) CompileScript(ctx context.Context, name, source string) (*scripting.CompiledScript, error) {
	if ms.wasmEngine == nil {
		return nil, fmt.Errorf("%w (%v)", errWasmUnavailable, ms.wasmUnavailable)
	}
	return ms.wasmEngine.Compile(name, source)
}

// ExecuteScript runs a previously compiled script by name.
func (ms *MeshStore) ExecuteScript(ctx context.Context, name, input string) (string, error) {
	if ms.wasmEngine == nil {
		return "", fmt.Errorf("%w (%v)", errWasmUnavailable, ms.wasmUnavailable)
	}
	return ms.wasmEngine.Execute(name, input)
}

// ExecuteInlineScript compiles and immediately runs Go source without
// registering it.
func (ms *MeshStore) ExecuteInlineScript(ctx context.Context, source, input string) (string, error) {
	if ms.wasmEngine == nil {
		return "", fmt.Errorf("%w (%v)", errWasmUnavailable, ms.wasmUnavailable)
	}
	return ms.wasmEngine.ExecuteInline(source, input)
}

// GetScript retrieves a registered script by name.
func (ms *MeshStore) GetScript(ctx context.Context, name string) (*scripting.CompiledScript, error) {
	if ms.wasmEngine == nil {
		return nil, fmt.Errorf("%w (%v)", errWasmUnavailable, ms.wasmUnavailable)
	}
	return ms.wasmEngine.GetScript(name)
}

// ListScripts returns all registered scripts.
func (ms *MeshStore) ListScripts(ctx context.Context) []*scripting.CompiledScript {
	if ms.wasmEngine == nil {
		return nil
	}
	return ms.wasmEngine.ListScripts()
}

// DeleteScript removes a registered script.
func (ms *MeshStore) DeleteScript(ctx context.Context, name string) error {
	if ms.wasmEngine == nil {
		return fmt.Errorf("%w (%v)", errWasmUnavailable, ms.wasmUnavailable)
	}
	return ms.wasmEngine.DeleteScript(name)
}

// ===== Search =====

// IndexDocument adds a document to the search index.
func (ms *MeshStore) IndexDocument(ctx context.Context, doc *search.Document) error {
	return ms.searchEngine.IndexDocument(doc)
}

// SearchBM25 performs BM25 full-text search.
func (ms *MeshStore) SearchBM25(ctx context.Context, query string, topK int) []search.SearchResult {
	return ms.searchEngine.SearchBM25(query, topK)
}

// SearchVector performs vector similarity search.
func (ms *MeshStore) SearchVector(ctx context.Context, vector []float32, topK int) []search.SearchResult {
	return ms.searchEngine.SearchVector(vector, topK)
}

// SearchHybrid performs hybrid BM25 + vector search.
func (ms *MeshStore) SearchHybrid(ctx context.Context, query string, vector []float32, topK int) []search.SearchResult {
	return ms.searchEngine.SearchHybrid(query, vector, topK)
}

// DeleteSearchDocument removes a document from the search index.
func (ms *MeshStore) DeleteSearchDocument(ctx context.Context, id string) error {
	return ms.searchEngine.DeleteDocument(id)
}

// ===== Ranking =====

// Rank re-ranks a list of already-scored items using the named strategy
// ("bm25", "vector", "llm", or "context"; unknown names fall back to
// "bm25" -- see ranking.LLMRanker's doc comment for why "llm" is not an
// actual LLM call). boosts, if non-nil, applies to the "context" strategy
// as per-ID score multipliers.
func (ms *MeshStore) Rank(ctx context.Context, items []ranking.RankedItem, strategy string, boosts map[string]float32) []ranking.RankedItem {
	var ranker ranking.Ranker
	switch strategy {
	case "vector":
		ranker = ranking.NewVectorRanker()
	case "llm":
		ranker = ranking.NewLLMRanker()
	case "context":
		ctxMap := map[string]interface{}{}
		if boosts != nil {
			ctxMap["boosts"] = boosts
		}
		ranker = ranking.NewContextRanker(ctxMap)
	default:
		ranker = ranking.NewBM25Ranker()
	}
	return ranker.Rank(items)
}

// ===== Metrics =====

// GetMetrics returns current operational metrics as a map.
func (ms *MeshStore) GetMetrics(ctx context.Context) map[string]interface{} {
	return ms.metricsColl.GetStats()
}

// GetPrometheusMetrics returns metrics formatted for Prometheus scraping.
func (ms *MeshStore) GetPrometheusMetrics(ctx context.Context) string {
	return ms.metricsColl.PrometheusMetrics()
}

// backgroundCleanup removes expired cache entries.
func (ms *MeshStore) backgroundCleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ms.stopChan:
			return
		case <-ticker.C:
			ms.mu.Lock()
			now := time.Now()
			for _, kv := range ms.cache {
				for key, entry := range kv {
					if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
						delete(kv, key)
					}
				}
			}
			ms.mu.Unlock()
		}
	}
}
