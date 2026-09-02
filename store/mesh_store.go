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

// MeshStore implements the Store interface using CRDTs and gossip protocol.
type MeshStore struct {
	mu               sync.RWMutex
	config           *core.ClusterConfig
	rateLimiters     map[string]*core.GCounter
	replayProtection *core.GSet
	cache            map[string]map[string][]byte
	cacheTTL         map[string]map[string]time.Time
	stopChan         chan struct{}

	jobManager *queue.JobManager

	zsetsMu sync.RWMutex
	zsets   map[string]*sortedset.SortedSet

	streamsMu sync.RWMutex
	streams   map[string]*stream.Stream

	groupsMu sync.RWMutex
	// keyed by "streamName:groupName"
	groups map[string]*stream.ConsumerGroup

	pubsubBroker *pubsub.PubSubBroker
	txnManager   *transactions.TransactionManager
	persistence  *persistence.PersistenceEngine
	pipelines    *scripting.Engine
	searchEngine *search.HybridSearchEngine
	metricsColl  *metrics.Metrics
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
		cache:            make(map[string]map[string][]byte),
		cacheTTL:         make(map[string]map[string]time.Time),
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

	ms.registerPipelineHandlers()

	go ms.backgroundCleanup()
	return ms, nil
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

	value, exists := nsCache[key]
	if !exists {
		return nil, false, nil
	}

	ttlMap, ttlExists := ms.cacheTTL[ns]
	if ttlExists {
		if expiry, ok := ttlMap[key]; ok {
			if time.Now().After(expiry) {
				return nil, false, nil
			}
		}
	}

	hit = true
	return value, true, nil
}

// Set stores a value with TTL.
func (ms *MeshStore) Set(ctx context.Context, ns, key string, value []byte, ttl time.Duration) error {
	start := time.Now()
	defer func() { ms.metricsColl.RecordSet(time.Since(start).Microseconds()) }()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.cache[ns]; !exists {
		ms.cache[ns] = make(map[string][]byte)
		ms.cacheTTL[ns] = make(map[string]time.Time)
	}

	ms.cache[ns][key] = value

	// ttl <= 0 means "no expiration" (this is the documented contract every
	// SDK exposes, e.g. Python's cache_set(..., ttl=None)). Get() treats a
	// key absent from cacheTTL as never expiring, so simply don't record an
	// expiry for it -- previously this always wrote time.Now().Add(ttl),
	// which for ttl=0 set the expiry to right now, making every "no TTL"
	// cache_set dead on arrival.
	if ttl > 0 {
		ms.cacheTTL[ns][key] = time.Now().Add(ttl)
	} else {
		delete(ms.cacheTTL[ns], key)
	}
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
			ms.cache[op.Namespace] = make(map[string][]byte)
			ms.cacheTTL[op.Namespace] = make(map[string]time.Time)
		}
		ms.cache[op.Namespace][op.Key] = []byte(valueStr)
		delete(ms.cacheTTL[op.Namespace], op.Key)
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

// CreateSnapshot captures the current live store state to disk.
func (ms *MeshStore) CreateSnapshot(ctx context.Context) error {
	ms.mu.RLock()
	rateLimiters := make(map[string]interface{}, len(ms.rateLimiters))
	for k, v := range ms.rateLimiters {
		rateLimiters[k] = v.Snapshot()
	}
	replayProtection := ms.replayProtection.Snapshot()

	cacheCopy := make(map[string]map[string][]byte, len(ms.cache))
	for ns, kv := range ms.cache {
		nsCopy := make(map[string][]byte, len(kv))
		for k, v := range kv {
			nsCopy[k] = v
		}
		cacheCopy[ns] = nsCopy
	}

	cacheTTLCopy := make(map[string]map[string]int64, len(ms.cacheTTL))
	for ns, kv := range ms.cacheTTL {
		nsCopy := make(map[string]int64, len(kv))
		for k, v := range kv {
			nsCopy[k] = v.UnixMilli()
		}
		cacheTTLCopy[ns] = nsCopy
	}
	ms.mu.RUnlock()

	snap := &persistence.Snapshot{
		RateLimiters:     rateLimiters,
		ReplayProtection: replayProtection,
		Cache:            cacheCopy,
		CacheTTL:         cacheTTLCopy,
	}

	return ms.persistence.CreateSnapshot(snap)
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

	ms.cache = make(map[string]map[string][]byte, len(snap.Cache))
	for ns, kv := range snap.Cache {
		nsCopy := make(map[string][]byte, len(kv))
		for k, v := range kv {
			nsCopy[k] = v
		}
		ms.cache[ns] = nsCopy
	}

	ms.cacheTTL = make(map[string]map[string]time.Time, len(snap.CacheTTL))
	for ns, kv := range snap.CacheTTL {
		nsCopy := make(map[string]time.Time, len(kv))
		for k, v := range kv {
			nsCopy[k] = time.UnixMilli(v)
		}
		ms.cacheTTL[ns] = nsCopy
	}

	return nil
}

// GetPersistenceStats returns persistence statistics.
func (ms *MeshStore) GetPersistenceStats(ctx context.Context) map[string]interface{} {
	return ms.persistence.GetStats()
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
			for ns, ttlMap := range ms.cacheTTL {
				for key, expiry := range ttlMap {
					if now.After(expiry) {
						delete(ms.cache[ns], key)
						delete(ttlMap, key)
					}
				}
			}
			ms.mu.Unlock()
		}
	}
}
