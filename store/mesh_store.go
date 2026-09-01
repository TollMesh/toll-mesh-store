package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/toll-mesh/store/core"
	"github.com/toll-mesh/store/queue"
	"github.com/toll-mesh/store/sortedset"
	"github.com/toll-mesh/store/stream"
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
}

// NewMeshStore creates a new MeshStore instance.
func NewMeshStore(config *core.ClusterConfig) (*MeshStore, error) {
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
	}
	go ms.backgroundCleanup()
	return ms, nil
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
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.replayProtection.Contains(key) {
		return true, nil
	}

	ms.replayProtection.Add(key)
	return false, nil
}

// Get retrieves a cached value.
func (ms *MeshStore) Get(ctx context.Context, ns, key string) ([]byte, bool, error) {
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

	return value, true, nil
}

// Set stores a value with TTL.
func (ms *MeshStore) Set(ctx context.Context, ns, key string, value []byte, ttl time.Duration) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.cache[ns]; !exists {
		ms.cache[ns] = make(map[string][]byte)
		ms.cacheTTL[ns] = make(map[string]time.Time)
	}

	ms.cache[ns][key] = value
	ms.cacheTTL[ns][key] = time.Now().Add(ttl)
	return nil
}

// Close gracefully shuts down the store.
func (ms *MeshStore) Close() error {
	ms.jobManager.Stop()
	close(ms.stopChan)
	return nil
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
