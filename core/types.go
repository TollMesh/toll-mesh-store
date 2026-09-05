package core

import (
	"context"
	"time"

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

// Store is the distributed store interface for Toll Mesh coordination.
type Store interface {
	Consume(ctx context.Context, key string, limit int, window time.Duration) (ConsumeResult, error)
	Seen(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Get(ctx context.Context, ns, key string) ([]byte, bool, error)
	Set(ctx context.Context, ns, key string, value []byte, ttl time.Duration) error
	Close() error

	// GetState and MergeState drive multi-node gossip replication of the
	// rate limiter/replay-protection/cache CRDTs. See MeshStore's doc
	// comments on these two methods for what is and isn't covered.
	GetState() *MeshStoreState
	MergeState(peer *MeshStoreState)

	// Job Queues
	Enqueue(ctx context.Context, queueName string, payload []byte, priority, maxRetries int, deadline time.Duration) (*queue.Job, error)
	ClaimJob(ctx context.Context, queueName, workerID string) (*queue.Job, error)
	CompleteJob(ctx context.Context, queueName, jobID string, result []byte) error
	FailJob(ctx context.Context, queueName, jobID, errMsg string) error
	GetJobStatus(ctx context.Context, queueName, jobID string) (*queue.Job, error)
	GetQueueStats(ctx context.Context, queueName string) (map[string]interface{}, error)

	// Sorted Sets
	ZAdd(ctx context.Context, key, member string, score float64) error
	ZRem(ctx context.Context, key, member string) error
	ZScore(ctx context.Context, key, member string) (float64, bool)
	ZRank(ctx context.Context, key, member string) (int64, bool)
	ZRevRank(ctx context.Context, key, member string) (int64, bool)
	ZRange(ctx context.Context, key string, min, max float64, limit int64) []*sortedset.SortedSetMember
	ZRevRange(ctx context.Context, key string, max, min float64, limit int64) []*sortedset.SortedSetMember
	ZRangeByRank(ctx context.Context, key string, start, stop int64) []*sortedset.SortedSetMember
	ZCard(ctx context.Context, key string) int64

	// Streams
	XAdd(ctx context.Context, streamName string, fields map[string]string) (*stream.StreamEntry, error)
	XRange(ctx context.Context, streamName, startID, endID string, limit int64) []*stream.StreamEntry
	XLen(ctx context.Context, streamName string) int64
	XGroupCreate(ctx context.Context, streamName, groupName string) error
	XReadGroup(ctx context.Context, streamName, groupName, consumerID string, limit int64) ([]*stream.StreamEntry, error)
	XAck(ctx context.Context, streamName, groupName, consumerID, entryID string) error

	// Pub/Sub
	Subscribe(ctx context.Context, subscriberID, topic, pattern string) error
	Unsubscribe(ctx context.Context, subscriberID, topic string) error
	Publish(ctx context.Context, topic, publisher string, payload []byte) (int, error)
	PollMessages(ctx context.Context, subscriberID string, limit int, timeout time.Duration) ([]pubsub.Message, error)
	GetTopics(ctx context.Context) []string
	GetTopicSubscribers(ctx context.Context, topic string) []string
	GetPubSubStats(ctx context.Context) map[string]interface{}

	// Transactions
	BeginTransaction(ctx context.Context, txnID string) (*transactions.Transaction, error)
	AddTransactionOperation(ctx context.Context, txnID string, op transactions.Operation) error
	CommitTransaction(ctx context.Context, txnID string) error
	RollbackTransaction(ctx context.Context, txnID string) error
	GetTransactionStatus(ctx context.Context, txnID string) (transactions.TransactionStatus, error)

	// Persistence
	CreateSnapshot(ctx context.Context) error
	GetLatestSnapshot(ctx context.Context) (*persistence.Snapshot, error)
	RestoreFromLatestSnapshot(ctx context.Context) error
	GetPersistenceStats(ctx context.Context) map[string]interface{}

	// Scripting (Pipelines) -- safe, no-code-execution operation composition
	RegisterPipeline(ctx context.Context, p *scripting.Pipeline) error
	ExecutePipeline(ctx context.Context, name string) (*scripting.ExecutionResult, error)
	ExecuteInlinePipeline(ctx context.Context, steps []scripting.Step) (*scripting.ExecutionResult, error)
	GetPipeline(ctx context.Context, name string) (*scripting.Pipeline, error)
	ListPipelines(ctx context.Context) []*scripting.Pipeline
	DeletePipeline(ctx context.Context, name string) error

	// Scripting (WASM) -- real arbitrary Go code, compiled via TinyGo and
	// run sandboxed via wazero. Returns an error naming the reason (e.g.
	// "tinygo not found") if the TinyGo toolchain wasn't available at
	// startup; every other feature keeps working regardless.
	CompileScript(ctx context.Context, name, source string) (*scripting.CompiledScript, error)
	ExecuteScript(ctx context.Context, name, input string) (string, error)
	ExecuteInlineScript(ctx context.Context, source, input string) (string, error)
	GetScript(ctx context.Context, name string) (*scripting.CompiledScript, error)
	ListScripts(ctx context.Context) []*scripting.CompiledScript
	DeleteScript(ctx context.Context, name string) error

	// Search
	IndexDocument(ctx context.Context, doc *search.Document) error
	SearchBM25(ctx context.Context, query string, topK int) []search.SearchResult
	SearchVector(ctx context.Context, vector []float32, topK int) []search.SearchResult
	SearchHybrid(ctx context.Context, query string, vector []float32, topK int) []search.SearchResult
	DeleteSearchDocument(ctx context.Context, id string) error

	// Ranking
	Rank(ctx context.Context, items []ranking.RankedItem, strategy string, boosts map[string]float32) []ranking.RankedItem

	// Metrics
	GetMetrics(ctx context.Context) map[string]interface{}
	GetPrometheusMetrics(ctx context.Context) string
}

// ConsumeResult represents the outcome of a rate limit check.
type ConsumeResult struct {
	OK        bool
	Remaining int
	ResetAt   int64
}

// Node represents a member in the mesh network.
type Node struct {
	ID      string
	Address string
	Port    int
}

// ClusterConfig holds configuration for the mesh cluster.
type ClusterConfig struct {
	NodeName      string
	BindAddr      string
	BindPort      int
	AdvertiseAddr string
	AdvertisePort int
	Nodes         []Node
	EncryptionKey []byte
	// DataDir is where persistence (WAL segments, snapshots) is written.
	// Defaults to "./data/<NodeName>" if empty.
	DataDir string
}

// MeshStoreState is a serializable snapshot of a MeshStore's CRDT state,
// used by the coordination package to gossip and merge state between peers.
type MeshStoreState struct {
	RateLimiters     map[string]interface{}
	ReplayProtection map[string]bool
	Cache            map[string]map[string][]byte
	// CacheTTL holds each cache entry's expiry as Unix millis, mirroring
	// Cache's namespace/key shape. A zero or absent entry means no TTL.
	CacheTTL map[string]map[string]int64
	// CacheTimestamp/CacheNode mirror Cache's namespace/key shape and
	// carry each entry's LWW-register version: MergeState uses these to
	// decide, for a key present on both sides, whether the peer's value
	// is newer (adopt it) or older/equal (keep local) -- the later
	// Timestamp wins, Node breaks an exact tie. Without these, cache
	// merge can only safely do a conservative union (peer fills in keys
	// the local side lacks), which doesn't converge two nodes' concurrent
	// writes to the *same* key the way GCounter/GSet do.
	CacheTimestamp map[string]map[string]int64
	CacheNode      map[string]map[string]string
}
