package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/toll-mesh/store/pubsub"
	"github.com/toll-mesh/store/queue"
	"github.com/toll-mesh/store/ranking"
	"github.com/toll-mesh/store/scripting"
	"github.com/toll-mesh/store/search"
	"github.com/toll-mesh/store/sortedset"
	"github.com/toll-mesh/store/stream"
	"github.com/toll-mesh/store/transactions"
)

// PersistenceEngine handles disk-based persistence with WAL and snapshots
type PersistenceEngine struct {
	mu               sync.RWMutex
	walPath          string
	snapshotPath     string
	snapshotInterval time.Duration
	walFile          *os.File
	stopChan         chan struct{}
}

// currentSnapshotFormatVersion is bumped whenever a change to Snapshot's
// fields would change how an OLDER snapshot must be interpreted (not for
// purely additive changes -- JSON already handles a new optional field
// gracefully, an old snapshot simply won't have it and restore treats
// that the same as "nothing to restore for this feature", exactly how
// every feature group added after the original three already works).
// Bump this, and add a case to upgradeSnapshot, only when an existing
// field's meaning or shape changes in a way older data must be
// transformed to match. Snapshots taken before this field existed at all
// decode with FormatVersion == 0, treated as version 1 (see
// LoadLatestSnapshot).
const currentSnapshotFormatVersion = 1

// Snapshot represents a point-in-time snapshot of store state
type Snapshot struct {
	// FormatVersion identifies which shape of Snapshot this is. Without
	// it, a future format change has no way to tell "old data in the old
	// shape" apart from "old data that happens to look like the new
	// shape but isn't" -- silently misinterpreting a field, rather than
	// failing loudly or upgrading it correctly, is the failure mode this
	// exists to prevent.
	FormatVersion    int                          `json:"format_version"`
	Timestamp        int64                        `json:"timestamp"`
	RateLimiters     map[string]interface{}       `json:"rate_limiters"`
	ReplayProtection []string                     `json:"replay_protection"`
	Cache            map[string]map[string][]byte `json:"cache"`
	CacheTTL         map[string]map[string]int64  `json:"cache_ttl"`
	// CacheTimestamp/CacheNode mirror Cache's namespace/key shape and
	// carry each entry's LWW-register version (see cacheEntry in
	// store/mesh_store.go) -- needed so a snapshot round-trip doesn't
	// lose the version info a later cache merge needs.
	CacheTimestamp map[string]map[string]int64  `json:"cache_timestamp,omitempty"`
	CacheNode      map[string]map[string]string `json:"cache_node,omitempty"`

	// The fields below cover the nine feature groups that gained gossip
	// replication after the original three primitives above -- each one
	// is exactly what that feature's own Snapshot() method (used for
	// gossip too) returns, so restoring one just means feeding it back
	// through that same feature's own MergeSnapshot(), the identical
	// operation gossip already performs when learning state from a peer,
	// just sourced from local disk instead of the network. Without these,
	// a lone node with no peers (or an entire cluster restarting at once,
	// with no peer left holding the state to re-gossip it) permanently
	// lost all of Sorted Sets/Streams/Pipelines/Search/Job Queues/Pub-Sub/
	// Transactions/WASM Scripts/Metrics on every restart, since none of
	// them were ever WAL-logged either -- this closes that gap for the
	// snapshot/restore path specifically (WAL logging for these remains
	// out of scope; only the explicit CreateSnapshot/RestoreFromLatest
	// path covers them).
	SortedSets      map[string][]sortedset.SortedSetMember `json:"sorted_sets,omitempty"`
	Streams         map[string][]stream.StreamEntry        `json:"streams,omitempty"`
	Pipelines       []scripting.Pipeline                   `json:"pipelines,omitempty"`
	SearchDocuments []search.Document                      `json:"search_documents,omitempty"`
	JobQueues       map[string][]queue.Job                 `json:"job_queues,omitempty"`
	PubSubMessages  map[string][]pubsub.Message            `json:"pubsub_messages,omitempty"`
	Transactions    []transactions.Transaction             `json:"transactions,omitempty"`
	// WasmScripts is included so a lone restarted node recovers its
	// scripts from disk rather than needing a peer -- at the cost of
	// real recompilation time (the same "real seconds" TinyGo cost
	// described on scripting.WasmEngine.MergeSnapshot) added to this
	// node's own startup, once per script, if any were registered.
	WasmScripts []scripting.CompiledScript  `json:"wasm_scripts,omitempty"`
	Metrics     map[string]map[string]int64 `json:"metrics,omitempty"`
	// RankingConfigs holds every named ranking configuration -- see
	// ranking.Registry's doc comments; it's a tenth feature group added
	// after the original nine, following the exact same Snapshot/
	// MergeSnapshot reuse pattern.
	RankingConfigs []ranking.RankingConfig `json:"ranking_configs,omitempty"`
}

// WALEntry represents a write-ahead log entry
type WALEntry struct {
	Timestamp int64       `json:"timestamp"`
	Operation string      `json:"operation"` // consume, seen, set
	Key       string      `json:"key"`
	Value     interface{} `json:"value,omitempty"`
	Namespace string      `json:"namespace,omitempty"`
	// ExpiresAt is the entry's absolute expiry (Unix millis), for "set"
	// entries with a TTL. Storing the absolute time rather than a
	// duration means replaying this entry long after it was written still
	// produces the correct expiry -- including correctly producing an
	// already-expired entry, which callers already treat as absent.
	// Zero means no expiration.
	ExpiresAt int64 `json:"expires_at,omitempty"`
	// Node is the writer's node ID, for "set" entries -- part of the
	// cache LWW-register version alongside Version (not Timestamp; see
	// Version's doc comment for why these two are different fields).
	Node string `json:"node,omitempty"`
	// Version is the cache LWW-register version for "set" entries --
	// deliberately separate from Timestamp. Timestamp always means "when
	// this node wrote this WAL entry" and must stay in this node's own
	// WAL-sequence order for ReplayWAL's snapshot-cutoff filtering to work
	// (entry.Timestamp > afterTimestamp). Version is the CRDT version a
	// cache entry actually carries: for a normal local Set it's the same
	// value as Timestamp, but for an entry this node is persisting *after
	// learning it via gossip from a peer*, Version is the peer's original
	// write time, which can be earlier than this node's own snapshots --
	// logging that under Timestamp would make ReplayWAL wrongly treat it
	// as already covered by an intervening local snapshot and drop it on
	// recovery. Confirmed live: without this split, a gossip-merged value
	// vanished on the receiving node's next restart, reverting to that
	// node's own older local write.
	Version int64 `json:"version,omitempty"`
}

// NewPersistenceEngine creates a new persistence engine
func NewPersistenceEngine(walPath, snapshotPath string, snapshotInterval time.Duration) (*PersistenceEngine, error) {
	// Create directories if they don't exist
	if err := os.MkdirAll(walPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}
	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	pe := &PersistenceEngine{
		walPath:          walPath,
		snapshotPath:     snapshotPath,
		snapshotInterval: snapshotInterval,
		stopChan:         make(chan struct{}),
	}

	// Open WAL file for appending
	walFile, err := os.OpenFile(
		filepath.Join(walPath, "wal.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	pe.walFile = walFile

	return pe, nil
}

// LogOperation writes an operation to the WAL. expiresAt is the entry's
// absolute expiry in Unix millis (0 for no expiration); it's only
// meaningful for "set". node is the writer's node ID, also only
// meaningful for "set" (part of the cache LWW-register version). version
// is the cache LWW-register version to record; pass 0 for a normal local
// write (it defaults to this call's own timestamp) -- pass the original
// write's own timestamp when persisting a value learned via gossip from a
// peer, so recovery reconstructs the correct CRDT version rather than one
// stamped at merge time (see WALEntry.Version's doc comment for why this
// distinction matters).
func (pe *PersistenceEngine) LogOperation(op string, key string, value interface{}, namespace string, expiresAt int64, node string, version int64) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// Nanosecond resolution matters here: this timestamp is compared
	// against a snapshot's timestamp in ReplayWAL to decide whether an
	// entry is "already covered" by that snapshot. Millisecond resolution
	// let a snapshot and the very next write -- executed as two separate
	// statements immediately after each other, e.g. in a test -- land in
	// the same millisecond, making a real write silently indistinguishable
	// from "before the snapshot" and dropped by recovery. Confirmed live
	// via a failing test.
	now := time.Now().UnixNano()
	if version == 0 {
		version = now
	}

	entry := WALEntry{
		Timestamp: now,
		Operation: op,
		Key:       key,
		Value:     value,
		Namespace: namespace,
		ExpiresAt: expiresAt,
		Node:      node,
		Version:   version,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	_, err = pe.walFile.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	return nil
}

// CreateSnapshot creates a point-in-time snapshot
func (pe *PersistenceEngine) CreateSnapshot(snapshot *Snapshot) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// Same nanosecond-resolution reasoning as LogOperation's Timestamp --
	// this value is the cutoff ReplayWAL filters against.
	snapshot.Timestamp = time.Now().UnixNano()
	snapshot.FormatVersion = currentSnapshotFormatVersion

	snapshotFile := filepath.Join(
		pe.snapshotPath,
		fmt.Sprintf("snapshot-%d.json", snapshot.Timestamp),
	)

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	if err := os.WriteFile(snapshotFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	return nil
}

// LoadLatestSnapshot loads the most recent snapshot
func (pe *PersistenceEngine) LoadLatestSnapshot() (*Snapshot, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	entries, err := os.ReadDir(pe.snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot directory: %w", err)
	}

	if len(entries) == 0 {
		return nil, nil // No snapshots yet
	}

	// Get the last file (snapshots are named with timestamp)
	lastEntry := entries[len(entries)-1]
	snapshotFile := filepath.Join(pe.snapshotPath, lastEntry.Name())

	data, err := os.ReadFile(snapshotFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	// A snapshot taken before FormatVersion existed decodes with the
	// JSON-unmarshal zero value (0) for that field -- that's the original
	// shape, equivalent to version 1, not an error.
	if snapshot.FormatVersion == 0 {
		snapshot.FormatVersion = 1
	}
	if snapshot.FormatVersion > currentSnapshotFormatVersion {
		// This binary is OLDER than whatever wrote this snapshot (e.g. a
		// rollback to a previous release after a newer one already ran
		// and took a snapshot in a shape this code doesn't understand).
		// Refusing to load is the safe choice -- silently proceeding
		// could misinterpret a field this version doesn't know the
		// meaning of.
		return nil, fmt.Errorf(
			"snapshot %s has format_version %d, but this build only understands up to version %d -- refusing to load it silently (this usually means a newer version of tollmeshcache wrote this snapshot; upgrade before restoring, or restore an older snapshot file instead)",
			snapshotFile, snapshot.FormatVersion, currentSnapshotFormatVersion,
		)
	}
	if snapshot.FormatVersion < currentSnapshotFormatVersion {
		if err := upgradeSnapshot(&snapshot); err != nil {
			return nil, fmt.Errorf("upgrading snapshot %s from format_version %d to %d: %w", snapshotFile, snapshot.FormatVersion, currentSnapshotFormatVersion, err)
		}
	}

	return &snapshot, nil
}

// upgradeSnapshot transforms an older-format Snapshot in place to the
// current format. There is only one version so far (nothing to upgrade
// from yet); this exists as the place a real transformation gets added
// alongside the next currentSnapshotFormatVersion bump, rather than
// scattering ad hoc "if old field is set" checks through LoadLatestSnapshot
// itself.
func upgradeSnapshot(snapshot *Snapshot) error {
	switch snapshot.FormatVersion {
	case 1:
		snapshot.FormatVersion = currentSnapshotFormatVersion
		return nil
	default:
		return fmt.Errorf("no upgrade path known from format_version %d", snapshot.FormatVersion)
	}
}

// ReplayWAL replays all WAL entries after a given timestamp
func (pe *PersistenceEngine) ReplayWAL(afterTimestamp int64) ([]WALEntry, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	walFile := filepath.Join(pe.walPath, "wal.log")
	data, err := os.ReadFile(walFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No WAL yet
		}
		return nil, fmt.Errorf("failed to read WAL: %w", err)
	}

	var entries []WALEntry
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}

		var entry WALEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue // Skip malformed entries
		}

		if entry.Timestamp > afterTimestamp {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// RotateWAL creates a new WAL file and archives the old one
func (pe *PersistenceEngine) RotateWAL() error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// Close current WAL file
	if pe.walFile != nil {
		pe.walFile.Close()
	}

	// Archive old WAL
	oldWAL := filepath.Join(pe.walPath, "wal.log")
	archivedWAL := filepath.Join(pe.walPath, fmt.Sprintf("wal-%d.log", time.Now().UnixMilli()))

	if err := os.Rename(oldWAL, archivedWAL); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to archive WAL: %w", err)
	}

	// Open new WAL file
	walFile, err := os.OpenFile(
		oldWAL,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return fmt.Errorf("failed to open new WAL file: %w", err)
	}
	pe.walFile = walFile

	return nil
}

// Close gracefully shuts down the persistence engine
func (pe *PersistenceEngine) Close() error {
	close(pe.stopChan)
	if pe.walFile != nil {
		return pe.walFile.Close()
	}
	return nil
}

// GetStats returns persistence statistics
func (pe *PersistenceEngine) GetStats() map[string]interface{} {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	walInfo, _ := os.Stat(filepath.Join(pe.walPath, "wal.log"))
	walSize := int64(0)
	if walInfo != nil {
		walSize = walInfo.Size()
	}

	entries, _ := os.ReadDir(pe.snapshotPath)
	snapshotCount := len(entries)

	return map[string]interface{}{
		"wal_size_bytes":    walSize,
		"snapshot_count":    snapshotCount,
		"snapshot_interval": pe.snapshotInterval.String(),
		"wal_path":          pe.walPath,
		"snapshot_path":     pe.snapshotPath,
	}
}
