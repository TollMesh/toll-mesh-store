package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
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

// Snapshot represents a point-in-time snapshot of store state
type Snapshot struct {
	Timestamp        int64                        `json:"timestamp"`
	RateLimiters     map[string]interface{}       `json:"rate_limiters"`
	ReplayProtection []string                     `json:"replay_protection"`
	Cache            map[string]map[string][]byte `json:"cache"`
	CacheTTL         map[string]map[string]int64  `json:"cache_ttl"`
}

// WALEntry represents a write-ahead log entry
type WALEntry struct {
	Timestamp int64       `json:"timestamp"`
	Operation string      `json:"operation"` // consume, seen, set
	Key       string      `json:"key"`
	Value     interface{} `json:"value,omitempty"`
	Namespace string      `json:"namespace,omitempty"`
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

// LogOperation writes an operation to the WAL
func (pe *PersistenceEngine) LogOperation(op string, key string, value interface{}, namespace string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	entry := WALEntry{
		Timestamp: time.Now().UnixMilli(),
		Operation: op,
		Key:       key,
		Value:     value,
		Namespace: namespace,
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

	snapshot.Timestamp = time.Now().UnixMilli()

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

	return &snapshot, nil
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
