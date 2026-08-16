package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SnapshotConfig holds configuration for snapshots
type SnapshotConfig struct {
	SnapshotInterval time.Duration // Interval between snapshots
	MaxSnapshots     int           // Maximum number of snapshots to keep
	CompressionLevel int           // Compression level (0-9)
}

// SnapshotMetadata contains metadata about a snapshot
type SnapshotMetadata struct {
	ID        string    `json:"id"`
	Timestamp int64     `json:"timestamp"`
	Size      int64     `json:"size"`
	Entries   int64     `json:"entries"`
	Hash      string    `json:"hash"`
	CreatedAt time.Time `json:"created_at"`
}

// SnapshotManager manages point-in-time snapshots
type SnapshotManager struct {
	mu               sync.RWMutex
	config           SnapshotConfig
	snapshotDir      string
	snapshots        []*SnapshotMetadata
	lastSnapshotTime time.Time
	snapshotTicker   *time.Ticker
	stopChan         chan struct{}
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(snapshotDir string, config SnapshotConfig) (*SnapshotManager, error) {
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	sm := &SnapshotManager{
		config:           config,
		snapshotDir:      snapshotDir,
		snapshots:        make([]*SnapshotMetadata, 0),
		lastSnapshotTime: time.Now(),
		stopChan:         make(chan struct{}),
	}

	// Load existing snapshots
	if err := sm.loadExistingSnapshots(); err != nil {
		return nil, fmt.Errorf("failed to load existing snapshots: %w", err)
	}

	return sm, nil
}

// CreateSnapshot creates a new snapshot of the current state
func (sm *SnapshotManager) CreateSnapshot(snapshot *Snapshot) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Generate snapshot ID
	snapshotID := fmt.Sprintf("snapshot-%d", time.Now().UnixNano())
	snapshotPath := filepath.Join(sm.snapshotDir, snapshotID+".json")

	// Marshal snapshot to JSON
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	// Write snapshot to file
	if err := os.WriteFile(snapshotPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	// Create metadata
	metadata := &SnapshotMetadata{
		ID:        snapshotID,
		Timestamp: snapshot.Timestamp,
		Size:      int64(len(data)),
		Entries:   int64(len(snapshot.RateLimiters) + len(snapshot.ReplayProtection)),
		Hash:      sm.calculateHash(data),
		CreatedAt: time.Now(),
	}

	sm.snapshots = append(sm.snapshots, metadata)

	// Clean up old snapshots if needed
	if len(sm.snapshots) > sm.config.MaxSnapshots {
		sm.cleanupOldSnapshots()
	}

	sm.lastSnapshotTime = time.Now()
	return nil
}

// GetLatestSnapshot retrieves the latest snapshot
func (sm *SnapshotManager) GetLatestSnapshot() (*Snapshot, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if len(sm.snapshots) == 0 {
		return nil, fmt.Errorf("no snapshots available")
	}

	latestMetadata := sm.snapshots[len(sm.snapshots)-1]
	return sm.loadSnapshot(latestMetadata)
}

// GetSnapshotByID retrieves a specific snapshot by ID
func (sm *SnapshotManager) GetSnapshotByID(snapshotID string) (*Snapshot, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, metadata := range sm.snapshots {
		if metadata.ID == snapshotID {
			return sm.loadSnapshot(metadata)
		}
	}

	return nil, fmt.Errorf("snapshot %s not found", snapshotID)
}

// ListSnapshots returns a list of all available snapshots
func (sm *SnapshotManager) ListSnapshots() []*SnapshotMetadata {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	snapshots := make([]*SnapshotMetadata, len(sm.snapshots))
	copy(snapshots, sm.snapshots)
	return snapshots
}

// DeleteSnapshot deletes a specific snapshot
func (sm *SnapshotManager) DeleteSnapshot(snapshotID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snapshotPath := filepath.Join(sm.snapshotDir, snapshotID+".json")

	// Remove from disk
	if err := os.Remove(snapshotPath); err != nil {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}

	// Remove from list
	for i, metadata := range sm.snapshots {
		if metadata.ID == snapshotID {
			sm.snapshots = append(sm.snapshots[:i], sm.snapshots[i+1:]...)
			break
		}
	}

	return nil
}

// GetStats returns snapshot statistics
func (sm *SnapshotManager) GetStats() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	totalSize := int64(0)
	totalEntries := int64(0)

	for _, metadata := range sm.snapshots {
		totalSize += metadata.Size
		totalEntries += metadata.Entries
	}

	return map[string]interface{}{
		"total_snapshots":    len(sm.snapshots),
		"total_size_bytes":   totalSize,
		"total_entries":      totalEntries,
		"snapshot_directory": sm.snapshotDir,
		"last_snapshot_time": sm.lastSnapshotTime.Format(time.RFC3339),
		"snapshot_interval":  sm.config.SnapshotInterval.String(),
		"max_snapshots":      sm.config.MaxSnapshots,
	}
}

// Start begins the snapshot timer
func (sm *SnapshotManager) Start() {
	sm.snapshotTicker = time.NewTicker(sm.config.SnapshotInterval)
	go sm.snapshotLoop()
}

// Stop gracefully shuts down the snapshot manager
func (sm *SnapshotManager) Stop() error {
	close(sm.stopChan)
	if sm.snapshotTicker != nil {
		sm.snapshotTicker.Stop()
	}
	return nil
}

// Private helper methods

func (sm *SnapshotManager) loadExistingSnapshots() error {
	entries, err := os.ReadDir(sm.snapshotDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		// Parse snapshot ID from filename
		snapshotID := entry.Name()[:len(entry.Name())-5] // Remove .json

		// Read metadata
		path := filepath.Join(sm.snapshotDir, entry.Name())
		info, err := os.Stat(path)
		if err != nil {
			continue
		}

		metadata := &SnapshotMetadata{
			ID:        snapshotID,
			Size:      info.Size(),
			CreatedAt: info.ModTime(),
		}

		sm.snapshots = append(sm.snapshots, metadata)
	}

	return nil
}

func (sm *SnapshotManager) loadSnapshot(metadata *SnapshotMetadata) (*Snapshot, error) {
	snapshotPath := filepath.Join(sm.snapshotDir, metadata.ID+".json")

	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	return &snapshot, nil
}

func (sm *SnapshotManager) cleanupOldSnapshots() {
	// Keep only the most recent MaxSnapshots
	if len(sm.snapshots) > sm.config.MaxSnapshots {
		toDelete := len(sm.snapshots) - sm.config.MaxSnapshots
		for i := 0; i < toDelete; i++ {
			snapshotPath := filepath.Join(sm.snapshotDir, sm.snapshots[i].ID+".json")
			_ = os.Remove(snapshotPath)
		}
		sm.snapshots = sm.snapshots[toDelete:]
	}
}

func (sm *SnapshotManager) calculateHash(data []byte) string {
	hash := 0
	for _, b := range data {
		hash = ((hash << 5) - hash) + int(b)
	}
	return fmt.Sprintf("%x", hash)
}

func (sm *SnapshotManager) snapshotLoop() {
	for {
		select {
		case <-sm.stopChan:
			return
		case <-sm.snapshotTicker.C:
			// Snapshot creation is triggered externally
		}
	}
}
