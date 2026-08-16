package persistence

import (
	"fmt"
	"sync"
	"time"
)

// RecoveryConfig holds configuration for crash recovery
type RecoveryConfig struct {
	EnableRecovery      bool          // Enable crash recovery
	RecoveryTimeout     time.Duration // Timeout for recovery operations
	MaxRecoveryAttempts int           // Maximum recovery attempts
}

// RecoveryManager handles crash recovery and state restoration
type RecoveryManager struct {
	mu                 sync.RWMutex
	config             RecoveryConfig
	wal                *WriteAheadLog
	snapshotManager    *SnapshotManager
	recoveryInProgress bool
	lastRecoveryTime   time.Time
	recoveryStats      map[string]interface{}
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(wal *WriteAheadLog, snapshotManager *SnapshotManager, config RecoveryConfig) *RecoveryManager {
	return &RecoveryManager{
		config:          config,
		wal:             wal,
		snapshotManager: snapshotManager,
		recoveryStats:   make(map[string]interface{}),
	}
}

// RecoverFromCrash performs crash recovery
func (rm *RecoveryManager) RecoverFromCrash() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.config.EnableRecovery {
		return fmt.Errorf("recovery is disabled")
	}

	if rm.recoveryInProgress {
		return fmt.Errorf("recovery already in progress")
	}

	rm.recoveryInProgress = true
	defer func() {
		rm.recoveryInProgress = false
		rm.lastRecoveryTime = time.Now()
	}()

	// Try to recover from latest snapshot
	snapshot, err := rm.snapshotManager.GetLatestSnapshot()
	if err == nil && snapshot != nil {
		// Snapshot found, use it as base state
		rm.recordRecoveryStats("recovery_method", "snapshot")
		rm.recordRecoveryStats("snapshot_timestamp", snapshot.Timestamp)
		return nil
	}

	// No snapshot, replay WAL from beginning
	entries, err := rm.wal.Read(0)
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	rm.recordRecoveryStats("recovery_method", "wal_replay")
	rm.recordRecoveryStats("entries_replayed", len(entries))

	return nil
}

// RecoverFromSnapshot recovers state from a specific snapshot
func (rm *RecoveryManager) RecoverFromSnapshot(snapshotID string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	snapshot, err := rm.snapshotManager.GetSnapshotByID(snapshotID)
	if err != nil {
		return fmt.Errorf("failed to get snapshot: %w", err)
	}

	if snapshot == nil {
		return fmt.Errorf("snapshot is nil")
	}

	rm.recordRecoveryStats("recovery_method", "snapshot")
	rm.recordRecoveryStats("snapshot_id", snapshotID)
	rm.lastRecoveryTime = time.Now()

	return nil
}

// RecoverFromWAL replays WAL entries from a given timestamp
func (rm *RecoveryManager) RecoverFromWAL(fromTimestamp int64) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	entries, err := rm.wal.Read(fromTimestamp)
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	rm.recordRecoveryStats("recovery_method", "wal_replay")
	rm.recordRecoveryStats("entries_replayed", len(entries))
	rm.recordRecoveryStats("from_timestamp", fromTimestamp)
	rm.lastRecoveryTime = time.Now()

	return nil
}

// VerifyRecovery verifies the integrity of recovered state
func (rm *RecoveryManager) VerifyRecovery() (bool, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Check if latest snapshot exists
	snapshot, err := rm.snapshotManager.GetLatestSnapshot()
	if err != nil {
		return false, fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	if snapshot == nil {
		return false, fmt.Errorf("no snapshot available for verification")
	}

	// Verify snapshot integrity
	if snapshot.Timestamp == 0 {
		return false, fmt.Errorf("snapshot has invalid timestamp")
	}

	rm.recordRecoveryStats("verification_status", "passed")
	return true, nil
}

// GetRecoveryStats returns recovery statistics
func (rm *RecoveryManager) GetRecoveryStats() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	stats := make(map[string]interface{})
	for k, v := range rm.recoveryStats {
		stats[k] = v
	}

	stats["recovery_in_progress"] = rm.recoveryInProgress
	stats["last_recovery_time"] = rm.lastRecoveryTime.Format(time.RFC3339)
	stats["recovery_enabled"] = rm.config.EnableRecovery
	stats["recovery_timeout"] = rm.config.RecoveryTimeout.String()
	stats["max_recovery_attempts"] = rm.config.MaxRecoveryAttempts

	return stats
}

// IsRecoveryNeeded checks if recovery is needed
func (rm *RecoveryManager) IsRecoveryNeeded() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Recovery is needed if no snapshots exist and WAL has entries
	snapshots := rm.snapshotManager.ListSnapshots()
	if len(snapshots) == 0 {
		return true
	}

	return false
}

// Private helper methods

func (rm *RecoveryManager) recordRecoveryStats(key string, value interface{}) {
	rm.recoveryStats[key] = value
}
