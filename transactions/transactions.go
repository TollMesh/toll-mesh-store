package transactions

import (
	"fmt"
	"sync"
	"time"
)

// TransactionStatus represents the state of a transaction
type TransactionStatus string

const (
	StatusPending    TransactionStatus = "pending"
	StatusCommitted  TransactionStatus = "committed"
	StatusRolledBack TransactionStatus = "rolled_back"
	StatusFailed     TransactionStatus = "failed"
)

// OperationType represents the type of operation
type OperationType string

const (
	OpConsume OperationType = "consume"
	OpSeen    OperationType = "seen"
	OpGet     OperationType = "get"
	OpSet     OperationType = "set"
)

// Operation represents a single operation in a transaction
type Operation struct {
	Type      OperationType
	Key       string
	Namespace string
	Value     interface{}
	Result    interface{}
	Error     error
	Timestamp int64
}

// Transaction represents an ACID transaction
type Transaction struct {
	ID         string
	Status     TransactionStatus
	Operations []Operation
	Created    int64
	Committed  int64
	Snapshot   map[string]interface{}
	mu         sync.RWMutex
}

// TransactionManager manages ACID transactions
type TransactionManager struct {
	mu           sync.RWMutex
	transactions map[string]*Transaction
	maxTxns      int
	txnTimeout   time.Duration
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(maxTxns int, txnTimeout time.Duration) *TransactionManager {
	tm := &TransactionManager{
		transactions: make(map[string]*Transaction),
		maxTxns:      maxTxns,
		txnTimeout:   txnTimeout,
	}

	// Start cleanup goroutine
	go tm.cleanupExpiredTransactions()

	return tm
}

// BeginTransaction starts a new transaction
func (tm *TransactionManager) BeginTransaction(txnID string) (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if len(tm.transactions) >= tm.maxTxns {
		return nil, fmt.Errorf("max transactions reached")
	}

	if _, exists := tm.transactions[txnID]; exists {
		return nil, fmt.Errorf("transaction already exists: %s", txnID)
	}

	txn := &Transaction{
		ID:         txnID,
		Status:     StatusPending,
		Operations: make([]Operation, 0),
		Created:    time.Now().UnixMilli(),
		Snapshot:   make(map[string]interface{}),
	}

	tm.transactions[txnID] = txn
	return txn, nil
}

// AddOperation adds an operation to a transaction
func (tm *TransactionManager) AddOperation(txnID string, op Operation) error {
	tm.mu.RLock()
	txn, exists := tm.transactions[txnID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("transaction not found: %s", txnID)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != StatusPending {
		return fmt.Errorf("transaction not in pending state: %s", txnID)
	}

	op.Timestamp = time.Now().UnixMilli()
	txn.Operations = append(txn.Operations, op)
	return nil
}

// CommitTransaction commits a transaction
func (tm *TransactionManager) CommitTransaction(txnID string) error {
	tm.mu.RLock()
	txn, exists := tm.transactions[txnID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("transaction not found: %s", txnID)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != StatusPending {
		return fmt.Errorf("transaction not in pending state: %s", txnID)
	}

	// Validate all operations
	for _, op := range txn.Operations {
		if op.Error != nil {
			txn.Status = StatusFailed
			return fmt.Errorf("operation failed: %w", op.Error)
		}
	}

	txn.Status = StatusCommitted
	txn.Committed = time.Now().UnixMilli()
	return nil
}

// RollbackTransaction rolls back a transaction
func (tm *TransactionManager) RollbackTransaction(txnID string) error {
	tm.mu.RLock()
	txn, exists := tm.transactions[txnID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("transaction not found: %s", txnID)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != StatusPending {
		return fmt.Errorf("transaction not in pending state: %s", txnID)
	}

	txn.Status = StatusRolledBack
	return nil
}

// GetTransaction retrieves a transaction
func (tm *TransactionManager) GetTransaction(txnID string) (*Transaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	txn, exists := tm.transactions[txnID]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", txnID)
	}

	return txn, nil
}

// GetTransactionStatus returns the status of a transaction
func (tm *TransactionManager) GetTransactionStatus(txnID string) (TransactionStatus, error) {
	txn, err := tm.GetTransaction(txnID)
	if err != nil {
		return "", err
	}

	txn.mu.RLock()
	defer txn.mu.RUnlock()

	return txn.Status, nil
}

// GetTransactionOperations returns operations in a transaction
func (tm *TransactionManager) GetTransactionOperations(txnID string) ([]Operation, error) {
	txn, err := tm.GetTransaction(txnID)
	if err != nil {
		return nil, err
	}

	txn.mu.RLock()
	defer txn.mu.RUnlock()

	ops := make([]Operation, len(txn.Operations))
	copy(ops, txn.Operations)
	return ops, nil
}

// DeleteTransaction removes a transaction
func (tm *TransactionManager) DeleteTransaction(txnID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	delete(tm.transactions, txnID)
	return nil
}

// cleanupExpiredTransactions removes old transactions
func (tm *TransactionManager) cleanupExpiredTransactions() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		tm.mu.Lock()
		now := time.Now().UnixMilli()
		for txnID, txn := range tm.transactions {
			if now-txn.Created > int64(tm.txnTimeout.Milliseconds()) {
				delete(tm.transactions, txnID)
			}
		}
		tm.mu.Unlock()
	}
}

// GetStats returns transaction statistics
func (tm *TransactionManager) GetStats() map[string]interface{} {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	pending := 0
	committed := 0
	rolledBack := 0
	failed := 0

	for _, txn := range tm.transactions {
		txn.mu.RLock()
		switch txn.Status {
		case StatusPending:
			pending++
		case StatusCommitted:
			committed++
		case StatusRolledBack:
			rolledBack++
		case StatusFailed:
			failed++
		}
		txn.mu.RUnlock()
	}

	return map[string]interface{}{
		"total_transactions": len(tm.transactions),
		"pending":            pending,
		"committed":          committed,
		"rolled_back":        rolledBack,
		"failed":             failed,
		"max_transactions":   tm.maxTxns,
		"timeout":            tm.txnTimeout.String(),
	}
}

// SnapshotIsolation provides snapshot isolation for transactions
type SnapshotIsolation struct {
	txnID    string
	snapshot map[string]interface{}
	mu       sync.RWMutex
}

// NewSnapshotIsolation creates a new snapshot isolation context
func NewSnapshotIsolation(txnID string, snapshot map[string]interface{}) *SnapshotIsolation {
	si := &SnapshotIsolation{
		txnID:    txnID,
		snapshot: make(map[string]interface{}),
	}

	// Deep copy snapshot
	for k, v := range snapshot {
		si.snapshot[k] = v
	}

	return si
}

// Read reads a value from the snapshot
func (si *SnapshotIsolation) Read(key string) (interface{}, bool) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	val, exists := si.snapshot[key]
	return val, exists
}

// Write writes a value to the snapshot
func (si *SnapshotIsolation) Write(key string, value interface{}) {
	si.mu.Lock()
	defer si.mu.Unlock()

	si.snapshot[key] = value
}

// GetSnapshot returns the current snapshot
func (si *SnapshotIsolation) GetSnapshot() map[string]interface{} {
	si.mu.RLock()
	defer si.mu.RUnlock()

	snapshot := make(map[string]interface{})
	for k, v := range si.snapshot {
		snapshot[k] = v
	}
	return snapshot
}
