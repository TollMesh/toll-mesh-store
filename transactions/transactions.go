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
	// UpdatedAt and Node are this transaction's LWW-register version for
	// gossip replication, alongside Created (which, unlike UpdatedAt, is
	// stamped once and never changes -- it exists for the timeout-based
	// cleanup deadline, not versioning).
	UpdatedAt int64
	Node      string
	mu        sync.RWMutex
}

// TransactionManager manages ACID transactions
type TransactionManager struct {
	mu           sync.RWMutex
	transactions map[string]*Transaction
	maxTxns      int
	txnTimeout   time.Duration
	nodeID       string // stamped onto Transaction.Node on every mutation, for gossip LWW
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(maxTxns int, txnTimeout time.Duration, nodeID string) *TransactionManager {
	tm := &TransactionManager{
		transactions: make(map[string]*Transaction),
		maxTxns:      maxTxns,
		txnTimeout:   txnTimeout,
		nodeID:       nodeID,
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

	now := time.Now().UnixMilli()
	txn := &Transaction{
		ID:         txnID,
		Status:     StatusPending,
		Operations: make([]Operation, 0),
		Created:    now,
		Snapshot:   make(map[string]interface{}),
		UpdatedAt:  now,
		Node:       tm.nodeID,
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
	txn.UpdatedAt = op.Timestamp
	txn.Node = tm.nodeID
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
	txn.UpdatedAt = txn.Committed
	txn.Node = tm.nodeID
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
	txn.UpdatedAt = time.Now().UnixMilli()
	txn.Node = tm.nodeID
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

// Snapshot returns a copy of every transaction (including terminal ones,
// until cleanup evicts them), for gossip replication. Each Transaction's
// mutex is never copied -- fields are copied individually into a fresh
// Transaction value instead of dereferencing the original.
func (tm *TransactionManager) Snapshot() []Transaction {
	tm.mu.RLock()
	txns := make([]*Transaction, 0, len(tm.transactions))
	for _, t := range tm.transactions {
		txns = append(txns, t)
	}
	tm.mu.RUnlock()

	out := make([]Transaction, 0, len(txns))
	for _, t := range txns {
		t.mu.RLock()
		ops := make([]Operation, len(t.Operations))
		copy(ops, t.Operations)
		snap := make(map[string]interface{}, len(t.Snapshot))
		for k, v := range t.Snapshot {
			snap[k] = v
		}
		out = append(out, Transaction{
			ID:         t.ID,
			Status:     t.Status,
			Operations: ops,
			Created:    t.Created,
			Committed:  t.Committed,
			Snapshot:   snap,
			UpdatedAt:  t.UpdatedAt,
			Node:       t.Node,
		})
		t.mu.RUnlock()
	}
	return out
}

// MergeSnapshot merges a peer's Snapshot output: a (UpdatedAt, Node)
// LWW-register comparison per transaction ID, the same pattern as
// Cache/Pipelines/Search/Job Queues -- a peer's version of a transaction is
// adopted only when it's strictly newer, and a transaction unknown locally
// is inserted outright. This does not enforce maxTxns on the inserted
// side, so a merge can transiently push the local transaction count above
// it -- the periodic timeout-based cleanup (keyed off the transaction's
// original, replicated Created timestamp, so it fires at roughly the same
// wall-clock deadline on every node) brings it back down, it just isn't
// instantaneous.
//
// Known limitation, the same shape as Job Queues: this converges
// transaction *metadata* (status, queued operations) across nodes, but
// does not provide cross-node atomicity. A client that calls
// AddTransactionOperation against one node and then CommitTransaction
// against a different node (e.g. behind a load balancer) before a gossip
// round has run will see "transaction not found" on the second node, not
// a merged view -- callers should route all calls for one transaction ID
// to the same node, the same constraint Job Queues' claim exclusivity has
// in the other direction (see JobQueue.MergeSnapshot's doc comment). The
// *effects* of a committed transaction's Set operations do not depend on
// this: they're applied directly into MeshStore's cache, which already
// gossip-replicates via Cache's own LWW-register merge, independent of
// whether the Transaction object itself has converged yet.
func (tm *TransactionManager) MergeSnapshot(peerTxns []Transaction) {
	for i := range peerTxns {
		peer := &peerTxns[i]

		tm.mu.Lock()
		local, exists := tm.transactions[peer.ID]
		if !exists {
			tm.transactions[peer.ID] = clonePeerTransaction(peer)
			tm.mu.Unlock()
			continue
		}
		tm.mu.Unlock()

		local.mu.Lock()
		if !txnLess(local.UpdatedAt, local.Node, peer.UpdatedAt, peer.Node) {
			local.mu.Unlock()
			continue
		}
		local.Status = peer.Status
		local.Operations = append([]Operation(nil), peer.Operations...)
		local.Created = peer.Created
		local.Committed = peer.Committed
		snap := make(map[string]interface{}, len(peer.Snapshot))
		for k, v := range peer.Snapshot {
			snap[k] = v
		}
		local.Snapshot = snap
		local.UpdatedAt = peer.UpdatedAt
		local.Node = peer.Node
		local.mu.Unlock()
	}
}

// clonePeerTransaction builds a fresh *Transaction (with its own,
// never-locked mutex) from a peer's snapshot entry.
func clonePeerTransaction(peer *Transaction) *Transaction {
	ops := append([]Operation(nil), peer.Operations...)
	snap := make(map[string]interface{}, len(peer.Snapshot))
	for k, v := range peer.Snapshot {
		snap[k] = v
	}
	return &Transaction{
		ID:         peer.ID,
		Status:     peer.Status,
		Operations: ops,
		Created:    peer.Created,
		Committed:  peer.Committed,
		Snapshot:   snap,
		UpdatedAt:  peer.UpdatedAt,
		Node:       peer.Node,
	}
}

// txnLess reports whether (tsA, nodeA) sorts strictly before (tsB, nodeB)
// in the transaction LWW-register's version order.
func txnLess(tsA int64, nodeA string, tsB int64, nodeB string) bool {
	if tsA != tsB {
		return tsA < tsB
	}
	return nodeA < nodeB
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
