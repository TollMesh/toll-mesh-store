package transactions

import (
	"testing"
	"time"
)

func TestBeginAddCommit(t *testing.T) {
	tm := NewTransactionManager(100, time.Minute, "node-1")

	txn, err := tm.BeginTransaction("txn-1")
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if txn.Status != StatusPending {
		t.Errorf("expected pending, got %s", txn.Status)
	}

	err = tm.AddOperation("txn-1", Operation{Type: OpSet, Namespace: "ns", Key: "k", Value: "v"})
	if err != nil {
		t.Fatalf("add operation failed: %v", err)
	}

	err = tm.CommitTransaction("txn-1")
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	status, _ := tm.GetTransactionStatus("txn-1")
	if status != StatusCommitted {
		t.Errorf("expected committed, got %s", status)
	}
}

func TestDuplicateBeginFails(t *testing.T) {
	tm := NewTransactionManager(100, time.Minute, "node-1")
	tm.BeginTransaction("txn-1")
	_, err := tm.BeginTransaction("txn-1")
	if err == nil {
		t.Error("expected error for duplicate transaction ID")
	}
}

func TestCannotAddAfterCommit(t *testing.T) {
	tm := NewTransactionManager(100, time.Minute, "node-1")
	tm.BeginTransaction("txn-1")
	tm.CommitTransaction("txn-1")

	err := tm.AddOperation("txn-1", Operation{Type: OpSet, Key: "k"})
	if err == nil {
		t.Error("expected error adding operation to committed transaction")
	}
}

func TestRollback(t *testing.T) {
	tm := NewTransactionManager(100, time.Minute, "node-1")
	tm.BeginTransaction("txn-1")
	tm.AddOperation("txn-1", Operation{Type: OpSet, Key: "k", Value: "v"})

	if err := tm.RollbackTransaction("txn-1"); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	status, _ := tm.GetTransactionStatus("txn-1")
	if status != StatusRolledBack {
		t.Errorf("expected rolled_back, got %s", status)
	}
}

func TestMaxTransactionsLimit(t *testing.T) {
	tm := NewTransactionManager(1, time.Minute, "node-1")
	tm.BeginTransaction("txn-1")
	_, err := tm.BeginTransaction("txn-2")
	if err == nil {
		t.Error("expected error when exceeding max transactions")
	}
}

// TestMergeSnapshotAdoptsNewerPeerTransaction is the regression test for
// transaction gossip replication: MergeSnapshot must adopt a peer's
// version of a transaction only when it's strictly newer (by UpdatedAt,
// then Node), the same LWW-register rule as Cache/Pipelines/Search/Job
// Queues, and must not corrupt the receiving TransactionManager's own
// locking (Transaction.mu is never copied by value).
func TestMergeSnapshotAdoptsNewerPeerTransaction(t *testing.T) {
	tm := NewTransactionManager(100, time.Minute, "node-1")
	tm.BeginTransaction("txn-1")
	tm.AddOperation("txn-1", Operation{Type: OpSet, Namespace: "ns", Key: "k", Value: "local-value"})

	local, _ := tm.GetTransaction("txn-1")
	local.mu.RLock()
	localUpdatedAt := local.UpdatedAt
	local.mu.RUnlock()

	// A stale peer version (still pending, older UpdatedAt) must not
	// overwrite the newer local one.
	stalePeer := Transaction{
		ID:        "txn-1",
		Status:    StatusRolledBack,
		UpdatedAt: localUpdatedAt - 1000,
		Node:      "node-2",
	}
	tm.MergeSnapshot([]Transaction{stalePeer})

	status, _ := tm.GetTransactionStatus("txn-1")
	if status != StatusPending {
		t.Fatalf("stale peer transaction incorrectly adopted, status = %s", status)
	}

	// A newer peer version (committed on node-2) must be adopted.
	newerPeer := Transaction{
		ID:         "txn-1",
		Status:     StatusCommitted,
		Operations: []Operation{{Type: OpSet, Namespace: "ns", Key: "k", Value: "peer-value"}},
		UpdatedAt:  localUpdatedAt + 1000,
		Node:       "node-2",
	}
	tm.MergeSnapshot([]Transaction{newerPeer})

	status, _ = tm.GetTransactionStatus("txn-1")
	if status != StatusCommitted {
		t.Fatalf("newer peer transaction not adopted, status = %s", status)
	}
	ops, _ := tm.GetTransactionOperations("txn-1")
	if len(ops) != 1 || ops[0].Value != "peer-value" {
		t.Fatalf("newer peer transaction's operations not adopted: %+v", ops)
	}
}

// TestMergeSnapshotInsertsUnknownPeerTransaction verifies a transaction
// begun on a peer and never seen locally is inserted outright.
func TestMergeSnapshotInsertsUnknownPeerTransaction(t *testing.T) {
	tm := NewTransactionManager(100, time.Minute, "node-1")

	peerTxn := Transaction{
		ID:        "peer-txn-1",
		Status:    StatusPending,
		UpdatedAt: time.Now().UnixMilli(),
		Node:      "node-2",
	}
	tm.MergeSnapshot([]Transaction{peerTxn})

	status, err := tm.GetTransactionStatus("peer-txn-1")
	if err != nil {
		t.Fatalf("peer transaction not found after merge: %v", err)
	}
	if status != StatusPending {
		t.Fatalf("unexpected merged transaction status: %s", status)
	}
}
