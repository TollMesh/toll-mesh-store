package transactions

import (
	"testing"
	"time"
)

func TestBeginAddCommit(t *testing.T) {
	tm := NewTransactionManager(100, time.Minute)

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
	tm := NewTransactionManager(100, time.Minute)
	tm.BeginTransaction("txn-1")
	_, err := tm.BeginTransaction("txn-1")
	if err == nil {
		t.Error("expected error for duplicate transaction ID")
	}
}

func TestCannotAddAfterCommit(t *testing.T) {
	tm := NewTransactionManager(100, time.Minute)
	tm.BeginTransaction("txn-1")
	tm.CommitTransaction("txn-1")

	err := tm.AddOperation("txn-1", Operation{Type: OpSet, Key: "k"})
	if err == nil {
		t.Error("expected error adding operation to committed transaction")
	}
}

func TestRollback(t *testing.T) {
	tm := NewTransactionManager(100, time.Minute)
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
	tm := NewTransactionManager(1, time.Minute)
	tm.BeginTransaction("txn-1")
	_, err := tm.BeginTransaction("txn-2")
	if err == nil {
		t.Error("expected error when exceeding max transactions")
	}
}
