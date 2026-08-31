package core

import (
	"context"
	"time"
)

// Store is the distributed store interface for Toll Mesh coordination.
type Store interface {
	Consume(ctx context.Context, key string, limit int, window time.Duration) (ConsumeResult, error)
	Seen(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Get(ctx context.Context, ns, key string) ([]byte, bool, error)
	Set(ctx context.Context, ns, key string, value []byte, ttl time.Duration) error
	Close() error
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
}
