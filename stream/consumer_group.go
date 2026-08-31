package stream

import (
	"fmt"
	"sync"
	"time"
)

// ConsumerGroupMember represents a consumer in a group
type ConsumerGroupMember struct {
	ID              string            `json:"id"`              // Consumer ID
	Group           string            `json:"group"`           // Consumer group name
	Stream          string            `json:"stream"`          // Stream being consumed
	LastOffset      string            `json:"last_offset"`     // Last acknowledged entry ID
	JoinedAt        int64             `json:"joined_at"`       // Timestamp
	LastHeartbeat   int64             `json:"last_heartbeat"`  // Distributed coordination
	Node            string            `json:"node"`            // Producer node
	VectorClock     map[string]int64   `json:"vector_clock"`
}

// ConsumerGroupPending represents an unconsumed entry
type ConsumerGroupPending struct {
	EntryID    string
	ConsumerID string
	Timestamp  int64
}

// ConsumerGroup manages a group of consumers reading from a stream
type ConsumerGroup struct {
	Name        string
	Stream      string
	Members     map[string]*ConsumerGroupMember // Consumer ID -> Member
	Offsets     map[string]string               // Consumer ID -> Last offset
	PendingLog  []*ConsumerGroupPending          // Unacknowledged entries
	mu          sync.RWMutex
	CreatedAt   int64
	LastModified int64
	NodeID      string
	MaxRetention time.Duration // How long to keep pending entries
	Timestamp   int64 // Lamport clock
}

// NewConsumerGroup creates a new consumer group
func NewConsumerGroup(name string, stream string, nodeID string) *ConsumerGroup {
	return &ConsumerGroup{
		Name:        name,
		Stream:      stream,
		Members:     make(map[string]*ConsumerGroupMember),
		Offsets:     make(map[string]string),
		PendingLog:  make([]*ConsumerGroupPending, 0),
		CreatedAt:   time.Now().UnixMilli(),
		LastModified: time.Now().UnixMilli(),
		NodeID:      nodeID,
		MaxRetention: 24 * time.Hour,
		Timestamp:   0,
	}
}

// AddConsumer registers a new consumer in the group
func (cg *ConsumerGroup) AddConsumer(consumerID string, nodeID string) (*ConsumerGroupMember, error) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if _, exists := cg.Members[consumerID]; exists {
		return nil, fmt.Errorf("consumer already exists: %s", consumerID)
	}

	cg.Timestamp++

	member := &ConsumerGroupMember{
		ID:            consumerID,
		Group:         cg.Name,
		Stream:        cg.Stream,
		JoinedAt:      time.Now().UnixMilli(),
		LastHeartbeat: time.Now().UnixMilli(),
		Node:          nodeID,
		VectorClock:   make(map[string]int64),
	}

	cg.Members[consumerID] = member
	cg.Offsets[consumerID] = "0" // Start from beginning

	cg.LastModified = time.Now().UnixMilli()
	cg.triggerRebalance()

	return member, nil
}

// RemoveConsumer removes a consumer from the group
func (cg *ConsumerGroup) RemoveConsumer(consumerID string) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if _, exists := cg.Members[consumerID]; !exists {
		return fmt.Errorf("consumer not found: %s", consumerID)
	}

	delete(cg.Members, consumerID)
	delete(cg.Offsets, consumerID)

	cg.Timestamp++
	cg.LastModified = time.Now().UnixMilli()
	cg.triggerRebalance()

	return nil
}

// UpdateOffset records that a consumer has processed an entry
func (cg *ConsumerGroup) UpdateOffset(consumerID string, entryID string) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	member, exists := cg.Members[consumerID]
	if !exists {
		return fmt.Errorf("consumer not found: %s", consumerID)
	}

	member.LastOffset = entryID
	member.LastHeartbeat = time.Now().UnixMilli()
	cg.Offsets[consumerID] = entryID

	// Remove from pending log
	newPending := make([]*ConsumerGroupPending, 0)
	for _, p := range cg.PendingLog {
		if p.EntryID != entryID || p.ConsumerID != consumerID {
			newPending = append(newPending, p)
		}
	}
	cg.PendingLog = newPending

	cg.Timestamp++
	cg.LastModified = time.Now().UnixMilli()

	return nil
}

// GetConsumer returns information about a consumer
func (cg *ConsumerGroup) GetConsumer(consumerID string) (*ConsumerGroupMember, error) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	member, exists := cg.Members[consumerID]
	if !exists {
		return nil, fmt.Errorf("consumer not found: %s", consumerID)
	}

	return member, nil
}

// GetOffset returns the last offset for a consumer
func (cg *ConsumerGroup) GetOffset(consumerID string) (string, error) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	offset, exists := cg.Offsets[consumerID]
	if !exists {
		return "", fmt.Errorf("consumer not found: %s", consumerID)
	}

	return offset, nil
}

// GetMembers returns all members in the group
func (cg *ConsumerGroup) GetMembers() []*ConsumerGroupMember {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	members := make([]*ConsumerGroupMember, 0, len(cg.Members))
	for _, m := range cg.Members {
		members = append(members, m)
	}

	return members
}

// PendingEntries returns entries that are pending acknowledgment
func (cg *ConsumerGroup) PendingEntries() []*ConsumerGroupPending {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	pending := make([]*ConsumerGroupPending, 0, len(cg.PendingLog))
	now := time.Now().UnixMilli()

	for _, p := range cg.PendingLog {
		age := now - p.Timestamp
		if age <= cg.MaxRetention.Milliseconds() {
			pending = append(pending, p)
		}
	}

	return pending
}

// RebalanceAssignment distributes entries among consumers
// Returns: map of consumer ID -> slice of entry IDs
func (cg *ConsumerGroup) RebalanceAssignment(entries []*StreamEntry) map[string][]*StreamEntry {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	assignment := make(map[string][]*StreamEntry)

	// Initialize empty slices for each consumer
	for consumerID := range cg.Members {
		assignment[consumerID] = make([]*StreamEntry, 0)
	}

	if len(cg.Members) == 0 {
		return assignment
	}

	// Distribute entries in round-robin fashion
	// Each entry goes to a consumer that hasn't processed it yet
	consumerList := make([]string, 0, len(cg.Members))
	for consumerID := range cg.Members {
		consumerList = append(consumerList, consumerID)
	}

	for _, entry := range entries {
		// Find consumer with highest offset
		bestConsumer := ""
		bestTimestamp := int64(0)

		for _, consumerID := range consumerList {
			lastOffset := cg.Offsets[consumerID]
			// Simple heuristic: use first consumer that hasn't processed this entry
			if lastOffset == "0" || lastOffset == entry.ID {
				bestConsumer = consumerID
				break
			}

			member := cg.Members[consumerID]
			if member.LastHeartbeat > bestTimestamp {
				bestTimestamp = member.LastHeartbeat
				bestConsumer = consumerID
			}
		}

		if bestConsumer != "" {
			assignment[bestConsumer] = append(assignment[bestConsumer], entry)
		}
	}

	return assignment
}

// Heartbeat updates a consumer's heartbeat (for liveness detection)
func (cg *ConsumerGroup) Heartbeat(consumerID string) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	member, exists := cg.Members[consumerID]
	if !exists {
		return fmt.Errorf("consumer not found: %s", consumerID)
	}

	member.LastHeartbeat = time.Now().UnixMilli()

	return nil
}

// DetectDeadConsumers returns consumers that haven't sent heartbeat recently
func (cg *ConsumerGroup) DetectDeadConsumers(timeout time.Duration) []string {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	var dead []string
	now := time.Now().UnixMilli()

	for consumerID, member := range cg.Members {
		elapsed := now - member.LastHeartbeat
		if elapsed > timeout.Milliseconds() {
			dead = append(dead, consumerID)
		}
	}

	return dead
}

// GetStats returns statistics about the consumer group
func (cg *ConsumerGroup) GetStats() map[string]interface{} {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	return map[string]interface{}{
		"name":         cg.Name,
		"stream":       cg.Stream,
		"consumers":    len(cg.Members),
		"pending":      len(cg.PendingLog),
		"created_at":   cg.CreatedAt,
		"last_modified": cg.LastModified,
		"timestamp":    cg.Timestamp,
	}
}

// Internal helpers

func (cg *ConsumerGroup) triggerRebalance() {
	// In a real implementation, this would trigger a rebalance across the cluster
	// For now, it's a no-op
}

// AddPending adds an entry to the pending log
func (cg *ConsumerGroup) AddPending(entryID string, consumerID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	pending := &ConsumerGroupPending{
		EntryID:    entryID,
		ConsumerID: consumerID,
		Timestamp:  time.Now().UnixMilli(),
	}

	cg.PendingLog = append(cg.PendingLog, pending)
}
