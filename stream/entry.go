package stream

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// StreamEntry represents an event in the stream
type StreamEntry struct {
	ID         string            `json:"id"`          // Timestamp-based ID: "1234567890-0"
	Timestamp  int64             `json:"timestamp"`   // Milliseconds
	Fields     map[string]string `json:"fields"`      // Event data
	Node       string            `json:"node"`        // Producer node
	Sequence   int64             `json:"sequence"`    // Sequence number
	VectorClock map[string]int64  `json:"vector_clock"`
}

// Stream is an append-only log of events
type Stream struct {
	Name            string
	Entries         []*StreamEntry        // Append-only log
	EntryIndex      map[string]*StreamEntry // Quick lookup by ID
	mu              sync.RWMutex
	LastSequence    int64
	Timestamp       int64
	NodeID          string
	RetentionPolicy RetentionPolicy
}

// RetentionPolicy defines when entries are removed
type RetentionPolicy struct {
	MaxAge     time.Duration // Keep entries younger than this
	MaxSize    int64         // Keep last N bytes
	MaxEntries int64         // Keep last N entries
}

// NewStream creates a new append-only stream
func NewStream(name string, nodeID string) *Stream {
	return &Stream{
		Name:       name,
		Entries:    make([]*StreamEntry, 0),
		EntryIndex: make(map[string]*StreamEntry),
		NodeID:     nodeID,
		Timestamp:  time.Now().UnixMilli(),
		RetentionPolicy: RetentionPolicy{
			MaxAge:     24 * time.Hour,
			MaxSize:    1 * 1024 * 1024 * 1024, // 1GB
			MaxEntries: 1000000,                 // 1M entries
		},
	}
}

// Add appends a new entry to the stream
func (s *Stream) Add(fields map[string]string) (*StreamEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Timestamp = time.Now().UnixMilli()
	s.LastSequence++

	// Generate entry ID: timestamp-sequence-node. The node suffix is what
	// makes this globally unique across a cluster, not just within this
	// one Stream instance: LastSequence is a plain per-instance counter
	// that any two nodes' independent Stream objects for the same stream
	// name both start at 0 and increment on their own, with no
	// coordination between them. Two nodes producing entries in the same
	// millisecond (easy under any real write volume) previously could
	// produce the exact same "<timestamp>-<sequence>" ID for two
	// completely different entries -- fine for a single node, but fatal
	// for a union-merge across nodes, since it's exactly the identity a
	// merge would use to decide "have I already seen this entry?". No
	// code parses this ID's structure (confirmed via a repo-wide search
	// before making this change) -- every use is an opaque string
	// equality/map-key lookup -- so widening the format doesn't break
	// existing Range/EntryIndex behavior.
	entryID := fmt.Sprintf("%d-%d-%s", s.Timestamp, s.LastSequence, s.NodeID)

	entry := &StreamEntry{
		ID:        entryID,
		Timestamp: s.Timestamp,
		Fields:    copyFields(fields),
		Node:      s.NodeID,
		Sequence:  s.LastSequence,
		VectorClock: make(map[string]int64),
	}

	// Add to log (append-only)
	s.Entries = append(s.Entries, entry)
	s.EntryIndex[entryID] = entry

	// Apply retention policy
	s.applyRetention()

	return entry, nil
}

// Get retrieves an entry by ID
func (s *Stream) Get(entryID string) (*StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.EntryIndex[entryID]
	if !exists {
		return nil, fmt.Errorf("entry not found: %s", entryID)
	}

	return entry, nil
}

// Range returns entries between startID and endID
// If startID is "0", starts from beginning
// If endID is "-", returns up to current last entry
func (s *Stream) Range(startID, endID string, limit int64) []*StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*StreamEntry

	startIdx := 0
	endIdx := len(s.Entries) - 1

	// Parse start position
	if startID != "0" && startID != "" {
		if startEntry, exists := s.EntryIndex[startID]; exists {
			// Find index of start entry
			for i, e := range s.Entries {
				if e.ID == startEntry.ID {
					startIdx = i + 1 // Start from next entry
					break
				}
			}
		}
	}

	// Parse end position
	if endID != "-" && endID != "" {
		if endEntry, exists := s.EntryIndex[endID]; exists {
			// Find index of end entry
			for i, e := range s.Entries {
				if e.ID == endEntry.ID {
					endIdx = i
					break
				}
			}
		}
	}

	// Collect entries in range with limit
	for i := startIdx; i <= endIdx && int64(len(result)) < limit; i++ {
		if i >= 0 && i < len(s.Entries) {
			result = append(result, s.Entries[i])
		}
	}

	return result
}

// Snapshot returns a copy of every entry, for gossip replication.
func (s *Stream) Snapshot() []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]StreamEntry, len(s.Entries))
	for i, e := range s.Entries {
		out[i] = *e
	}
	return out
}

// MergeSnapshot merges a peer's Snapshot output. Since entries are
// immutable once appended and IDs are globally unique (see Add's comment
// on why the ID includes the node), merging is a straightforward set
// union keyed by ID -- no per-entry conflict resolution is needed the way
// cache's LWW-register or SortedSet's (score, timestamp, node) comparison
// are, since two entries can never legitimately compete to be "the same"
// entry. The one thing merging must get right is ordering: Range and
// GetFirst/GetLast are positional over s.Entries, not derived from the ID,
// so newly-merged entries (which can be chronologically "in the past"
// relative to this node's own more recent local entries) are inserted in
// the correct position by re-sorting on (Timestamp, Sequence, Node),
// rather than simply appended.
func (s *Stream) MergeSnapshot(entries []StreamEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	changed := false
	for i := range entries {
		e := &entries[i]
		if _, exists := s.EntryIndex[e.ID]; exists {
			continue
		}
		entryCopy := *e
		s.Entries = append(s.Entries, &entryCopy)
		s.EntryIndex[e.ID] = &entryCopy
		changed = true
	}

	if !changed {
		return
	}

	sort.Slice(s.Entries, func(i, j int) bool {
		a, b := s.Entries[i], s.Entries[j]
		if a.Timestamp != b.Timestamp {
			return a.Timestamp < b.Timestamp
		}
		if a.Sequence != b.Sequence {
			return a.Sequence < b.Sequence
		}
		return a.Node < b.Node
	})

	s.applyRetention()
}

// Len returns total number of entries
func (s *Stream) Len() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.Entries))
}

// GetLast returns the most recent entry
func (s *Stream) GetLast() (*StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.Entries) == 0 {
		return nil, fmt.Errorf("stream is empty")
	}

	return s.Entries[len(s.Entries)-1], nil
}

// GetFirst returns the oldest entry
func (s *Stream) GetFirst() (*StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.Entries) == 0 {
		return nil, fmt.Errorf("stream is empty")
	}

	return s.Entries[0], nil
}

// Trim removes old entries based on retention policy
func (s *Stream) Trim() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.applyRetention()
}

// Helper functions

func (s *Stream) applyRetention() {
	now := time.Now().UnixMilli()

	// Remove entries exceeding max age
	newEntries := make([]*StreamEntry, 0)
	for _, e := range s.Entries {
		age := now - e.Timestamp
		if age <= s.RetentionPolicy.MaxAge.Milliseconds() {
			newEntries = append(newEntries, e)
		} else {
			// Remove from index
			delete(s.EntryIndex, e.ID)
		}
	}
	s.Entries = newEntries

	// Keep only last N entries
	if int64(len(s.Entries)) > s.RetentionPolicy.MaxEntries {
		toRemove := len(s.Entries) - int(s.RetentionPolicy.MaxEntries)
		for i := 0; i < toRemove; i++ {
			delete(s.EntryIndex, s.Entries[i].ID)
		}
		s.Entries = s.Entries[toRemove:]
	}

	// Keep only last N bytes (approximate)
	size := int64(0)
	for _, e := range s.Entries {
		size += int64(len(e.ID) + len(e.Node))
		for k, v := range e.Fields {
			size += int64(len(k) + len(v))
		}
	}

	if size > s.RetentionPolicy.MaxSize {
		// Remove oldest entries until size is under limit
		for len(s.Entries) > 0 && size > s.RetentionPolicy.MaxSize {
			e := s.Entries[0]
			size -= int64(len(e.ID) + len(e.Node))
			for k, v := range e.Fields {
				size -= int64(len(k) + len(v))
			}
			delete(s.EntryIndex, e.ID)
			s.Entries = s.Entries[1:]
		}
	}
}

func copyFields(fields map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range fields {
		result[k] = v
	}
	return result
}

// GetStats returns stream statistics
func (s *Stream) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"name":        s.Name,
		"entries":     len(s.Entries),
		"last_seq":    s.LastSequence,
		"timestamp":   s.Timestamp,
		"node":        s.NodeID,
	}
}
