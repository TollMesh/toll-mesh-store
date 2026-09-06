package pubsub

import (
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"
)

// Message represents a published message
type Message struct {
	Topic     string
	Payload   []byte
	Timestamp int64
	Publisher string
	ID        string
}

// Subscriber represents a subscription
type Subscriber struct {
	ID      string
	Channel chan Message
	Pattern string
	regex   *regexp.Regexp
	Created int64
}

// Topic represents a pub/sub topic
type Topic struct {
	name        string
	subscribers map[string]*Subscriber
	mu          sync.RWMutex
	messages    []Message // Message history
	maxHistory  int
}

// PubSubBroker manages pub/sub operations
type PubSubBroker struct {
	mu              sync.RWMutex
	topics          map[string]*Topic
	subscribers     map[string]*Subscriber
	deadLetterQueue []Message
	maxDLQSize      int
	nodeID          string // stamped into every published message's ID, for gossip global-uniqueness
}

// NewPubSubBroker creates a new pub/sub broker
func NewPubSubBroker(maxDLQSize int, nodeID string) *PubSubBroker {
	return &PubSubBroker{
		topics:          make(map[string]*Topic),
		subscribers:     make(map[string]*Subscriber),
		deadLetterQueue: make([]Message, 0, maxDLQSize),
		maxDLQSize:      maxDLQSize,
		nodeID:          nodeID,
	}
}

// Subscribe subscribes to a topic with optional pattern matching
func (pb *PubSubBroker) Subscribe(subscriberID, topic, pattern string) (chan Message, error) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Get or create topic
	t, exists := pb.topics[topic]
	if !exists {
		t = &Topic{
			name:        topic,
			subscribers: make(map[string]*Subscriber),
			messages:    make([]Message, 0, 100),
			maxHistory:  100,
		}
		pb.topics[topic] = t
	}

	// Create subscriber
	sub := &Subscriber{
		ID:      subscriberID,
		Channel: make(chan Message, 100),
		Pattern: pattern,
		Created: time.Now().UnixMilli(),
	}

	// Compile regex if pattern provided
	if pattern != "" {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid pattern: %w", err)
		}
		sub.regex = regex
	}

	t.mu.Lock()
	t.subscribers[subscriberID] = sub
	t.mu.Unlock()

	pb.subscribers[subscriberID] = sub

	return sub.Channel, nil
}

// Unsubscribe removes a subscription
func (pb *PubSubBroker) Unsubscribe(subscriberID, topic string) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	t, exists := pb.topics[topic]
	if !exists {
		return fmt.Errorf("topic not found: %s", topic)
	}

	t.mu.Lock()
	sub, exists := t.subscribers[subscriberID]
	if exists {
		close(sub.Channel)
		delete(t.subscribers, subscriberID)
	}
	t.mu.Unlock()

	delete(pb.subscribers, subscriberID)
	return nil
}

// Publish publishes a message to a topic. Matching Redis's PUBLISH
// semantics, publishing to a topic with no subscribers (or one that has
// never been subscribed to) is not an error -- it simply delivers to zero
// receivers.
func (pb *PubSubBroker) Publish(topic, publisher string, payload []byte) (int, error) {
	pb.mu.Lock()
	t, exists := pb.topics[topic]
	if !exists {
		t = &Topic{
			name:        topic,
			subscribers: make(map[string]*Subscriber),
			messages:    make([]Message, 0, 100),
			maxHistory:  100,
		}
		pb.topics[topic] = t
	}
	pb.mu.Unlock()

	msg := Message{
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
		Publisher: publisher,
		// Node is folded into the ID (not just a separate field) because,
		// unlike Job or Document, Message has no per-entry LWW-register --
		// history is a set union keyed by ID, the same shape as Stream
		// entries, so ID uniqueness across nodes is what merge correctness
		// depends on. A plain "topic-nanotime" ID (the previous scheme) can
		// collide across two different node processes publishing to the
		// same topic in the same nanosecond, silently dropping one message
		// from a union-merge -- the exact bug already found and fixed for
		// Stream entry IDs earlier in this project.
		ID: fmt.Sprintf("%s-%d-%s", topic, time.Now().UnixNano(), pb.nodeID),
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Add to history
	t.messages = append(t.messages, msg)
	if len(t.messages) > t.maxHistory {
		t.messages = t.messages[1:]
	}

	// Send to subscribers
	count := 0
	for _, sub := range t.subscribers {
		// Check pattern match if pattern exists
		if sub.regex != nil && !sub.regex.MatchString(topic) {
			continue
		}

		select {
		case sub.Channel <- msg:
			count++
		default:
			// Channel full, add to DLQ
			pb.addToDeadLetterQueue(msg)
		}
	}

	return count, nil
}

// Poll retrieves up to limit currently-available messages for a subscriber,
// waiting up to timeout if none are immediately available. This exists
// because a Go channel cannot cross an HTTP request/response boundary --
// callers over HTTP poll for delivery instead of receiving a push.
func (pb *PubSubBroker) Poll(subscriberID string, limit int, timeout time.Duration) ([]Message, error) {
	pb.mu.RLock()
	sub, exists := pb.subscribers[subscriberID]
	pb.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("subscriber not found: %s", subscriberID)
	}

	if limit <= 0 {
		limit = 1
	}

	messages := make([]Message, 0, limit)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	// Block until the first message (or timeout with none available).
	select {
	case msg, ok := <-sub.Channel:
		if !ok {
			return messages, nil // channel closed (unsubscribed)
		}
		messages = append(messages, msg)
	case <-deadline.C:
		return messages, nil
	}

	// Then drain any additional already-buffered messages without waiting
	// for more to arrive -- a caller asking for up to `limit` shouldn't
	// block for the full timeout just because fewer than `limit` messages
	// happened to be available right now.
	for len(messages) < limit {
		select {
		case msg, ok := <-sub.Channel:
			if !ok {
				return messages, nil
			}
			messages = append(messages, msg)
		default:
			return messages, nil
		}
	}

	return messages, nil
}

// GetTopics returns list of all topics
func (pb *PubSubBroker) GetTopics() []string {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	topics := make([]string, 0, len(pb.topics))
	for name := range pb.topics {
		topics = append(topics, name)
	}
	return topics
}

// GetSubscribers returns subscribers for a topic
func (pb *PubSubBroker) GetSubscribers(topic string) []string {
	pb.mu.RLock()
	t, exists := pb.topics[topic]
	pb.mu.RUnlock()

	if !exists {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	subs := make([]string, 0, len(t.subscribers))
	for id := range t.subscribers {
		subs = append(subs, id)
	}
	return subs
}

// GetMessageHistory returns recent messages for a topic
func (pb *PubSubBroker) GetMessageHistory(topic string, limit int) []Message {
	pb.mu.RLock()
	t, exists := pb.topics[topic]
	pb.mu.RUnlock()

	if !exists {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	if limit > len(t.messages) {
		limit = len(t.messages)
	}

	return t.messages[len(t.messages)-limit:]
}

// GetDeadLetterQueue returns messages that failed to deliver
func (pb *PubSubBroker) GetDeadLetterQueue() []Message {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	dlq := make([]Message, len(pb.deadLetterQueue))
	copy(dlq, pb.deadLetterQueue)
	return dlq
}

// addToDeadLetterQueue adds a message to the DLQ
func (pb *PubSubBroker) addToDeadLetterQueue(msg Message) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.deadLetterQueue = append(pb.deadLetterQueue, msg)
	if len(pb.deadLetterQueue) > pb.maxDLQSize {
		pb.deadLetterQueue = pb.deadLetterQueue[1:]
	}
}

// Snapshot returns each topic's current message history, for gossip
// replication. Subscribers and the dead-letter queue are not included --
// see MergeSnapshot's doc comment for what does and doesn't replicate.
func (pb *PubSubBroker) Snapshot() map[string][]Message {
	pb.mu.RLock()
	topics := make(map[string]*Topic, len(pb.topics))
	for name, t := range pb.topics {
		topics[name] = t
	}
	pb.mu.RUnlock()

	out := make(map[string][]Message, len(topics))
	for name, t := range topics {
		t.mu.RLock()
		msgs := make([]Message, len(t.messages))
		copy(msgs, t.messages)
		t.mu.RUnlock()
		out[name] = msgs
	}
	return out
}

// MergeSnapshot merges a peer's Snapshot output: each topic's message
// history is a set union keyed by Message.ID (messages are immutable once
// published, like Stream entries, so there's no per-message conflict to
// resolve, only "have I seen this ID"), re-sorted by Timestamp and trimmed
// to maxHistory (oldest evicted first), since gossip can deliver messages
// out of chronological order.
//
// Known limitation: this converges GetMessageHistory/GetTopics/GetStats
// across nodes, but does NOT deliver merged messages to live Subscriber
// channels on this node. A Subscriber's Channel is in-process memory --
// nothing about it can cross a gossip round -- so, independent of gossip
// entirely, Subscribe and Poll for a given subscriber ID must land on the
// same node; that was already true before this change and gossip does not
// alter it. What gossip adds is that a message published on any node
// eventually shows up in every node's history/stats view, rather than only
// the node it was published on.
func (pb *PubSubBroker) MergeSnapshot(topicMessages map[string][]Message) {
	for name, peerMsgs := range topicMessages {
		if len(peerMsgs) == 0 {
			continue
		}

		pb.mu.Lock()
		t, exists := pb.topics[name]
		if !exists {
			t = &Topic{
				name:        name,
				subscribers: make(map[string]*Subscriber),
				messages:    make([]Message, 0, 100),
				maxHistory:  100,
			}
			pb.topics[name] = t
		}
		pb.mu.Unlock()

		t.mu.Lock()
		seen := make(map[string]bool, len(t.messages))
		for _, m := range t.messages {
			seen[m.ID] = true
		}
		for _, m := range peerMsgs {
			if !seen[m.ID] {
				t.messages = append(t.messages, m)
				seen[m.ID] = true
			}
		}
		sort.Slice(t.messages, func(i, j int) bool {
			return t.messages[i].Timestamp < t.messages[j].Timestamp
		})
		if len(t.messages) > t.maxHistory {
			t.messages = t.messages[len(t.messages)-t.maxHistory:]
		}
		t.mu.Unlock()
	}
}

// GetStats returns pub/sub statistics
func (pb *PubSubBroker) GetStats() map[string]interface{} {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	totalMessages := 0
	for _, t := range pb.topics {
		t.mu.RLock()
		totalMessages += len(t.messages)
		t.mu.RUnlock()
	}

	return map[string]interface{}{
		"topic_count":      len(pb.topics),
		"subscriber_count": len(pb.subscribers),
		"total_messages":   totalMessages,
		"dlq_size":         len(pb.deadLetterQueue),
	}
}
