package pubsub

import (
	"fmt"
	"regexp"
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
}

// NewPubSubBroker creates a new pub/sub broker
func NewPubSubBroker(maxDLQSize int) *PubSubBroker {
	return &PubSubBroker{
		topics:          make(map[string]*Topic),
		subscribers:     make(map[string]*Subscriber),
		deadLetterQueue: make([]Message, 0, maxDLQSize),
		maxDLQSize:      maxDLQSize,
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

// Publish publishes a message to a topic
func (pb *PubSubBroker) Publish(topic, publisher string, payload []byte) (int, error) {
	pb.mu.RLock()
	t, exists := pb.topics[topic]
	pb.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	msg := Message{
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
		Publisher: publisher,
		ID:        fmt.Sprintf("%s-%d", topic, time.Now().UnixNano()),
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
