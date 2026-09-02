package pubsub

import (
	"testing"
	"time"
)

func TestSubscribePublish(t *testing.T) {
	pb := NewPubSubBroker(100)

	ch, err := pb.Subscribe("sub-1", "news", "")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	count, err := pb.Publish("news", "publisher-1", []byte("hello"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 delivery, got %d", count)
	}

	select {
	case msg := <-ch:
		if string(msg.Payload) != "hello" {
			t.Errorf("expected 'hello', got %s", msg.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestPublishToTopicWithNoSubscribers(t *testing.T) {
	pb := NewPubSubBroker(100)
	// Redis PUBLISH never errors for a channel with no subscribers, it just
	// delivers to zero. A topic nobody has ever subscribed to should behave
	// the same way, not error.
	count, err := pb.Publish("nobody-listening", "publisher-1", []byte("hello"))
	if err != nil {
		t.Fatalf("publish to topic with no subscribers should not error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 deliveries, got %d", count)
	}
}

func TestPatternMatching(t *testing.T) {
	pb := NewPubSubBroker(100)

	ch, err := pb.Subscribe("sub-1", "events.orders", "^events\\.")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	count, _ := pb.Publish("events.orders", "pub", []byte("matched"))
	if count != 1 {
		t.Errorf("expected pattern to match, got count=%d", count)
	}

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected matched message")
	}
}

func TestUnsubscribe(t *testing.T) {
	pb := NewPubSubBroker(100)

	pb.Subscribe("sub-1", "news", "")
	if err := pb.Unsubscribe("sub-1", "news"); err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}

	subs := pb.GetSubscribers("news")
	if len(subs) != 0 {
		t.Errorf("expected 0 subscribers after unsubscribe, got %d", len(subs))
	}
}

func TestMessageHistory(t *testing.T) {
	pb := NewPubSubBroker(100)
	pb.Subscribe("sub-1", "news", "")

	for i := 0; i < 5; i++ {
		pb.Publish("news", "pub", []byte("msg"))
	}

	history := pb.GetMessageHistory("news", 3)
	if len(history) != 3 {
		t.Errorf("expected 3 history entries, got %d", len(history))
	}
}

func TestDeadLetterQueueOnFullChannel(t *testing.T) {
	pb := NewPubSubBroker(10)
	pb.Subscribe("slow-sub", "news", "")
	// Don't drain the channel; fill it past its buffer (100) to force DLQ.
	for i := 0; i < 105; i++ {
		pb.Publish("news", "pub", []byte("msg"))
	}

	dlq := pb.GetDeadLetterQueue()
	if len(dlq) == 0 {
		t.Error("expected some messages to be dead-lettered when subscriber channel is full")
	}
}

func TestPollReturnsAvailableMessagesWithoutWaitingForLimit(t *testing.T) {
	pb := NewPubSubBroker(100)
	pb.Subscribe("sub-1", "news", "")
	pb.Publish("news", "pub", []byte("msg-1"))

	start := time.Now()
	messages, err := pb.Poll("sub-1", 10, 2*time.Second)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("poll failed: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("poll should return as soon as available messages are drained, took %v", elapsed)
	}
}

func TestPollTimesOutWithNoMessages(t *testing.T) {
	pb := NewPubSubBroker(100)
	pb.Subscribe("sub-1", "news", "")

	start := time.Now()
	messages, err := pb.Poll("sub-1", 10, 200*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("poll failed: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(messages))
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("poll returned too early, took %v", elapsed)
	}
}

func TestPollUnknownSubscriber(t *testing.T) {
	pb := NewPubSubBroker(100)
	_, err := pb.Poll("ghost", 10, 100*time.Millisecond)
	if err == nil {
		t.Error("expected error for unknown subscriber")
	}
}

func TestGetStats(t *testing.T) {
	pb := NewPubSubBroker(100)
	pb.Subscribe("sub-1", "news", "")
	pb.Publish("news", "pub", []byte("msg"))

	stats := pb.GetStats()
	if stats["topic_count"] != 1 {
		t.Errorf("expected 1 topic, got %v", stats["topic_count"])
	}
	if stats["subscriber_count"] != 1 {
		t.Errorf("expected 1 subscriber, got %v", stats["subscriber_count"])
	}
}
