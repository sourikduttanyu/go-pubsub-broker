package broker

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/sourik/go-pubsub-broker/internal/config"
)

func testConfig() *config.Config {
	return &config.Config{
		MaxDeliveryAttempts: 3,
		AckTimeout:          200 * time.Millisecond,
		InboxBufferSize:     16,
		DLQMaxSize:          100,
		DropOnFull:          true,
	}
}

func testMsg() *Message {
	return &Message{ID: "test-1", Topic: "t", Data: []byte("hello"), PublishedAt: time.Now()}
}

func TestDeliverACKOnFirstAttempt(t *testing.T) {
	dlq := newDLQ(100)
	cfg := testConfig()
	sub := newSubscription("s1", "t", func(a *DeliveryAttempt) {
		a.Token.Ack()
	}, dlq, cfg)

	got := deliver(sub, testMsg())
	if !got {
		t.Fatal("expected deliver to return true on immediate ack")
	}
	if dlq.Size() != 0 {
		t.Fatal("expected empty DLQ")
	}
}

func TestDeliverNACKRetries(t *testing.T) {
	dlq := newDLQ(100)
	cfg := testConfig()
	var calls atomic.Int32

	sub := newSubscription("s1", "t", func(a *DeliveryAttempt) {
		n := calls.Add(1)
		if n < 3 {
			a.Token.Nack()
		} else {
			a.Token.Ack()
		}
	}, dlq, cfg)

	got := deliver(sub, testMsg())
	if !got {
		t.Fatal("expected deliver to return true after retries")
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 3 handler calls, got %d", calls.Load())
	}
	if dlq.Size() != 0 {
		t.Fatal("expected empty DLQ")
	}
}

func TestDeliverTimeoutRetries(t *testing.T) {
	dlq := newDLQ(100)
	cfg := testConfig()
	var calls atomic.Int32

	sub := newSubscription("s1", "t", func(a *DeliveryAttempt) {
		n := calls.Add(1)
		if int(n) == cfg.MaxDeliveryAttempts {
			a.Token.Ack()
		}
		// else: do nothing — let AckTimeout fire
	}, dlq, cfg)

	got := deliver(sub, testMsg())
	if !got {
		t.Fatal("expected deliver to succeed after timeout retries")
	}
}

func TestDeliverExhaustedGoesToDLQ(t *testing.T) {
	dlq := newDLQ(100)
	cfg := testConfig()

	sub := newSubscription("s1", "t", func(a *DeliveryAttempt) {
		a.Token.Nack()
	}, dlq, cfg)

	got := deliver(sub, testMsg())
	if got {
		t.Fatal("expected deliver to return false when all attempts exhausted")
	}
	if dlq.Size() != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", dlq.Size())
	}
}

func TestDeliverPanicGoesToDLQ(t *testing.T) {
	dlq := newDLQ(100)
	cfg := testConfig()

	sub := newSubscription("s1", "t", func(a *DeliveryAttempt) {
		panic("handler panic")
	}, dlq, cfg)

	got := deliver(sub, testMsg())
	if got {
		t.Fatal("expected deliver to return false when handler panics every attempt")
	}
	if dlq.Size() != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", dlq.Size())
	}
}
