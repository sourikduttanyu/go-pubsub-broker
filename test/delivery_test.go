package test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sourik/go-pubsub-broker/internal/broker"
	"github.com/sourik/go-pubsub-broker/internal/config"
)

func newBroker(maxAttempts int) *broker.Broker {
	return broker.NewBroker(&config.Config{
		MaxDeliveryAttempts: maxAttempts,
		AckTimeout:          200 * time.Millisecond,
		InboxBufferSize:     2048,
		DLQMaxSize:          10000,
		DropOnFull:          false, // block on full to guarantee at-least-once delivery
	})
}

func TestAtLeastOnce_1000Messages(t *testing.T) {
	b := newBroker(5)
	_ = b.CreateTopic("t")

	const n = 1000
	var received atomic.Int32

	done := make(chan struct{})
	_ = b.Subscribe("t", "s1", func(a *broker.DeliveryAttempt) {
		a.Token.Ack()
		if received.Add(1) == n {
			close(done)
		}
	})

	for i := 0; i < n; i++ {
		_, err := b.Publish("t", []byte("x"), nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out: only received %d/%d messages", received.Load(), n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = b.Shutdown(ctx)
}

func TestRetryOnNack_AckOnThirdAttempt(t *testing.T) {
	b := newBroker(5)
	_ = b.CreateTopic("t")

	var calls atomic.Int32
	done := make(chan struct{})

	_ = b.Subscribe("t", "s1", func(a *broker.DeliveryAttempt) {
		n := calls.Add(1)
		if n < 3 {
			a.Token.Nack()
		} else {
			a.Token.Ack()
			close(done)
		}
	})

	_, _ = b.Publish("t", []byte("retry-me"), nil)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ack on third attempt")
	}

	if calls.Load() != 3 {
		t.Fatalf("expected 3 handler calls, got %d", calls.Load())
	}

	dlq, _ := b.GetDLQ("t")
	if dlq.Size() != 0 {
		t.Fatal("expected empty DLQ when message was eventually ack'd")
	}
}

func TestExhaustedAttemptsGoToDLQ(t *testing.T) {
	b := newBroker(3)
	_ = b.CreateTopic("t")

	var calls atomic.Int32
	done := make(chan struct{})

	_ = b.Subscribe("t", "s1", func(a *broker.DeliveryAttempt) {
		n := calls.Add(1)
		a.Token.Nack()
		if int(n) == 3 {
			// Last attempt — give the loop a moment to write to DLQ.
			go func() { time.Sleep(50 * time.Millisecond); close(done) }()
		}
	})

	_, _ = b.Publish("t", []byte("always-fails"), nil)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	dlq, _ := b.GetDLQ("t")
	if dlq.Size() != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", dlq.Size())
	}
	if string(dlq.Entries()[0].Message.Data) != "always-fails" {
		t.Fatal("unexpected DLQ message data")
	}
}

func TestShutdownWithInflightMessages(t *testing.T) {
	b := newBroker(1)
	_ = b.CreateTopic("t")

	_ = b.Subscribe("t", "s1", func(a *broker.DeliveryAttempt) {
		time.Sleep(10 * time.Millisecond)
		a.Token.Ack()
	})

	for i := 0; i < 50; i++ {
		_, _ = b.Publish("t", []byte("x"), nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
}
