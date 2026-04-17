package test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/sourik/go-pubsub-broker/internal/broker"
	"github.com/sourik/go-pubsub-broker/internal/config"
)

func TestDLQEviction(t *testing.T) {
	b := broker.NewBroker(&config.Config{
		MaxDeliveryAttempts: 1,
		AckTimeout:          50 * time.Millisecond,
		InboxBufferSize:     512,
		DLQMaxSize:          3, // tiny DLQ
		DropOnFull:          true,
	})
	_ = b.CreateTopic("t")

	var dlqWrites atomic.Int32
	done := make(chan struct{})

	_ = b.Subscribe("t", "s1", func(a *broker.DeliveryAttempt) {
		a.Token.Nack()
		if dlqWrites.Add(1) == 5 {
			go func() { time.Sleep(50 * time.Millisecond); close(done) }()
		}
	})

	for i := 0; i < 5; i++ {
		_, _ = b.Publish("t", []byte("fail"), nil)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DLQ writes")
	}

	dlq, _ := b.GetDLQ("t")
	// DLQ max is 3 — oldest 2 should have been evicted
	if dlq.Size() != 3 {
		t.Fatalf("expected DLQ size=3 (capped), got %d", dlq.Size())
	}
}

func TestDLQDrain(t *testing.T) {
	b := newBroker(1)
	_ = b.CreateTopic("t")

	var done atomic.Int32
	ch := make(chan struct{})

	_ = b.Subscribe("t", "s1", func(a *broker.DeliveryAttempt) {
		a.Token.Nack()
		if done.Add(1) == 2 {
			go func() { time.Sleep(50 * time.Millisecond); close(ch) }()
		}
	})

	_, _ = b.Publish("t", []byte("msg1"), nil)
	_, _ = b.Publish("t", []byte("msg2"), nil)

	<-ch

	dlq, _ := b.GetDLQ("t")
	drained := dlq.Drain()
	if len(drained) != 2 {
		t.Fatalf("expected 2 drained entries, got %d", len(drained))
	}
	if dlq.Size() != 0 {
		t.Fatal("DLQ should be empty after drain")
	}
}
