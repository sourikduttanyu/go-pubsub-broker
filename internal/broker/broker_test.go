package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sourik/go-pubsub-broker/internal/config"
)

func fastConfig() *config.Config {
	return &config.Config{
		MaxDeliveryAttempts: 3,
		AckTimeout:          100 * time.Millisecond,
		InboxBufferSize:     256,
		DLQMaxSize:          1000,
		DropOnFull:          true,
	}
}

func TestPublishToNonexistentTopic(t *testing.T) {
	b := NewBroker(fastConfig())
	_, err := b.Publish("no-such-topic", []byte("x"), nil)
	if err == nil {
		t.Fatal("expected error publishing to missing topic")
	}
}

func TestFanOutToMultipleSubscribers(t *testing.T) {
	b := NewBroker(fastConfig())
	if err := b.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	const numSubs = 3
	var counts [numSubs]atomic.Int32
	var wg sync.WaitGroup
	wg.Add(numSubs)

	for i := 0; i < numSubs; i++ {
		i := i
		id := SubscriptionID("sub-" + string(rune('0'+i)))
		_ = b.Subscribe("t", id, func(a *DeliveryAttempt) {
			if counts[i].Add(1) == 1 {
				wg.Done()
			}
			a.Token.Ack()
		})
	}

	if _, err := b.Publish("t", []byte("hello"), nil); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for all subscribers to receive message")
	}

	for i := 0; i < numSubs; i++ {
		if counts[i].Load() == 0 {
			t.Fatalf("subscriber %d did not receive the message", i)
		}
	}
}

func TestAllNackGoesToDLQ(t *testing.T) {
	b := NewBroker(fastConfig())
	_ = b.CreateTopic("t")

	var done sync.WaitGroup
	done.Add(1)
	var called atomic.Int32

	_ = b.Subscribe("t", "s1", func(a *DeliveryAttempt) {
		n := called.Add(1)
		a.Token.Nack()
		if int(n) == fastConfig().MaxDeliveryAttempts {
			done.Done()
		}
	})

	if _, err := b.Publish("t", []byte("fail"), nil); err != nil {
		t.Fatal(err)
	}

	ch := make(chan struct{})
	go func() { done.Wait(); close(ch) }()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for all nack attempts")
	}

	// Give the delivery loop a moment to write to DLQ after the last nack.
	time.Sleep(50 * time.Millisecond)

	dlq, _ := b.GetDLQ("t")
	if dlq.Size() != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", dlq.Size())
	}
}

func TestShutdownClean(t *testing.T) {
	b := NewBroker(fastConfig())
	_ = b.CreateTopic("t")
	_ = b.Subscribe("t", "s1", func(a *DeliveryAttempt) { a.Token.Ack() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
}
