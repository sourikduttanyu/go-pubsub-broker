package bench

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sourik/go-pubsub-broker/internal/broker"
	"github.com/sourik/go-pubsub-broker/internal/config"
)

func newBenchBroker() *broker.Broker {
	return broker.NewBroker(&config.Config{
		MaxDeliveryAttempts: 3,
		AckTimeout:          5 * time.Second,
		InboxBufferSize:     4096,
		DLQMaxSize:          10000,
		DropOnFull:          false, // block publisher if inbox full — no silent drops
	})
}

func BenchmarkPublish1Sub(b *testing.B) {
	br := newBenchBroker()
	_ = br.CreateTopic("t")

	var done atomic.Int64
	ch := make(chan struct{})

	_ = br.Subscribe("t", "s1", func(a *broker.DeliveryAttempt) {
		a.Token.Ack()
		if done.Add(1) == int64(b.N) {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = br.Publish("t", []byte("hello"), nil)
	}
	<-ch
	b.StopTimer()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = br.Shutdown(ctx)
}

func BenchmarkPublish10Subs(b *testing.B) {
	benchmarkFanOut(b, 10)
}

func BenchmarkPublish100Subs(b *testing.B) {
	benchmarkFanOut(b, 100)
}

func benchmarkFanOut(b *testing.B, numSubs int) {
	b.Helper()
	br := newBenchBroker()
	_ = br.CreateTopic("t")

	var done atomic.Int64
	target := int64(b.N * numSubs)
	ch := make(chan struct{})

	for i := 0; i < numSubs; i++ {
		id := broker.SubscriptionID(fmt.Sprintf("sub-%d", i))
		_ = br.Subscribe("t", id, func(a *broker.DeliveryAttempt) {
			a.Token.Ack()
			if done.Add(1) == target {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = br.Publish("t", []byte("x"), nil)
	}
	<-ch
	b.StopTimer()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = br.Shutdown(ctx)
}

func BenchmarkPublishParallel(b *testing.B) {
	br := newBenchBroker()
	_ = br.CreateTopic("t")
	_ = br.Subscribe("t", "s1", func(a *broker.DeliveryAttempt) { a.Token.Ack() })

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = br.Publish("t", []byte("x"), nil)
		}
	})
	b.StopTimer()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = br.Shutdown(ctx)
}

func BenchmarkPublishLargePayload(b *testing.B) {
	br := newBenchBroker()
	_ = br.CreateTopic("t")

	var done atomic.Int64
	ch := make(chan struct{})
	_ = br.Subscribe("t", "s1", func(a *broker.DeliveryAttempt) {
		a.Token.Ack()
		if done.Add(1) == int64(b.N) {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})

	payload := make([]byte, 1<<20) // 1 MB
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = br.Publish("t", payload, nil)
	}
	<-ch
	b.StopTimer()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = br.Shutdown(ctx)
}

func BenchmarkPublishWithRetries(b *testing.B) {
	br := newBenchBroker()
	_ = br.CreateTopic("t")

	var done atomic.Int64
	ch := make(chan struct{})
	_ = br.Subscribe("t", "s1", func(a *broker.DeliveryAttempt) {
		if a.AttemptNum < 2 {
			a.Token.Nack() // nack on first attempt, ack on second
		} else {
			a.Token.Ack()
			if done.Add(1) == int64(b.N) {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		}
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = br.Publish("t", []byte("x"), nil)
	}
	<-ch
	b.StopTimer()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = br.Shutdown(ctx)
}
