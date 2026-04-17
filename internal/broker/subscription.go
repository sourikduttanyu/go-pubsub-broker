package broker

import (
	"context"
	"sync"
	"time"

	"github.com/sourik/go-pubsub-broker/internal/config"
)

type Subscription struct {
	ID      SubscriptionID
	Topic   string
	Handler SubscriberFunc
	inbox   chan *Message
	dlq     *DeadLetterQueue
	cfg     *config.Config
	ctx     context.Context
	cancel  context.CancelFunc
}

func newSubscription(id SubscriptionID, topic string, fn SubscriberFunc, dlq *DeadLetterQueue, cfg *config.Config) *Subscription {
	ctx, cancel := context.WithCancel(context.Background())
	return &Subscription{
		ID:      id,
		Topic:   topic,
		Handler: fn,
		inbox:   make(chan *Message, cfg.InboxBufferSize),
		dlq:     dlq,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (s *Subscription) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		deliveryLoop(s)
	}()
}

func (s *Subscription) stop() {
	s.cancel()
}

func deliveryLoop(sub *Subscription) {
	for {
		select {
		case <-sub.ctx.Done():
			return
		case msg, ok := <-sub.inbox:
			if !ok {
				return
			}
			deliver(sub, msg)
		}
	}
}

// deliver attempts to deliver msg up to MaxDeliveryAttempts times.
// Returns true if the message was ack'd, false if it ended up in the DLQ.
func deliver(sub *Subscription, msg *Message) bool {
	for attempt := 1; attempt <= sub.cfg.MaxDeliveryAttempts; attempt++ {
		ackCh := make(chan ackSignal, 1)
		token := newAckToken(msg.ID, sub.ID, ackCh)

		// Run handler in its own goroutine so a panic can't crash the loop.
		go func() {
			defer func() {
				if r := recover(); r != nil {
					token.Nack()
				}
			}()
			sub.Handler(&DeliveryAttempt{Message: msg, AttemptNum: attempt, Token: token})
		}()

		select {
		case sig := <-ackCh:
			if sig.success {
				return true
			}
			// nack — retry
		case <-time.After(sub.cfg.AckTimeout):
			// timeout counts as nack
		case <-sub.ctx.Done():
			return false
		}
	}

	sub.dlq.Append(DLQEntry{
		Message:      msg,
		Subscription: sub.ID,
		FailedAt:     time.Now(),
		Attempts:     sub.cfg.MaxDeliveryAttempts,
	})
	return false
}
