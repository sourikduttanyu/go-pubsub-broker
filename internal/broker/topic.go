package broker

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sourik/go-pubsub-broker/internal/config"
)

type Topic struct {
	Name          string
	mu            sync.RWMutex
	subscriptions map[SubscriptionID]*Subscription
	dlq           *DeadLetterQueue
	msgSeq        atomic.Uint64
	cfg           *config.Config
	wg            *sync.WaitGroup
}

func newTopic(name string, cfg *config.Config, wg *sync.WaitGroup) *Topic {
	return &Topic{
		Name:          name,
		subscriptions: make(map[SubscriptionID]*Subscription),
		dlq:           newDLQ(cfg.DLQMaxSize),
		cfg:           cfg,
		wg:            wg,
	}
}

func (t *Topic) Subscribe(id SubscriptionID, fn SubscriberFunc) (*Subscription, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.subscriptions[id]; exists {
		return nil, fmt.Errorf("subscription %q already exists on topic %q", id, t.Name)
	}
	sub := newSubscription(id, t.Name, fn, t.dlq, t.cfg)
	t.subscriptions[id] = sub
	sub.start(t.wg)
	return sub, nil
}

func (t *Topic) Unsubscribe(id SubscriptionID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	sub, exists := t.subscriptions[id]
	if !exists {
		return fmt.Errorf("subscription %q not found on topic %q", id, t.Name)
	}
	sub.stop()
	delete(t.subscriptions, id)
	return nil
}

func (t *Topic) fanOut(msg *Message) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, sub := range t.subscriptions {
		if t.cfg.DropOnFull {
			select {
			case sub.inbox <- msg:
			default:
				// inbox full — drop
			}
		} else {
			sub.inbox <- msg
		}
	}
}

func (t *Topic) nextMsgID() MessageID {
	seq := t.msgSeq.Add(1)
	return MessageID(fmt.Sprintf("%s-%d", t.Name, seq))
}

func (t *Topic) shutdown() {
	t.mu.Lock()
	subs := make([]*Subscription, 0, len(t.subscriptions))
	for _, s := range t.subscriptions {
		subs = append(subs, s)
	}
	t.mu.Unlock()

	for _, s := range subs {
		s.stop()
	}
}

func (t *Topic) DLQ() *DeadLetterQueue {
	return t.dlq
}
