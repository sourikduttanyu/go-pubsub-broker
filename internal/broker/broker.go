package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sourik/go-pubsub-broker/internal/config"
)

type Broker struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	cfg    *config.Config
	wg     sync.WaitGroup
}

func NewBroker(cfg *config.Config) *Broker {
	if cfg == nil {
		cfg = config.Default()
	}
	return &Broker{
		topics: make(map[string]*Topic),
		cfg:    cfg,
	}
}

func (b *Broker) CreateTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}
	b.topics[name] = newTopic(name, b.cfg, &b.wg)
	return nil
}

func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, exists := b.topics[name]
	if !exists {
		return fmt.Errorf("topic %q not found", name)
	}
	t.shutdown()
	delete(b.topics, name)
	return nil
}

func (b *Broker) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.topics))
	for n := range b.topics {
		names = append(names, n)
	}
	return names
}

func (b *Broker) getTopic(name string) (*Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	if !ok {
		return nil, fmt.Errorf("topic %q not found", name)
	}
	return t, nil
}

func (b *Broker) Publish(topicName string, data []byte, attrs map[string]string) (MessageID, error) {
	t, err := b.getTopic(topicName)
	if err != nil {
		return "", err
	}
	msg := &Message{
		ID:          t.nextMsgID(),
		Topic:       topicName,
		Data:        data,
		Attributes:  attrs,
		PublishedAt: time.Now(),
	}
	t.fanOut(msg)
	return msg.ID, nil
}

func (b *Broker) Subscribe(topicName string, subID SubscriptionID, fn SubscriberFunc) error {
	t, err := b.getTopic(topicName)
	if err != nil {
		return err
	}
	_, err = t.Subscribe(subID, fn)
	return err
}

func (b *Broker) Unsubscribe(topicName string, subID SubscriptionID) error {
	t, err := b.getTopic(topicName)
	if err != nil {
		return err
	}
	return t.Unsubscribe(subID)
}

func (b *Broker) GetDLQ(topicName string) (*DeadLetterQueue, error) {
	t, err := b.getTopic(topicName)
	if err != nil {
		return nil, err
	}
	return t.DLQ(), nil
}

// Shutdown gracefully stops all subscribers, waiting up to the deadline in ctx.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.mu.RLock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.RUnlock()

	for _, t := range topics {
		t.shutdown()
	}

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
