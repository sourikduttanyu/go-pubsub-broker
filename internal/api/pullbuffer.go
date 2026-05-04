package api

import (
	"sync"

	"github.com/sourik/go-pubsub-broker/internal/broker"
)

const pullBufferSize = 512

type pullBuffer struct {
	mu     sync.Mutex
	bufs   map[broker.SubscriptionID]chan *broker.Message
	topics map[broker.SubscriptionID]string // subID -> topic name
}

func newPullBuffer() *pullBuffer {
	return &pullBuffer{
		bufs:   make(map[broker.SubscriptionID]chan *broker.Message),
		topics: make(map[broker.SubscriptionID]string),
	}
}

func (pb *pullBuffer) add(subID broker.SubscriptionID, topic string) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.bufs[subID] = make(chan *broker.Message, pullBufferSize)
	pb.topics[subID] = topic
}

func (pb *pullBuffer) remove(subID broker.SubscriptionID) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	delete(pb.bufs, subID)
	delete(pb.topics, subID)
}

func (pb *pullBuffer) topic(subID broker.SubscriptionID) (string, bool) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	t, ok := pb.topics[subID]
	return t, ok
}

func (pb *pullBuffer) exists(subID broker.SubscriptionID) bool {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	_, ok := pb.bufs[subID]
	return ok
}

// handler returns a SubscriberFunc that writes messages into the pull buffer.
// Messages are auto-acked on receipt; if the buffer is full they are nacked.
func (pb *pullBuffer) handler(subID broker.SubscriptionID) broker.SubscriberFunc {
	return func(a *broker.DeliveryAttempt) {
		pb.mu.Lock()
		ch, ok := pb.bufs[subID]
		pb.mu.Unlock()
		if !ok {
			a.Token.Nack()
			return
		}
		select {
		case ch <- a.Message:
			a.Token.Ack()
		default:
			a.Token.Nack() // buffer full
		}
	}
}

func (pb *pullBuffer) pull(subID broker.SubscriptionID, max int) []*broker.Message {
	pb.mu.Lock()
	ch, ok := pb.bufs[subID]
	pb.mu.Unlock()
	if !ok {
		return nil
	}
	msgs := make([]*broker.Message, 0, max)
	for i := 0; i < max; i++ {
		select {
		case m := <-ch:
			msgs = append(msgs, m)
		default:
			return msgs
		}
	}
	return msgs
}
