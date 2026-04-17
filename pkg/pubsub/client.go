// Package pubsub provides a stable public API over the in-memory broker.
// The shape mirrors cloud.google.com/go/pubsub to make the comparison natural.
package pubsub

import (
	"context"

	"github.com/sourik/go-pubsub-broker/internal/broker"
	"github.com/sourik/go-pubsub-broker/internal/config"
)

// MessageID is an opaque identifier returned by Publish.
type MessageID = broker.MessageID

// SubscriptionID identifies a subscriber on a topic.
type SubscriptionID = broker.SubscriptionID

// DeliveryAttempt is passed to the subscriber handler on each delivery try.
type DeliveryAttempt = broker.DeliveryAttempt

// SubscriberFunc is the handler signature for subscribers.
type SubscriberFunc = broker.SubscriberFunc

// DLQEntry holds a message that exhausted all delivery attempts.
type DLQEntry = broker.DLQEntry

// Client is the top-level handle to the broker.
type Client struct {
	b *broker.Broker
}

// NewClient creates a Client backed by a new in-memory broker.
// Pass nil to use default configuration.
func NewClient(cfg *config.Config) *Client {
	return &Client{b: broker.NewBroker(cfg)}
}

// CreateTopic registers a new topic. Returns an error if the topic already exists.
func (c *Client) CreateTopic(name string) error {
	return c.b.CreateTopic(name)
}

// DeleteTopic removes a topic and stops all its subscribers.
func (c *Client) DeleteTopic(name string) error {
	return c.b.DeleteTopic(name)
}

// Topics returns the names of all registered topics.
func (c *Client) Topics() []string {
	return c.b.Topics()
}

// Publish sends data to the named topic and returns the assigned MessageID.
func (c *Client) Publish(topic string, data []byte, attrs map[string]string) (MessageID, error) {
	return c.b.Publish(topic, data, attrs)
}

// Subscribe registers fn as a subscriber on topic. fn is called in a separate
// goroutine for each delivery attempt.
func (c *Client) Subscribe(topic string, subID SubscriptionID, fn SubscriberFunc) error {
	return c.b.Subscribe(topic, subID, fn)
}

// Unsubscribe removes the subscription and stops its delivery goroutine.
func (c *Client) Unsubscribe(topic string, subID SubscriptionID) error {
	return c.b.Unsubscribe(topic, subID)
}

// DLQEntries returns a snapshot of all dead-lettered messages for a topic.
func (c *Client) DLQEntries(topic string) ([]DLQEntry, error) {
	dlq, err := c.b.GetDLQ(topic)
	if err != nil {
		return nil, err
	}
	return dlq.Entries(), nil
}

// DLQDrain removes and returns all dead-lettered messages for a topic.
func (c *Client) DLQDrain(topic string) ([]DLQEntry, error) {
	dlq, err := c.b.GetDLQ(topic)
	if err != nil {
		return nil, err
	}
	return dlq.Drain(), nil
}

// Shutdown gracefully drains all delivery goroutines.
func (c *Client) Shutdown(ctx context.Context) error {
	return c.b.Shutdown(ctx)
}
