// Package store provides message history persistence for the broker.
// The default implementation is an in-memory ring buffer; PostgreSQL is
// opt-in via NewPostgres for durable storage across restarts.
package store

import "time"

// Message is a persisted record of a published message.
type Message struct {
	ID          string
	Topic       string
	Data        []byte
	Attributes  map[string]string
	PublishedAt time.Time
}

// Store persists message history for GET /topics/{topic}/messages.
type Store interface {
	// AppendMessage records a published message. Errors are non-fatal
	// (the message was already delivered to subscribers).
	AppendMessage(msg Message) error

	// LoadMessages returns the most-recent limit messages for topic,
	// ordered newest-first.
	LoadMessages(topic string, limit int) ([]Message, error)

	Close() error
}
