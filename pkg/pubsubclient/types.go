package pubsubclient

import "time"

// PulledMessage is a message returned by Pull or delivered via Subscribe.
type PulledMessage struct {
	MessageID   string            `json:"message_id"`
	Topic       string            `json:"topic"`
	Data        []byte            `json:"data"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	PublishedAt time.Time         `json:"published_at"`
}

// DLQEntry is a dead-lettered message returned by DLQEntries or DLQDrain.
type DLQEntry struct {
	MessageID    string            `json:"message_id"`
	Topic        string            `json:"topic"`
	Data         []byte            `json:"data"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	Subscription string            `json:"subscription"`
	FailedAt     time.Time         `json:"failed_at"`
	Attempts     int               `json:"attempts"`
}
