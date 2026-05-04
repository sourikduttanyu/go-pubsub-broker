package api

import "time"

type createTopicRequest struct {
	Name string `json:"name"`
}

type publishRequest struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type publishResponse struct {
	MessageID string `json:"message_id"`
}

type createSubscriptionRequest struct {
	Topic string `json:"topic"`
	SubID string `json:"sub_id"`
}

type pulledMessage struct {
	MessageID   string            `json:"message_id"`
	Topic       string            `json:"topic"`
	Data        []byte            `json:"data"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	PublishedAt time.Time         `json:"published_at"`
}

type dlqEntryResponse struct {
	MessageID    string            `json:"message_id"`
	Topic        string            `json:"topic"`
	Data         []byte            `json:"data"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	Subscription string            `json:"subscription"`
	FailedAt     time.Time         `json:"failed_at"`
	Attempts     int               `json:"attempts"`
}
