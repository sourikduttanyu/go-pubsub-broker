package broker

import "time"

type MessageID string
type SubscriptionID string

type Message struct {
	ID          MessageID
	Topic       string
	Data        []byte
	Attributes  map[string]string
	PublishedAt time.Time
}

type ackSignal struct {
	id      MessageID
	success bool
}

// AckToken is handed to the subscriber for each delivery attempt.
// Call Ack() to confirm delivery or Nack() to request a retry.
type AckToken struct {
	messageID MessageID
	subID     SubscriptionID
	ack       chan<- ackSignal
}

func newAckToken(msgID MessageID, subID SubscriptionID, ch chan<- ackSignal) *AckToken {
	return &AckToken{messageID: msgID, subID: subID, ack: ch}
}

func (t *AckToken) Ack() {
	select {
	case t.ack <- ackSignal{id: t.messageID, success: true}:
	default:
	}
}

func (t *AckToken) Nack() {
	select {
	case t.ack <- ackSignal{id: t.messageID, success: false}:
	default:
	}
}

type DeliveryAttempt struct {
	Message    *Message
	AttemptNum int
	Token      *AckToken
}

// SubscriberFunc is the handler signature subscribers implement.
type SubscriberFunc func(attempt *DeliveryAttempt)
