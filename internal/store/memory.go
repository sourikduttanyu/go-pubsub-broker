package store

import "sync"

const defaultRingSize = 1000

// Memory is an in-memory ring buffer store. Not durable across restarts.
type Memory struct {
	mu   sync.Mutex
	msgs map[string][]Message // topic -> append-only slice, trimmed to maxPer
	max  int
}

// NewMemory creates an in-memory store.
// maxPerTopic <= 0 uses the default of 1000 messages per topic.
func NewMemory(maxPerTopic int) *Memory {
	if maxPerTopic <= 0 {
		maxPerTopic = defaultRingSize
	}
	return &Memory{
		msgs: make(map[string][]Message),
		max:  maxPerTopic,
	}
}

func (m *Memory) AppendMessage(msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs := m.msgs[msg.Topic]
	msgs = append(msgs, msg)
	if len(msgs) > m.max {
		msgs = msgs[len(msgs)-m.max:]
	}
	m.msgs[msg.Topic] = msgs
	return nil
}

func (m *Memory) LoadMessages(topic string, limit int) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs := m.msgs[topic]
	if len(msgs) == 0 {
		return []Message{}, nil
	}
	start := len(msgs) - limit
	if start < 0 {
		start = 0
	}
	slice := msgs[start:]
	// return newest-first
	out := make([]Message, len(slice))
	for i, m := range slice {
		out[len(slice)-1-i] = m
	}
	return out, nil
}

func (m *Memory) Close() error { return nil }
