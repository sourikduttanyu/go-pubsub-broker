package broker

import (
	"sync"
	"time"
)

type DLQEntry struct {
	Message      *Message
	Subscription SubscriptionID
	FailedAt     time.Time
	Attempts     int
}

type DeadLetterQueue struct {
	mu      sync.Mutex
	entries []DLQEntry
	maxSize int
}

func newDLQ(maxSize int) *DeadLetterQueue {
	return &DeadLetterQueue{maxSize: maxSize}
}

func (d *DeadLetterQueue) Append(e DLQEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.entries) >= d.maxSize {
		// evict oldest
		d.entries = d.entries[1:]
	}
	d.entries = append(d.entries, e)
}

func (d *DeadLetterQueue) Entries() []DLQEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]DLQEntry, len(d.entries))
	copy(out, d.entries)
	return out
}

func (d *DeadLetterQueue) Drain() []DLQEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.entries
	d.entries = nil
	return out
}

func (d *DeadLetterQueue) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.entries)
}
