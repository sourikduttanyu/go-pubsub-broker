package pubsubclient

import (
	"context"
	"time"
)

// Subscribe polls the server for messages and calls fn for each one.
// Runs until ctx is cancelled. pollInterval controls the sleep between empty polls.
// Backs off exponentially (up to 30s) on HTTP errors, resets on success.
func (c *Client) Subscribe(ctx context.Context, subID string, pollInterval time.Duration, fn func(PulledMessage)) error {
	const maxBackoff = 30 * time.Second
	backoff := pollInterval

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		msgs, err := c.Pull(ctx, subID, 10)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}

		backoff = pollInterval
		for _, m := range msgs {
			fn(m)
		}

		if len(msgs) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
		}
	}
}
