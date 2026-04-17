package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"github.com/sourik/go-pubsub-broker/internal/broker"
	"github.com/sourik/go-pubsub-broker/internal/config"
)

var (
	demoMessages   int
	demoSubs       int
	demoFailRate   float64
	demoMaxAttempt int
	demoTopic      string
)

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "Run an interactive demonstration of broker features",
	Long: `Launches a self-contained demo that creates a topic, wires up N subscribers
(with a configurable nack rate to simulate flaky consumers), publishes M messages,
and prints delivery attempts, retries, and DLQ entries.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := &config.Config{
			MaxDeliveryAttempts: demoMaxAttempt,
			AckTimeout:          5 * time.Second,
			InboxBufferSize:     256,
			DLQMaxSize:          10000,
			DropOnFull:          true,
		}
		b := broker.NewBroker(cfg)

		fmt.Printf("\n=== Mini Pub/Sub Broker Demo ===\n")
		fmt.Printf("Topic: %s | Subscribers: %d | Messages: %d | Fail rate: %.0f%% | Max attempts: %d\n\n",
			demoTopic, demoSubs, demoMessages, demoFailRate*100, demoMaxAttempt)

		if err := b.CreateTopic(demoTopic); err != nil {
			return err
		}

		var mu sync.Mutex
		var totalDelivered, totalNacked, totalDLQ atomic.Int32
		var wg sync.WaitGroup
		wg.Add(demoMessages * demoSubs)

		for i := 0; i < demoSubs; i++ {
			subID := broker.SubscriptionID(fmt.Sprintf("sub-%d", i+1))
			failRate := demoFailRate
			err := b.Subscribe(demoTopic, subID, func(a *broker.DeliveryAttempt) {
				shouldFail := rand.Float64() < failRate

				mu.Lock()
				if shouldFail {
					fmt.Printf("[%s] msg=%-12s attempt=%d/%d → NACK\n",
						subID, a.Message.ID, a.AttemptNum, demoMaxAttempt)
				} else {
					fmt.Printf("[%s] msg=%-12s attempt=%d/%d → ACK  data=%q\n",
						subID, a.Message.ID, a.AttemptNum, demoMaxAttempt, string(a.Message.Data))
				}
				mu.Unlock()

				if shouldFail {
					totalNacked.Add(1)
					a.Token.Nack()
				} else {
					totalDelivered.Add(1)
					a.Token.Ack()
					wg.Done()
				}
			})
			if err != nil {
				return err
			}
		}

		fmt.Printf("Publishing %d messages...\n\n", demoMessages)
		for i := 0; i < demoMessages; i++ {
			data := fmt.Sprintf(`{"id":%d,"ts":"%s"}`, i+1, time.Now().Format(time.RFC3339))
			if _, err := b.Publish(demoTopic, []byte(data), nil); err != nil {
				return err
			}
		}

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()

		timeout := time.Duration(demoMaxAttempt) * cfg.AckTimeout * time.Duration(demoMessages+1)
		select {
		case <-done:
		case <-time.After(timeout):
			fmt.Println("\n[timeout — some messages may have gone to DLQ]")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)

		dlq, _ := b.GetDLQ(demoTopic)
		dlqEntries := dlq.Entries()
		totalDLQ.Store(int32(len(dlqEntries)))

		fmt.Printf("\n=== Results ===\n")
		fmt.Printf("  ACK'd deliveries : %d\n", totalDelivered.Load())
		fmt.Printf("  NACK attempts    : %d\n", totalNacked.Load())
		fmt.Printf("  DLQ entries      : %d\n", totalDLQ.Load())

		if len(dlqEntries) > 0 {
			fmt.Printf("\nDead Letter Queue:\n")
			for _, e := range dlqEntries {
				fmt.Printf("  [%s] sub=%-10s attempts=%d  data=%q\n",
					e.FailedAt.Format(time.RFC3339), e.Subscription, e.Attempts, string(e.Message.Data))
			}
		}

		return nil
	},
}

func init() {
	demoCmd.Flags().IntVarP(&demoMessages, "messages", "m", 5, "number of messages to publish")
	demoCmd.Flags().IntVarP(&demoSubs, "subs", "s", 2, "number of subscribers")
	demoCmd.Flags().Float64VarP(&demoFailRate, "fail-rate", "f", 0.3, "probability [0,1] that a delivery attempt is nack'd")
	demoCmd.Flags().IntVarP(&demoMaxAttempt, "max-attempts", "a", 3, "maximum delivery attempts before DLQ")
	demoCmd.Flags().StringVarP(&demoTopic, "topic", "t", "events", "topic name")
}
