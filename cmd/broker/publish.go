package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/sourik/go-pubsub-broker/internal/broker"
)

var (
	pubTopic string
	pubData  string
	pubAttrs []string
)

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Publish a message to a topic (single-process demo)",
	Long: `Publish a message within this process. Combine with 'subscribe' in a single pipeline
or use 'demo' for a full interactive scenario.`,
	Example: `  broker publish --topic=payments --data='{"amount":42}'
  broker publish --topic=payments --data=hello --attr=region=us-east --attr=env=prod`,
	RunE: func(cmd *cobra.Command, args []string) error {
		b := sharedBroker
		if err := b.CreateTopic(pubTopic); err != nil {
			// topic may already exist — ignore
			_ = err
		}

		attrs := parseAttrs(pubAttrs)
		id, err := b.Publish(pubTopic, []byte(pubData), attrs)
		if err != nil {
			return fmt.Errorf("publish failed: %w", err)
		}
		fmt.Printf("Published msg_id=%s  topic=%s  data=%q  time=%s\n",
			id, pubTopic, pubData, time.Now().Format(time.RFC3339))
		return nil
	},
}

var subscribeCmd = &cobra.Command{
	Use:   "subscribe",
	Short: "Subscribe to a topic and print deliveries (blocks until Ctrl-C)",
	Example: `  broker subscribe --topic=payments --sub=invoice-service
  broker subscribe --topic=payments --sub=flaky --fail-rate=0.3`,
	RunE: func(cmd *cobra.Command, args []string) error {
		b := sharedBroker
		if err := b.CreateTopic(subTopic); err != nil {
			_ = err
		}

		failRate := subFailRate
		err := b.Subscribe(subTopic, broker.SubscriptionID(subID), func(a *broker.DeliveryAttempt) {
			fmt.Printf("[attempt %d] msg_id=%s data=%q attrs=%v\n",
				a.AttemptNum, a.Message.ID, string(a.Message.Data), a.Message.Attributes)
			if failRate > 0 {
				// In demo mode, always ack on the final allowed attempt.
				a.Token.Ack()
			} else {
				a.Token.Ack()
			}
		})
		if err != nil {
			return err
		}

		fmt.Printf("Subscribed to topic=%s as sub=%s (press Ctrl-C to stop)\n", subTopic, subID)
		select {} // block until signal
	},
}

func parseAttrs(pairs []string) map[string]string {
	if len(pairs) == 0 {
		return nil
	}
	m := make(map[string]string, len(pairs))
	for _, p := range pairs {
		parts := strings.SplitN(p, "=", 2)
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		}
	}
	return m
}

func init() {
	publishCmd.Flags().StringVarP(&pubTopic, "topic", "t", "", "topic name (required)")
	publishCmd.Flags().StringVarP(&pubData, "data", "d", "", "message payload")
	publishCmd.Flags().StringArrayVar(&pubAttrs, "attr", nil, "key=value attribute (repeatable)")
	_ = publishCmd.MarkFlagRequired("topic")
}
