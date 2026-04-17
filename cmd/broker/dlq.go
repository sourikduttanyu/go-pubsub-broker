package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var dlqTopic string

var dlqCmd = &cobra.Command{
	Use:   "dlq",
	Short: "Inspect the dead-letter queue",
}

var dlqListCmd = &cobra.Command{
	Use:   "list",
	Short: "List dead-lettered messages for a topic",
	RunE: func(cmd *cobra.Command, args []string) error {
		dlq, err := sharedBroker.GetDLQ(dlqTopic)
		if err != nil {
			return err
		}
		entries := dlq.Entries()
		if len(entries) == 0 {
			fmt.Printf("DLQ for topic %q is empty.\n", dlqTopic)
			return nil
		}
		fmt.Printf("DLQ for topic %q (%d entries):\n", dlqTopic, len(entries))
		for i, e := range entries {
			fmt.Printf("  [%d] msg_id=%-16s sub=%-16s attempts=%d  failed_at=%s  data=%q\n",
				i+1, e.Message.ID, e.Subscription, e.Attempts,
				e.FailedAt.Format("15:04:05"), string(e.Message.Data))
		}
		return nil
	},
}

var dlqDrainCmd = &cobra.Command{
	Use:   "drain",
	Short: "Remove and print all dead-lettered messages for a topic",
	RunE: func(cmd *cobra.Command, args []string) error {
		dlq, err := sharedBroker.GetDLQ(dlqTopic)
		if err != nil {
			return err
		}
		entries := dlq.Drain()
		fmt.Printf("Drained %d entries from DLQ for topic %q.\n", len(entries), dlqTopic)
		for i, e := range entries {
			fmt.Printf("  [%d] msg_id=%s  sub=%s  data=%q\n", i+1, e.Message.ID, e.Subscription, string(e.Message.Data))
		}
		return nil
	},
}

func init() {
	dlqCmd.PersistentFlags().StringVarP(&dlqTopic, "topic", "t", "", "topic name (required)")
	_ = dlqCmd.MarkPersistentFlagRequired("topic")

	dlqCmd.AddCommand(dlqListCmd)
	dlqCmd.AddCommand(dlqDrainCmd)
}
