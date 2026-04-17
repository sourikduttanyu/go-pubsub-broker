package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/sourik/go-pubsub-broker/internal/broker"
	"github.com/sourik/go-pubsub-broker/internal/config"
)

// sharedBroker is the single in-process broker instance used by all subcommands.
var sharedBroker *broker.Broker

var rootCmd = &cobra.Command{
	Use:   "broker",
	Short: "Mini in-memory pub/sub broker",
	Long: `A lightweight pub/sub broker with at-least-once delivery, per-subscription retries,
and a dead-letter queue. Mirrors the semantics of Google Cloud Pub/Sub.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		cfg := config.Default()
		sharedBroker = broker.NewBroker(cfg)
	},
}

func main() {
	rootCmd.AddCommand(demoCmd)
	rootCmd.AddCommand(publishCmd)
	rootCmd.AddCommand(subscribeCmd)
	rootCmd.AddCommand(dlqCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
