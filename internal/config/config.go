package config

import "time"

type Config struct {
	MaxDeliveryAttempts int
	AckTimeout          time.Duration
	InboxBufferSize     int
	DLQMaxSize          int
	DropOnFull          bool // if true, fanOut drops msg when inbox is full; if false, blocks
}

func Default() *Config {
	return &Config{
		MaxDeliveryAttempts: 5,
		AckTimeout:          10 * time.Second,
		InboxBufferSize:     256,
		DLQMaxSize:          10000,
		DropOnFull:          true,
	}
}
