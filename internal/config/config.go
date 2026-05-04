package config

import "time"

type Config struct {
	MaxDeliveryAttempts int
	AckTimeout          time.Duration
	InboxBufferSize     int
	DLQMaxSize          int
	DropOnFull          bool // if true, fanOut drops msg when inbox is full; if false, blocks
}

type ServerConfig struct {
	Addr   string // e.g. ":8080"
	APIKey string // sourced from BROKER_API_KEY env var
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
