# go-pubsub-broker

A lightweight in-memory pub/sub broker written in Go. Implements the core delivery semantics of Google Cloud Pub/Sub: topics, per-subscription fan-out, **at-least-once delivery** with configurable retries, and a **dead-letter queue** (DLQ) for messages that exhaust all delivery attempts.

Built to understand the architecture from the inside — not just use the API.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                           Broker                                  │
│   topics: map[string]*Topic                                       │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │  Topic "payments"                                         │    │
│  │                                                           │    │
│  │  Publisher ──► fanOut() ──┬──► Sub "invoice"  inbox(256) │    │
│  │                           │         └──► deliveryLoop     │    │
│  │                           │               ├── attempt 1   │    │
│  │                           │               ├── attempt 2   │    │
│  │                           │               └──► DLQ        │    │
│  │                           │                               │    │
│  │                           └──► Sub "audit"    inbox(256)  │    │
│  │                                     └──► deliveryLoop     │    │
│  │                                           └── attempt 1   │    │
│  │                                                           │    │
│  │  DLQ ◄─────────────────── exhausted attempts             │    │
│  └──────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

### Concurrency model

```
Publisher goroutine
    │  broker.Publish(topic, msg)
    ▼
 Topic.fanOut()  [holds RLock, non-blocking send to each inbox]
    ├──► Sub A  inbox chan (buffered, size=256)
    ├──► Sub B  inbox chan
    └──► Sub N  inbox chan

Per subscription — one long-lived deliveryLoop goroutine:

  for msg := range sub.inbox {
    for attempt := 1..MaxAttempts {
      go handler(DeliveryAttempt{msg, attempt, token})  // isolated goroutine per attempt
      select {
        case <-ackCh:       // Ack() or Nack()
        case <-timeout:     // implicit nack
        case <-ctx.Done():  // shutdown
      }
    }
    dlq.Append(...)  // all attempts exhausted
  }
```

**Key design decisions:**

| Decision | Rationale |
|---|---|
| One goroutine per subscription | O(subs) goroutines, not O(messages in flight) |
| Handler in its own goroutine per attempt | Panic in user code can't crash the delivery loop |
| AckToken uses buffered(1) channel | Handler never blocks on Ack/Nack |
| RWMutex on subscription registry | Concurrent fan-out + safe subscribe/unsubscribe |

---

## Features

- **Topics** — create, delete, list
- **Subscriptions** — multiple subscribers per topic; each gets every message
- **At-least-once delivery** — message not advanced until Ack or retry budget exhausted
- **Configurable retry** — max attempts, per-attempt timeout
- **Dead-letter queue** — per-topic DLQ with Entries / Drain API
- **Graceful shutdown** — `Shutdown(ctx)` drains all delivery loops via WaitGroup
- **CLI** — `visual` (live TUI dashboard), `demo`, `publish`, `subscribe`, `dlq` commands

---

## Quick start

```bash
go install github.com/sourik/go-pubsub-broker/cmd/broker@latest
```

### Live TUI dashboard

```bash
# Default: 8 messages, 3 subscribers, 35% nack rate, max 3 attempts
broker visual

# Custom run
broker visual --messages=10 --subs=4 --fail-rate=0.5 --max-attempts=4 --topic=payments
```

The `visual` command opens a full-screen terminal dashboard powered by [Bubbletea](https://github.com/charmbracelet/bubbletea) and [Lipgloss](https://github.com/charmbracelet/lipgloss):

```
  ⬡  go-pubsub-broker  ·  topic: events  ·  8 msgs  ·  fail-rate: 35%

 ╭──────────────────────────────────────────────────────────────────╮
 │ SUBSCRIBERS                                                       │
 │                                                                   │
 │  sub-1   [████████████░░░░░░░░]  inbox:12   ✓ 5    ↺ 2    ☠ 0   │
 │  sub-2   [████░░░░░░░░░░░░░░░░]  inbox:4    ✓ 3    ↺ 4    ☠ 1   │
 │  sub-3   [██████████░░░░░░░░░░]  inbox:10   ✓ 6    ↺ 1    ☠ 0   │
 │                                                                   │
 │  progress  ████████████████░░░░░░░░░░░░  18/24 delivered         │
 │  published: 8   dlq total: 1   ● running                         │
 ╰──────────────────────────────────────────────────────────────────╯

 ╭──────────────────────────────────────────────────────────────────╮
 │ EVENT LOG                                                         │
 │                                                                   │
 │   0.1s  sub-1     events-5        attempt 1/3    ✓ ACK           │
 │   0.4s  sub-2     events-5        attempt 1/3    ↺ NACK  retry...│
 │   0.7s  sub-3     events-4        attempt 2/3    ✓ ACK           │
 │   1.1s  sub-2     events-5        attempt 2/3    ✓ ACK           │
 │   ...                                                             │
 ╰──────────────────────────────────────────────────────────────────╯

  q / ctrl+c  exit
```

Press `q` or `ctrl+c` to exit.

| Flag | Default | Description |
|---|---|---|
| `--messages` / `-m` | `8` | Number of messages to publish |
| `--subs` / `-s` | `3` | Number of subscribers |
| `--fail-rate` / `-f` | `0.35` | Probability a delivery attempt is nack'd |
| `--max-attempts` / `-a` | `3` | Max attempts before DLQ |
| `--topic` / `-t` | `events` | Topic name |

---

### Run the demo

```bash
# 5 messages, 2 subscribers, 30% nack rate, max 3 delivery attempts
broker demo --messages=5 --subs=2 --fail-rate=0.3 --max-attempts=3

# All messages fail → everything goes to DLQ
broker demo --messages=3 --fail-rate=1.0 --max-attempts=2
```

Sample output:

```
=== Mini Pub/Sub Broker Demo ===
Topic: events | Subscribers: 2 | Messages: 5 | Fail rate: 30% | Max attempts: 3

[sub-1] msg=events-1    attempt=1/3 → ACK  data="{\"id\":1,...}"
[sub-2] msg=events-1    attempt=1/3 → NACK
[sub-2] msg=events-1    attempt=2/3 → ACK  data="{\"id\":1,...}"
[sub-1] msg=events-2    attempt=1/3 → ACK  data="{\"id\":2,...}"
...

=== Results ===
  ACK'd deliveries : 10
  NACK attempts    : 3
  DLQ entries      : 0
```

### Programmatic API

```go
import "github.com/sourik/go-pubsub-broker/pkg/pubsub"

client := pubsub.NewClient(nil) // nil = default config

_ = client.CreateTopic("payments")

_ = client.Subscribe("payments", "invoice-service", func(a *pubsub.DeliveryAttempt) {
    fmt.Printf("received: %s\n", a.Message.Data)
    a.Token.Ack()
})

id, _ := client.Publish("payments", []byte(`{"amount":42}`), map[string]string{"env": "prod"})

// Inspect DLQ
entries, _ := client.DLQEntries("payments")
```

---

## Configuration

```go
cfg := &config.Config{
    MaxDeliveryAttempts: 5,            // default
    AckTimeout:          10 * time.Second,
    InboxBufferSize:     256,
    DLQMaxSize:          10000,
    DropOnFull:          true,         // false = block publisher when inbox full (at-least-once)
}
```

---

## Benchmarks

Measured on Apple M4, Go 1.26.2, `go test -bench=. -benchmem -benchtime=3s ./bench/`.

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| Publish, 1 subscriber | **1,540,000** | 649 | 647 | 13 |
| Publish, 10 subscribers (fan-out) | **352,000** | 2,838 | 5,181 | 85 |
| Publish, 100 subscribers (fan-out) | **38,400** | 26,038 | 50,538 | 805 |
| Publish, parallel publishers | **1,329,000** | 752 | 646 | 12 |
| Publish, 1 MB payload | **1,245,000** | 803 | 639 | 12 |
| Publish with 1 retry per message | **800,000** | 1,250 | 1,149 | 21 |

Fan-out cost scales linearly with subscriber count (~2.6 µs per additional 10 subscribers), consistent with the O(subs) channel-send model.

---

## Comparison to Google Cloud Pub/Sub

| Feature | This broker | Google Cloud Pub/Sub |
|---|---|---|
| Delivery guarantee | At-least-once | At-least-once |
| Retry mechanism | Fixed N attempts | Exponential backoff (10s–600s) |
| Dead-letter queue | Per-topic, in-memory | Per-subscription, durable |
| Message ordering | Not guaranteed | Optional ordering keys |
| Fan-out model | Goroutine + channel | Distributed push/pull |
| Persistence | In-memory only | Durable (Bigtable-backed) |
| Throughput (single sub) | ~1.5M msg/s | ~1M msg/s (pull mode) |
| ACK timeout | Configurable | 10s–600s |
| Subscription filter | Not implemented | Server-side CEL filter |
| Flow control | Inbox buffer (drop or block) | Client-side lease extension |

The core delivery contract — fan-out to all subscriptions, hold a message until Ack, retry on Nack or timeout, DLQ after N failures — is identical. What's missing is persistence, network transport, and the operational features (snapshot/seek, filtering, push endpoints) that make it a managed service.

---

## Project structure

```
cmd/broker/          CLI entry point (cobra)
  ├── demo.go        interactive feature demonstration
  ├── publish.go     publish subcommand + subscribe subcommand
  ├── subscribe.go   subscribe flag registration
  └── dlq.go         dlq list/drain subcommands

internal/
  ├── broker/
  │   ├── message.go       Message, AckToken, DeliveryAttempt
  │   ├── subscription.go  Subscription, deliveryLoop, retry logic
  │   ├── topic.go         Topic, fanOut, subscribe/unsubscribe
  │   ├── broker.go        Broker registry, Publish, Shutdown
  │   └── dlq.go           DeadLetterQueue
  └── config/config.go     Config with defaults

pkg/pubsub/client.go     Stable public API (mirrors cloud.google.com/go/pubsub shape)

bench/               Throughput benchmarks (go test -bench=.)
test/                Integration tests (at-least-once contract, DLQ scenarios)
```

---

## Running tests

```bash
go test ./...                          # all tests
go test -race ./...                    # with race detector
go test -bench=. -benchmem ./bench/   # benchmarks
```
