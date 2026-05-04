package api

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sourik/go-pubsub-broker/internal/store"
	"github.com/sourik/go-pubsub-broker/pkg/pubsub"
)

// Options configures the HTTP server.
type Options struct {
	Addr   string      // e.g. ":8080"
	APIKey string      // value for Authorization: Bearer <key>
	Store  store.Store // always set; use store.NewMemory(0) for default
	RPS    float64     // publish requests/sec per API key; 0 = unlimited
}

// Server is the HTTP API server wrapping a pubsub.Client.
type Server struct {
	client  *pubsub.Client
	pb      *pullBuffer
	store   store.Store
	wsHubs  map[string]*wsHub
	wsMu    sync.Mutex
	limiter *rateLimiter // nil when RPS == 0
	srv     *http.Server
}

// NewServer constructs and wires up the HTTP server.
//
// Exempt from auth: GET /healthz, GET /metrics.
// All other routes require Authorization: Bearer <APIKey>.
func NewServer(opts Options, client *pubsub.Client) *Server {
	s := &Server{
		client: client,
		pb:     newPullBuffer(),
		store:  opts.Store,
		wsHubs: make(map[string]*wsHub),
	}
	if opts.RPS > 0 {
		s.limiter = newRateLimiter(opts.RPS)
	}

	mux := http.NewServeMux()

	// Admin / observability (no auth)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.Handle("GET /metrics", promhttp.Handler())

	// Topics
	mux.HandleFunc("POST /topics", s.handleCreateTopic)
	mux.HandleFunc("GET /topics", s.handleListTopics)
	mux.HandleFunc("DELETE /topics/{id}", s.handleDeleteTopic)
	mux.HandleFunc("POST /topics/{id}/publish", s.handlePublish)
	mux.HandleFunc("GET /topics/{id}/messages", s.handleGetMessages)
	mux.HandleFunc("GET /topics/{id}/dlq", s.handleGetDLQ)
	mux.HandleFunc("DELETE /topics/{id}/dlq", s.handleDrainDLQ)
	mux.HandleFunc("GET /topics/{id}/subscribers", s.handleGetSubscribers)

	// WebSocket real-time push
	mux.HandleFunc("GET /topics/{id}/subscribe", s.handleWSSubscribe)

	// Pull-based subscriptions
	mux.HandleFunc("POST /subscriptions", s.handleCreateSubscription)
	mux.HandleFunc("DELETE /subscriptions/{id}", s.handleDeleteSubscription)
	mux.HandleFunc("POST /subscriptions/{id}/pull", s.handlePull)

	s.srv = &http.Server{
		Addr:         opts.Addr,
		Handler:      requireAPIKey(opts.APIKey, s.limiter, mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // 0 = no timeout (WebSocket connections are long-lived)
	}
	return s
}

func (s *Server) ListenAndServe() error {
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
