package api

import (
	"context"
	"net/http"
	"time"

	"github.com/sourik/go-pubsub-broker/pkg/pubsub"
)

// Server is the HTTP API server wrapping a pubsub.Client.
type Server struct {
	client *pubsub.Client
	pb     *pullBuffer
	srv    *http.Server
}

// NewServer constructs and wires up the HTTP server.
// All routes except GET /healthz require Authorization: Bearer <apiKey>.
func NewServer(addr, apiKey string, client *pubsub.Client) *Server {
	s := &Server{
		client: client,
		pb:     newPullBuffer(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("POST /topics", s.handleCreateTopic)
	mux.HandleFunc("GET /topics", s.handleListTopics)
	mux.HandleFunc("DELETE /topics/{id}", s.handleDeleteTopic)
	mux.HandleFunc("POST /topics/{id}/publish", s.handlePublish)
	mux.HandleFunc("POST /subscriptions", s.handleCreateSubscription)
	mux.HandleFunc("DELETE /subscriptions/{id}", s.handleDeleteSubscription)
	mux.HandleFunc("POST /subscriptions/{id}/pull", s.handlePull)
	mux.HandleFunc("GET /topics/{id}/dlq", s.handleGetDLQ)
	mux.HandleFunc("DELETE /topics/{id}/dlq", s.handleDrainDLQ)

	s.srv = &http.Server{
		Addr:         addr,
		Handler:      requireAPIKey(apiKey, mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	return s
}

func (s *Server) ListenAndServe() error {
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
