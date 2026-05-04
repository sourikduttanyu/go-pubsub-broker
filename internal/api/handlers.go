package api

import (
	"encoding/json"
	"net/http"

	"github.com/sourik/go-pubsub-broker/internal/broker"
)

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req createTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required", "BAD_REQUEST")
		return
	}
	if err := s.client.CreateTopic(req.Name); err != nil {
		writeError(w, http.StatusConflict, err.Error(), "CONFLICT")
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	topics := s.client.Topics()
	if topics == nil {
		topics = []string{}
	}
	writeJSON(w, http.StatusOK, topics)
}

func (s *Server) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	if err := s.client.DeleteTopic(r.PathValue("id")); err != nil {
		writeError(w, http.StatusNotFound, err.Error(), "NOT_FOUND")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("id")
	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
		return
	}
	id, err := s.client.Publish(topic, req.Data, req.Attributes)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error(), "NOT_FOUND")
		return
	}
	writeJSON(w, http.StatusOK, publishResponse{MessageID: string(id)})
}

func (s *Server) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req createSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Topic == "" || req.SubID == "" {
		writeError(w, http.StatusBadRequest, "topic and sub_id are required", "BAD_REQUEST")
		return
	}
	subID := broker.SubscriptionID(req.SubID)
	s.pb.add(subID, req.Topic)
	if err := s.client.Subscribe(req.Topic, subID, s.pb.handler(subID)); err != nil {
		s.pb.remove(subID)
		writeError(w, http.StatusConflict, err.Error(), "CONFLICT")
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	subID := broker.SubscriptionID(r.PathValue("id"))
	topic, ok := s.pb.topic(subID)
	if !ok {
		writeError(w, http.StatusNotFound, "subscription not found", "NOT_FOUND")
		return
	}
	if err := s.client.Unsubscribe(topic, subID); err != nil {
		writeError(w, http.StatusNotFound, err.Error(), "NOT_FOUND")
		return
	}
	s.pb.remove(subID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePull(w http.ResponseWriter, r *http.Request) {
	subID := broker.SubscriptionID(r.PathValue("id"))
	if !s.pb.exists(subID) {
		writeError(w, http.StatusNotFound, "subscription not found", "NOT_FOUND")
		return
	}
	var req struct {
		MaxMessages int `json:"max_messages"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	max := req.MaxMessages
	if max <= 0 {
		max = 10
	}
	msgs := s.pb.pull(subID, max)
	out := make([]pulledMessage, len(msgs))
	for i, m := range msgs {
		out[i] = pulledMessage{
			MessageID:   string(m.ID),
			Topic:       m.Topic,
			Data:        m.Data,
			Attributes:  m.Attributes,
			PublishedAt: m.PublishedAt,
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetDLQ(w http.ResponseWriter, r *http.Request) {
	entries, err := s.client.DLQEntries(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error(), "NOT_FOUND")
		return
	}
	out := make([]dlqEntryResponse, len(entries))
	for i, e := range entries {
		out[i] = dlqEntryResponse{
			MessageID:    string(e.Message.ID),
			Topic:        e.Message.Topic,
			Data:         e.Message.Data,
			Attributes:   e.Message.Attributes,
			Subscription: string(e.Subscription),
			FailedAt:     e.FailedAt,
			Attempts:     e.Attempts,
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleDrainDLQ(w http.ResponseWriter, r *http.Request) {
	entries, err := s.client.DLQDrain(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error(), "NOT_FOUND")
		return
	}
	out := make([]dlqEntryResponse, len(entries))
	for i, e := range entries {
		out[i] = dlqEntryResponse{
			MessageID:    string(e.Message.ID),
			Topic:        e.Message.Topic,
			Data:         e.Message.Data,
			Attributes:   e.Message.Attributes,
			Subscription: string(e.Subscription),
			FailedAt:     e.FailedAt,
			Attempts:     e.Attempts,
		}
	}
	writeJSON(w, http.StatusOK, out)
}
