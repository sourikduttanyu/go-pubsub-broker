package api

import (
	"encoding/json"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sourik/go-pubsub-broker/internal/broker"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// wsConn is a single WebSocket connection's send queue.
type wsConn struct {
	send chan []byte
}

// wsHub fans messages out to all connected WebSocket clients for one topic.
type wsHub struct {
	mu      sync.Mutex
	clients map[*wsConn]struct{}
}

func newHub() *wsHub {
	return &wsHub{clients: make(map[*wsConn]struct{})}
}

func (h *wsHub) add() *wsConn {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := &wsConn{send: make(chan []byte, 256)}
	h.clients[c] = struct{}{}
	return c
}

// remove removes c and returns the remaining client count.
func (h *wsHub) remove(c *wsConn) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
	return len(h.clients)
}

func (h *wsHub) broadcast(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c.send <- data:
		default: // slow client — skip frame
		}
	}
}

func (h *wsHub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// getOrCreateHub returns the existing hub for topic, or creates one and
// registers a broker subscription that broadcasts into it.
func (s *Server) getOrCreateHub(topic string) (*wsHub, error) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if h, ok := s.wsHubs[topic]; ok {
		return h, nil
	}
	h := newHub()
	subID := broker.SubscriptionID("__ws_hub__" + topic)
	if err := s.client.Subscribe(topic, subID, func(a *broker.DeliveryAttempt) {
		data, _ := json.Marshal(map[string]any{
			"id":           string(a.Message.ID),
			"topic":        a.Message.Topic,
			"data":         a.Message.Data,
			"attributes":   a.Message.Attributes,
			"published_at": a.Message.PublishedAt,
		})
		h.broadcast(data)
		a.Token.Ack()
	}); err != nil {
		return nil, err
	}
	s.wsHubs[topic] = h
	return h, nil
}

// maybeRemoveHub removes the hub for topic if it has no remaining clients.
func (s *Server) maybeRemoveHub(topic string, h *wsHub) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if h.count() > 0 {
		return
	}
	delete(s.wsHubs, topic)
	_ = s.client.Unsubscribe(topic, broker.SubscriptionID("__ws_hub__"+topic))
}

// hubCount returns the number of active WebSocket clients for topic.
func (s *Server) hubCount(topic string) int {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	h, ok := s.wsHubs[topic]
	if !ok {
		return 0
	}
	return h.count()
}

// handleWSSubscribe upgrades the connection to WebSocket and streams
// all messages published to the topic in real time.
//
// Route: GET /topics/{id}/subscribe
func (s *Server) handleWSSubscribe(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("id")

	// Pre-flight: return HTTP 404 before the upgrade so clients get a real status code.
	if !slices.Contains(s.client.Topics(), topic) {
		writeError(w, http.StatusNotFound, "topic not found", "NOT_FOUND")
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	hub, err := s.getOrCreateHub(topic)
	if err != nil {
		conn.WriteMessage( //nolint:errcheck
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, ""),
		)
		conn.Close()
		return
	}

	c := hub.add()
	metricWSSubscribers.WithLabelValues(topic).Inc()

	done := make(chan struct{})

	// Read loop — required for ping/pong and disconnect detection.
	go func() {
		defer close(done)
		conn.SetReadLimit(512)
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Write loop — delivers messages and sends keepalive pings.
	pingTicker := time.NewTicker(30 * time.Second)
	defer func() {
		pingTicker.Stop()
		conn.Close()
		if hub.remove(c) == 0 {
			s.maybeRemoveHub(topic, hub)
		}
		metricWSSubscribers.WithLabelValues(topic).Dec()
	}()

	for {
		select {
		case data := <-c.send:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		case <-pingTicker.C:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}
