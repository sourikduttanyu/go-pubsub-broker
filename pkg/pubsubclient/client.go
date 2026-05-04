// Package pubsubclient is an HTTP client for the go-pubsub-broker REST API.
// It mirrors the shape of pkg/pubsub but speaks over the wire instead of in-process.
package pubsubclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

// Config holds connection parameters for the HTTP client.
type Config struct {
	BaseURL    string       // e.g. "http://localhost:8080"
	APIKey     string       // value sent as "Authorization: Bearer <key>"
	HTTPClient *http.Client // optional; defaults to a 30s-timeout client
}

// Client is an HTTP client for the broker REST API.
type Client struct {
	cfg  Config
	http *http.Client
}

// New creates a Client. Returns an error if BaseURL or APIKey are empty.
func New(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("BaseURL is required")
	}
	if cfg.APIKey == "" {
		return nil, errors.New("APIKey is required")
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{cfg: cfg, http: hc}, nil
}

// do executes an authenticated HTTP request. body may be nil.
func (c *Client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var br io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		br = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.BaseURL+path, br)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.http.Do(req)
}

// decode closes the response body and decodes JSON into dst.
// On 4xx/5xx it returns an *APIError. dst may be nil (e.g. 204 No Content).
func (c *Client) decode(resp *http.Response, dst any) error {
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		return &APIError{StatusCode: resp.StatusCode, Message: e.Error, Code: e.Code}
	}
	if dst == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

func (c *Client) CreateTopic(ctx context.Context, name string) error {
	resp, err := c.do(ctx, http.MethodPost, "/topics", map[string]string{"name": name})
	if err != nil {
		return err
	}
	return c.decode(resp, nil)
}

func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	resp, err := c.do(ctx, http.MethodDelete, "/topics/"+name, nil)
	if err != nil {
		return err
	}
	return c.decode(resp, nil)
}

func (c *Client) Topics(ctx context.Context) ([]string, error) {
	resp, err := c.do(ctx, http.MethodGet, "/topics", nil)
	if err != nil {
		return nil, err
	}
	var out []string
	return out, c.decode(resp, &out)
}

func (c *Client) Publish(ctx context.Context, topic string, data []byte, attrs map[string]string) (string, error) {
	resp, err := c.do(ctx, http.MethodPost, "/topics/"+topic+"/publish", map[string]any{
		"data":       data,
		"attributes": attrs,
	})
	if err != nil {
		return "", err
	}
	var out struct {
		MessageID string `json:"message_id"`
	}
	return out.MessageID, c.decode(resp, &out)
}

func (c *Client) CreateSubscription(ctx context.Context, topic, subID string) error {
	resp, err := c.do(ctx, http.MethodPost, "/subscriptions", map[string]string{
		"topic":  topic,
		"sub_id": subID,
	})
	if err != nil {
		return err
	}
	return c.decode(resp, nil)
}

func (c *Client) DeleteSubscription(ctx context.Context, subID string) error {
	resp, err := c.do(ctx, http.MethodDelete, "/subscriptions/"+subID, nil)
	if err != nil {
		return err
	}
	return c.decode(resp, nil)
}

func (c *Client) Pull(ctx context.Context, subID string, maxMessages int) ([]PulledMessage, error) {
	resp, err := c.do(ctx, http.MethodPost, "/subscriptions/"+subID+"/pull",
		map[string]int{"max_messages": maxMessages})
	if err != nil {
		return nil, err
	}
	var out []PulledMessage
	return out, c.decode(resp, &out)
}

func (c *Client) DLQEntries(ctx context.Context, topic string) ([]DLQEntry, error) {
	resp, err := c.do(ctx, http.MethodGet, "/topics/"+topic+"/dlq", nil)
	if err != nil {
		return nil, err
	}
	var out []DLQEntry
	return out, c.decode(resp, &out)
}

func (c *Client) DLQDrain(ctx context.Context, topic string) ([]DLQEntry, error) {
	resp, err := c.do(ctx, http.MethodDelete, "/topics/"+topic+"/dlq", nil)
	if err != nil {
		return nil, err
	}
	var out []DLQEntry
	return out, c.decode(resp, &out)
}
