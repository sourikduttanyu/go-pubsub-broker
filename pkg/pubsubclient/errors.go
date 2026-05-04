package pubsubclient

import "fmt"

// APIError is returned when the server responds with a 4xx or 5xx status.
type APIError struct {
	StatusCode int
	Message    string
	Code       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Message)
}
