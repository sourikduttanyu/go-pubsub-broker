package api

import (
	"crypto/hmac"
	"net/http"
	"strings"
)

// requireAPIKey gates all routes except GET /healthz and GET /metrics.
// If limiter is non-nil, each API key is additionally rate-limited.
func requireAPIKey(key string, limiter *rateLimiter, next http.Handler) http.Handler {
	exempt := map[string]bool{
		"/healthz": true,
		"/metrics": true,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exempt[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(auth, "Bearer ")
		if !ok || !hmac.Equal([]byte(token), []byte(key)) {
			writeError(w, http.StatusUnauthorized, "invalid or missing API key", "UNAUTHORIZED")
			return
		}
		if limiter != nil && !limiter.allow(token) {
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded", "RATE_LIMITED")
			return
		}
		next.ServeHTTP(w, r)
	})
}
