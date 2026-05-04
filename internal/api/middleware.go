package api

import (
	"crypto/hmac"
	"net/http"
	"strings"
)

func requireAPIKey(key string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(auth, "Bearer ")
		if !ok || !hmac.Equal([]byte(token), []byte(key)) {
			writeError(w, http.StatusUnauthorized, "invalid or missing API key", "UNAUTHORIZED")
			return
		}
		next.ServeHTTP(w, r)
	})
}
