// Package httpauth gates the whole http surface behind Basic Auth when the
// operator configures it: the RabbitMQ-management model for instances that
// expose a public domain. It sits at the presentation edge — nothing inside
// the pipeline knows it exists — and holds no state: every request is
// checked against boot configuration.
package httpauth

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
)

// probePaths are reachable without credentials: orchestrator health checks
// cannot carry them, and neither path serves data.
var probePaths = map[string]bool{
	"/health": true,
	"/ready":  true,
}

// Wrap requires Basic credentials on every path except the orchestrator
// probes. Comparison is constant-time over fixed-size digests so neither
// user nor password length leaks.
func Wrap(next http.Handler, user, password string) http.Handler {
	wantUser := sha256.Sum256([]byte(user))
	wantPass := sha256.Sum256([]byte(password))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if probePaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		gotUser, gotPass, ok := r.BasicAuth()
		if ok {
			u := sha256.Sum256([]byte(gotUser))
			p := sha256.Sum256([]byte(gotPass))
			if subtle.ConstantTimeCompare(u[:], wantUser[:])&subtle.ConstantTimeCompare(p[:], wantPass[:]) == 1 {
				next.ServeHTTP(w, r)
				return
			}
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="sierpe", charset="UTF-8"`)
		http.Error(w, "credentials required", http.StatusUnauthorized)
	})
}
