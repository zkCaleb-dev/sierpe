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
	"strings"
)

// probePaths are reachable without credentials: orchestrator health checks
// cannot carry them, and neither path serves data.
var probePaths = map[string]bool{
	"/health": true,
	"/ready":  true,
}

// Wrap requires credentials on every path except the orchestrator probes:
// either the Basic user and password, or the admin bearer token. The
// bearer alternative exists because Basic and Bearer share the one
// Authorization header — an admin mutation carrying its bearer token
// physically cannot also carry the Basic credentials, and the admin token
// is the higher-privilege credential anyway (its own handler validates it
// again). Comparisons are constant-time over fixed-size digests so no
// credential length leaks.
func Wrap(next http.Handler, user, password, adminToken string) http.Handler {
	wantUser := sha256.Sum256([]byte(user))
	wantPass := sha256.Sum256([]byte(password))
	wantAdmin := sha256.Sum256([]byte(adminToken))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if probePaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		if gotUser, gotPass, ok := r.BasicAuth(); ok {
			u := sha256.Sum256([]byte(gotUser))
			p := sha256.Sum256([]byte(gotPass))
			if subtle.ConstantTimeCompare(u[:], wantUser[:])&subtle.ConstantTimeCompare(p[:], wantPass[:]) == 1 {
				next.ServeHTTP(w, r)
				return
			}
		}
		if bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok && adminToken != "" {
			b := sha256.Sum256([]byte(bearer))
			if subtle.ConstantTimeCompare(b[:], wantAdmin[:]) == 1 {
				next.ServeHTTP(w, r)
				return
			}
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="sierpe", charset="UTF-8"`)
		http.Error(w, "credentials required", http.StatusUnauthorized)
	})
}
