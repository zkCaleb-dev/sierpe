// Package ui serves the embedded management page: one self-contained HTML
// file, no build system, no external assets — the binary stays the whole
// product (RabbitMQ-management style, sierpe-web aesthetic).
//
// The page is a pure consumer of the public and admin HTTP APIs: it holds
// no state and adds no surface of its own. Reads work without credentials
// (matching the open-reads access model); mutations prompt for the admin
// token and send it as the same bearer header curl would.
package ui

import (
	_ "embed"
	"net/http"
)

//go:embed index.html
var indexHTML []byte

// Register mounts the UI at the root path. "GET /{$}" matches exactly "/",
// so it never shadows the API routes.
func Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
}
