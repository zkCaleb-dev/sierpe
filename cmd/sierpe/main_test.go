package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestProbeAcceptsOnly200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	if err := probe(srv.URL + "/health"); err != nil {
		t.Errorf("200 must pass: %v", err)
	}
	if err := probe(srv.URL + "/ready"); err == nil {
		t.Error("503 must fail the probe")
	}
	srv.Close()
	if err := probe(srv.URL + "/health"); err == nil {
		t.Error("a dead listener must fail the probe")
	}
}

func TestPoolerHintRecognizesPreparedStatementFailures(t *testing.T) {
	for _, code := range []string{"26000", "42P05"} {
		err := fmt.Errorf("store: migrate: %w", &pgconn.PgError{Code: code, Message: "prepared statement does not exist"})
		if poolerHint(err) == "" {
			t.Errorf("SQLSTATE %s must produce the pooler hint", code)
		}
	}
	if poolerHint(errors.New("dial tcp: connection refused")) != "" {
		t.Error("an unrelated error must not get the pooler hint")
	}
	if poolerHint(fmt.Errorf("x: %w", &pgconn.PgError{Code: "28P01"})) != "" {
		t.Error("a bad password is not a pooler problem")
	}
}
