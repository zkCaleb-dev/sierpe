package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zkCaleb-dev/sierpe/internal/source"
)

// stubRPC answers getLedgers with a window error and getLatestLedger as
// configured, so classification paths can be pinned down.
func stubRPC(t *testing.T, latest uint32, tipProbeFails bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch req.Method {
		case "getLedgers":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"start ledger must be between the oldest and latest ledgers"}}`))
		case "getLatestLedger":
			if tipProbeFails {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			resp := map[string]any{
				"jsonrpc": "2.0", "id": 1,
				"result": map[string]any{"sequence": latest},
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
}

func TestWindowErrorBeyondTipWaits(t *testing.T) {
	srv := stubRPC(t, 100, false)
	defer srv.Close()
	c, _ := New([]string{srv.URL})

	_, err := c.GetLedger(context.Background(), 150)
	if !errors.Is(err, source.ErrNotYetAvailable) {
		t.Errorf("err = %v, want ErrNotYetAvailable", err)
	}
}

func TestWindowErrorBelowRetentionFailsFast(t *testing.T) {
	srv := stubRPC(t, 100_000, false)
	defer srv.Close()
	c, _ := New([]string{srv.URL})

	_, err := c.GetLedger(context.Background(), 5)
	if !errors.Is(err, source.ErrBelowRetention) {
		t.Errorf("err = %v, want ErrBelowRetention", err)
	}
	if _, err := c.GetLedgerBatch(context.Background(), 5, 10); !errors.Is(err, source.ErrBelowRetention) {
		t.Errorf("batch err = %v, want ErrBelowRetention", err)
	}
}

// Regression: a window error whose tip probe ALSO fails must classify as
// transient, never below-retention — the loop treats below-retention as
// fatal, and a flaky probe under load must not kill the process.
func TestWindowErrorWithFailingProbeIsTransient(t *testing.T) {
	srv := stubRPC(t, 0, true)
	defer srv.Close()
	c, _ := New([]string{srv.URL})

	_, err := c.GetLedger(context.Background(), 150)
	if err == nil {
		t.Fatal("expected an error")
	}
	if errors.Is(err, source.ErrBelowRetention) || errors.Is(err, source.ErrNotYetAvailable) {
		t.Errorf("err = %v, must stay unclassified (transient)", err)
	}

	_, err = c.GetLedgerBatch(context.Background(), 150, 10)
	if errors.Is(err, source.ErrBelowRetention) || errors.Is(err, source.ErrNotYetAvailable) {
		t.Errorf("batch err = %v, must stay unclassified (transient)", err)
	}
}
