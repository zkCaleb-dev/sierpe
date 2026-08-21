package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

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

// Regression: a window error AT the reported tip must classify as
// not-yet-available even when seq <= latest. getLatestLedger can announce a
// ledger before getLedgers serves it; treating that race as below-retention
// killed the live loop at the tip twice during M1/M2 smoke tests.
func TestWindowErrorAtTipWaits(t *testing.T) {
	// getLedgers refuses the request while getLatestLedger claims the very
	// same sequence exists.
	srv := stubRPC(t, 150, false)
	defer srv.Close()
	c, _ := New([]string{srv.URL})

	for _, seq := range []uint32{150, 149, 150 - tipAmbiguitySlack} {
		if _, err := c.GetLedger(context.Background(), seq); !errors.Is(err, source.ErrNotYetAvailable) {
			t.Errorf("GetLedger(%d) = %v, want ErrNotYetAvailable (tip race is a wait, not a fatal)", seq, err)
		}
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

// sizedLedgerRPC answers getLedgers with a body whose size grows with the
// requested pagination limit, mimicking the real thing: ledger meta size is
// data-dependent, so the same batch size is fine over a quiet range and
// enormous over a busy one.
func sizedLedgerRPC(t *testing.T, servableLimit int, calls *[]int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params struct {
				StartLedger uint32 `json:"startLedger"`
				Pagination  struct {
					Limit int `json:"limit"`
				} `json:"pagination"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		limit := req.Params.Pagination.Limit
		*calls = append(*calls, limit)
		if limit > servableLimit {
			// Far more than the test client's cap, as a WELL-FORMED body:
			// the point is that the client, not the server, is what breaks
			// the JSON when it stops reading.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"ledgers":[{"sequence":1,"metadataXdr":"`))
			blob := make([]byte, 4096)
			for i := range blob {
				blob[i] = 'A'
			}
			for written := 0; written < 200_000; written += len(blob) {
				_, _ = w.Write(blob)
			}
			_, _ = w.Write([]byte(`"}],"latestLedger":100,"oldestLedger":1}}`))
			return
		}
		ledgers := make([]map[string]any, 0, limit)
		for i := 0; i < limit; i++ {
			seq := req.Params.StartLedger + uint32(i)
			ledgers = append(ledgers, map[string]any{
				"sequence":    seq,
				"metadataXdr": ledgerMetaBase64(t, seq),
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0", "id": 1,
			"result": map[string]any{
				"ledgers": ledgers, "latestLedger": 100, "oldestLedger": 1,
			},
		})
	}))
}

func ledgerMetaBase64(t *testing.T, seq uint32) string {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(seq),
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(1_700_000_000)},
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{V: 1, V1TxSet: &xdr.TransactionSetV1{}},
		},
	}
	out, err := xdr.MarshalBase64(lcm)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	return out
}

// An answer bigger than the client's cap must be reported as exactly that.
// Reading up to the cap and stopping hands the decoder a perfectly
// truncated document, so the failure surfaces as "unexpected end of JSON
// input" — the client blaming the server for its own cut. That error is
// also permanent: the same request produces the same oversized reply
// forever, so a backfill retrying it never advances again (observed on the
// live deployment: 200 ledgers of a busy testnet range weigh ~151 MB).
func TestOversizedResponseIsNamed(t *testing.T) {
	var calls []int
	srv := sizedLedgerRPC(t, 0, &calls) // nothing is small enough
	defer srv.Close()
	c, _ := New([]string{srv.URL})
	c.bodyCap = 64 << 10

	_, err := c.GetLedgerBatch(context.Background(), 10, 200)
	if !errors.Is(err, errResponseTooLarge) {
		t.Fatalf("err = %v, want errResponseTooLarge", err)
	}
	if strings.Contains(err.Error(), "JSON") {
		t.Errorf("err = %v: the client must not blame the JSON for its own truncation", err)
	}
	// It shrank all the way down before giving up, instead of hammering
	// the same oversized request.
	if len(calls) < 2 || calls[len(calls)-1] != 1 {
		t.Errorf("attempted limits = %v, want a halving sequence ending at 1", calls)
	}
}

// The shrink exists to keep the walk moving: a range too heavy for 200
// ledgers at a time is still perfectly servable in smaller bites.
func TestOversizedResponseShrinksUntilItFits(t *testing.T) {
	var calls []int
	srv := sizedLedgerRPC(t, 12, &calls)
	defer srv.Close()
	c, _ := New([]string{srv.URL})
	c.bodyCap = 64 << 10

	batch, err := c.GetLedgerBatch(context.Background(), 10, 200)
	if err != nil {
		t.Fatalf("GetLedgerBatch() error = %v", err)
	}
	if len(batch) == 0 {
		t.Fatal("no progress: the caller would stall exactly as before")
	}
	if got := len(calls); got < 2 {
		t.Errorf("attempted limits = %v, want the oversized try then a smaller one", calls)
	}
	if last := calls[len(calls)-1]; last > 12 {
		t.Errorf("settled on limit %d, which the endpoint cannot serve", last)
	}
}
