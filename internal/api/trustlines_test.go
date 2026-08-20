package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

type fakeTrustlinesReader struct {
	entries []store.TrustlineEntry
	changes []store.TrustlineChange
	hasMore bool
	queries []store.TrustlineQuery
}

func (f *fakeTrustlinesReader) QueryTrustlineEntries(_ context.Context, _ string, q store.TrustlineQuery) ([]store.TrustlineEntry, bool, error) {
	f.queries = append(f.queries, q)
	return f.entries, f.hasMore, nil
}

func (f *fakeTrustlinesReader) QueryTrustlineChanges(_ context.Context, _ string, q store.TrustlineQuery) ([]store.TrustlineChange, bool, error) {
	f.queries = append(f.queries, q)
	return f.changes, f.hasMore, nil
}

func newTestAPIWithTrustlines(ev *fakeEventReader, cr *fakeContractReader, tr *fakeTrustlinesReader) *httptest.Server {
	mux := http.NewServeMux()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewServer("testnet", ev, cr, log)
	server.Register(mux)
	server.RegisterTrustlines(mux, tr)
	return httptest.NewServer(mux)
}

func i64p(v int64) *int64 { return &v }

func TestTrustlineSnapshotEndpoint(t *testing.T) {
	tr := &fakeTrustlinesReader{
		entries: []store.TrustlineEntry{{
			ContractID: registered, AccountID: "GALICE", Asset: "USDA:GISSUER",
			Balance: i64p(500), Limit: i64p(1000), Flags: 1,
			LastLedger: 5500, ClosedAt: time.Unix(1_700_000_000, 0).UTC(),
		}},
		hasMore: true,
	}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTrustlines(ev, defaultContractReader(), tr)
	defer srv.Close()

	var resp trustlineSnapshotResponse
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/trustlines?limit=1", &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if len(resp.Trustlines) != 1 {
		t.Fatalf("trustlines = %+v", resp.Trustlines)
	}
	got := resp.Trustlines[0]
	if got.AccountID != "GALICE" || got.Balance != "500" || got.Limit != "1000" || got.Flags != 1 {
		t.Errorf("record = %+v", got)
	}
	if resp.Cursor == "" {
		t.Fatal("hasMore snapshot must return a cursor")
	}

	// The cursor resumes the account walk with the same limit.
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/trustlines?cursor="+resp.Cursor, &trustlineSnapshotResponse{}); code != 200 {
		t.Fatalf("cursor page status = %d", code)
	}
	q := tr.queries[1]
	if q.AfterAccount != "GALICE" || q.Limit != 1 {
		t.Errorf("cursor query = %+v", q)
	}
}

func TestTrustlineHistoryEndpoint(t *testing.T) {
	tr := &fakeTrustlinesReader{
		changes: []store.TrustlineChange{{
			ID: "0000000000000000100-0000000000", ContractID: registered,
			AccountID: "GALICE", Asset: "USDA:GISSUER", LedgerSequence: 5500,
			ClosedAt: time.Unix(1_700_000_000, 0).UTC(), TxHash: "beef",
			ChangeType: "updated", PreBalance: i64p(1), PostBalance: i64p(2), Flags: 1,
		}},
	}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTrustlines(ev, defaultContractReader(), tr)
	defer srv.Close()

	var resp trustlineHistoryResponse
	url := srv.URL + "/v1/contracts/" + registered + "/trustlines/history?account=GALICE&startLedger=2000"
	if code := getJSON(t, url, &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if len(resp.Changes) != 1 || resp.Changes[0].PreBalance != "1" || resp.Changes[0].PostBalance != "2" {
		t.Errorf("changes = %+v", resp.Changes)
	}
	q := tr.queries[0]
	if q.AccountID != "GALICE" || q.FromLedger != 2000 {
		t.Errorf("query = %+v", q)
	}

	// Cursor pages keep the account filter and bounds.
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/trustlines/history?cursor="+resp.Cursor, &trustlineHistoryResponse{}); code != 200 {
		t.Fatalf("cursor page status = %d", code)
	}
	q = tr.queries[1]
	if q.AccountID != "GALICE" || q.FromLedger != 2000 || q.AfterID != "0000000000000000100-0000000000" {
		t.Errorf("cursor query = %+v", q)
	}
}

func TestTrustlineCursorsAreEndpointBound(t *testing.T) {
	tr := &fakeTrustlinesReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTrustlines(ev, defaultContractReader(), tr)
	defer srv.Close()

	historyCursor := encodeTrustlinesCursor("testnet", kindTrustlineHistory,
		store.TrustlineQuery{ContractID: registered, Limit: 10})
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/trustlines?cursor="+historyCursor, nil); code != 400 {
		t.Errorf("snapshot with history cursor: status = %d, want 400", code)
	}
	transfersCursor := encodeTransfersCursor("testnet", store.TransferQuery{ContractID: registered, Limit: 10})
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/trustlines/history?cursor="+transfersCursor, nil); code != 400 {
		t.Errorf("history with transfers cursor: status = %d, want 400", code)
	}
}

func TestTrustlineValidation(t *testing.T) {
	tr := &fakeTrustlinesReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTrustlines(ev, defaultContractReader(), tr)
	defer srv.Close()

	base := srv.URL + "/v1/contracts/" + registered
	cases := map[string]string{
		"bad limit":          base + "/trustlines?limit=0",
		"bad history start":  base + "/trustlines/history?startLedger=x",
		"inverted history":   base + "/trustlines/history?startLedger=10&endLedger=5",
		"cursor plus params": base + "/trustlines?cursor=abc&account=GALICE",
	}
	for name, url := range cases {
		t.Run(name, func(t *testing.T) {
			if code := getJSON(t, url, nil); code != 400 {
				t.Errorf("status = %d, want 400", code)
			}
		})
	}
	if len(tr.queries) != 0 {
		t.Error("invalid requests must never reach the store")
	}
}
