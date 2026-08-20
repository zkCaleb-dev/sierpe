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

type fakeTransfersReader struct {
	transfers []store.Transfer
	hasMore   bool
	queries   []store.TransferQuery
}

func (f *fakeTransfersReader) QueryTransfers(_ context.Context, _ string, q store.TransferQuery) ([]store.Transfer, bool, error) {
	f.queries = append(f.queries, q)
	return f.transfers, f.hasMore, nil
}

func newTestAPIWithTransfers(ev *fakeEventReader, cr *fakeContractReader, tr *fakeTransfersReader) *httptest.Server {
	mux := http.NewServeMux()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewServer("testnet", ev, cr, log)
	server.Register(mux)
	server.RegisterTransfers(mux, tr)
	return httptest.NewServer(mux)
}

func sampleTransfer(id string, ledger uint32) store.Transfer {
	return store.Transfer{
		ID: id, ContractID: registered, LedgerSequence: ledger,
		ClosedAt: time.Unix(1_700_000_000, 0).UTC(), TxHash: "beef",
		TxIndex: 1, OpIndex: 0, EventIndex: 0,
		TransferType: store.TransferTypeTransfer,
		FromAddress:  "GFROM", ToAddress: "GTO",
		Amount: "5000", Asset: "native",
	}
}

func TestTransfersHappyPath(t *testing.T) {
	tr := &fakeTransfersReader{
		transfers: []store.Transfer{sampleTransfer("0000000000000000100-0000000000", 5500)},
		hasMore:   true,
	}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTransfers(ev, defaultContractReader(), tr)
	defer srv.Close()

	var resp transfersResponse
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/transfers?limit=1", &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if len(resp.Transfers) != 1 {
		t.Fatalf("transfers = %+v", resp.Transfers)
	}
	got := resp.Transfers[0]
	if got.TransferType != "transfer" || got.From != "GFROM" || got.To != "GTO" ||
		got.Amount != "5000" || got.Asset != "native" {
		t.Errorf("record = %+v", got)
	}
	if resp.ScanStatus != scanHasMore {
		t.Errorf("scanStatus = %s, want HAS_MORE", resp.ScanStatus)
	}
	if resp.Coverage.IndexedToLedger != 6000 || resp.LatestLedger != 6000 {
		t.Errorf("coverage = %+v latest = %d", resp.Coverage, resp.LatestLedger)
	}

	// The cursor resumes the walk with the same limit and bounds.
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/transfers?cursor="+resp.Cursor, &transfersResponse{}); code != 200 {
		t.Fatalf("cursor page status = %d", code)
	}
	q := tr.queries[1]
	if q.AfterID != "0000000000000000100-0000000000" || q.Limit != 1 {
		t.Errorf("cursor query = %+v", q)
	}
}

func TestTransfersFiltersReachTheStore(t *testing.T) {
	tr := &fakeTransfersReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTransfers(ev, defaultContractReader(), tr)
	defer srv.Close()

	url := srv.URL + "/v1/contracts/" + registered +
		"/transfers?from=GFROM&to=GTO&type=mint&startLedger=2000&endLedger=5000"
	if code := getJSON(t, url, &transfersResponse{}); code != 200 {
		t.Fatalf("status = %d", code)
	}
	q := tr.queries[0]
	if q.From != "GFROM" || q.To != "GTO" || q.TransferType != "mint" ||
		q.FromLedger != 2000 || q.ToLedger != 5000 {
		t.Errorf("query = %+v", q)
	}

	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/transfers?account=GANY", &transfersResponse{}); code != 200 {
		t.Fatalf("account status = %d", code)
	}
	if q := tr.queries[1]; q.Account != "GANY" || q.From != "" || q.To != "" {
		t.Errorf("account query = %+v", q)
	}
}

func TestTransfersValidation(t *testing.T) {
	tr := &fakeTransfersReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTransfers(ev, defaultContractReader(), tr)
	defer srv.Close()

	base := srv.URL + "/v1/contracts/" + registered + "/transfers"
	cases := map[string]string{
		"unknown type":        base + "?type=teleport",
		"bad limit":           base + "?limit=0",
		"bad startLedger":     base + "?startLedger=x",
		"inverted bounds":     base + "?startLedger=10&endLedger=5",
		"account plus from":   base + "?account=GANY&from=GFROM",
		"account plus to":     base + "?account=GANY&to=GTO",
		"cursor plus filters": base + "?cursor=abc&type=mint",
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

func TestTransfersCursorIsEndpointBound(t *testing.T) {
	tr := &fakeTransfersReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTransfers(ev, defaultContractReader(), tr)
	defer srv.Close()

	eventsCursor := encodeCursor("testnet", store.EventQuery{ContractID: registered, Limit: 10}, "")
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/transfers?cursor="+eventsCursor, nil); code != 400 {
		t.Errorf("transfers with events cursor: status = %d, want 400", code)
	}

	stateCursor := encodeStateCursor("testnet", kindSnapshot, store.StateQuery{ContractID: registered, Limit: 10})
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/transfers?cursor="+stateCursor, nil); code != 400 {
		t.Errorf("transfers with state cursor: status = %d, want 400", code)
	}
}

func TestTransfersCursorRoundTrip(t *testing.T) {
	q := store.TransferQuery{
		ContractID: registered, Account: "GANY", TransferType: "burn",
		FromLedger: 100, ToLedger: 200, Limit: 7, AfterID: "0000000000000000100-0000000003",
	}
	decoded, err := decodeTransfersCursor("testnet", registered, encodeTransfersCursor("testnet", q))
	if err != nil {
		t.Fatalf("round trip error = %v", err)
	}
	if decoded != q {
		t.Errorf("decoded = %+v, want %+v", decoded, q)
	}

	if _, err := decodeTransfersCursor("testnet", "CDIFFERENT", encodeTransfersCursor("testnet", q)); err == nil {
		t.Error("cursor must reject a different contract")
	}
}

func TestTransfersUnknownContractIs404(t *testing.T) {
	tr := &fakeTransfersReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTransfers(ev, defaultContractReader(), tr)
	defer srv.Close()

	other := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	if code := getJSON(t, srv.URL+"/v1/contracts/"+other+"/transfers", nil); code != 404 {
		t.Errorf("status = %d, want 404", code)
	}
}
