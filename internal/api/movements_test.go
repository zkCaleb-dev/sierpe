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

type fakeMovementReader struct {
	movements []store.Movement
	hasMore   bool
	queries   []store.MovementQuery
}

func (f *fakeMovementReader) QueryMovements(_ context.Context, _ string, q store.MovementQuery) ([]store.Movement, bool, error) {
	f.queries = append(f.queries, q)
	return f.movements, f.hasMore, nil
}

func newTestAPIWithMovements(ev *fakeEventReader, cr *fakeContractReader, mr *fakeMovementReader) *httptest.Server {
	mux := http.NewServeMux()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewServer("testnet", ev, cr, log)
	server.Register(mux)
	server.RegisterMovements(mux, mr)
	return httptest.NewServer(mux)
}

func sampleMovement(id, role string) store.Movement {
	return store.Movement{
		ContractID:      registered,
		TransferID:      id,
		Role:            role,
		TokenContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		TransferType:    store.TransferTypeTransfer,
		Counterparty:    "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H",
		Amount:          "690000000",
		LedgerSequence:  5500,
		ClosedAt:        time.Unix(1_700_000_000, 0).UTC(),
	}
}

func TestMovementsHappyPath(t *testing.T) {
	mr := &fakeMovementReader{
		movements: []store.Movement{sampleMovement("0000000000000000100-0000000000", store.RoleRecipient)},
		hasMore:   true,
	}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	cr := defaultContractReader()
	cr.contract.Kinds = append(cr.contract.Kinds, store.KindMovements)
	cr.backfill.CoveredKinds = append(cr.backfill.CoveredKinds, store.KindMovements)
	srv := newTestAPIWithMovements(ev, cr, mr)
	defer srv.Close()

	var resp movementsResponse
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/movements?limit=1", &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if len(resp.Movements) != 1 {
		t.Fatalf("movements = %+v", resp.Movements)
	}
	m := resp.Movements[0]
	if m.Role != store.RoleRecipient || m.Amount != "690000000" || m.TokenContractID == "" {
		t.Errorf("record = %+v", m)
	}
	if resp.ScanStatus != scanHasMore {
		t.Errorf("scanStatus = %s", resp.ScanStatus)
	}
	if resp.Coverage.Kind != store.KindMovements || !resp.Coverage.KindDerived {
		t.Errorf("coverage = %+v", resp.Coverage)
	}
	if resp.Note == "" {
		t.Error("the response must state that movements are not a balance")
	}

	// The cursor resumes the walk with the same limit.
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/movements?cursor="+resp.Cursor, &movementsResponse{}); code != 200 {
		t.Fatalf("cursor page status = %d", code)
	}
	if q := mr.queries[1]; q.AfterID != "0000000000000000100-0000000000" || q.Limit != 1 {
		t.Errorf("cursor query = %+v", q)
	}
}

func TestMovementsFiltersReachTheStore(t *testing.T) {
	mr := &fakeMovementReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithMovements(ev, defaultContractReader(), mr)
	defer srv.Close()

	token := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	url := srv.URL + "/v1/contracts/" + registered +
		"/movements?role=recipient&token=" + token + "&type=mint&startLedger=2000&endLedger=5000"
	if code := getJSON(t, url, &movementsResponse{}); code != 200 {
		t.Fatalf("status = %d", code)
	}
	q := mr.queries[0]
	if q.Role != store.RoleRecipient || q.Token != token || q.Type != store.TransferTypeMint ||
		q.FromLedger != 2000 || q.ToLedger != 5000 {
		t.Errorf("query = %+v", q)
	}
}

func TestMovementsValidation(t *testing.T) {
	mr := &fakeMovementReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithMovements(ev, defaultContractReader(), mr)
	defer srv.Close()

	base := srv.URL + "/v1/contracts/" + registered + "/movements"
	cases := map[string]string{
		"bad role":                     base + "?role=beneficiary",
		"bad type":                     base + "?type=teleport",
		"asset string, not a contract": base + "?token=USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		"bad limit":                    base + "?limit=0",
		"inverted bounds":              base + "?startLedger=10&endLedger=5",
		"cursor plus filters":          base + "?cursor=abc&role=sender",
	}
	for name, url := range cases {
		t.Run(name, func(t *testing.T) {
			if code := getJSON(t, url, nil); code != 400 {
				t.Errorf("status = %d, want 400", code)
			}
		})
	}
	if len(mr.queries) != 0 {
		t.Error("invalid requests must never reach the store")
	}
}

func TestMovementsCursorIsEndpointBound(t *testing.T) {
	mr := &fakeMovementReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithMovements(ev, defaultContractReader(), mr)
	defer srv.Close()

	foreign := encodeTransfersCursor("testnet", store.TransferQuery{ContractID: registered, Limit: 10})
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/movements?cursor="+foreign, nil); code != 400 {
		t.Errorf("a transfers cursor must not work here: status = %d", code)
	}
}

func TestMovementsCursorRoundTrip(t *testing.T) {
	q := store.MovementQuery{
		ContractID: registered, Role: store.RoleSender, Token: "CTOKEN", Type: store.TransferTypeBurn,
		FromLedger: 100, ToLedger: 200, Limit: 7, AfterID: "0000000000000000100-0000000003",
	}
	decoded, err := decodeMovementsCursor("testnet", registered, encodeMovementsCursor("testnet", q))
	if err != nil {
		t.Fatalf("round trip error = %v", err)
	}
	if decoded != q {
		t.Errorf("decoded = %+v, want %+v", decoded, q)
	}
}

// The row key is (transfer_id, role): a cursor that only carries the id
// resumes past every row sharing it, so a page boundary inside a self
// transfer drops one of its two rows permanently.
func TestMovementsCursorCarriesTheRole(t *testing.T) {
	id := "0000000000000000100-0000000000"
	mr := &fakeMovementReader{
		movements: []store.Movement{sampleMovement(id, store.RoleSender)},
		hasMore:   true,
	}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithMovements(ev, defaultContractReader(), mr)
	defer srv.Close()

	var resp movementsResponse
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/movements?limit=1", &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	mr.movements = []store.Movement{sampleMovement(id, store.RoleRecipient)}
	mr.hasMore = false
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/movements?cursor="+resp.Cursor, &movementsResponse{}); code != 200 {
		t.Fatalf("cursor page status = %d", code)
	}
	q := mr.queries[1]
	if q.AfterID != id || q.AfterRole != store.RoleSender {
		t.Errorf("resume position = (%q, %q), want the full row key", q.AfterID, q.AfterRole)
	}
}

// An empty page for a kind the registration never derived is not a scan
// that completed: nothing was ever there to scan (rule 7).
func TestMovementsNeverReportCompleteForAnUnderivedKind(t *testing.T) {
	mr := &fakeMovementReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	// defaultContractReader does not derive movements.
	srv := newTestAPIWithMovements(ev, defaultContractReader(), mr)
	defer srv.Close()

	var resp movementsResponse
	url := srv.URL + "/v1/contracts/" + registered + "/movements?startLedger=1&endLedger=5000"
	if code := getJSON(t, url, &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if resp.Coverage.KindDerived {
		t.Fatal("fixture drifted: this contract must not derive movements")
	}
	if resp.ScanStatus == scanComplete {
		t.Errorf("scanStatus = %s: an empty page from a kind that was never derived is not a completed scan", resp.ScanStatus)
	}
}
