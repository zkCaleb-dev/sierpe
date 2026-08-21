package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

const registered = "CBF64DEOVQAXJFBSNGFEUT2AH4H7K5JBY3ZYJ5GVEINMNSDISWRG5N3F"

// validTopic is a base64 ScSymbol("transfer") for filter parameters.
const validTopic = "AAAADwAAAAh0cmFuc2Zlcg=="

// fakeEventReader serves canned events and records the queries it saw.
type fakeEventReader struct {
	events  []store.Event
	hasMore bool
	queries []store.EventQuery
	cursor  store.Cursor
}

func (f *fakeEventReader) QueryEvents(_ context.Context, _ string, q store.EventQuery) ([]store.Event, bool, error) {
	f.queries = append(f.queries, q)
	return f.events, f.hasMore, nil
}

func (f *fakeEventReader) LoadCursor(context.Context, string) (store.Cursor, error) {
	if f.cursor.Sequence == 0 {
		return store.Cursor{}, store.ErrNoCursor
	}
	return f.cursor, nil
}

// fakeContractReader serves one registered contract with backfill state.
type fakeContractReader struct {
	contract     store.Contract
	backfill     store.Backfill
	counts       map[string]int64
	stateEntries int64
}

func (f *fakeContractReader) GetContract(_ context.Context, _, contractID string) (store.Contract, error) {
	if contractID != f.contract.ContractID {
		return store.Contract{}, store.ErrNoContract
	}
	return f.contract, nil
}

func (f *fakeContractReader) ListContracts(context.Context, string) ([]store.Contract, error) {
	if f.contract.ContractID == "" {
		return nil, nil
	}
	return []store.Contract{f.contract}, nil
}

func (f *fakeContractReader) GetBackfill(context.Context, string, string) (store.Backfill, error) {
	if f.backfill.ContractID == "" {
		return store.Backfill{}, store.ErrNoBackfill
	}
	return f.backfill, nil
}

func (f *fakeContractReader) EventCountsByName(context.Context, string, string) (map[string]int64, error) {
	if f.counts == nil {
		return map[string]int64{}, nil
	}
	return f.counts, nil
}

func (f *fakeContractReader) CountStateEntries(context.Context, string, string) (int64, error) {
	return f.stateEntries, nil
}

func sampleEvent(id string, ledger uint32) store.Event {
	return store.Event{
		ID:             id,
		ContractID:     registered,
		LedgerSequence: ledger,
		ClosedAt:       time.Unix(1_700_000_000, 0).UTC(),
		TxHash:         "beef",
		TxIndex:        1,
		EventIndex:     0,
		EventName:      "transfer",
		Topics:         []string{validTopic},
		ValueXDR:       "AAAAAQ==",
		RawXDR:         "AAAAAQ==",
	}
}

func newTestAPI(ev *fakeEventReader, cr *fakeContractReader) *httptest.Server {
	mux := http.NewServeMux()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	NewServer("testnet", ev, cr, log).Register(mux)
	return httptest.NewServer(mux)
}

func defaultContractReader() *fakeContractReader {
	return &fakeContractReader{
		contract: store.Contract{
			Network: "testnet", ContractID: registered, Source: store.SourceAPI,
			Kinds:        []string{store.KindEvents, store.KindState, store.KindTransfers, store.KindTrustlines},
			RegisteredAt: time.Unix(1_700_000_000, 0),
		},
		backfill: store.Backfill{ContractID: registered, TargetFrom: 1, NextTo: 999, Done: true,
			CoveredKinds: []string{store.KindEvents, store.KindState, store.KindTransfers, store.KindTrustlines}},
	}
}

func getJSON(t *testing.T, url string, out any) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, out); err != nil {
			t.Fatalf("decode %s: %v (%s)", url, err, body)
		}
	}
	return resp.StatusCode
}

func TestEventsHappyPath(t *testing.T) {
	ev := &fakeEventReader{
		events: []store.Event{sampleEvent("0000000000000000100-0000000000", 5000)},
		cursor: store.Cursor{Sequence: 6000},
	}
	srv := newTestAPI(ev, defaultContractReader())
	defer srv.Close()

	var resp eventsResponse
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/events", &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if len(resp.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(resp.Events))
	}
	e := resp.Events[0]
	if e.Type != "contract" || !e.InSuccessfulContractCall || e.EventName != "transfer" {
		t.Errorf("record = %+v", e)
	}
	if e.LedgerClosedAt != "2023-11-14T22:13:20Z" {
		t.Errorf("closed at = %s (business clock must be RFC3339 UTC)", e.LedgerClosedAt)
	}
	// Unbounded end: indexed data stops at the live cursor, so the client
	// must know more may come.
	if resp.ScanStatus != scanWaitingForLedgers {
		t.Errorf("scanStatus = %s, want WAITING_FOR_LEDGERS", resp.ScanStatus)
	}
	if resp.Coverage.IndexedFromLedger != 1000 || resp.Coverage.IndexedToLedger != 6000 {
		t.Errorf("coverage = %+v", resp.Coverage)
	}
	if resp.LatestLedger != 6000 {
		t.Errorf("latestLedger = %d", resp.LatestLedger)
	}
	if resp.Cursor == "" {
		t.Error("cursor must always be returned")
	}
}

func TestEventsFiltersReachTheStore(t *testing.T) {
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPI(ev, defaultContractReader())
	defer srv.Close()

	url := srv.URL + "/v1/contracts/" + registered + "/events?topic0=" + validTopic +
		"&startLedger=2000&endLedger=3000&limit=7&type=contract"
	if code := getJSON(t, url, &eventsResponse{}); code != 200 {
		t.Fatalf("status = %d", code)
	}
	q := ev.queries[0]
	if q.Topics[0] == nil || *q.Topics[0] != validTopic || q.Topics[1] != nil {
		t.Errorf("topics = %+v", q.Topics)
	}
	if q.FromLedger != 2000 || q.ToLedger != 3000 || q.Limit != 7 {
		t.Errorf("query = %+v", q)
	}
}

func TestEventsCursorRoundTrip(t *testing.T) {
	events := []store.Event{sampleEvent("0000000000000000100-0000000000", 2500)}
	ev := &fakeEventReader{events: events, hasMore: true, cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPI(ev, defaultContractReader())
	defer srv.Close()

	var first eventsResponse
	url := srv.URL + "/v1/contracts/" + registered + "/events?topic0=" + validTopic +
		"&startLedger=2000&endLedger=3000&limit=1"
	if code := getJSON(t, url, &first); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if first.ScanStatus != scanHasMore {
		t.Errorf("scanStatus = %s, want HAS_MORE", first.ScanStatus)
	}

	// The next page carries only the cursor; the original query must survive
	// inside it.
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/events?cursor="+first.Cursor,
		&eventsResponse{}); code != 200 {
		t.Fatalf("cursor page status = %d", code)
	}
	q := ev.queries[1]
	if q.FromLedger != 2000 || q.ToLedger != 3000 || q.Limit != 1 {
		t.Errorf("cursor lost the original bounds: %+v", q)
	}
	if q.Topics[0] == nil || *q.Topics[0] != validTopic {
		t.Errorf("cursor lost the topic filter: %+v", q.Topics)
	}
	if q.AfterID != "0000000000000000100-0000000000" {
		t.Errorf("cursor resume position = %q", q.AfterID)
	}
}

func TestEventsCursorRejectsCombinedParams(t *testing.T) {
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPI(ev, defaultContractReader())
	defer srv.Close()

	cursor := encodeCursor("testnet", store.EventQuery{ContractID: registered, FromLedger: 1, Limit: 10}, "")
	url := srv.URL + "/v1/contracts/" + registered + "/events?cursor=" + cursor + "&startLedger=5"
	if code := getJSON(t, url, nil); code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400: cursor plus params is ambiguous", code)
	}
}

func TestEventsCursorRejectsForeignCursor(t *testing.T) {
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPI(ev, defaultContractReader())
	defer srv.Close()

	other := encodeCursor("mainnet", store.EventQuery{ContractID: registered, FromLedger: 1, Limit: 10}, "")
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/events?cursor="+other, nil); code != 400 {
		t.Errorf("status = %d, want 400: cursor minted for another network", code)
	}
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/events?cursor=not-a-cursor", nil); code != 400 {
		t.Errorf("status = %d, want 400: garbage cursor", code)
	}
}

func TestEventsValidation(t *testing.T) {
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPI(ev, defaultContractReader())
	defer srv.Close()

	base := srv.URL + "/v1/contracts/" + registered + "/events"
	cases := map[string]string{
		"bad topic":        base + "?topic0=!!!",
		"topic not scval":  base + "?topic0=bm90LXNjdmFs",
		"bad start":        base + "?startLedger=zero",
		"inverted range":   base + "?startLedger=100&endLedger=50",
		"limit too big":    base + "?limit=99999",
		"unsupported type": base + "?type=diagnostic",
	}
	for name, url := range cases {
		if code := getJSON(t, url, nil); code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, code)
		}
	}
	if len(ev.queries) != 0 {
		t.Error("invalid requests must never reach the store")
	}
}

func TestEventsUnknownContractIs404(t *testing.T) {
	srv := newTestAPI(&fakeEventReader{}, defaultContractReader())
	defer srv.Close()

	other := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	if code := getJSON(t, srv.URL+"/v1/contracts/"+other+"/events", nil); code != 404 {
		t.Errorf("status = %d, want 404", code)
	}
	if code := getJSON(t, srv.URL+"/v1/contracts/not-valid/events", nil); code != 400 {
		t.Errorf("status = %d, want 400", code)
	}
}

// The honesty contract: an empty page inside fully indexed bounds is
// COMPLETE (nothing happened); the same empty page reaching below coverage
// says OLDEST_REACHED (we cannot vouch).
func TestEmptyPageSaysWhy(t *testing.T) {
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	cr := defaultContractReader()
	clamped := uint32(1000)
	cr.backfill.ClampedAt = &clamped // history below 1000 unreachable
	srv := newTestAPI(ev, cr)
	defer srv.Close()

	var resp eventsResponse
	url := srv.URL + "/v1/contracts/" + registered + "/events?startLedger=2000&endLedger=3000"
	if code := getJSON(t, url, &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if resp.ScanStatus != scanComplete {
		t.Errorf("scanStatus = %s, want COMPLETE (bounds fully indexed, truly empty)", resp.ScanStatus)
	}

	var below eventsResponse
	url = srv.URL + "/v1/contracts/" + registered + "/events?startLedger=10&endLedger=3000"
	if code := getJSON(t, url, &below); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if below.ScanStatus != scanOldestReached {
		t.Errorf("scanStatus = %s, want OLDEST_REACHED (request dips below coverage)", below.ScanStatus)
	}
	if below.Coverage.ClampedAt == nil || *below.Coverage.ClampedAt != 1000 {
		t.Errorf("coverage must declare the clamp: %+v", below.Coverage)
	}
}

func TestContractDetail(t *testing.T) {
	cr := defaultContractReader()
	cr.contract.Classification = []byte(`{"type":"wasm","events":["transfer"],"method":"spec_events"}`)
	cr.counts = map[string]int64{"transfer": 40, "mint": 2}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPI(ev, cr)
	defer srv.Close()

	var detail contractDetail
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered, &detail); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if detail.ContractID != registered || detail.Events.Total != 42 {
		t.Errorf("detail = %+v", detail)
	}
	if detail.Events.ByName["transfer"] != 40 {
		t.Errorf("by_name = %v", detail.Events.ByName)
	}
	var cls map[string]any
	if err := json.Unmarshal(detail.Classification, &cls); err != nil || cls["type"] != "wasm" {
		t.Errorf("classification = %s", detail.Classification)
	}
	if len(detail.Coverage) != len(cr.contract.Kinds) {
		t.Fatalf("coverage must carry one declaration per registered kind, got %+v", detail.Coverage)
	}
	cov := detail.Coverage[0]
	if cov.Kind != store.KindEvents || !cov.KindDerived {
		t.Errorf("coverage kind = %+v", cov)
	}
	if cov.IndexedFromLedger != 1000 || cov.IndexedToLedger != 6000 {
		t.Errorf("coverage = %+v", cov)
	}

	if code := getJSON(t, srv.URL+"/v1/contracts/CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC", nil); code != 404 {
		t.Errorf("unknown contract status = %d, want 404", code)
	}
}

// --- state endpoints (M2) ---------------------------------------------------

type fakeStateReader struct {
	entries []store.StateEntry
	changes []store.StateChange
	hasMore bool
	queries []store.StateQuery
}

func (f *fakeStateReader) QueryStateEntries(_ context.Context, _ string, q store.StateQuery) ([]store.StateEntry, bool, error) {
	f.queries = append(f.queries, q)
	return f.entries, f.hasMore, nil
}

func (f *fakeStateReader) QueryStateChanges(_ context.Context, _ string, q store.StateQuery) ([]store.StateChange, bool, error) {
	f.queries = append(f.queries, q)
	return f.changes, f.hasMore, nil
}

func newTestAPIWithState(ev *fakeEventReader, cr *fakeContractReader, st *fakeStateReader) *httptest.Server {
	mux := http.NewServeMux()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewServer("testnet", ev, cr, log)
	server.Register(mux)
	server.RegisterState(mux, st)
	return httptest.NewServer(mux)
}

func TestStateSnapshotEndpoint(t *testing.T) {
	st := &fakeStateReader{
		entries: []store.StateEntry{{
			ContractID: registered, KeyXDR: validTopic, Durability: "persistent",
			ValueXDR: "AAAAAQ==", LastLedger: 5500, ClosedAt: time.Unix(1_700_000_000, 0).UTC(),
		}},
		hasMore: true,
	}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithState(ev, defaultContractReader(), st)
	defer srv.Close()

	var resp stateSnapshotResponse
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/state?limit=1", &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if len(resp.Entries) != 1 || resp.Entries[0].LastModifiedLedger != 5500 {
		t.Errorf("entries = %+v", resp.Entries)
	}
	if resp.Cursor == "" {
		t.Fatal("hasMore snapshot must return a cursor")
	}
	if resp.Coverage.IndexedToLedger != 6000 {
		t.Errorf("coverage = %+v", resp.Coverage)
	}

	// The cursor resumes the (key, durability) walk with the same limit.
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/state?cursor="+resp.Cursor, &stateSnapshotResponse{}); code != 200 {
		t.Fatalf("cursor page status = %d", code)
	}
	q := st.queries[1]
	if q.AfterKey != validTopic || q.AfterDurability != "persistent" || q.Limit != 1 {
		t.Errorf("cursor query = %+v", q)
	}
}

func TestStateHistoryEndpoint(t *testing.T) {
	st := &fakeStateReader{
		changes: []store.StateChange{{
			ID: "0000000000000000100-0000000000", ContractID: registered,
			LedgerSequence: 5500, ClosedAt: time.Unix(1_700_000_000, 0).UTC(),
			TxHash: "beef", ChangeType: "updated", KeyXDR: validTopic,
			Durability: "persistent", PreXDR: "AAAAAQ==", PostXDR: "AAAAAg==",
		}},
	}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithState(ev, defaultContractReader(), st)
	defer srv.Close()

	var resp stateHistoryResponse
	url := srv.URL + "/v1/contracts/" + registered + "/state/history?key=" + validTopic + "&startLedger=2000"
	if code := getJSON(t, url, &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if len(resp.Changes) != 1 || resp.Changes[0].ChangeType != "updated" {
		t.Errorf("changes = %+v", resp.Changes)
	}
	if resp.ScanStatus != scanWaitingForLedgers {
		t.Errorf("scanStatus = %s, want WAITING_FOR_LEDGERS (unbounded end)", resp.ScanStatus)
	}
	q := st.queries[0]
	if q.KeyXDR != validTopic || q.FromLedger != 2000 {
		t.Errorf("query = %+v", q)
	}

	// Cursor pages keep the key filter and bounds.
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/state/history?cursor="+resp.Cursor, &stateHistoryResponse{}); code != 200 {
		t.Fatalf("cursor page status = %d", code)
	}
	q = st.queries[1]
	if q.KeyXDR != validTopic || q.FromLedger != 2000 || q.AfterID != "0000000000000000100-0000000000" {
		t.Errorf("cursor query = %+v", q)
	}
}

func TestStateCursorsAreEndpointBound(t *testing.T) {
	st := &fakeStateReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithState(ev, defaultContractReader(), st)
	defer srv.Close()

	historyCursor := encodeStateCursor("testnet", kindHistory,
		store.StateQuery{ContractID: registered, Limit: 10})
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/state?cursor="+historyCursor, nil); code != 400 {
		t.Errorf("snapshot with history cursor: status = %d, want 400", code)
	}
	eventsCursor := encodeCursor("testnet", store.EventQuery{ContractID: registered, Limit: 10}, "")
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/state/history?cursor="+eventsCursor, nil); code != 400 {
		t.Errorf("history with events cursor: status = %d, want 400", code)
	}
}

func TestStateValidation(t *testing.T) {
	st := &fakeStateReader{}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithState(ev, defaultContractReader(), st)
	defer srv.Close()

	base := srv.URL + "/v1/contracts/" + registered
	cases := map[string]string{
		"bad key":           base + "/state?key=!!!",
		"bad limit":         base + "/state?limit=0",
		"bad history start": base + "/state/history?startLedger=x",
		"inverted history":  base + "/state/history?startLedger=10&endLedger=5",
	}
	for name, url := range cases {
		if code := getJSON(t, url, nil); code != 400 {
			t.Errorf("%s: status = %d, want 400", name, code)
		}
	}
	if len(st.queries) != 0 {
		t.Error("invalid requests must never reach the store")
	}

	other := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	if code := getJSON(t, srv.URL+"/v1/contracts/"+other+"/state", nil); code != 404 {
		t.Errorf("unregistered contract: status = %d, want 404", code)
	}
}

func TestContractDetailIncludesState(t *testing.T) {
	cr := defaultContractReader()
	cr.stateEntries = 7
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPI(ev, cr)
	defer srv.Close()

	var detail contractDetail
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered, &detail); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if detail.State.Entries != 7 {
		t.Errorf("state entries = %d, want 7", detail.State.Entries)
	}
}

func TestContractList(t *testing.T) {
	cr := defaultContractReader()
	srv := newTestAPI(&fakeEventReader{}, cr)
	defer srv.Close()

	var resp contractListResponse
	if code := getJSON(t, srv.URL+"/v1/contracts", &resp); code != 200 {
		t.Fatalf("status = %d, want 200", code)
	}
	if len(resp.Contracts) != 1 {
		t.Fatalf("contracts = %+v, want 1 row", resp.Contracts)
	}
	got := resp.Contracts[0]
	if got.ContractID != registered || got.Network != "testnet" || len(got.Kinds) == 0 {
		t.Errorf("row = %+v", got)
	}
	if got.RegisteredAt == "" {
		t.Error("registered_at must be set")
	}
}

func TestCoverageRefusesToVouchForAnUnregisteredKind(t *testing.T) {
	// The contract derives events only: its transfers page is empty because
	// nothing was ever derived, not because nothing happened. Rule 7 says the
	// response has to carry that difference.
	cr := defaultContractReader()
	cr.contract.Kinds = []string{store.KindEvents}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTransfers(ev, cr, &fakeTransfersReader{})
	defer srv.Close()

	var resp transfersResponse
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/transfers", &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if resp.Coverage.KindDerived {
		t.Error("kindDerived must be false for a kind the registration does not derive")
	}
	if resp.Coverage.IndexedFromLedger != 0 || resp.Coverage.IndexedToLedger != 0 {
		t.Errorf("an underived kind must vouch for nothing, got %+v", resp.Coverage)
	}
}

func TestCoverageRefusesToVouchForAKindAddedAfterTheWalk(t *testing.T) {
	// The kind is registered but the finished walk never derived it: history
	// below the anchor is not covered, and saying otherwise is the bug this
	// whole change exists to prevent.
	cr := defaultContractReader()
	cr.backfill.CoveredKinds = []string{store.KindEvents}
	ev := &fakeEventReader{cursor: store.Cursor{Sequence: 6000}}
	srv := newTestAPIWithTransfers(ev, cr, &fakeTransfersReader{})
	defer srv.Close()

	var resp transfersResponse
	if code := getJSON(t, srv.URL+"/v1/contracts/"+registered+"/transfers", &resp); code != 200 {
		t.Fatalf("status = %d", code)
	}
	if !resp.Coverage.KindDerived {
		t.Error("the kind IS registered; kindDerived must be true")
	}
	if resp.Coverage.IndexedFromLedger != 6000 {
		t.Errorf("coverage must start at the anchor, not at the walk floor: %+v", resp.Coverage)
	}
	if !resp.Coverage.BackfillPending {
		t.Error("a kind awaiting its reopened walk must report backfillPending")
	}
}
