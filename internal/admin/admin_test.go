package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

const (
	testToken = "correct-horse-battery-staple"
	// validContract is a real testnet contract address; any well-formed
	// C... strkey works for offline validation.
	validContract = "CBF64DEOVQAXJFBSNGFEUT2AH4H7K5JBY3ZYJ5GVEINMNSDISWRG5N3F"
)

type fakeStore struct {
	upserted      []store.Contract
	upsertErr     error
	deleted       []string
	deleteExisted bool
	deleteErr     error
}

func (f *fakeStore) UpsertContract(_ context.Context, c store.Contract) (store.Contract, error) {
	if f.upsertErr != nil {
		return store.Contract{}, f.upsertErr
	}
	f.upserted = append(f.upserted, c)
	return c, nil
}

func (f *fakeStore) DeleteContract(_ context.Context, _, contractID string) (bool, error) {
	if f.deleteErr != nil {
		return false, f.deleteErr
	}
	f.deleted = append(f.deleted, contractID)
	return f.deleteExisted, nil
}

type fakeReloader struct {
	calls int
	err   error
}

func (f *fakeReloader) Reload(context.Context) error {
	f.calls++
	return f.err
}

// fakeClassifier returns a canned classification (or error).
type fakeClassifier struct {
	cls   registry.Classification
	err   error
	calls int
}

func (f *fakeClassifier) Classify(context.Context, string) (registry.Classification, error) {
	f.calls++
	return f.cls, f.err
}

func okClassifier() *fakeClassifier {
	return &fakeClassifier{cls: registry.Classification{
		Type: registry.TypeWasm, Events: []string{"transfer"}, Method: registry.MethodSpecEvents,
	}}
}

// fakePlanner records backfill anchoring.
type fakePlanner struct {
	cursor    store.Cursor
	cursorErr error
	ensured   []string // "contract:target:nextTo"
	ensureErr error
}

func (f *fakePlanner) LoadCursor(context.Context, string) (store.Cursor, error) {
	if f.cursorErr != nil {
		return store.Cursor{}, f.cursorErr
	}
	return f.cursor, nil
}

func (f *fakePlanner) EnsureBackfill(_ context.Context, _ string, contractID string, targetFrom, nextTo uint32, kinds []string) error {
	if f.ensureErr != nil {
		return f.ensureErr
	}
	f.ensured = append(f.ensured, fmt.Sprintf("%s:%d:%d", contractID, targetFrom, nextTo))
	return nil
}

func newTestServerWithPlanner(st *fakeStore, planner *fakePlanner, reg *fakeReloader, cls *fakeClassifier) *httptest.Server {
	mux := http.NewServeMux()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	NewServer("testnet", testToken, st, planner, reg, cls, log).Register(mux)
	return httptest.NewServer(mux)
}

func newTestServer(st *fakeStore, reg *fakeReloader, cls *fakeClassifier) *httptest.Server {
	return newTestServerWithPlanner(st, &fakePlanner{cursor: store.Cursor{Sequence: 500}}, reg, cls)
}

func doRequest(t *testing.T, method, url, token, body string) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestAuthRejectsBadCredentials(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(st, &fakeReloader{}, okClassifier())
	defer srv.Close()

	cases := []struct {
		name  string
		token string
	}{
		{"missing token", ""},
		{"wrong token", "wrong-token-entirely-here"},
		{"prefix of real token", testToken[:len(testToken)-1]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", tc.token,
				`{"contract_id":"`+validContract+`"}`)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", resp.StatusCode)
			}
		})
	}
	if len(st.upserted) != 0 {
		t.Error("unauthenticated requests must never reach the store")
	}
}

func TestRegisterContract(t *testing.T) {
	st := &fakeStore{}
	reg := &fakeReloader{}
	srv := newTestServer(st, reg, okClassifier())
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken,
		`{"contract_id":"`+validContract+`"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(st.upserted) != 1 {
		t.Fatalf("upserted %d contracts, want 1", len(st.upserted))
	}
	got := st.upserted[0]
	if got.ContractID != validContract || got.Network != "testnet" || got.Source != store.SourceAPI {
		t.Errorf("upserted contract = %+v", got)
	}
	if len(got.Kinds) != 2 || got.Kinds[0] != store.KindEvents || got.Kinds[1] != store.KindState {
		t.Errorf("kinds must default to [events state], got %v", got.Kinds)
	}
	if reg.calls != 1 {
		t.Errorf("registry reloads = %d, want 1", reg.calls)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), validContract) {
		t.Errorf("response must echo the registration, got %s", body)
	}
}

func TestRegisterSACDefaultsToTransfers(t *testing.T) {
	st := &fakeStore{}
	sac := &fakeClassifier{cls: registry.Classification{
		Type: registry.TypeSAC, Events: []string{"transfer"}, Method: registry.MethodSACBuiltin,
	}}
	srv := newTestServer(st, &fakeReloader{}, sac)
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken,
		`{"contract_id":"`+validContract+`"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got := st.upserted[0]
	want := []string{store.KindEvents, store.KindState, store.KindTransfers}
	if len(got.Kinds) != len(want) || got.Kinds[0] != want[0] || got.Kinds[1] != want[1] || got.Kinds[2] != want[2] {
		t.Errorf("SAC kinds must default to %v, got %v", want, got.Kinds)
	}
}

func TestRegisterExplicitTransfersKindAccepted(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(st, &fakeReloader{}, okClassifier())
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken,
		`{"contract_id":"`+validContract+`","kinds":["events","transfers"]}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got := st.upserted[0]
	if len(got.Kinds) != 2 || got.Kinds[1] != store.KindTransfers {
		t.Errorf("explicit kinds must persist, got %v", got.Kinds)
	}
}

func TestRegisterValidation(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(st, &fakeReloader{}, okClassifier())
	defer srv.Close()

	cases := []struct {
		name string
		body string
	}{
		{"malformed json", `{`},
		{"invalid strkey", `{"contract_id":"CINVALID"}`},
		{"account not contract", `{"contract_id":"GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"}`},
		{"unsupported kind", `{"contract_id":"` + validContract + `","kinds":["balances"]}`},
		{"unknown field", `{"contract_id":"` + validContract + `","kind":["events"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken, tc.body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", resp.StatusCode)
			}
		})
	}
	if len(st.upserted) != 0 {
		t.Error("invalid requests must never reach the store")
	}
}

func TestRegisterStoreErrorIs500(t *testing.T) {
	st := &fakeStore{upsertErr: errors.New("db down")}
	reg := &fakeReloader{}
	srv := newTestServer(st, reg, okClassifier())
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken,
		`{"contract_id":"`+validContract+`"}`)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
	if reg.calls != 0 {
		t.Error("a failed mutation must not trigger a reload")
	}
}

func TestRegisterSucceedsWhenReloadFails(t *testing.T) {
	st := &fakeStore{}
	reg := &fakeReloader{err: errors.New("transient")}
	srv := newTestServer(st, reg, okClassifier())
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken,
		`{"contract_id":"`+validContract+`"}`)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200: the row is durable, reload converges later", resp.StatusCode)
	}
}

func TestDeleteContractIsIdempotent(t *testing.T) {
	for _, existed := range []bool{true, false} {
		st := &fakeStore{deleteExisted: existed}
		reg := &fakeReloader{}
		srv := newTestServer(st, reg, okClassifier())

		resp := doRequest(t, http.MethodDelete, srv.URL+"/v1/contracts/"+validContract, testToken, "")
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("existed=%v: status = %d, want 204", existed, resp.StatusCode)
		}
		if len(st.deleted) != 1 {
			t.Errorf("existed=%v: delete calls = %d, want 1", existed, len(st.deleted))
		}
		if reg.calls != 1 {
			t.Errorf("existed=%v: registry reloads = %d, want 1", existed, reg.calls)
		}
		srv.Close()
	}
}

func TestRegisterStoresClassification(t *testing.T) {
	st := &fakeStore{}
	cls := okClassifier()
	srv := newTestServer(st, &fakeReloader{}, cls)
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken,
		`{"contract_id":"`+validContract+`"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if cls.calls != 1 {
		t.Errorf("classifier calls = %d, want 1", cls.calls)
	}
	stored := string(st.upserted[0].Classification)
	for _, fragment := range []string{`"type":"wasm"`, `"transfer"`, `"method":"spec_events"`} {
		if !strings.Contains(stored, fragment) {
			t.Errorf("stored classification %s must contain %s", stored, fragment)
		}
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"type":"wasm"`) {
		t.Errorf("response must echo the classification, got %s", body)
	}
}

func TestRegisterUnknownContractIs404(t *testing.T) {
	st := &fakeStore{}
	cls := &fakeClassifier{err: registry.ErrContractNotFound}
	srv := newTestServer(st, &fakeReloader{}, cls)
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken,
		`{"contract_id":"`+validContract+`"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	if len(st.upserted) != 0 {
		t.Error("a contract missing on chain must not be registered")
	}
}

func TestRegisterChainUnreachableIs502(t *testing.T) {
	st := &fakeStore{}
	cls := &fakeClassifier{err: errors.New("all rpc endpoints failed")}
	srv := newTestServer(st, &fakeReloader{}, cls)
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken,
		`{"contract_id":"`+validContract+`"}`)
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
	if len(st.upserted) != 0 {
		t.Error("an unclassifiable contract must not be silently registered")
	}
}

func TestRegisterPlansBackfill(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"default genesis", `{"contract_id":"` + validContract + `"}`,
			validContract + ":1:520"},
		{"explicit genesis", `{"contract_id":"` + validContract + `","from":"genesis"}`,
			validContract + ":1:520"},
		{"ledger number", `{"contract_id":"` + validContract + `","from":123}`,
			validContract + ":123:520"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			planner := &fakePlanner{cursor: store.Cursor{Sequence: 500}}
			srv := newTestServerWithPlanner(&fakeStore{}, planner, &fakeReloader{}, okClassifier())
			defer srv.Close()

			resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken, tc.body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if len(planner.ensured) != 1 || planner.ensured[0] != tc.want {
				t.Errorf("ensured = %v, want [%s]", planner.ensured, tc.want)
			}
		})
	}
}

// The anchor must sit PAST the live cursor, not on it. Live ingestion only
// starts deriving a contract once the ingesting process reloads its
// registry; the ledgers closing inside that window would otherwise be
// derived by nobody while coverage happily claimed them.
func TestRegisterAnchorsPastTheLiveCursor(t *testing.T) {
	planner := &fakePlanner{cursor: store.Cursor{Sequence: 500}}
	srv := newTestServerWithPlanner(&fakeStore{}, planner, &fakeReloader{}, okClassifier())
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken,
		`{"contract_id":"`+validContract+`"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(planner.ensured) != 1 {
		t.Fatalf("ensured = %v", planner.ensured)
	}
	var anchor uint32
	if _, err := fmt.Sscanf(planner.ensured[0], validContract+":1:%d", &anchor); err != nil {
		t.Fatalf("parse anchor %q: %v", planner.ensured[0], err)
	}
	if anchor <= 500 {
		t.Errorf("anchor = %d, must exceed the cursor (500) so the backfill overlaps the reload window", anchor)
	}
}

func TestRegisterRejectsBadFrom(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(st, &fakeReloader{}, okClassifier())
	defer srv.Close()

	for _, body := range []string{
		`{"contract_id":"` + validContract + `","from":"yesterday"}`,
		`{"contract_id":"` + validContract + `","from":0}`,
		`{"contract_id":"` + validContract + `","from":-5}`,
	} {
		resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken, body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, resp.StatusCode)
		}
	}
	if len(st.upserted) != 0 {
		t.Error("invalid from must reject before touching the store")
	}
}

func TestRegisterWithoutCursorStillRegisters(t *testing.T) {
	planner := &fakePlanner{cursorErr: store.ErrNoCursor}
	srv := newTestServerWithPlanner(&fakeStore{}, planner, &fakeReloader{}, okClassifier())
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/v1/contracts", testToken,
		`{"contract_id":"`+validContract+`"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: no cursor yet is a warning, not a failure", resp.StatusCode)
	}
	if len(planner.ensured) != 1 || planner.ensured[0] != validContract+":1:0" {
		t.Errorf("ensured = %v, want an immediately-done backfill anchor", planner.ensured)
	}
}

func TestDeleteRejectsInvalidID(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(st, &fakeReloader{}, okClassifier())
	defer srv.Close()

	resp := doRequest(t, http.MethodDelete, srv.URL+"/v1/contracts/not-a-contract", testToken, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if len(st.deleted) != 0 {
		t.Error("invalid ids must never reach the store")
	}
}
