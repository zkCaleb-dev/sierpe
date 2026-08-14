package admin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func newTestServer(st *fakeStore, reg *fakeReloader) *httptest.Server {
	mux := http.NewServeMux()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	NewServer("testnet", testToken, st, reg, log).Register(mux)
	return httptest.NewServer(mux)
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
	srv := newTestServer(st, &fakeReloader{})
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
	srv := newTestServer(st, reg)
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
	if len(got.Kinds) != 1 || got.Kinds[0] != store.KindEvents {
		t.Errorf("kinds must default to [events], got %v", got.Kinds)
	}
	if reg.calls != 1 {
		t.Errorf("registry reloads = %d, want 1", reg.calls)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), validContract) {
		t.Errorf("response must echo the registration, got %s", body)
	}
}

func TestRegisterValidation(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(st, &fakeReloader{})
	defer srv.Close()

	cases := []struct {
		name string
		body string
	}{
		{"malformed json", `{`},
		{"invalid strkey", `{"contract_id":"CINVALID"}`},
		{"account not contract", `{"contract_id":"GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"}`},
		{"unsupported kind", `{"contract_id":"` + validContract + `","kinds":["transfers"]}`},
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
	srv := newTestServer(st, reg)
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
	srv := newTestServer(st, reg)
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
		srv := newTestServer(st, reg)

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

func TestDeleteRejectsInvalidID(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(st, &fakeReloader{})
	defer srv.Close()

	resp := doRequest(t, http.MethodDelete, srv.URL+"/v1/contracts/not-a-contract", testToken, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if len(st.deleted) != 0 {
		t.Error("invalid ids must never reach the store")
	}
}
