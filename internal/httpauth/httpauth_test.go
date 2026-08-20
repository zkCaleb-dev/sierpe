package httpauth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func testHandler(t *testing.T) http.Handler {
	t.Helper()
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return Wrap(inner, "operator", "correct-horse-battery", "admin-bearer-token-123")
}

func get(t *testing.T, h http.Handler, path, user, pass string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	if user != "" {
		r.SetBasicAuth(user, pass)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestRejectsWithoutCredentials(t *testing.T) {
	w := get(t, testHandler(t), "/v1/contracts", "", "")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if w.Header().Get("WWW-Authenticate") == "" {
		t.Error("401 must carry WWW-Authenticate so browsers prompt")
	}
}

func TestRejectsWrongCredentials(t *testing.T) {
	cases := [][2]string{
		{"operator", "wrong"},
		{"intruder", "correct-horse-battery"},
		{"operator", "correct-horse-batteryX"},
	}
	for _, c := range cases {
		if w := get(t, testHandler(t), "/", c[0], c[1]); w.Code != http.StatusUnauthorized {
			t.Errorf("%s:%s → %d, want 401", c[0], c[1], w.Code)
		}
	}
}

func TestAcceptsRightCredentials(t *testing.T) {
	for _, path := range []string{"/", "/v1/contracts", "/status", "/metrics"} {
		if w := get(t, testHandler(t), path, "operator", "correct-horse-battery"); w.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, w.Code)
		}
	}
}

func TestProbesBypassAuth(t *testing.T) {
	for _, path := range []string{"/health", "/ready"} {
		if w := get(t, testHandler(t), path, "", ""); w.Code != http.StatusOK {
			t.Errorf("GET %s without creds = %d, want 200 (orchestrator probes)", path, w.Code)
		}
	}
}

func bearer(t *testing.T, h http.Handler, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAdminBearerPassesTheGate(t *testing.T) {
	// Basic and Bearer share the Authorization header: a mutation carrying
	// the admin bearer cannot also carry Basic credentials, so the gate
	// must accept the higher-privilege token on its own.
	if w := bearer(t, testHandler(t), "/v1/contracts", "admin-bearer-token-123"); w.Code != http.StatusOK {
		t.Errorf("admin bearer = %d, want 200", w.Code)
	}
}

func TestWrongBearerIsRejected(t *testing.T) {
	if w := bearer(t, testHandler(t), "/v1/contracts", "not-the-admin-token"); w.Code != http.StatusUnauthorized {
		t.Errorf("wrong bearer = %d, want 401", w.Code)
	}
}

func TestEmptyAdminTokenNeverMatchesBearer(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := Wrap(inner, "operator", "correct-horse-battery", "")
	if w := bearer(t, h, "/", ""); w.Code != http.StatusUnauthorized {
		t.Errorf("empty bearer against empty admin token = %d, want 401", w.Code)
	}
}
