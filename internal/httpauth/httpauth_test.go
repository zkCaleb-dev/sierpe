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
	return Wrap(inner, "operator", "correct-horse-battery")
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
