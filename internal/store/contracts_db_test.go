package store

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

// sameJSON compares two JSON documents semantically; Postgres jsonb does not
// preserve the input's byte formatting.
func sameJSON(t *testing.T, got, want []byte) bool {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("got is not JSON: %v", err)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("want is not JSON: %v", err)
	}
	return reflect.DeepEqual(g, w)
}

// openTestStore connects to the throwaway Postgres named by
// SIERPE_TEST_DATABASE_URL, migrates it, and empties the contracts table.
// Without the variable the test skips, keeping the default unit run
// hermetic; the e2e harness (Docker scratch Postgres) provides it.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("SIERPE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("SIERPE_TEST_DATABASE_URL not set; skipping database-backed test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := Open(ctx, url)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := s.pool.Exec(ctx, `TRUNCATE contracts`); err != nil {
		t.Fatalf("truncate contracts: %v", err)
	}
	return s
}

func TestUpsertContractIsIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	in := Contract{
		Network:    "testnet",
		ContractID: "CAAA",
		Source:     SourceAPI,
		Kinds:      []string{KindEvents},
	}
	first, err := s.UpsertContract(ctx, in)
	if err != nil {
		t.Fatalf("first UpsertContract() error = %v", err)
	}
	if first.RegisteredAt.IsZero() {
		t.Error("registered_at must be set by the database")
	}

	second, err := s.UpsertContract(ctx, in)
	if err != nil {
		t.Fatalf("second UpsertContract() error = %v", err)
	}
	if !second.RegisteredAt.Equal(first.RegisteredAt) {
		t.Errorf("re-registering must preserve registered_at: %v != %v",
			second.RegisteredAt, first.RegisteredAt)
	}

	list, err := s.ListContracts(ctx, "testnet")
	if err != nil {
		t.Fatalf("ListContracts() error = %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("after double register, len = %d, want 1", len(list))
	}
}

func TestUpsertContractReconcilesKinds(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	base := Contract{Network: "testnet", ContractID: "CAAA", Source: SourceAPI,
		Kinds: []string{KindEvents}}
	if _, err := s.UpsertContract(ctx, base); err != nil {
		t.Fatalf("UpsertContract() error = %v", err)
	}

	base.Kinds = []string{KindEvents, "state"}
	out, err := s.UpsertContract(ctx, base)
	if err != nil {
		t.Fatalf("UpsertContract() error = %v", err)
	}
	if len(out.Kinds) != 2 {
		t.Errorf("kinds must follow the new request, got %v", out.Kinds)
	}
}

func TestUpsertContractKeepsClassificationWhenAbsent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	classified := Contract{Network: "testnet", ContractID: "CAAA", Source: SourceAPI,
		Kinds: []string{KindEvents}, Classification: []byte(`{"type":"wasm"}`)}
	if _, err := s.UpsertContract(ctx, classified); err != nil {
		t.Fatalf("UpsertContract() error = %v", err)
	}

	// Re-register without classification: the stored one must survive.
	unclassified := classified
	unclassified.Classification = nil
	out, err := s.UpsertContract(ctx, unclassified)
	if err != nil {
		t.Fatalf("UpsertContract() error = %v", err)
	}
	if !sameJSON(t, out.Classification, classified.Classification) {
		t.Errorf("classification must survive a nil re-register, got %s", out.Classification)
	}

	// Re-register with a new classification: it must win.
	unclassified.Classification = []byte(`{"type":"sac"}`)
	out, err = s.UpsertContract(ctx, unclassified)
	if err != nil {
		t.Fatalf("UpsertContract() error = %v", err)
	}
	if !sameJSON(t, out.Classification, unclassified.Classification) {
		t.Errorf("new classification must overwrite, got %s", out.Classification)
	}
}

func TestDeleteContractIsIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.UpsertContract(ctx, Contract{Network: "testnet",
		ContractID: "CAAA", Source: SourceAPI, Kinds: []string{KindEvents}}); err != nil {
		t.Fatalf("UpsertContract() error = %v", err)
	}

	existed, err := s.DeleteContract(ctx, "testnet", "CAAA")
	if err != nil {
		t.Fatalf("DeleteContract() error = %v", err)
	}
	if !existed {
		t.Error("first delete must report the row existed")
	}

	existed, err = s.DeleteContract(ctx, "testnet", "CAAA")
	if err != nil {
		t.Fatalf("second DeleteContract() error = %v", err)
	}
	if existed {
		t.Error("second delete must be a no-op")
	}
}

func TestListContractsFiltersByNetwork(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for _, c := range []Contract{
		{Network: "testnet", ContractID: "CAAA", Source: SourceAPI, Kinds: []string{KindEvents}},
		{Network: "mainnet", ContractID: "CBBB", Source: SourceAPI, Kinds: []string{KindEvents}},
	} {
		if _, err := s.UpsertContract(ctx, c); err != nil {
			t.Fatalf("UpsertContract(%s) error = %v", c.ContractID, err)
		}
	}

	list, err := s.ListContracts(ctx, "testnet")
	if err != nil {
		t.Fatalf("ListContracts() error = %v", err)
	}
	if len(list) != 1 || list[0].ContractID != "CAAA" {
		t.Errorf("testnet list = %+v, want only CAAA", list)
	}
}
