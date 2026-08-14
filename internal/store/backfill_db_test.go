package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func seedContract(t *testing.T, s *Store, contractID string) {
	t.Helper()
	if _, err := s.UpsertContract(context.Background(), Contract{
		Network: "testnet", ContractID: contractID, Source: SourceAPI,
		Kinds: []string{KindEvents},
	}); err != nil {
		t.Fatalf("seed contract: %v", err)
	}
}

func TestEnsureBackfillLifecycle(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE backfill`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	seedContract(t, s, "CAAA")

	// Create with target 100.
	if err := s.EnsureBackfill(ctx, "testnet", "CAAA", 100, 5000); err != nil {
		t.Fatalf("EnsureBackfill() error = %v", err)
	}
	b, err := s.GetBackfill(ctx, "testnet", "CAAA")
	if err != nil {
		t.Fatalf("GetBackfill() error = %v", err)
	}
	if b.TargetFrom != 100 || b.NextTo != 5000 || b.Done {
		t.Errorf("backfill = %+v", b)
	}

	// Same target again: progress untouched.
	if err := s.CommitBackfillChunk(ctx, "testnet",
		Backfill{ContractID: "CAAA", TargetFrom: 100, NextTo: 3000}, nil); err != nil {
		t.Fatalf("CommitBackfillChunk() error = %v", err)
	}
	if err := s.EnsureBackfill(ctx, "testnet", "CAAA", 100, 5000); err != nil {
		t.Fatalf("EnsureBackfill() repeat error = %v", err)
	}
	b, _ = s.GetBackfill(ctx, "testnet", "CAAA")
	if b.NextTo != 3000 {
		t.Errorf("re-register must not reset progress, next_to = %d", b.NextTo)
	}

	// Walk finishes at 100; a deeper target must reopen it.
	done := Backfill{ContractID: "CAAA", TargetFrom: 100, NextTo: 99, Done: true}
	if err := s.CommitBackfillChunk(ctx, "testnet", done, nil); err != nil {
		t.Fatalf("CommitBackfillChunk() error = %v", err)
	}
	if err := s.EnsureBackfill(ctx, "testnet", "CAAA", 1, 5000); err != nil {
		t.Fatalf("EnsureBackfill() deepen error = %v", err)
	}
	b, _ = s.GetBackfill(ctx, "testnet", "CAAA")
	if b.Done || b.TargetFrom != 1 {
		t.Errorf("deeper target must reopen: %+v", b)
	}
	if b.NextTo != 99 {
		t.Errorf("deepening must keep the watermark, next_to = %d", b.NextTo)
	}
}

func TestBackfillCreatedDoneWhenNothingToWalk(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE backfill`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	seedContract(t, s, "CAAA")

	// nextTo 0 (no cursor yet): nothing to walk, done at birth.
	if err := s.EnsureBackfill(ctx, "testnet", "CAAA", 1, 0); err != nil {
		t.Fatalf("EnsureBackfill() error = %v", err)
	}
	b, err := s.GetBackfill(ctx, "testnet", "CAAA")
	if err != nil {
		t.Fatalf("GetBackfill() error = %v", err)
	}
	if !b.Done {
		t.Errorf("backfill with nothing to walk must be done: %+v", b)
	}

	pending, err := s.ListPendingBackfills(ctx, "testnet")
	if err != nil {
		t.Fatalf("ListPendingBackfills() error = %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("pending = %v, want none", pending)
	}
}

func TestListPendingBackfillsJoinsContract(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE backfill`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	seedContract(t, s, "CAAA")
	if err := s.EnsureBackfill(ctx, "testnet", "CAAA", 1, 5000); err != nil {
		t.Fatalf("EnsureBackfill() error = %v", err)
	}

	pending, err := s.ListPendingBackfills(ctx, "testnet")
	if err != nil {
		t.Fatalf("ListPendingBackfills() error = %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(pending))
	}
	if pending[0].Contract.ContractID != "CAAA" || !pending[0].Contract.HasKind(KindEvents) {
		t.Errorf("joined contract = %+v", pending[0].Contract)
	}

	n, err := s.CountPendingBackfills(ctx, "testnet")
	if err != nil || n != 1 {
		t.Errorf("CountPendingBackfills() = %d, %v", n, err)
	}
}

func TestCommitBackfillChunkPersistsEventsAtomically(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE backfill, events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	seedContract(t, s, "CAAA")
	if err := s.EnsureBackfill(ctx, "testnet", "CAAA", 1, 5000); err != nil {
		t.Fatalf("EnsureBackfill() error = %v", err)
	}

	clamped := uint32(3001)
	b := Backfill{ContractID: "CAAA", TargetFrom: 1, NextTo: 3000, Done: true, ClampedAt: &clamped}
	events := []Event{testEvent("0000000000000004000-0000000000", "CAAA", "feed", 0)}
	if err := s.CommitBackfillChunk(ctx, "testnet", b, events); err != nil {
		t.Fatalf("CommitBackfillChunk() error = %v", err)
	}

	got, err := s.GetBackfill(ctx, "testnet", "CAAA")
	if err != nil {
		t.Fatalf("GetBackfill() error = %v", err)
	}
	if got.NextTo != 3000 || !got.Done || got.ClampedAt == nil || *got.ClampedAt != 3001 {
		t.Errorf("backfill = %+v", got)
	}
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil || n != 1 {
		t.Errorf("events = %d, %v", n, err)
	}
}

func TestCommitBackfillChunkRefusesVanishedRegistration(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE backfill, events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	b := Backfill{ContractID: "CGONE", TargetFrom: 1, NextTo: 3000}
	events := []Event{testEvent("0000000000000004100-0000000000", "CGONE", "dead", 0)}
	if err := s.CommitBackfillChunk(ctx, "testnet", b, events); err == nil {
		t.Fatal("committing a chunk for an unregistered contract must fail")
	}
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil || n != 0 {
		t.Errorf("orphan events = %d, %v: rollback must cover the events too", n, err)
	}
}

func TestDeleteContractRemovesBackfill(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE backfill`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	seedContract(t, s, "CAAA")
	if err := s.EnsureBackfill(ctx, "testnet", "CAAA", 1, 5000); err != nil {
		t.Fatalf("EnsureBackfill() error = %v", err)
	}

	if _, err := s.DeleteContract(ctx, "testnet", "CAAA"); err != nil {
		t.Fatalf("DeleteContract() error = %v", err)
	}
	if _, err := s.GetBackfill(ctx, "testnet", "CAAA"); !errors.Is(err, ErrNoBackfill) {
		t.Errorf("GetBackfill() after delete = %v, want ErrNoBackfill", err)
	}
}

// Guard against clock misuse: updated_at exists for operations, but nothing
// in the backfill flow sorts business data by it (CLAUDE.md rule 8). This
// test simply pins that the ordering column used by ListPendingBackfills is
// the operational clock on purpose (oldest progress first).
func TestListPendingBackfillsOrdersByProgress(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE backfill`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	seedContract(t, s, "CAAA")
	seedContract(t, s, "CBBB")
	if err := s.EnsureBackfill(ctx, "testnet", "CAAA", 1, 5000); err != nil {
		t.Fatalf("EnsureBackfill() error = %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := s.EnsureBackfill(ctx, "testnet", "CBBB", 1, 5000); err != nil {
		t.Fatalf("EnsureBackfill() error = %v", err)
	}

	pending, err := s.ListPendingBackfills(ctx, "testnet")
	if err != nil {
		t.Fatalf("ListPendingBackfills() error = %v", err)
	}
	if len(pending) != 2 || pending[0].Contract.ContractID != "CAAA" {
		t.Errorf("order = %v, want CAAA first (stalest progress)", pending)
	}
}
