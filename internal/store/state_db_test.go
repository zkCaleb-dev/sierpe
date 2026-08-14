package store

import (
	"context"
	"testing"
	"time"
)

func stateChange(id string, ledger uint32, key, changeType, post string) StateChange {
	return StateChange{
		ID:             id,
		ContractID:     "CAAA",
		LedgerSequence: ledger,
		ClosedAt:       time.Unix(1_700_000_000+int64(ledger), 0).UTC(),
		TxHash:         "hash-" + id,
		TxIndex:        1,
		ChangeIndex:    0,
		ChangeType:     changeType,
		KeyXDR:         key,
		Durability:     "persistent",
		PostXDR:        post,
	}
}

func commitStates(t *testing.T, s *Store, seq uint32, changes ...StateChange) {
	t.Helper()
	rec := LedgerRecord{Sequence: seq, Hash: "h", PreviousHash: "p", ClosedAt: time.Now().UTC()}
	if err := s.CommitLedger(context.Background(), "testnet", rec, nil, changes); err != nil {
		t.Fatalf("CommitLedger(states) error = %v", err)
	}
}

func snapshotValue(t *testing.T, s *Store, key string) (string, bool) {
	t.Helper()
	entries, _, err := s.QueryStateEntries(context.Background(), "testnet", StateQuery{
		ContractID: "CAAA", KeyXDR: key, Limit: 10,
	})
	if err != nil {
		t.Fatalf("QueryStateEntries() error = %v", err)
	}
	if len(entries) == 0 {
		return "", false
	}
	return entries[0].ValueXDR, true
}

func truncateState(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`TRUNCATE state_changes, state_entries, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func TestStateSnapshotOrderingGuard(t *testing.T) {
	s := openTestStore(t)
	truncateState(t, s)

	// Live writes ledger 200 first.
	commitStates(t, s, 200, stateChange("0000000000000000200-0000000000", 200, "kA", "updated", "v200"))

	// A descending backfill then replays ledger 100: it must NOT overwrite.
	b := Backfill{ContractID: "CAAA", TargetFrom: 1, NextTo: 99}
	seedContract(t, s, "CAAA")
	if err := s.EnsureBackfill(context.Background(), "testnet", "CAAA", 1, 150); err != nil {
		t.Fatalf("EnsureBackfill() error = %v", err)
	}
	old := stateChange("0000000000000000100-0000000000", 100, "kA", "updated", "v100")
	if err := s.CommitBackfillChunk(context.Background(), "testnet", b, nil, []StateChange{old}); err != nil {
		t.Fatalf("CommitBackfillChunk() error = %v", err)
	}

	if v, ok := snapshotValue(t, s, "kA"); !ok || v != "v200" {
		t.Errorf("snapshot = %q, want v200 (older replay must lose)", v)
	}

	// History keeps BOTH rows: the snapshot converges, the log is complete.
	history, _, err := s.QueryStateChanges(context.Background(), "testnet", StateQuery{
		ContractID: "CAAA", Limit: 10,
	})
	if err != nil {
		t.Fatalf("QueryStateChanges() error = %v", err)
	}
	if len(history) != 2 {
		t.Errorf("history rows = %d, want 2", len(history))
	}

	// A genuinely newer write still wins.
	commitStates(t, s, 300, stateChange("0000000000000000300-0000000000", 300, "kA", "updated", "v300"))
	if v, _ := snapshotValue(t, s, "kA"); v != "v300" {
		t.Errorf("snapshot = %q, want v300", v)
	}
}

func TestStateTombstoneBlocksStaleResurrection(t *testing.T) {
	s := openTestStore(t)
	truncateState(t, s)

	// Entry removed at ledger 300.
	removed := stateChange("0000000000000000300-0000000000", 300, "kA", "removed", "")
	commitStates(t, s, 300, removed)

	if _, ok := snapshotValue(t, s, "kA"); ok {
		t.Fatal("removed entry must not appear in the snapshot")
	}

	// Backfill replays its creation at ledger 100: the tombstone must hold.
	seedContract(t, s, "CAAA")
	if err := s.EnsureBackfill(context.Background(), "testnet", "CAAA", 1, 200); err != nil {
		t.Fatalf("EnsureBackfill() error = %v", err)
	}
	b := Backfill{ContractID: "CAAA", TargetFrom: 1, NextTo: 99}
	created := stateChange("0000000000000000100-0000000000", 100, "kA", "created", "v100")
	if err := s.CommitBackfillChunk(context.Background(), "testnet", b, nil, []StateChange{created}); err != nil {
		t.Fatalf("CommitBackfillChunk() error = %v", err)
	}
	if _, ok := snapshotValue(t, s, "kA"); ok {
		t.Error("stale create resurrected a deleted entry")
	}
}

func TestStateSameLedgerLastChangeWins(t *testing.T) {
	s := openTestStore(t)
	truncateState(t, s)

	// Two changes to the same key in one ledger, chain order: final wins.
	first := stateChange("0000000000000000400-0000000000", 400, "kA", "created", "v1")
	second := stateChange("0000000000000000400-0000000001", 400, "kA", "updated", "v2")
	second.ChangeIndex = 1
	second.TxHash = "other"
	commitStates(t, s, 400, first, second)

	if v, _ := snapshotValue(t, s, "kA"); v != "v2" {
		t.Errorf("snapshot = %q, want v2 (last change in the ledger)", v)
	}

	// Idempotent re-commit of the same ledger changes nothing.
	commitStates(t, s, 400, first, second)
	var n int64
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM state_changes`).Scan(&n); err != nil || n != 2 {
		t.Errorf("history rows = %d, %v (re-commit must be a no-op)", n, err)
	}
}

func TestQueryStateEntriesPagination(t *testing.T) {
	s := openTestStore(t)
	truncateState(t, s)

	changes := []StateChange{
		stateChange("0000000000000000500-0000000000", 500, "kA", "created", "vA"),
		stateChange("0000000000000000500-0000000001", 500, "kB", "created", "vB"),
		stateChange("0000000000000000500-0000000002", 500, "kC", "created", "vC"),
	}
	for i := range changes {
		changes[i].ChangeIndex = int32(i)
	}
	commitStates(t, s, 500, changes...)

	page1, hasMore, err := s.QueryStateEntries(context.Background(), "testnet", StateQuery{
		ContractID: "CAAA", Limit: 2,
	})
	if err != nil || len(page1) != 2 || !hasMore {
		t.Fatalf("page1 = %d hasMore=%v err=%v", len(page1), hasMore, err)
	}
	page2, hasMore, err := s.QueryStateEntries(context.Background(), "testnet", StateQuery{
		ContractID: "CAAA", Limit: 2,
		AfterKey: page1[1].KeyXDR, AfterDurability: page1[1].Durability,
	})
	if err != nil || len(page2) != 1 || hasMore {
		t.Fatalf("page2 = %d hasMore=%v err=%v", len(page2), hasMore, err)
	}
	if page2[0].KeyXDR == page1[1].KeyXDR {
		t.Error("pagination overlapped")
	}

	n, err := s.CountStateEntries(context.Background(), "testnet", "CAAA")
	if err != nil || n != 3 {
		t.Errorf("CountStateEntries() = %d, %v", n, err)
	}
}

func TestQueryStateChangesKeyFilter(t *testing.T) {
	s := openTestStore(t)
	truncateState(t, s)

	a1 := stateChange("0000000000000000600-0000000000", 600, "kA", "created", "v1")
	b1 := stateChange("0000000000000000600-0000000001", 600, "kB", "created", "v1")
	b1.ChangeIndex = 1
	a2 := stateChange("0000000000000000601-0000000000", 601, "kA", "updated", "v2")
	a2.TxHash = "second"
	commitStates(t, s, 601, a1, b1, a2)

	history, _, err := s.QueryStateChanges(context.Background(), "testnet", StateQuery{
		ContractID: "CAAA", KeyXDR: "kA", Limit: 10,
	})
	if err != nil {
		t.Fatalf("QueryStateChanges() error = %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("kA history = %d, want 2", len(history))
	}
	if history[0].ID >= history[1].ID {
		t.Error("history not in chain order")
	}
}
