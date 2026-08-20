package store

import (
	"context"
	"testing"
	"time"
)

func i64(v int64) *int64 { return &v }

func testTrustlineChange(id string, ledger uint32, changeType, account string, post *int64) TrustlineChange {
	return TrustlineChange{
		ID:             id,
		ContractID:     "CAAA",
		AccountID:      account,
		Asset:          "USDA:GISSUER",
		LedgerSequence: ledger,
		ClosedAt:       time.Unix(1_700_000_000, 0).UTC(),
		TxHash:         "hash-" + id,
		TxIndex:        1,
		ChangeIndex:    0,
		ChangeType:     changeType,
		PreBalance:     i64(1),
		PostBalance:    post,
		PostLimit:      i64(1000),
		Flags:          1,
	}
}

func TestCommitLedgerWithTrustlinesIsIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE trustline_changes, trustline_entries, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	rec := LedgerRecord{Sequence: 100, Hash: "aa", PreviousHash: "99", ClosedAt: time.Now().UTC()}
	changes := []TrustlineChange{
		testTrustlineChange("0000000000000000100-0000000000", 100, "created", "GALICE", i64(500)),
	}
	for i := 0; i < 2; i++ {
		if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, nil, changes); err != nil {
			t.Fatalf("CommitLedger() attempt %d error = %v", i+1, err)
		}
	}

	var history, entries int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM trustline_changes`).Scan(&history); err != nil {
		t.Fatalf("count changes: %v", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM trustline_entries`).Scan(&entries); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	if history != 1 || entries != 1 {
		t.Errorf("history = %d entries = %d, want 1/1", history, entries)
	}
}

func TestTrustlineEntriesGuardAndTombstones(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE trustline_changes, trustline_entries, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	commit := func(seq uint32, hash string, changes ...TrustlineChange) {
		t.Helper()
		rec := LedgerRecord{Sequence: seq, Hash: hash, PreviousHash: "p" + hash, ClosedAt: time.Now().UTC()}
		if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, nil, changes); err != nil {
			t.Fatalf("CommitLedger(%d) error = %v", seq, err)
		}
	}

	// Ledger 300: balance 900. Ledger 200 replayed afterwards must lose.
	commit(300, "a", testTrustlineChange("0000000000000000300-0000000000", 300, "updated", "GALICE", i64(900)))
	commit(200, "b", testTrustlineChange("0000000000000000200-0000000000", 200, "updated", "GALICE", i64(100)))

	var balance int64
	if err := s.pool.QueryRow(ctx,
		`SELECT balance FROM trustline_entries WHERE account_id = 'GALICE'`).Scan(&balance); err != nil {
		t.Fatalf("read balance: %v", err)
	}
	if balance != 900 {
		t.Errorf("balance = %d, want 900: older replay must not overwrite newer state", balance)
	}

	// Removal at 400 leaves a tombstone; a stale 350 update must not
	// resurrect it.
	commit(400, "c", testTrustlineChange("0000000000000000400-0000000000", 400, "removed", "GALICE", nil))
	commit(350, "d", testTrustlineChange("0000000000000000350-0000000000", 350, "updated", "GALICE", i64(700)))

	var deleted bool
	if err := s.pool.QueryRow(ctx,
		`SELECT deleted FROM trustline_entries WHERE account_id = 'GALICE'`).Scan(&deleted); err != nil {
		t.Fatalf("read tombstone: %v", err)
	}
	if !deleted {
		t.Error("tombstone resurrected by a stale replay")
	}

	// The snapshot query never serves tombstones.
	rows, _, err := s.QueryTrustlineEntries(ctx, "testnet", TrustlineQuery{ContractID: "CAAA", Limit: 10})
	if err != nil {
		t.Fatalf("QueryTrustlineEntries() error = %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("snapshot rows = %d, want 0 (deleted entries hidden)", len(rows))
	}
}

func TestQueryTrustlines(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE trustline_changes, trustline_entries, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	rec := LedgerRecord{Sequence: 400, Hash: "x", PreviousHash: "y", ClosedAt: time.Now().UTC()}
	changes := []TrustlineChange{
		testTrustlineChange("0000000000000000100-0000000000", 100, "created", "GALICE", i64(500)),
		testTrustlineChange("0000000000000000200-0000000000", 200, "created", "GBOB", i64(300)),
		testTrustlineChange("0000000000000000300-0000000000", 300, "updated", "GALICE", i64(600)),
	}
	if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, nil, changes); err != nil {
		t.Fatalf("CommitLedger() error = %v", err)
	}

	// History: account filter and ledger bounds.
	hist, _, err := s.QueryTrustlineChanges(ctx, "testnet",
		TrustlineQuery{ContractID: "CAAA", AccountID: "GALICE", Limit: 10})
	if err != nil || len(hist) != 2 {
		t.Errorf("account history rows = %d err = %v, want 2", len(hist), err)
	}
	hist, _, err = s.QueryTrustlineChanges(ctx, "testnet",
		TrustlineQuery{ContractID: "CAAA", FromLedger: 150, ToLedger: 250, Limit: 10})
	if err != nil || len(hist) != 1 || hist[0].AccountID != "GBOB" {
		t.Errorf("bounded history rows = %+v err = %v", hist, err)
	}

	// Snapshot: two live entries in account order; pagination has no overlap.
	entries, hasMore, err := s.QueryTrustlineEntries(ctx, "testnet",
		TrustlineQuery{ContractID: "CAAA", Limit: 1})
	if err != nil || !hasMore || len(entries) != 1 || entries[0].AccountID != "GALICE" {
		t.Fatalf("snapshot page 1 = %+v hasMore = %v err = %v", entries, hasMore, err)
	}
	if *entries[0].Balance != 600 {
		t.Errorf("balance = %d, want 600 (latest change wins)", *entries[0].Balance)
	}
	page2, hasMore2, err := s.QueryTrustlineEntries(ctx, "testnet",
		TrustlineQuery{ContractID: "CAAA", AfterAccount: "GALICE", Limit: 1})
	if err != nil || hasMore2 || len(page2) != 1 || page2[0].AccountID != "GBOB" {
		t.Errorf("snapshot page 2 = %+v hasMore = %v err = %v", page2, hasMore2, err)
	}
}
