package store

import (
	"context"
	"testing"
	"time"
)

func testEvent(id, contractID, txHash string, eventIndex int32) Event {
	return Event{
		ID:             id,
		ContractID:     contractID,
		LedgerSequence: 100,
		ClosedAt:       time.Unix(1_700_000_000, 0).UTC(),
		TxHash:         txHash,
		TxIndex:        1,
		OpIndex:        0,
		EventIndex:     eventIndex,
		EventName:      "transfer",
		Topics:         []string{"dG9waWMw", "dG9waWMx"},
		ValueXDR:       "dmFsdWU=",
		RawXDR:         "cmF3",
	}
}

func TestCommitLedgerWithEventsIsIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE events, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	rec := LedgerRecord{Sequence: 100, Hash: "aa", PreviousHash: "99", ClosedAt: time.Now().UTC()}
	events := []Event{
		testEvent("0000000000000000100-0000000000", "CAAA", "dead", 0),
		testEvent("0000000000000000100-0000000001", "CAAA", "dead", 1),
	}

	for i := 0; i < 2; i++ {
		if err := s.CommitLedger(ctx, "testnet", rec, events); err != nil {
			t.Fatalf("CommitLedger() attempt %d error = %v", i+1, err)
		}
	}

	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 2 {
		t.Errorf("events rows = %d, want 2: the idempotency key must absorb replays", n)
	}

	counts, err := s.EventCountsByName(ctx, "testnet", "CAAA")
	if err != nil {
		t.Fatalf("EventCountsByName() error = %v", err)
	}
	if counts["transfer"] != 2 {
		t.Errorf("counts = %v, want transfer:2", counts)
	}
}

func TestCommitLedgerRollsBackEventsWithCursor(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE events, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// A batch with a duplicated primary key inside the same ledger must fail
	// the whole transaction: no cursor advance, no partial events (rule 1).
	rec := LedgerRecord{Sequence: 100, Hash: "aa", PreviousHash: "99", ClosedAt: time.Now().UTC()}
	events := []Event{
		testEvent("0000000000000000100-0000000000", "CAAA", "dead", 0),
		testEvent("0000000000000000100-0000000000", "CBBB", "beef", 0),
	}
	if err := s.CommitLedger(ctx, "testnet", rec, events); err == nil {
		t.Fatal("CommitLedger() with conflicting ids must fail")
	}

	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 0 {
		t.Errorf("events rows = %d, want 0 after rollback", n)
	}
	if _, err := s.LoadCursor(ctx, "testnet"); err == nil {
		t.Error("cursor must not advance when the event batch fails")
	}
}
