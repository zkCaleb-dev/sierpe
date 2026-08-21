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
		if err := s.CommitLedger(ctx, "testnet", rec, events, nil, nil, nil, nil); err != nil {
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

func TestQueryEvents(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	mk := func(id string, ledger uint32, name, topic0 string) Event {
		e := testEvent(id, "CAAA", "hash-"+id, 0)
		e.LedgerSequence = ledger
		e.EventName = name
		e.Topics = []string{topic0, "dG9waWMx"}
		return e
	}
	seed := []Event{
		mk("0000000000000001000-0000000000", 100, "transfer", "dDA="),
		mk("0000000000000002000-0000000000", 200, "mint", "dDE="),
		mk("0000000000000003000-0000000000", 300, "transfer", "dDA="),
		mk("0000000000000004000-0000000000", 400, "transfer", "dDA="),
	}
	// Insert through the real commit path.
	if err := s.CommitLedger(ctx, "testnet", LedgerRecord{Sequence: 400, Hash: "x", PreviousHash: "y", ClosedAt: time.Now().UTC()}, seed, nil, nil, nil, nil); err != nil {
		t.Fatalf("CommitLedger() error = %v", err)
	}

	t0 := "dDA="
	all, hasMore, err := s.QueryEvents(ctx, "testnet", EventQuery{
		ContractID: "CAAA", FromLedger: 1, Limit: 10,
	})
	if err != nil {
		t.Fatalf("QueryEvents() error = %v", err)
	}
	if len(all) != 4 || hasMore {
		t.Errorf("all = %d hasMore=%v, want 4 false", len(all), hasMore)
	}
	for i := 1; i < len(all); i++ {
		if all[i].ID <= all[i-1].ID {
			t.Errorf("events not in chain order: %s after %s", all[i].ID, all[i-1].ID)
		}
	}

	filtered, _, err := s.QueryEvents(ctx, "testnet", EventQuery{
		ContractID: "CAAA", FromLedger: 1, Limit: 10, Topics: [4]*string{&t0},
	})
	if err != nil {
		t.Fatalf("QueryEvents(topic) error = %v", err)
	}
	if len(filtered) != 3 {
		t.Errorf("topic filter = %d, want 3", len(filtered))
	}

	ranged, _, err := s.QueryEvents(ctx, "testnet", EventQuery{
		ContractID: "CAAA", FromLedger: 150, ToLedger: 350, Limit: 10,
	})
	if err != nil {
		t.Fatalf("QueryEvents(range) error = %v", err)
	}
	if len(ranged) != 2 {
		t.Errorf("range = %d, want 2 (ledgers 200 and 300)", len(ranged))
	}

	page1, more, err := s.QueryEvents(ctx, "testnet", EventQuery{
		ContractID: "CAAA", FromLedger: 1, Limit: 2,
	})
	if err != nil || len(page1) != 2 || !more {
		t.Fatalf("page1 = %d more=%v err=%v, want 2 true nil", len(page1), more, err)
	}
	page2, more, err := s.QueryEvents(ctx, "testnet", EventQuery{
		ContractID: "CAAA", FromLedger: 1, Limit: 10, AfterID: page1[1].ID,
	})
	if err != nil || len(page2) != 2 || more {
		t.Fatalf("page2 = %d more=%v err=%v, want 2 false nil", len(page2), more, err)
	}
	if page2[0].ID <= page1[1].ID {
		t.Errorf("pagination overlapped: %s then %s", page1[1].ID, page2[0].ID)
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
	if err := s.CommitLedger(ctx, "testnet", rec, events, nil, nil, nil, nil); err == nil {
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
