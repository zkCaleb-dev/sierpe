package store

import (
	"context"
	"testing"
	"time"
)

func testMovement(transferID, role, token string, ledger uint32) Movement {
	return Movement{
		ContractID:      "CWATCHER",
		TransferID:      transferID,
		Role:            role,
		TokenContractID: token,
		TransferType:    TransferTypeTransfer,
		Counterparty:    "GOTHER",
		Amount:          "690000000",
		LedgerSequence:  ledger,
		ClosedAt:        time.Unix(1_700_000_000, 0).UTC(),
	}
}

func TestCommitLedgerWithMovementsIsIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE movements, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	rec := LedgerRecord{Sequence: 100, Hash: "aa", PreviousHash: "99", ClosedAt: time.Now().UTC()}
	// One event attributed to the same contract in both roles: a self
	// transfer, which must survive as two rows rather than collide.
	movements := []Movement{
		testMovement("0000000000000000100-0000000000", RoleSender, "CTOKEN", 100),
		testMovement("0000000000000000100-0000000000", RoleRecipient, "CTOKEN", 100),
	}
	for i := 0; i < 2; i++ {
		if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, nil, nil, movements); err != nil {
			t.Fatalf("CommitLedger() attempt %d error = %v", i+1, err)
		}
	}
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM movements`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("rows = %d, want 2 (one per role, replays absorbed)", n)
	}
}

func TestQueryMovements(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE movements, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	mk := func(id string, ledger uint32, role, token, typ string) Movement {
		m := testMovement(id, role, token, ledger)
		m.TransferType = typ
		return m
	}
	seed := []Movement{
		mk("0000000000000000100-0000000000", 100, RoleRecipient, "CUSDC", TransferTypeTransfer),
		mk("0000000000000000200-0000000000", 200, RoleSender, "CUSDC", TransferTypeTransfer),
		mk("0000000000000000300-0000000000", 300, RoleRecipient, "CEURC", TransferTypeMint),
	}
	rec := LedgerRecord{Sequence: 300, Hash: "x", PreviousHash: "y", ClosedAt: time.Now().UTC()}
	if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, nil, nil, seed); err != nil {
		t.Fatalf("CommitLedger() error = %v", err)
	}

	base := MovementQuery{ContractID: "CWATCHER", Limit: 10}

	if rows, _, err := s.QueryMovements(ctx, "testnet", base); err != nil || len(rows) != 3 {
		t.Errorf("unfiltered rows = %d err = %v, want 3", len(rows), err)
	}

	q := base
	q.Role = RoleRecipient
	if rows, _, err := s.QueryMovements(ctx, "testnet", q); err != nil || len(rows) != 2 {
		t.Errorf("role filter rows = %d err = %v, want 2", len(rows), err)
	}

	q = base
	q.Token = "CUSDC"
	if rows, _, err := s.QueryMovements(ctx, "testnet", q); err != nil || len(rows) != 2 {
		t.Errorf("token filter rows = %d err = %v, want 2", len(rows), err)
	}

	q = base
	q.Type = TransferTypeMint
	if rows, _, err := s.QueryMovements(ctx, "testnet", q); err != nil || len(rows) != 1 {
		t.Errorf("type filter rows = %d err = %v, want 1", len(rows), err)
	}

	q = base
	q.FromLedger, q.ToLedger = 150, 250
	if rows, _, err := s.QueryMovements(ctx, "testnet", q); err != nil || len(rows) != 1 {
		t.Errorf("bounds rows = %d err = %v, want 1", len(rows), err)
	}

	// Pagination without overlap.
	q = base
	q.Limit = 2
	page1, hasMore, err := s.QueryMovements(ctx, "testnet", q)
	if err != nil || !hasMore || len(page1) != 2 {
		t.Fatalf("page 1 = %d hasMore = %v err = %v", len(page1), hasMore, err)
	}
	// The resume position is the whole row key; half of it re-serves the
	// boundary row (see TestQueryMovementsPagesAcrossRolesOfOneEvent).
	last := page1[len(page1)-1]
	q.AfterID, q.AfterRole = last.TransferID, last.Role
	page2, hasMore2, err := s.QueryMovements(ctx, "testnet", q)
	if err != nil || hasMore2 || len(page2) != 1 {
		t.Fatalf("page 2 = %d hasMore = %v err = %v", len(page2), hasMore2, err)
	}
	if page2[0].TransferID <= page1[1].TransferID {
		t.Errorf("page 2 overlaps page 1")
	}
}

func TestMovementAmountIsExact(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE movements, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	max128 := "170141183460469231731687303715884105727"
	m := testMovement("0000000000000000100-0000000000", RoleRecipient, "CTOKEN", 100)
	m.Amount = max128
	m.Counterparty = "" // mint: no sender
	m.TransferType = TransferTypeMint

	rec := LedgerRecord{Sequence: 100, Hash: "aa", PreviousHash: "99", ClosedAt: time.Now().UTC()}
	if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, nil, nil, []Movement{m}); err != nil {
		t.Fatalf("CommitLedger() error = %v", err)
	}
	rows, _, err := s.QueryMovements(ctx, "testnet", MovementQuery{ContractID: "CWATCHER", Limit: 10})
	if err != nil || len(rows) != 1 {
		t.Fatalf("read back: %d rows err = %v", len(rows), err)
	}
	if rows[0].Amount != max128 {
		t.Errorf("amount = %q, want %q", rows[0].Amount, max128)
	}
	if rows[0].Counterparty != "" {
		t.Errorf("a mint has no counterparty, got %q", rows[0].Counterparty)
	}
}

// The row key is (transfer_id, role), so the keyset must be too. A page
// boundary falling inside a self transfer used to drop one of its two rows
// forever: resuming at transfer_id alone skipped every row sharing that id.
func TestQueryMovementsPagesAcrossRolesOfOneEvent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE movements, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	selfTransfer := "0000000000000000100-0000000000"
	seed := []Movement{
		testMovement(selfTransfer, RoleRecipient, "CTOKEN", 100),
		testMovement(selfTransfer, RoleSender, "CTOKEN", 100),
		testMovement("0000000000000000200-0000000000", RoleRecipient, "CTOKEN", 200),
	}
	rec := LedgerRecord{Sequence: 200, Hash: "x", PreviousHash: "y", ClosedAt: time.Now().UTC()}
	if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, nil, nil, seed); err != nil {
		t.Fatalf("CommitLedger() error = %v", err)
	}

	q := MovementQuery{ContractID: "CWATCHER", Limit: 1}
	seen := map[string]bool{}
	for page := 0; page < 5; page++ {
		rows, hasMore, err := s.QueryMovements(ctx, "testnet", q)
		if err != nil {
			t.Fatalf("page %d error = %v", page, err)
		}
		for _, m := range rows {
			key := m.TransferID + "/" + m.Role
			if seen[key] {
				t.Fatalf("page %d served %s twice", page, key)
			}
			seen[key] = true
		}
		if !hasMore {
			break
		}
		last := rows[len(rows)-1]
		q.AfterID, q.AfterRole = last.TransferID, last.Role
	}
	if len(seen) != 3 {
		t.Errorf("walked %d rows across pages, want 3: %v", len(seen), seen)
	}
}
