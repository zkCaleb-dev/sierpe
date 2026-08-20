package store

import (
	"context"
	"testing"
	"time"
)

func testTransfer(id, txHash string, eventIndex int32) Transfer {
	return Transfer{
		ID:             id,
		ContractID:     "CAAA",
		LedgerSequence: 100,
		ClosedAt:       time.Unix(1_700_000_000, 0).UTC(),
		TxHash:         txHash,
		TxIndex:        1,
		OpIndex:        0,
		EventIndex:     eventIndex,
		TransferType:   TransferTypeTransfer,
		FromAddress:    "GFROM",
		ToAddress:      "GTO",
		Amount:         "5000",
		Asset:          "native",
	}
}

func TestCommitLedgerWithTransfersIsIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE transfers, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	rec := LedgerRecord{Sequence: 100, Hash: "aa", PreviousHash: "99", ClosedAt: time.Now().UTC()}
	transfers := []Transfer{
		testTransfer("0000000000000000100-0000000000", "dead", 0),
		testTransfer("0000000000000000100-0000000001", "dead", 1),
	}

	for i := 0; i < 2; i++ {
		if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, transfers, nil); err != nil {
			t.Fatalf("CommitLedger() attempt %d error = %v", i+1, err)
		}
	}

	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM transfers`).Scan(&n); err != nil {
		t.Fatalf("count transfers: %v", err)
	}
	if n != 2 {
		t.Errorf("transfers rows = %d, want 2: the idempotency key must absorb replays", n)
	}
}

func TestQueryTransfers(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE transfers, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	mk := func(id string, ledger uint32, tt, from, to string) Transfer {
		tr := testTransfer(id, "hash-"+id, 0)
		tr.LedgerSequence = ledger
		tr.TransferType = tt
		tr.FromAddress = from
		tr.ToAddress = to
		return tr
	}
	seed := []Transfer{
		mk("0000000000000000100-0000000000", 100, TransferTypeTransfer, "GALICE", "GBOB"),
		mk("0000000000000000200-0000000000", 200, TransferTypeMint, "", "GALICE"),
		mk("0000000000000000300-0000000000", 300, TransferTypeBurn, "GCAROL", ""),
		mk("0000000000000000400-0000000000", 400, TransferTypeTransfer, "GBOB", "GCAROL"),
	}
	rec := LedgerRecord{Sequence: 400, Hash: "x", PreviousHash: "y", ClosedAt: time.Now().UTC()}
	if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, seed, nil); err != nil {
		t.Fatalf("CommitLedger() error = %v", err)
	}

	base := TransferQuery{ContractID: "CAAA", Limit: 10}

	q := base
	q.Account = "GALICE"
	rows, _, err := s.QueryTransfers(ctx, "testnet", q)
	if err != nil {
		t.Fatalf("QueryTransfers(account) error = %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("account filter matched %d rows, want 2 (both sides)", len(rows))
	}

	q = base
	q.From = "GBOB"
	if rows, _, err = s.QueryTransfers(ctx, "testnet", q); err != nil || len(rows) != 1 || rows[0].ToAddress != "GCAROL" {
		t.Errorf("from filter rows = %+v err = %v", rows, err)
	}

	q = base
	q.TransferType = TransferTypeMint
	if rows, _, err = s.QueryTransfers(ctx, "testnet", q); err != nil || len(rows) != 1 || rows[0].FromAddress != "" {
		t.Errorf("type filter rows = %+v err = %v", rows, err)
	}

	q = base
	q.FromLedger, q.ToLedger = 150, 350
	if rows, _, err = s.QueryTransfers(ctx, "testnet", q); err != nil || len(rows) != 2 {
		t.Errorf("ledger bounds rows = %d err = %v, want 2", len(rows), err)
	}

	// Pagination: limit 2 has more; the cursor walk has no overlap.
	q = base
	q.Limit = 2
	rows, hasMore, err := s.QueryTransfers(ctx, "testnet", q)
	if err != nil || !hasMore || len(rows) != 2 {
		t.Fatalf("page 1 rows = %d hasMore = %v err = %v", len(rows), hasMore, err)
	}
	q.AfterID = rows[len(rows)-1].ID
	rows2, hasMore2, err := s.QueryTransfers(ctx, "testnet", q)
	if err != nil || hasMore2 || len(rows2) != 2 {
		t.Fatalf("page 2 rows = %d hasMore = %v err = %v", len(rows2), hasMore2, err)
	}
	if rows2[0].ID <= rows[1].ID {
		t.Errorf("page 2 overlaps page 1: %s <= %s", rows2[0].ID, rows[1].ID)
	}
}

func TestTransfersNumericAmountIsExact(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE transfers, cursor, ledger_hashes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Full i128 max: any float or bigint column would mangle this.
	max128 := "170141183460469231731687303715884105727"
	tr := testTransfer("0000000000000000100-0000000000", "dead", 0)
	tr.Amount = max128
	tr.ToMuxedID = "18446744073709551615" // u64 max
	tr.ToAddress = ""                     // burn-style null
	tr.TransferType = TransferTypeBurn

	rec := LedgerRecord{Sequence: 100, Hash: "aa", PreviousHash: "99", ClosedAt: time.Now().UTC()}
	if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, []Transfer{tr}, nil); err != nil {
		t.Fatalf("CommitLedger() error = %v", err)
	}

	var amount, muxed string
	var to *string
	err := s.pool.QueryRow(ctx,
		`SELECT amount::text, to_muxed_id, to_address FROM transfers WHERE id = $1`,
		tr.ID,
	).Scan(&amount, &muxed, &to)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if amount != max128 {
		t.Errorf("amount = %q, want %q", amount, max128)
	}
	if muxed != tr.ToMuxedID {
		t.Errorf("to_muxed_id = %q, want %q", muxed, tr.ToMuxedID)
	}
	if to != nil {
		t.Errorf("to_address = %v, want NULL", *to)
	}
}
