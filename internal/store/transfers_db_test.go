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
		if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, transfers); err != nil {
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
	if err := s.CommitLedger(ctx, "testnet", rec, nil, nil, []Transfer{tr}); err != nil {
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
