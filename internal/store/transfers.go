package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// KindTransfers derives decoded token movements from SEP-41 token events.
// SAC registrations get it by default; custom tokens opt in through kinds.
const KindTransfers = "transfers"

// Transfer types: the four SEP-41 movement events.
const (
	TransferTypeTransfer = "transfer"
	TransferTypeMint     = "mint"
	TransferTypeBurn     = "burn"
	TransferTypeClawback = "clawback"
)

// Transfer is one decoded token movement. It shares its identity with the
// event it was derived from ({toid}-{event_index}), so raw and derived rows
// always join and the derivation can be re-run offline.
type Transfer struct {
	ID             string
	ContractID     string
	LedgerSequence uint32
	ClosedAt       time.Time
	TxHash         string
	TxIndex        int32
	OpIndex        int32
	EventIndex     int32
	TransferType   string // transfer | mint | burn | clawback
	FromAddress    string // empty on mint
	ToAddress      string // empty on burn and clawback
	ToMuxedID      string // CAP-67 map destination id, when present
	Amount         string // i128 as decimal text, raw token units
	Asset          string // SEP-0011 string from the SAC topic; empty otherwise
}

// insertTransfers batches the ledger's transfers into the open transaction.
// The idempotency key absorbs replays: conflicting rows are left untouched.
func insertTransfers(ctx context.Context, tx pgx.Tx, network string, transfers []Transfer) error {
	if len(transfers) == 0 {
		return nil
	}
	nullable := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	batch := &pgx.Batch{}
	for _, t := range transfers {
		batch.Queue(`
			INSERT INTO transfers (
				network, id, contract_id, ledger_sequence, closed_at,
				tx_hash, tx_index, op_index, event_index, transfer_type,
				from_address, to_address, to_muxed_id, amount, asset
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::numeric,$15)
			ON CONFLICT ON CONSTRAINT transfers_idempotency DO NOTHING`,
			network, t.ID, t.ContractID, int64(t.LedgerSequence), t.ClosedAt,
			t.TxHash, t.TxIndex, t.OpIndex, t.EventIndex, t.TransferType,
			nullable(t.FromAddress), nullable(t.ToAddress), nullable(t.ToMuxedID),
			t.Amount, nullable(t.Asset),
		)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range transfers {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("store: insert transfer: %w", err)
		}
	}
	return nil
}
