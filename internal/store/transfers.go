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

// TransferQuery selects transfers for the public API. Zero values widen:
// empty filters are wildcards, ToLedger 0 is unbounded, AfterID empty
// starts at the beginning.
type TransferQuery struct {
	ContractID string
	// Account matches either side of the movement; From and To match one
	// side exactly. Account cannot be combined with From/To (validated at
	// the handler).
	Account      string
	From         string
	To           string
	TransferType string // transfer | mint | burn | clawback; "" = all
	FromLedger   uint32
	ToLedger     uint32
	AfterID      string
	Limit        int
}

// QueryTransfers returns up to Limit transfers in chain order (id ascending)
// and whether more rows match beyond the page.
func (s *Store) QueryTransfers(ctx context.Context, network string, q TransferQuery) ([]Transfer, bool, error) {
	sql := `
		SELECT id, contract_id, ledger_sequence, closed_at, tx_hash,
		       tx_index, op_index, event_index, transfer_type,
		       COALESCE(from_address, ''), COALESCE(to_address, ''),
		       COALESCE(to_muxed_id, ''), amount::text, COALESCE(asset, '')
		FROM transfers
		WHERE network = $1 AND contract_id = $2 AND ledger_sequence >= $3`
	args := []any{network, q.ContractID, int64(q.FromLedger)}
	if q.ToLedger > 0 {
		args = append(args, int64(q.ToLedger))
		sql += fmt.Sprintf(" AND ledger_sequence <= $%d", len(args))
	}
	if q.AfterID != "" {
		args = append(args, q.AfterID)
		sql += fmt.Sprintf(" AND id > $%d", len(args))
	}
	if q.TransferType != "" {
		args = append(args, q.TransferType)
		sql += fmt.Sprintf(" AND transfer_type = $%d", len(args))
	}
	if q.Account != "" {
		args = append(args, q.Account)
		sql += fmt.Sprintf(" AND (from_address = $%d OR to_address = $%d)", len(args), len(args))
	}
	if q.From != "" {
		args = append(args, q.From)
		sql += fmt.Sprintf(" AND from_address = $%d", len(args))
	}
	if q.To != "" {
		args = append(args, q.To)
		sql += fmt.Sprintf(" AND to_address = $%d", len(args))
	}
	// One extra row answers has-more without a second query.
	args = append(args, q.Limit+1)
	sql += fmt.Sprintf(" ORDER BY id LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, false, fmt.Errorf("store: query transfers: %w", err)
	}
	defer rows.Close()

	var out []Transfer
	for rows.Next() {
		var t Transfer
		var seq int64
		if err := rows.Scan(&t.ID, &t.ContractID, &seq, &t.ClosedAt, &t.TxHash,
			&t.TxIndex, &t.OpIndex, &t.EventIndex, &t.TransferType,
			&t.FromAddress, &t.ToAddress, &t.ToMuxedID, &t.Amount, &t.Asset); err != nil {
			return nil, false, fmt.Errorf("store: scan transfer: %w", err)
		}
		t.LedgerSequence = uint32(seq)
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("store: query transfers: %w", err)
	}
	hasMore := len(out) > q.Limit
	if hasMore {
		out = out[:q.Limit]
	}
	return out, hasMore, nil
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
