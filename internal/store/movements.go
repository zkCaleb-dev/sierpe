package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// KindMovements derives token movements a registered contract takes part
// in, in either direction, whoever emitted the event. It answers "what
// moved in and out of my contract" — the question KindTransfers cannot,
// because a payment to a contract is emitted by the asset's SAC.
//
// Deliberately not called "deposits": a one-directional view is a
// monotonically increasing number that reads exactly like a balance and is
// indistinguishable from one until the first outflow that never shows up.
const KindMovements = "movements"

// Movement roles.
const (
	RoleSender    = "sender"
	RoleRecipient = "recipient"
)

// Movement is one attribution of a token transfer to a contract that took
// part in it. TransferID is the shared identity of the underlying event,
// not a foreign key: the emitting token is usually NOT registered, so a
// transfers row generally does not exist. Every field a consumer needs is
// carried here for that reason.
type Movement struct {
	ContractID      string // the watcher
	TransferID      string // the event id, shared with transfers when that row exists
	Role            string // sender | recipient
	TokenContractID string // the emitting token contract: the asset's identity
	TransferType    string
	Counterparty    string // the other side; empty on mint (in) and burn (out)
	Amount          string // i128 as decimal text, raw token units
	LedgerSequence  uint32
	ClosedAt        time.Time
}

// insertMovements batches a ledger's movement attributions into the open
// transaction. Replays are absorbed by the primary key.
func insertMovements(ctx context.Context, tx pgx.Tx, network string, movements []Movement) error {
	if len(movements) == 0 {
		return nil
	}
	nullable := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	batch := &pgx.Batch{}
	for _, m := range movements {
		batch.Queue(`
			INSERT INTO movements (
				network, contract_id, transfer_id, role, token_contract_id,
				transfer_type, counterparty, amount, ledger_sequence, closed_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9,$10)
			ON CONFLICT (network, contract_id, transfer_id, role) DO NOTHING`,
			network, m.ContractID, m.TransferID, m.Role, m.TokenContractID,
			m.TransferType, nullable(m.Counterparty), m.Amount,
			int64(m.LedgerSequence), m.ClosedAt,
		)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range movements {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("store: insert movement: %w", err)
		}
	}
	return nil
}

// MovementQuery selects one contract's movements. Zero values widen.
type MovementQuery struct {
	ContractID string
	Role       string // sender | recipient; "" = both
	Token      string // emitting token contract; "" = any
	Type       string // transfer | mint | burn | clawback; "" = any
	FromLedger uint32
	ToLedger   uint32
	// AfterID and AfterRole resume the walk. Both are needed: a
	// self-transfer is one event with two attributions, so transfer_id
	// alone is not unique and a page boundary landing between the two
	// halves would skip one of them forever.
	AfterID   string
	AfterRole string
	Limit     int
}

// QueryMovements returns up to Limit movements in chain order and whether
// more rows match beyond the page.
func (s *Store) QueryMovements(ctx context.Context, network string, q MovementQuery) ([]Movement, bool, error) {
	sql := `
		SELECT contract_id, transfer_id, role, token_contract_id, transfer_type,
		       COALESCE(counterparty, ''), amount::text, ledger_sequence, closed_at
		FROM movements
		WHERE network = $1 AND contract_id = $2 AND ledger_sequence >= $3`
	args := []any{network, q.ContractID, int64(q.FromLedger)}
	if q.ToLedger > 0 {
		args = append(args, int64(q.ToLedger))
		sql += fmt.Sprintf(" AND ledger_sequence <= $%d", len(args))
	}
	if q.Role != "" {
		args = append(args, q.Role)
		sql += fmt.Sprintf(" AND role = $%d", len(args))
	}
	if q.Token != "" {
		args = append(args, q.Token)
		sql += fmt.Sprintf(" AND token_contract_id = $%d", len(args))
	}
	if q.Type != "" {
		args = append(args, q.Type)
		sql += fmt.Sprintf(" AND transfer_type = $%d", len(args))
	}
	if q.AfterID != "" {
		// Row-value comparison over the full sort key, matching the primary
		// key exactly so the walk stays index-only.
		args = append(args, q.AfterID, q.AfterRole)
		sql += fmt.Sprintf(" AND (transfer_id, role) > ($%d, $%d)", len(args)-1, len(args))
	}
	// One extra row answers has-more without a second query.
	args = append(args, q.Limit+1)
	sql += fmt.Sprintf(" ORDER BY transfer_id, role LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, false, fmt.Errorf("store: query movements: %w", err)
	}
	defer rows.Close()

	var out []Movement
	for rows.Next() {
		var m Movement
		var seq int64
		if err := rows.Scan(&m.ContractID, &m.TransferID, &m.Role, &m.TokenContractID,
			&m.TransferType, &m.Counterparty, &m.Amount, &seq, &m.ClosedAt); err != nil {
			return nil, false, fmt.Errorf("store: scan movement: %w", err)
		}
		m.LedgerSequence = uint32(seq)
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("store: query movements: %w", err)
	}
	hasMore := len(out) > q.Limit
	if hasMore {
		out = out[:q.Limit]
	}
	return out, hasMore, nil
}
