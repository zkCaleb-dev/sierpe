package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// KindTrustlines derives classic trustline changes for the asset a SAC
// wraps, plus the current holder snapshot. Opt-in through kinds: volume is
// proportional to the asset's classic activity, not to the contract's.
// Native XLM has no trustlines, so the kind observes issued assets only.
const KindTrustlines = "trustlines"

// TrustlineChange is one classic trustline entry change attributed to the
// SAC contract wrapping its asset.
type TrustlineChange struct {
	ID             string // {toid}-{change_index}, zero-padded, sortable
	ContractID     string // the SAC
	AccountID      string // the trustline holder
	Asset          string // SEP-0011 canonical CODE:ISSUER
	LedgerSequence uint32
	ClosedAt       time.Time
	TxHash         string
	TxIndex        int32
	OpIndex        int32
	ChangeIndex    int32
	ChangeType     string // created | updated | removed | restored
	PreBalance     *int64 // nil on create
	PostBalance    *int64 // nil on remove
	PreLimit       *int64
	PostLimit      *int64
	Flags          uint32 // surviving side's authorization flags
}

// TrustlineEntry is one row of the current holder snapshot.
type TrustlineEntry struct {
	ContractID string
	AccountID  string
	Asset      string
	Balance    *int64 // nil when deleted (tombstone)
	Limit      *int64
	Flags      uint32
	Deleted    bool
	LastLedger uint32
	ClosedAt   time.Time
}

// insertTrustlineChanges appends history rows; the idempotency key absorbs
// replays exactly like state changes.
func insertTrustlineChanges(ctx context.Context, tx pgx.Tx, network string, changes []TrustlineChange) error {
	if len(changes) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, c := range changes {
		batch.Queue(`
			INSERT INTO trustline_changes (
				network, id, contract_id, account_id, asset,
				ledger_sequence, closed_at, tx_hash, tx_index, op_index,
				change_index, change_type, pre_balance, post_balance,
				pre_limit, post_limit, flags
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
			ON CONFLICT ON CONSTRAINT trustline_changes_idempotency DO NOTHING`,
			network, c.ID, c.ContractID, c.AccountID, c.Asset,
			int64(c.LedgerSequence), c.ClosedAt, c.TxHash, c.TxIndex, c.OpIndex,
			c.ChangeIndex, c.ChangeType, c.PreBalance, c.PostBalance,
			c.PreLimit, c.PostLimit, int64(c.Flags),
		)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range changes {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("store: insert trustline change: %w", err)
		}
	}
	return nil
}

// applyTrustlineEntries folds a ledger's trustline changes into the holder
// snapshot with the same convergence guard as state entries: later changes
// in the slice win, and last_ledger keeps out-of-order replays out.
func applyTrustlineEntries(ctx context.Context, tx pgx.Tx, network string, changes []TrustlineChange) error {
	if len(changes) == 0 {
		return nil
	}
	type entryKey struct{ contract, account string }
	final := make(map[entryKey]TrustlineChange, len(changes))
	order := make([]entryKey, 0, len(changes))
	for _, c := range changes {
		k := entryKey{c.ContractID, c.AccountID}
		if _, seen := final[k]; !seen {
			order = append(order, k)
		}
		final[k] = c
	}

	batch := &pgx.Batch{}
	for _, k := range order {
		c := final[k]
		deleted := c.ChangeType == "removed"
		var balance, limit *int64
		if !deleted {
			balance, limit = c.PostBalance, c.PostLimit
		}
		batch.Queue(`
			INSERT INTO trustline_entries (
				network, contract_id, account_id, asset,
				balance, trust_limit, flags, deleted, last_ledger, closed_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			ON CONFLICT (network, contract_id, account_id) DO UPDATE
			SET asset       = EXCLUDED.asset,
			    balance     = EXCLUDED.balance,
			    trust_limit = EXCLUDED.trust_limit,
			    flags       = EXCLUDED.flags,
			    deleted     = EXCLUDED.deleted,
			    last_ledger = EXCLUDED.last_ledger,
			    closed_at   = EXCLUDED.closed_at,
			    updated_at  = now()
			WHERE trustline_entries.last_ledger <= EXCLUDED.last_ledger`,
			network, c.ContractID, c.AccountID, c.Asset,
			balance, limit, int64(c.Flags), deleted, int64(c.LedgerSequence), c.ClosedAt,
		)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range order {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("store: apply trustline entry: %w", err)
		}
	}
	return nil
}

// TrustlineQuery selects trustline snapshot rows or history for the public
// API. Zero values widen.
type TrustlineQuery struct {
	ContractID   string
	AccountID    string // exact holder filter; "" = all
	FromLedger   uint32
	ToLedger     uint32
	AfterID      string // history pagination (change id ascending)
	AfterAccount string // snapshot pagination (account ascending)
	Limit        int
}

// QueryTrustlineEntries returns live (non-tombstone) holder rows in account
// order and whether more rows match beyond the page.
func (s *Store) QueryTrustlineEntries(ctx context.Context, network string, q TrustlineQuery) ([]TrustlineEntry, bool, error) {
	sql := `
		SELECT contract_id, account_id, asset, balance, trust_limit,
		       flags, last_ledger, closed_at
		FROM trustline_entries
		WHERE network = $1 AND contract_id = $2 AND NOT deleted`
	args := []any{network, q.ContractID}
	if q.AccountID != "" {
		args = append(args, q.AccountID)
		sql += fmt.Sprintf(" AND account_id = $%d", len(args))
	}
	if q.AfterAccount != "" {
		args = append(args, q.AfterAccount)
		sql += fmt.Sprintf(" AND account_id > $%d", len(args))
	}
	args = append(args, q.Limit+1)
	sql += fmt.Sprintf(" ORDER BY account_id LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, false, fmt.Errorf("store: query trustline entries: %w", err)
	}
	defer rows.Close()

	var out []TrustlineEntry
	for rows.Next() {
		var e TrustlineEntry
		var lastLedger, flags int64
		if err := rows.Scan(&e.ContractID, &e.AccountID, &e.Asset, &e.Balance,
			&e.Limit, &flags, &lastLedger, &e.ClosedAt); err != nil {
			return nil, false, fmt.Errorf("store: scan trustline entry: %w", err)
		}
		e.LastLedger = uint32(lastLedger)
		e.Flags = uint32(flags)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("store: query trustline entries: %w", err)
	}
	hasMore := len(out) > q.Limit
	if hasMore {
		out = out[:q.Limit]
	}
	return out, hasMore, nil
}

// QueryTrustlineChanges returns history rows in chain order (id ascending)
// and whether more rows match beyond the page.
func (s *Store) QueryTrustlineChanges(ctx context.Context, network string, q TrustlineQuery) ([]TrustlineChange, bool, error) {
	sql := `
		SELECT id, contract_id, account_id, asset, ledger_sequence, closed_at,
		       tx_hash, tx_index, op_index, change_index, change_type,
		       pre_balance, post_balance, pre_limit, post_limit, flags
		FROM trustline_changes
		WHERE network = $1 AND contract_id = $2 AND ledger_sequence >= $3`
	args := []any{network, q.ContractID, int64(q.FromLedger)}
	if q.ToLedger > 0 {
		args = append(args, int64(q.ToLedger))
		sql += fmt.Sprintf(" AND ledger_sequence <= $%d", len(args))
	}
	if q.AccountID != "" {
		args = append(args, q.AccountID)
		sql += fmt.Sprintf(" AND account_id = $%d", len(args))
	}
	if q.AfterID != "" {
		args = append(args, q.AfterID)
		sql += fmt.Sprintf(" AND id > $%d", len(args))
	}
	args = append(args, q.Limit+1)
	sql += fmt.Sprintf(" ORDER BY id LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, false, fmt.Errorf("store: query trustline changes: %w", err)
	}
	defer rows.Close()

	var out []TrustlineChange
	for rows.Next() {
		var c TrustlineChange
		var seq, flags int64
		if err := rows.Scan(&c.ID, &c.ContractID, &c.AccountID, &c.Asset, &seq,
			&c.ClosedAt, &c.TxHash, &c.TxIndex, &c.OpIndex, &c.ChangeIndex,
			&c.ChangeType, &c.PreBalance, &c.PostBalance, &c.PreLimit,
			&c.PostLimit, &flags); err != nil {
			return nil, false, fmt.Errorf("store: scan trustline change: %w", err)
		}
		c.LedgerSequence = uint32(seq)
		c.Flags = uint32(flags)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("store: query trustline changes: %w", err)
	}
	hasMore := len(out) > q.Limit
	if hasMore {
		out = out[:q.Limit]
	}
	return out, hasMore, nil
}
