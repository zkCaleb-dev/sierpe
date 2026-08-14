package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// KindState derives contract storage changes and the current snapshot.
const KindState = "state"

// StateChange is one contract-data entry change with full provenance
// (decision D8: history first, snapshot derived).
type StateChange struct {
	ID             string // {toid}-{change_index}, zero-padded, sortable
	ContractID     string
	LedgerSequence uint32
	ClosedAt       time.Time
	TxHash         string
	TxIndex        int32
	OpIndex        int32
	ChangeIndex    int32  // position among the transaction's changes
	ChangeType     string // created | updated | removed | restored
	KeyXDR         string // base64 ScVal
	Durability     string // temporary | persistent
	PreXDR         string // base64 ScVal value before ("" on create)
	PostXDR        string // base64 ScVal value after ("" on remove)
}

// StateEntry is one row of the current-state snapshot.
type StateEntry struct {
	ContractID string
	KeyXDR     string
	Durability string
	ValueXDR   string // "" when deleted
	Deleted    bool
	LastLedger uint32
	ClosedAt   time.Time
}

// insertStateChanges appends history rows; the idempotency key absorbs
// replays exactly like events.
func insertStateChanges(ctx context.Context, tx pgx.Tx, network string, changes []StateChange) error {
	if len(changes) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, c := range changes {
		var pre, post *string
		if c.PreXDR != "" {
			pre = &c.PreXDR
		}
		if c.PostXDR != "" {
			post = &c.PostXDR
		}
		batch.Queue(`
			INSERT INTO state_changes (
				network, id, contract_id, ledger_sequence, closed_at,
				tx_hash, tx_index, op_index, change_index, change_type,
				key_xdr, durability, pre_xdr, post_xdr
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
			ON CONFLICT ON CONSTRAINT state_changes_idempotency DO NOTHING`,
			network, c.ID, c.ContractID, int64(c.LedgerSequence), c.ClosedAt,
			c.TxHash, c.TxIndex, c.OpIndex, c.ChangeIndex, c.ChangeType,
			c.KeyXDR, c.Durability, pre, post,
		)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range changes {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("store: insert state change: %w", err)
		}
	}
	return nil
}

// applyStateEntries folds a ledger's changes into the snapshot. Within the
// slice, later changes to the same key win (they are in chain order); across
// writes, the last_ledger guard keeps out-of-order replays (descending
// backfill, re-commits) from overwriting newer state.
func applyStateEntries(ctx context.Context, tx pgx.Tx, network string, changes []StateChange) error {
	if len(changes) == 0 {
		return nil
	}
	type entryKey struct{ contract, key, durability string }
	final := make(map[entryKey]StateChange, len(changes))
	order := make([]entryKey, 0, len(changes))
	for _, c := range changes {
		k := entryKey{c.ContractID, c.KeyXDR, c.Durability}
		if _, seen := final[k]; !seen {
			order = append(order, k)
		}
		final[k] = c
	}

	batch := &pgx.Batch{}
	for _, k := range order {
		c := final[k]
		deleted := c.ChangeType == "removed"
		var value *string
		if !deleted && c.PostXDR != "" {
			value = &c.PostXDR
		}
		batch.Queue(`
			INSERT INTO state_entries (
				network, contract_id, key_xdr, durability,
				value_xdr, deleted, last_ledger, closed_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (network, contract_id, key_xdr, durability) DO UPDATE
			SET value_xdr   = EXCLUDED.value_xdr,
			    deleted     = EXCLUDED.deleted,
			    last_ledger = EXCLUDED.last_ledger,
			    closed_at   = EXCLUDED.closed_at,
			    updated_at  = now()
			WHERE state_entries.last_ledger <= EXCLUDED.last_ledger`,
			network, c.ContractID, c.KeyXDR, c.Durability,
			value, deleted, int64(c.LedgerSequence), c.ClosedAt,
		)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range order {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("store: apply state entry: %w", err)
		}
	}
	return nil
}

// StateQuery selects snapshot entries or history for the public API.
type StateQuery struct {
	ContractID string
	KeyXDR     string // exact key filter; "" = all keys
	// AfterID paginates history (change id ascending).
	AfterID string
	// AfterKey and AfterDurability paginate the snapshot ((key, durability)
	// tuple ascending — the same key can exist in both durabilities).
	AfterKey        string
	AfterDurability string
	Limit           int
}

// QueryStateEntries returns current snapshot rows in (key, durability)
// order, tombstones excluded.
func (s *Store) QueryStateEntries(ctx context.Context, network string, q StateQuery) ([]StateEntry, bool, error) {
	sql := `
		SELECT contract_id, key_xdr, durability, COALESCE(value_xdr, ''),
		       deleted, last_ledger, closed_at
		FROM state_entries
		WHERE network = $1 AND contract_id = $2 AND NOT deleted`
	args := []any{network, q.ContractID}
	if q.KeyXDR != "" {
		args = append(args, q.KeyXDR)
		sql += fmt.Sprintf(" AND key_xdr = $%d", len(args))
	}
	if q.AfterKey != "" {
		args = append(args, q.AfterKey, q.AfterDurability)
		sql += fmt.Sprintf(" AND (key_xdr, durability) > ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, q.Limit+1)
	sql += fmt.Sprintf(" ORDER BY key_xdr, durability LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, false, fmt.Errorf("store: query state entries: %w", err)
	}
	defer rows.Close()

	var out []StateEntry
	for rows.Next() {
		var e StateEntry
		var ledger int64
		if err := rows.Scan(&e.ContractID, &e.KeyXDR, &e.Durability,
			&e.ValueXDR, &e.Deleted, &ledger, &e.ClosedAt); err != nil {
			return nil, false, fmt.Errorf("store: scan state entry: %w", err)
		}
		e.LastLedger = uint32(ledger)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("store: query state entries: %w", err)
	}
	hasMore := len(out) > q.Limit
	if hasMore {
		out = out[:q.Limit]
	}
	return out, hasMore, nil
}

// QueryStateChanges returns history rows in chain order (id ascending).
func (s *Store) QueryStateChanges(ctx context.Context, network string, q StateQuery) ([]StateChange, bool, error) {
	sql := `
		SELECT id, contract_id, ledger_sequence, closed_at, tx_hash,
		       tx_index, op_index, change_index, change_type,
		       key_xdr, durability, COALESCE(pre_xdr, ''), COALESCE(post_xdr, '')
		FROM state_changes
		WHERE network = $1 AND contract_id = $2`
	args := []any{network, q.ContractID}
	if q.KeyXDR != "" {
		args = append(args, q.KeyXDR)
		sql += fmt.Sprintf(" AND key_xdr = $%d", len(args))
	}
	if q.AfterID != "" {
		args = append(args, q.AfterID)
		sql += fmt.Sprintf(" AND id > $%d", len(args))
	}
	args = append(args, q.Limit+1)
	sql += fmt.Sprintf(" ORDER BY id LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, false, fmt.Errorf("store: query state changes: %w", err)
	}
	defer rows.Close()

	var out []StateChange
	for rows.Next() {
		var c StateChange
		var ledger int64
		if err := rows.Scan(&c.ID, &c.ContractID, &ledger, &c.ClosedAt, &c.TxHash,
			&c.TxIndex, &c.OpIndex, &c.ChangeIndex, &c.ChangeType,
			&c.KeyXDR, &c.Durability, &c.PreXDR, &c.PostXDR); err != nil {
			return nil, false, fmt.Errorf("store: scan state change: %w", err)
		}
		c.LedgerSequence = uint32(ledger)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("store: query state changes: %w", err)
	}
	hasMore := len(out) > q.Limit
	if hasMore {
		out = out[:q.Limit]
	}
	return out, hasMore, nil
}

// CountStateEntries feeds the contract detail (live entries only).
func (s *Store) CountStateEntries(ctx context.Context, network, contractID string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM state_entries
		WHERE network = $1 AND contract_id = $2 AND NOT deleted`,
		network, contractID,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: count state entries: %w", err)
	}
	return n, nil
}
