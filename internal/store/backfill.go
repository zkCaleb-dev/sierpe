package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Backfill is one contract's descending-backfill state.
type Backfill struct {
	ContractID string
	TargetFrom uint32
	NextTo     uint32
	Done       bool
	// ClampedAt is the oldest ledger actually served when the retention wall
	// cut the walk short; nil when no clamp happened (yet).
	ClampedAt *uint32
}

// BackfillJob couples pending backfill state with its contract registration
// (the worker needs kinds to build the extraction snapshot).
type BackfillJob struct {
	Backfill Backfill
	Contract Contract
}

// ErrNoBackfill is returned when a contract has no backfill row.
var ErrNoBackfill = errors.New("store: no backfill for contract")

// EnsureBackfill reconciles a contract's backfill target (idempotent, admin
// doctrine). A first registration creates the row walking down from nextTo;
// re-registering with an older target extends the walk and reopens it;
// anything else preserves progress untouched.
func (s *Store) EnsureBackfill(ctx context.Context, network, contractID string, targetFrom, nextTo uint32) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var current Backfill
		err := tx.QueryRow(ctx, `
			SELECT target_from, next_to, done FROM backfill
			WHERE network = $1 AND contract_id = $2
			FOR UPDATE`,
			network, contractID,
		).Scan(&current.TargetFrom, &current.NextTo, &current.Done)

		switch {
		case errors.Is(err, pgx.ErrNoRows):
			done := nextTo < targetFrom
			if _, err := tx.Exec(ctx, `
				INSERT INTO backfill (network, contract_id, target_from, next_to, done)
				VALUES ($1, $2, $3, $4, $5)`,
				network, contractID, int64(targetFrom), int64(nextTo), done,
			); err != nil {
				return fmt.Errorf("store: create backfill: %w", err)
			}
			return nil
		case err != nil:
			return fmt.Errorf("store: read backfill: %w", err)
		}

		if targetFrom >= current.TargetFrom {
			return nil // nothing new requested; keep progress
		}
		if _, err := tx.Exec(ctx, `
			UPDATE backfill
			SET target_from = $3, done = false, clamped_at = NULL, updated_at = now()
			WHERE network = $1 AND contract_id = $2`,
			network, contractID, int64(targetFrom),
		); err != nil {
			return fmt.Errorf("store: extend backfill: %w", err)
		}
		return nil
	})
}

// GetBackfill returns one contract's backfill state.
func (s *Store) GetBackfill(ctx context.Context, network, contractID string) (Backfill, error) {
	b := Backfill{ContractID: contractID}
	var target, next int64
	var clamped *int64
	err := s.pool.QueryRow(ctx, `
		SELECT target_from, next_to, done, clamped_at FROM backfill
		WHERE network = $1 AND contract_id = $2`,
		network, contractID,
	).Scan(&target, &next, &b.Done, &clamped)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, ErrNoBackfill
	}
	if err != nil {
		return b, fmt.Errorf("store: get backfill: %w", err)
	}
	b.TargetFrom, b.NextTo = uint32(target), uint32(next)
	if clamped != nil {
		c := uint32(*clamped)
		b.ClampedAt = &c
	}
	return b, nil
}

// ListPendingBackfills returns every unfinished backfill joined with its
// registration, oldest progress first so no contract starves.
func (s *Store) ListPendingBackfills(ctx context.Context, network string) ([]BackfillJob, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT b.contract_id, b.target_from, b.next_to, b.done,
		       c.network, c.contract_id, c.source, c.kinds, c.classification, c.registered_at
		FROM backfill b
		JOIN contracts c ON c.network = b.network AND c.contract_id = b.contract_id
		WHERE b.network = $1 AND NOT b.done
		ORDER BY b.updated_at`,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list backfills: %w", err)
	}
	defer rows.Close()

	var out []BackfillJob
	for rows.Next() {
		var j BackfillJob
		var target, next int64
		if err := rows.Scan(&j.Backfill.ContractID, &target, &next, &j.Backfill.Done,
			&j.Contract.Network, &j.Contract.ContractID, &j.Contract.Source,
			&j.Contract.Kinds, &j.Contract.Classification, &j.Contract.RegisteredAt); err != nil {
			return nil, fmt.Errorf("store: scan backfill: %w", err)
		}
		j.Backfill.TargetFrom, j.Backfill.NextTo = uint32(target), uint32(next)
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list backfills: %w", err)
	}
	return out, nil
}

// CountPendingBackfills feeds /status and the pending gauge.
func (s *Store) CountPendingBackfills(ctx context.Context, network string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM backfill WHERE network = $1 AND NOT done`, network,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: count backfills: %w", err)
	}
	return n, nil
}

// CommitBackfillChunk persists one processed chunk atomically: the chunk's
// records and the moved next_to watermark either both land or neither does
// (the backfill analog of CLAUDE.md rule 1). Interruption loses at most one
// chunk of work, never correctness. State entries stay convergent because
// their upsert is guarded by last_ledger — replaying older history never
// overwrites newer state.
func (s *Store) CommitBackfillChunk(ctx context.Context, network string, b Backfill, events []Event, states []StateChange, transfers []Transfer) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := insertEvents(ctx, tx, network, events); err != nil {
			return err
		}
		if err := insertStateChanges(ctx, tx, network, states); err != nil {
			return err
		}
		if err := applyStateEntries(ctx, tx, network, states); err != nil {
			return err
		}
		if err := insertTransfers(ctx, tx, network, transfers); err != nil {
			return err
		}
		var clamped *int64
		if b.ClampedAt != nil {
			c := int64(*b.ClampedAt)
			clamped = &c
		}
		tag, err := tx.Exec(ctx, `
			UPDATE backfill
			SET next_to = $3, done = $4, clamped_at = $5, updated_at = now()
			WHERE network = $1 AND contract_id = $2`,
			network, b.ContractID, int64(b.NextTo), b.Done, clamped,
		)
		if err != nil {
			return fmt.Errorf("store: advance backfill: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// The registration disappeared mid-chunk (contract deleted):
			// refuse to write orphan progress.
			return fmt.Errorf("store: backfill row for %s vanished; contract unregistered", b.ContractID)
		}
		return nil
	})
}
