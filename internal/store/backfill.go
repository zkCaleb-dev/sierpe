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
	// CoveredKinds are the kinds this walk derives. A kind outside this set
	// has no history below the walk's anchor, however finished the walk
	// looks — the API must not vouch for it (rule 7).
	CoveredKinds []string
}

// CoversKind reports whether the walk derives kind.
func (b Backfill) CoversKind(kind string) bool {
	for _, k := range b.CoveredKinds {
		if k == kind {
			return true
		}
	}
	return false
}

// intersect keeps only the covered kinds the registration still derives,
// preserving the covered order so the stored array stays stable.
func intersect(covered, kinds []string) []string {
	want := make(map[string]struct{}, len(kinds))
	for _, k := range kinds {
		want[k] = struct{}{}
	}
	out := make([]string, 0, len(covered))
	for _, k := range covered {
		if _, ok := want[k]; ok {
			out = append(out, k)
		}
	}
	return out
}

// coversAll reports whether every wanted kind is already covered.
func (b Backfill) coversAll(kinds []string) bool {
	for _, k := range kinds {
		if !b.CoversKind(k) {
			return false
		}
	}
	return true
}

// BackfillJob couples pending backfill state with its contract registration
// (the worker needs kinds to build the extraction snapshot).
type BackfillJob struct {
	Backfill Backfill
	Contract Contract
}

// ErrNoBackfill is returned when a contract has no backfill row.
var ErrNoBackfill = errors.New("store: no backfill for contract")

// EnsureBackfill reconciles a contract's backfill target and covered kinds
// (idempotent, admin doctrine). A first registration creates the row walking
// down from nextTo. Re-registering extends the walk when the target moved
// older, and REOPENS it when a kind was added: the finished walk never
// derived that kind, so its history has to be walked again rather than
// silently declared covered (rule 7). Everything else preserves progress.
//
// A reopened walk restarts at the current anchor, so while it runs the
// declared coverage of the already-covered kinds temporarily narrows to the
// moving frontier. That is conservative, never a lie, and it heals as the
// walk descends; the alternative (two watermarks) buys precision during a
// rare operation at the cost of a second thing that can be wrong.
func (s *Store) EnsureBackfill(ctx context.Context, network, contractID string, targetFrom, nextTo uint32, kinds []string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var current Backfill
		err := tx.QueryRow(ctx, `
			SELECT target_from, next_to, done, covered_kinds FROM backfill
			WHERE network = $1 AND contract_id = $2
			FOR UPDATE`,
			network, contractID,
		).Scan(&current.TargetFrom, &current.NextTo, &current.Done, &current.CoveredKinds)

		switch {
		case errors.Is(err, pgx.ErrNoRows):
			done := nextTo < targetFrom
			if _, err := tx.Exec(ctx, `
				INSERT INTO backfill (network, contract_id, target_from, next_to, done, covered_kinds)
				VALUES ($1, $2, $3, $4, $5, $6)`,
				network, contractID, int64(targetFrom), int64(nextTo), done, kinds,
			); err != nil {
				return fmt.Errorf("store: create backfill: %w", err)
			}
			return nil
		case err != nil:
			return fmt.Errorf("store: read backfill: %w", err)
		}

		extends := targetFrom < current.TargetFrom
		newKinds := !current.coversAll(kinds)
		// Coverage may never name a kind the registration no longer
		// derives: dropping a kind and adding it back would otherwise
		// leave the row vouching for history that was never walked with
		// it. Narrowing needs no new work, only an honest record.
		covered := intersect(current.CoveredKinds, kinds)
		narrowed := len(covered) != len(current.CoveredKinds)
		if !extends && !newKinds && !narrowed {
			return nil // nothing new requested; keep progress
		}

		target := current.TargetFrom
		if extends {
			target = targetFrom
		}
		// A new kind has no history at all below the anchor, so the walk
		// restarts from the anchor; extending only moves the floor.
		next := current.NextTo
		if newKinds {
			next = nextTo
		}
		// A pure narrowing must not disturb a finished walk: there is
		// nothing new to derive, only a smaller truth to record.
		if !extends && !newKinds {
			if _, err := tx.Exec(ctx, `
				UPDATE backfill SET covered_kinds = $3, updated_at = now()
				WHERE network = $1 AND contract_id = $2`,
				network, contractID, covered,
			); err != nil {
				return fmt.Errorf("store: narrow covered kinds: %w", err)
			}
			return nil
		}
		if _, err := tx.Exec(ctx, `
			UPDATE backfill
			SET target_from = $3, next_to = $4, done = false, clamped_at = NULL,
			    covered_kinds = $5, updated_at = now()
			WHERE network = $1 AND contract_id = $2`,
			network, contractID, int64(target), int64(next), kinds,
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
		SELECT target_from, next_to, done, clamped_at, covered_kinds FROM backfill
		WHERE network = $1 AND contract_id = $2`,
		network, contractID,
	).Scan(&target, &next, &b.Done, &clamped, &b.CoveredKinds)
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
func (s *Store) CommitBackfillChunk(ctx context.Context, network string, b Backfill, events []Event, states []StateChange, transfers []Transfer, trustlines []TrustlineChange, movements []Movement) error {
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
		if err := insertTrustlineChanges(ctx, tx, network, trustlines); err != nil {
			return err
		}
		if err := applyTrustlineEntries(ctx, tx, network, trustlines); err != nil {
			return err
		}
		if err := insertMovements(ctx, tx, network, movements); err != nil {
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
