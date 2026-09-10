package store

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/jackc/pgx/v5"
)

// Heal modes. A gap is a promise that a range is missing; the mode says who
// owns that promise now. `deferred` records a decision not to replay, never a
// claim that the range is empty (docs/SPARSE-HEAL.md §2).
const (
	HealModeReplay   = "replay"
	HealModeDeferred = "deferred"
)

// checkpointFrequency is the archives' checkpoint cadence. A bounded captive
// replay catches up from the checkpoint at or below its start, so snapping
// plan edges to those boundaries costs nothing and keeps the gap ids
// deterministic for a given plan.
const checkpointFrequency = 64

// Interval is a closed ledger range.
type Interval struct {
	From uint32 `json:"from"`
	To   uint32 `json:"to"`
}

// PlanResult reports the open gaps a plan left behind.
type PlanResult struct {
	ReplayGaps      int   `json:"replay_gaps"`
	DeferredGaps    int   `json:"deferred_gaps"`
	ReplayLedgers   int64 `json:"replay_ledgers"`
	DeferredLedgers int64 `json:"deferred_ledgers"`
}

// piece is one range of a partitioned gap remainder.
type piece struct {
	from, to uint32
	mode     string
}

// PlanHeal reconciles the open gaps against a replay plan: the ranges the
// operator wants replayed. Every other still-missing ledger becomes a
// deferred gap, so the union of the open gaps is exactly what it was before
// and nothing stops being declared (rule 7).
//
// It reconciles a set rather than applying an event (rule 11): the same plan
// twice is a no-op, a wider plan turns deferred ranges back into replay ones,
// and a narrower one defers what it dropped.
//
// Ledgers already healed inside a gap are left alone — only the remainder
// below the heal watermark is partitioned.
func (s *Store) PlanHeal(ctx context.Context, network string, replay []Interval, padding uint32) (PlanResult, error) {
	plan := normalizePlan(replay, padding)

	var res PlanResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		gaps, err := lockOpenGaps(ctx, tx, network)
		if err != nil {
			return err
		}
		for _, g := range gaps {
			// HealNextTo below From means every ledger was healed and the
			// row is only waiting to be resolved; there is nothing to plan.
			if g.HealNextTo < g.From {
				continue
			}
			pieces := partition(g.From, g.HealNextTo, plan)
			// One piece spanning the whole remainder in the mode the gap
			// already carries is the gap itself: leave its id, its progress
			// and its clamped registrations untouched.
			if len(pieces) == 1 && pieces[0].mode == g.HealMode {
				continue
			}
			if err := splitGap(ctx, tx, network, g, pieces); err != nil {
				return err
			}
		}
		return tx.QueryRow(ctx, `
			SELECT
			  count(*) FILTER (WHERE heal_mode = $2),
			  count(*) FILTER (WHERE heal_mode = $3),
			  COALESCE(sum(to_sequence - from_sequence + 1) FILTER (WHERE heal_mode = $2), 0),
			  COALESCE(sum(to_sequence - from_sequence + 1) FILTER (WHERE heal_mode = $3), 0)
			FROM gaps
			WHERE network = $1 AND resolved_at IS NULL`,
			network, HealModeReplay, HealModeDeferred,
		).Scan(&res.ReplayGaps, &res.DeferredGaps, &res.ReplayLedgers, &res.DeferredLedgers)
	})
	if err != nil {
		return PlanResult{}, err
	}
	return res, nil
}

// lockOpenGaps reads every unresolved gap for update, so a heal committing
// concurrently cannot move a watermark under the partition.
func lockOpenGaps(ctx context.Context, tx pgx.Tx, network string) ([]Gap, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, from_sequence, to_sequence, reason, heal_next_to, heal_mode, recorded_at
		FROM gaps
		WHERE network = $1 AND resolved_at IS NULL
		ORDER BY to_sequence DESC
		FOR UPDATE`, network)
	if err != nil {
		return nil, fmt.Errorf("store: lock open gaps: %w", err)
	}
	defer rows.Close()

	var out []Gap
	for rows.Next() {
		g, err := scanGap(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: lock open gaps: %w", err)
	}
	return out, nil
}

// splitGap replaces one gap with the pieces its remainder partitions into.
//
// The gap's identity is its range, so a split cannot mutate the row: the old
// one goes and the pieces arrive. Registrations clamped at the old wall are
// re-pointed at the topmost piece, which now owns their frontier — from there
// the existing handoff walks them down piece by piece as each one resolves,
// and parks them honestly at the first deferred piece it meets.
func splitGap(ctx context.Context, tx pgx.Tx, network string, g Gap, pieces []piece) error {
	if _, err := tx.Exec(ctx,
		`DELETE FROM gaps WHERE network = $1 AND id = $2`, network, g.ID); err != nil {
		return fmt.Errorf("store: drop split gap %s: %w", g.ID, err)
	}
	for _, p := range pieces {
		id := fmt.Sprintf("gap:%s:%d:%d", network, p.from, p.to)
		if _, err := tx.Exec(ctx, `
			INSERT INTO gaps (id, network, from_sequence, to_sequence, reason, heal_next_to, heal_mode)
			VALUES ($1, $2, $3, $4, $5, $4, $6)
			ON CONFLICT (id) DO UPDATE SET heal_mode = EXCLUDED.heal_mode
			WHERE gaps.resolved_at IS NULL`,
			id, network, int64(p.from), int64(p.to), g.Reason, p.mode,
		); err != nil {
			return fmt.Errorf("store: record split gap %s: %w", id, err)
		}
	}
	// The topmost piece ends where the remainder ended, so a gap that had
	// not been healed at all keeps the wall it already had.
	top := pieces[len(pieces)-1]
	if _, err := tx.Exec(ctx, `
		UPDATE backfill SET clamped_at = $3, updated_at = now()
		WHERE network = $1 AND clamped_at = $2`,
		network, int64(g.To)+1, int64(top.to)+1,
	); err != nil {
		return fmt.Errorf("store: re-point clamped backfills of gap %s: %w", g.ID, err)
	}
	return nil
}

// ReopenGaps hands every open gap overlapping [from, to] back to the healer
// in full, inside the caller's transaction: deferred ranges go back to
// replay, and a gap that has already healed part of itself is rewound to owe
// all of itself again.
//
// Both halves are the same rule. A plan is only valid for the contract set
// it was computed from, and a heal derives rows only for the registry as it
// stood when it ran — so a contract registering now is owed both the ranges
// a hint skipped on its behalf and the ranges healed before it existed.
// Overlapping gaps are reopened whole rather than re-partitioned: replaying
// more than asked is wasted time, replaying less is a silent hole.
//
// Rewinding in place keeps gap identity and ranges intact. Promising the
// healed stretch as a separate row would overlap the gap it came from, which
// is exactly what the subtraction in RecordGap exists to prevent.
func ReopenGaps(ctx context.Context, tx pgx.Tx, network string, from, to uint32) (int64, error) {
	tag, err := tx.Exec(ctx, `
		UPDATE gaps
		SET heal_mode = $4,
		    heal_next_to = to_sequence
		WHERE network = $1 AND resolved_at IS NULL
		  AND from_sequence <= $3 AND to_sequence >= $2
		  AND (heal_mode = $5 OR heal_next_to IS DISTINCT FROM to_sequence)`,
		network, int64(from), int64(to), HealModeReplay, HealModeDeferred,
	)
	if err != nil {
		return 0, fmt.Errorf("store: reopen gaps: %w", err)
	}
	return tag.RowsAffected(), nil
}

// DeferredGaps counts the deferred gaps and the ledgers they hold, for
// /status and metrics: an operator who sees the open-gap count jump after a
// plan needs the breakdown to read it as sparse healing rather than damage.
func (s *Store) DeferredGaps(ctx context.Context, network string) (gaps int64, ledgers int64, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(sum(to_sequence - from_sequence + 1), 0)
		FROM gaps
		WHERE network = $1 AND resolved_at IS NULL AND heal_mode = $2`,
		network, HealModeDeferred,
	).Scan(&gaps, &ledgers)
	if err != nil {
		return 0, 0, fmt.Errorf("store: count deferred gaps: %w", err)
	}
	return gaps, ledgers, nil
}

// normalizePlan pads, snaps to checkpoint boundaries, sorts and merges the
// requested intervals. Padding runs first so the margin is measured on what
// the operator asked for, and merging runs last so two intervals that grow
// into each other become one replay range instead of two spin-ups.
func normalizePlan(in []Interval, padding uint32) []Interval {
	if len(in) == 0 {
		return nil
	}
	out := make([]Interval, 0, len(in))
	for _, iv := range in {
		from, to := iv.From, iv.To
		if from > padding {
			from -= padding
		} else {
			from = 1
		}
		if to < math.MaxUint32-padding {
			to += padding
		} else {
			to = math.MaxUint32
		}
		from -= from % checkpointFrequency
		if from == 0 {
			from = 1
		}
		if pad := checkpointFrequency - 1 - to%checkpointFrequency; to < math.MaxUint32-pad {
			to += pad
		} else {
			to = math.MaxUint32
		}
		out = append(out, Interval{From: from, To: to})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].From < out[j].From })

	merged := out[:1]
	for _, iv := range out[1:] {
		last := &merged[len(merged)-1]
		if iv.From <= last.To+1 {
			if iv.To > last.To {
				last.To = iv.To
			}
			continue
		}
		merged = append(merged, iv)
	}
	return merged
}

// partition cuts [from, to] into alternating deferred and replay pieces.
// The result always covers the range exactly once: a gap never loses a
// ledger to a split.
func partition(from, to uint32, plan []Interval) []piece {
	var out []piece
	cur := from
	for _, iv := range plan {
		if iv.To < cur {
			continue
		}
		if iv.From > to {
			break
		}
		start, end := max32(iv.From, cur), min32(iv.To, to)
		if start > cur {
			out = append(out, piece{from: cur, to: start - 1, mode: HealModeDeferred})
		}
		out = append(out, piece{from: start, to: end, mode: HealModeReplay})
		if end == to {
			return out
		}
		cur = end + 1
	}
	return append(out, piece{from: cur, to: to, mode: HealModeDeferred})
}

func max32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func min32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}
