package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Gap is one recorded range of unserved history. HealNextTo is the heal
// watermark: the highest ledger still missing (To when untouched). HealMode
// says whether the healer owns the range or the range is deliberately not
// being replayed (docs/SPARSE-HEAL.md).
type Gap struct {
	ID         string
	From       uint32
	To         uint32
	Reason     string
	HealNextTo uint32
	HealMode   string
	RecordedAt time.Time
}

// scanGap reads one gap row in the column order every gap query uses.
func scanGap(row pgx.Row) (Gap, error) {
	var g Gap
	var from, to int64
	var healNextTo *int64
	if err := row.Scan(&g.ID, &from, &to, &g.Reason, &healNextTo, &g.HealMode, &g.RecordedAt); err != nil {
		return Gap{}, fmt.Errorf("store: scan gap: %w", err)
	}
	g.From, g.To = uint32(from), uint32(to)
	g.HealNextTo = g.To
	if healNextTo != nil {
		g.HealNextTo = uint32(*healNextTo)
	}
	return g, nil
}

// ListOpenGaps returns the unresolved gaps the healer owns, most recent
// history first — the same recent-first order the backfill walks, because
// fresh history is worth more to consumers than deep history.
//
// Deferred gaps are excluded: they stay open and declared, but replaying
// them is exactly what a plan decided not to do. Everything that reports
// coverage still counts them (rule 7).
func (s *Store) ListOpenGaps(ctx context.Context, network string) ([]Gap, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, from_sequence, to_sequence, reason, heal_next_to, heal_mode, recorded_at
		FROM gaps
		WHERE network = $1 AND resolved_at IS NULL AND heal_mode = $2
		ORDER BY to_sequence DESC`, network, HealModeReplay)
	if err != nil {
		return nil, fmt.Errorf("store: list open gaps: %w", err)
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
		return nil, fmt.Errorf("store: list open gaps: %w", err)
	}
	return out, nil
}

// CommitHealChunk persists one healed chunk atomically: the chunk's records,
// the moved heal watermark, and — when the heal has reached the gap's floor
// — the resolution, plus the un-clamping of every backfill row this gap was
// blocking. Either the chunk fully happened or it never did (rule 1).
//
// resolved must be true exactly when newNextTo < gap.From.
func (s *Store) CommitHealChunk(ctx context.Context, network string, gap Gap, newNextTo uint32, resolved bool,
	events []Event, states []StateChange, transfers []Transfer, trustlines []TrustlineChange, movements []Movement) error {

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

		var resolvedAt *time.Time
		if resolved {
			now := time.Now().UTC()
			resolvedAt = &now
		}
		tag, err := tx.Exec(ctx, `
			UPDATE gaps
			SET heal_next_to = $3, resolved_at = COALESCE($4, resolved_at)
			WHERE network = $1 AND id = $2 AND resolved_at IS NULL`,
			network, gap.ID, int64(newNextTo), resolvedAt,
		)
		if err != nil {
			return fmt.Errorf("store: advance heal watermark: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("store: gap %s vanished or resolved mid-heal", gap.ID)
		}

		// Coverage honesty (rule 7): a backfill clamped exactly at this gap
		// declared IndexedFromLedger at the wall. Move its frontier down as
		// the heal descends, and clear the clamp once the heal has covered
		// the contract's own target.
		if _, err := tx.Exec(ctx, `
			UPDATE backfill
			SET next_to = $3, updated_at = now()
			WHERE network = $1 AND clamped_at = $2 AND next_to > $3 AND target_from <= $3`,
			network, int64(gap.To)+1, int64(newNextTo),
		); err != nil {
			return fmt.Errorf("store: lower clamped backfill frontier: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE backfill
			SET next_to = GREATEST(target_from, 1) - 1, clamped_at = NULL, updated_at = now()
			WHERE network = $1 AND clamped_at = $2 AND target_from > $3`,
			network, int64(gap.To)+1, int64(newNextTo),
		); err != nil {
			return fmt.Errorf("store: clear healed backfill clamp: %w", err)
		}

		// Handoff on resolution. RecordGap trims a new gap past the open
		// ones below it, so a registration clamped at this gap's wall may
		// have a target deeper than the gap's own floor: its remaining
		// history is owed by an older, deeper gap. Point each such row at
		// the deepest still-open gap that reaches its target — that gap's
		// heal commits will keep dragging the row's frontier down — and
		// close the rows with nothing open below, whose deep history has
		// already been healed by gaps since resolved (rule 7: the declared
		// coverage follows what was actually derived, in both directions).
		if resolved {
			if _, err := tx.Exec(ctx, `
				UPDATE backfill b
				SET clamped_at = d.to_sequence + 1, updated_at = now()
				FROM gaps d
				WHERE b.network = $1 AND b.clamped_at = $2 AND b.target_from < $3
				  AND d.network = $1 AND d.resolved_at IS NULL
				  AND d.to_sequence < $3 AND d.to_sequence >= b.target_from
				  AND d.to_sequence = (
					SELECT max(g.to_sequence) FROM gaps g
					WHERE g.network = $1 AND g.resolved_at IS NULL AND g.to_sequence < $3)`,
				network, int64(gap.To)+1, int64(gap.From),
			); err != nil {
				return fmt.Errorf("store: hand clamp to the deeper gap: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE backfill
				SET next_to = GREATEST(target_from, 1) - 1, clamped_at = NULL, updated_at = now()
				WHERE network = $1 AND clamped_at = $2
				  AND NOT EXISTS (
					SELECT 1 FROM gaps g
					WHERE g.network = $1 AND g.resolved_at IS NULL
					  AND g.to_sequence < $3 AND g.to_sequence >= backfill.target_from)`,
				network, int64(gap.To)+1, int64(gap.From),
			); err != nil {
				return fmt.Errorf("store: close fully healed clamp: %w", err)
			}
		}
		return nil
	})
}
