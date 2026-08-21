package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Gap is one recorded range of unserved history. HealNextTo is the heal
// watermark: the highest ledger still missing (To when untouched).
type Gap struct {
	ID         string
	From       uint32
	To         uint32
	Reason     string
	HealNextTo uint32
	RecordedAt time.Time
}

// ListOpenGaps returns unresolved gaps, most recent history first — the
// same recent-first order the backfill walks, because fresh history is
// worth more to consumers than deep history.
func (s *Store) ListOpenGaps(ctx context.Context, network string) ([]Gap, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, from_sequence, to_sequence, reason, heal_next_to, recorded_at
		FROM gaps
		WHERE network = $1 AND resolved_at IS NULL
		ORDER BY to_sequence DESC`, network)
	if err != nil {
		return nil, fmt.Errorf("store: list open gaps: %w", err)
	}
	defer rows.Close()

	var out []Gap
	for rows.Next() {
		var g Gap
		var from, to int64
		var healNextTo *int64
		if err := rows.Scan(&g.ID, &from, &to, &g.Reason, &healNextTo, &g.RecordedAt); err != nil {
			return nil, fmt.Errorf("store: scan gap: %w", err)
		}
		g.From, g.To = uint32(from), uint32(to)
		g.HealNextTo = g.To
		if healNextTo != nil {
			g.HealNextTo = uint32(*healNextTo)
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
		return nil
	})
}
