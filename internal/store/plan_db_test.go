package store

import (
	"context"
	"testing"
)

// allGaps reads every unresolved gap, deferred ones included — what the
// healer sees is a subset, and a split has to be judged on the whole set.
func allGaps(t *testing.T, s *Store) []Gap {
	t.Helper()
	rows, err := s.pool.Query(context.Background(), `
		SELECT id, from_sequence, to_sequence, reason, heal_next_to, heal_mode, recorded_at
		FROM gaps WHERE network = 'testnet' AND resolved_at IS NULL
		ORDER BY from_sequence`)
	if err != nil {
		t.Fatalf("read gaps: %v", err)
	}
	defer rows.Close()
	var out []Gap
	for rows.Next() {
		g, err := scanGap(rows)
		if err != nil {
			t.Fatalf("scan gap: %v", err)
		}
		out = append(out, g)
	}
	return out
}

// assertTiles fails unless the gaps cover [from, to] exactly once, which is
// the invariant a split must never break: no ledger stops being declared.
func assertTiles(t *testing.T, gaps []Gap, from, to uint32) {
	t.Helper()
	if len(gaps) == 0 {
		t.Fatalf("no gaps left; [%d..%d] stopped being declared", from, to)
	}
	if gaps[0].From != from {
		t.Errorf("gaps start at %d, want %d", gaps[0].From, from)
	}
	if last := gaps[len(gaps)-1]; last.To != to {
		t.Errorf("gaps end at %d, want %d", last.To, to)
	}
	for i := 1; i < len(gaps); i++ {
		if gaps[i].From != gaps[i-1].To+1 {
			t.Errorf("gap %d starts at %d, want %d (hole or overlap)", i, gaps[i].From, gaps[i-1].To+1)
		}
	}
}

func TestPlanHealSplitsGapAndHidesDesertsFromTheHealer(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, backfill, contracts`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	seedGap(t, s, 10_000, 99_999)

	res, err := s.PlanHeal(ctx, "testnet", []Interval{{From: 50_000, To: 50_100}}, 0)
	if err != nil {
		t.Fatalf("PlanHeal() error = %v", err)
	}

	gaps := allGaps(t, s)
	assertTiles(t, gaps, 10_000, 99_999)
	if len(gaps) != 3 {
		t.Fatalf("gaps = %+v, want a desert, a cluster and a desert", gaps)
	}
	if gaps[1].HealMode != HealModeReplay || gaps[0].HealMode != HealModeDeferred || gaps[2].HealMode != HealModeDeferred {
		t.Errorf("modes = %s/%s/%s, want deferred/replay/deferred",
			gaps[0].HealMode, gaps[1].HealMode, gaps[2].HealMode)
	}
	// The cluster snapped out to its checkpoint boundaries.
	if gaps[1].From != 49_984 || gaps[1].To != 50_111 {
		t.Errorf("cluster = [%d..%d], want the checkpoint-aligned [49984..50111]", gaps[1].From, gaps[1].To)
	}
	// A fresh piece is entirely missing: its watermark starts at the top.
	if gaps[1].HealNextTo != gaps[1].To {
		t.Errorf("cluster HealNextTo = %d, want %d", gaps[1].HealNextTo, gaps[1].To)
	}

	// The healer only ever sees what it owns.
	open, err := s.ListOpenGaps(ctx, "testnet")
	if err != nil {
		t.Fatalf("ListOpenGaps() error = %v", err)
	}
	if len(open) != 1 || open[0].From != 49_984 {
		t.Fatalf("ListOpenGaps() = %+v, want only the replay cluster", open)
	}

	// Everything that reports coverage still counts the deserts.
	total, err := s.OpenGaps(ctx, "testnet")
	if err != nil || total != 3 {
		t.Errorf("OpenGaps() = %d, %v, want 3: deferred gaps stay open and declared", total, err)
	}
	deferredGaps, deferredLedgers, err := s.DeferredGaps(ctx, "testnet")
	if err != nil {
		t.Fatalf("DeferredGaps() error = %v", err)
	}
	if deferredGaps != 2 || deferredLedgers != 90_000-128 {
		t.Errorf("DeferredGaps() = %d gaps / %d ledgers, want 2 / %d",
			deferredGaps, deferredLedgers, 90_000-128)
	}
	if res.ReplayGaps != 1 || res.DeferredGaps != 2 || res.ReplayLedgers != 128 {
		t.Errorf("result = %+v, want 1 replay gap of 128 ledgers and 2 deferred", res)
	}
}

func TestPlanHealIsIdempotentAndReconcilesBothWays(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, backfill, contracts`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	seedGap(t, s, 10_000, 99_999)

	plan := []Interval{{From: 50_000, To: 50_100}}
	first, err := s.PlanHeal(ctx, "testnet", plan, 0)
	if err != nil {
		t.Fatalf("PlanHeal() error = %v", err)
	}
	before := allGaps(t, s)

	// The same plan again is a no-op, ids included: reconciling a set, not
	// applying an event (rule 11).
	second, err := s.PlanHeal(ctx, "testnet", plan, 0)
	if err != nil {
		t.Fatalf("PlanHeal() second error = %v", err)
	}
	if first != second {
		t.Errorf("re-applying the plan reported %+v then %+v", first, second)
	}
	after := allGaps(t, s)
	if len(before) != len(after) {
		t.Fatalf("re-applying the plan changed the gaps: %+v then %+v", before, after)
	}
	for i := range before {
		if before[i].ID != after[i].ID || before[i].HealMode != after[i].HealMode {
			t.Errorf("gap %d moved from %s/%s to %s/%s",
				i, before[i].ID, before[i].HealMode, after[i].ID, after[i].HealMode)
		}
	}

	// A wider plan takes deferred ranges back.
	if _, err := s.PlanHeal(ctx, "testnet", []Interval{{From: 10_000, To: 99_999}}, 0); err != nil {
		t.Fatalf("PlanHeal() widen error = %v", err)
	}
	gaps := allGaps(t, s)
	assertTiles(t, gaps, 10_000, 99_999)
	for _, g := range gaps {
		if g.HealMode != HealModeReplay {
			t.Errorf("gap %s is %s after widening the plan, want replay", g.ID, g.HealMode)
		}
	}

	// A narrower one defers what it dropped.
	if _, err := s.PlanHeal(ctx, "testnet", []Interval{{From: 50_000, To: 50_100}}, 0); err != nil {
		t.Fatalf("PlanHeal() narrow error = %v", err)
	}
	gaps = allGaps(t, s)
	assertTiles(t, gaps, 10_000, 99_999)
	var replay int
	for _, g := range gaps {
		if g.HealMode == HealModeReplay {
			replay++
		}
	}
	if replay != 1 {
		t.Errorf("after narrowing, %d gaps are replay, want 1", replay)
	}
}

func TestPlanHealKeepsHealedGroundAndRepointsTheClamp(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, backfill, contracts`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := s.UpsertContract(ctx, Contract{
		Network: "testnet", ContractID: "CAAA", Source: SourceAPI, Kinds: []string{KindEvents},
	}); err != nil {
		t.Fatalf("UpsertContract() error = %v", err)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO backfill (network, contract_id, target_from, next_to, done, clamped_at)
		VALUES ('testnet', 'CAAA', 10000, 99999, true, 100000)`); err != nil {
		t.Fatalf("seed backfill: %v", err)
	}
	gap := seedGap(t, s, 10_000, 99_999)

	// Heal the top of the gap, then plan the rest.
	if err := s.CommitHealChunk(ctx, "testnet", gap, 79_999, false, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("CommitHealChunk() error = %v", err)
	}
	if _, err := s.PlanHeal(ctx, "testnet", []Interval{{From: 50_000, To: 50_100}}, 0); err != nil {
		t.Fatalf("PlanHeal() error = %v", err)
	}

	// Only the remainder is partitioned: the healed ground above the
	// watermark is covered and needs no gap.
	gaps := allGaps(t, s)
	assertTiles(t, gaps, 10_000, 79_999)

	// The registration clamped at the old wall now follows the topmost
	// piece, so the existing handoff can keep walking it down.
	var clamped *int64
	if err := s.pool.QueryRow(ctx,
		`SELECT clamped_at FROM backfill WHERE contract_id = 'CAAA'`).Scan(&clamped); err != nil {
		t.Fatalf("read backfill: %v", err)
	}
	if clamped == nil || *clamped != 80_000 {
		t.Errorf("clamped_at = %v, want 80000: the split must not orphan the clamp", clamped)
	}
}

func TestRegisteringAContractReopensDeferredGaps(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, backfill, contracts`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	seedGap(t, s, 10_000, 99_999)
	if _, err := s.PlanHeal(ctx, "testnet", []Interval{{From: 50_000, To: 50_100}}, 0); err != nil {
		t.Fatalf("PlanHeal() error = %v", err)
	}
	if gaps, _, err := s.DeferredGaps(ctx, "testnet"); err != nil || gaps != 2 {
		t.Fatalf("DeferredGaps() = %d, %v, want 2 before the registration", gaps, err)
	}

	// The plan deferred those ranges on the strength of a hint that never
	// looked for this contract, so registering it hands them back.
	if err := s.EnsureBackfill(ctx, "testnet", "CBBB", 20_000, 99_999, []string{KindEvents}); err != nil {
		t.Fatalf("EnsureBackfill() error = %v", err)
	}
	gaps, ledgers, err := s.DeferredGaps(ctx, "testnet")
	if err != nil {
		t.Fatalf("DeferredGaps() error = %v", err)
	}
	if gaps != 0 || ledgers != 0 {
		t.Errorf("DeferredGaps() = %d gaps / %d ledgers, want none: a new registration invalidates the deferrals", gaps, ledgers)
	}
	open, err := s.ListOpenGaps(ctx, "testnet")
	if err != nil || len(open) != 3 {
		t.Errorf("ListOpenGaps() = %d gaps, %v, want the healer to own all 3 again", len(open), err)
	}
}

func TestPlanHealLeavesUnplannedNetworksAlone(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, backfill, contracts`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	seedGap(t, s, 10_000, 99_999)
	if err := s.RecordGap(ctx, "mainnet", 10_000, 99_999, "other network"); err != nil {
		t.Fatalf("RecordGap() error = %v", err)
	}

	if _, err := s.PlanHeal(ctx, "testnet", []Interval{{From: 50_000, To: 50_100}}, 0); err != nil {
		t.Fatalf("PlanHeal() error = %v", err)
	}
	var mode string
	var count int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*), min(heal_mode) FROM gaps
		WHERE network = 'mainnet' AND resolved_at IS NULL`).Scan(&count, &mode); err != nil {
		t.Fatalf("read mainnet gaps: %v", err)
	}
	if count != 1 || mode != HealModeReplay {
		t.Errorf("mainnet has %d gaps in mode %s, want its single replay gap untouched", count, mode)
	}
}
