package store

import (
	"context"
	"testing"
	"time"
)

func seedGap(t *testing.T, s *Store, from, to uint32) Gap {
	t.Helper()
	if err := s.RecordGap(context.Background(), "testnet", from, to, "test gap"); err != nil {
		t.Fatalf("RecordGap() error = %v", err)
	}
	gaps, err := s.ListOpenGaps(context.Background(), "testnet")
	if err != nil || len(gaps) != 1 {
		t.Fatalf("ListOpenGaps() = %v, %v", gaps, err)
	}
	return gaps[0]
}

func TestCommitHealChunkAdvancesAndResolves(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, events, backfill`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	gap := seedGap(t, s, 1000, 5499)
	if gap.HealNextTo != 5499 {
		t.Fatalf("fresh gap HealNextTo = %d, want to_sequence", gap.HealNextTo)
	}

	// First chunk lands records and moves the watermark.
	ev := testEvent("0000000000000003500-0000000000", "CAAA", "feed", 0)
	if err := s.CommitHealChunk(ctx, "testnet", gap, 3499, false, []Event{ev}, nil, nil, nil, nil); err != nil {
		t.Fatalf("CommitHealChunk() error = %v", err)
	}
	gaps, err := s.ListOpenGaps(ctx, "testnet")
	if err != nil || len(gaps) != 1 || gaps[0].HealNextTo != 3499 {
		t.Fatalf("after chunk 1: gaps = %+v err = %v, want HealNextTo 3499", gaps, err)
	}
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil || n != 1 {
		t.Errorf("events = %d err = %v, want 1", n, err)
	}

	// Final chunk resolves the gap.
	if err := s.CommitHealChunk(ctx, "testnet", gaps[0], 999, true, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("CommitHealChunk(final) error = %v", err)
	}
	if gaps, err = s.ListOpenGaps(ctx, "testnet"); err != nil || len(gaps) != 0 {
		t.Errorf("resolved gap still open: %+v err = %v", gaps, err)
	}
	open, err := s.OpenGaps(ctx, "testnet")
	if err != nil || open != 0 {
		t.Errorf("OpenGaps = %d err = %v, want 0", open, err)
	}
}

func TestCommitHealChunkUnclampsBackfills(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, backfill, contracts`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// A contract clamped at wall 5500: gap [1000..5499], backfill frontier
	// stuck at 5499 with clamped_at 5500.
	if _, err := s.UpsertContract(ctx, Contract{
		Network: "testnet", ContractID: "CAAA", Source: SourceAPI, Kinds: []string{KindEvents},
	}); err != nil {
		t.Fatalf("UpsertContract() error = %v", err)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO backfill (network, contract_id, target_from, next_to, done, clamped_at)
		VALUES ('testnet', 'CAAA', 1000, 5499, true, 5500)`); err != nil {
		t.Fatalf("seed backfill: %v", err)
	}
	gap := seedGap(t, s, 1000, 5499)

	// A partial heal lowers the declared frontier but keeps the clamp.
	if err := s.CommitHealChunk(ctx, "testnet", gap, 3499, false, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("CommitHealChunk() error = %v", err)
	}
	var nextTo int64
	var clamped *int64
	if err := s.pool.QueryRow(ctx,
		`SELECT next_to, clamped_at FROM backfill WHERE contract_id = 'CAAA'`).Scan(&nextTo, &clamped); err != nil {
		t.Fatalf("read backfill: %v", err)
	}
	if nextTo != 3499 || clamped == nil {
		t.Errorf("after partial heal: next_to = %d clamped = %v, want 3499 and still clamped", nextTo, clamped)
	}

	// The final heal clears the clamp and settles the frontier at the
	// contract's own target.
	gaps, _ := s.ListOpenGaps(ctx, "testnet")
	if err := s.CommitHealChunk(ctx, "testnet", gaps[0], 999, true, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("CommitHealChunk(final) error = %v", err)
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT next_to, clamped_at FROM backfill WHERE contract_id = 'CAAA'`).Scan(&nextTo, &clamped); err != nil {
		t.Fatalf("read backfill: %v", err)
	}
	if nextTo != 999 || clamped != nil {
		t.Errorf("after full heal: next_to = %d clamped = %v, want 999 and no clamp", nextTo, clamped)
	}
}

// Batches register over days and clamp at ever-higher walls; a new gap
// must not re-promise ledgers an open gap already owes, or the healer
// replays the same range once per batch.
func TestRecordGapTrimsAgainstOpenGaps(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, backfill`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	seedGap(t, s, 1000, 5499)
	// A later batch clamps at wall 7500: only [5500..7499] is new debt.
	if err := s.RecordGap(ctx, "testnet", 1000, 7499, "second batch"); err != nil {
		t.Fatalf("RecordGap() error = %v", err)
	}
	gaps, err := s.ListOpenGaps(ctx, "testnet")
	if err != nil || len(gaps) != 2 {
		t.Fatalf("gaps = %+v err = %v, want 2", gaps, err)
	}
	if gaps[0].From != 5500 || gaps[0].To != 7499 {
		t.Errorf("trimmed gap = [%d..%d], want [5500..7499]", gaps[0].From, gaps[0].To)
	}
	// A range fully promised by open gaps records nothing new.
	if err := s.RecordGap(ctx, "testnet", 1000, 5000, "fully covered"); err != nil {
		t.Fatalf("RecordGap(covered) error = %v", err)
	}
	if gaps, _ = s.ListOpenGaps(ctx, "testnet"); len(gaps) != 2 {
		t.Errorf("a fully covered range must not add a gap, got %+v", gaps)
	}
}

// The staged-batches scenario end to end: a later batch's gap is trimmed
// to its own wall window, and when it resolves, its clamped registrations
// are handed to the deeper still-open gap so their coverage keeps
// descending with that gap's heal — never claiming more than what was
// derived, never freezing above their target.
func TestGapResolutionHandsClampToTheDeeperGap(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, backfill, contracts`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	for _, id := range []string{"CAAA", "CBBB"} {
		if _, err := s.UpsertContract(ctx, Contract{
			Network: "testnet", ContractID: id, Source: SourceAPI, Kinds: []string{KindEvents},
		}); err != nil {
			t.Fatalf("UpsertContract(%s) error = %v", id, err)
		}
	}
	// Batch A: clamped at wall 5500, gap [1000..5499].
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO backfill (network, contract_id, target_from, next_to, done, clamped_at)
		VALUES ('testnet', 'CAAA', 1000, 5499, true, 5500)`); err != nil {
		t.Fatalf("seed batch A: %v", err)
	}
	seedGap(t, s, 1000, 5499)
	// Batch B, days later: clamped at wall 7500; its gap trims to
	// [5500..7499] because gap A already owes the deep range.
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO backfill (network, contract_id, target_from, next_to, done, clamped_at)
		VALUES ('testnet', 'CBBB', 1000, 7499, true, 7500)`); err != nil {
		t.Fatalf("seed batch B: %v", err)
	}
	if err := s.RecordGap(ctx, "testnet", 1000, 7499, "batch B"); err != nil {
		t.Fatalf("RecordGap(B) error = %v", err)
	}
	gaps, _ := s.ListOpenGaps(ctx, "testnet") // DESC by to: [0]=B trimmed, [1]=A
	if len(gaps) != 2 || gaps[0].From != 5500 {
		t.Fatalf("gaps = %+v, want trimmed B first", gaps)
	}

	// B resolves: CBBB's clamp is handed to gap A's wall.
	if err := s.CommitHealChunk(ctx, "testnet", gaps[0], 5499, true, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("resolve B: %v", err)
	}
	var nextTo int64
	var clamped *int64
	readRow := func(id string) {
		t.Helper()
		if err := s.pool.QueryRow(ctx,
			`SELECT next_to, clamped_at FROM backfill WHERE contract_id = $1`, id).Scan(&nextTo, &clamped); err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
	}
	readRow("CBBB")
	if nextTo != 5499 || clamped == nil || *clamped != 5500 {
		t.Fatalf("after B resolves: CBBB next_to=%d clamped=%v, want 5499 handed to wall 5500", nextTo, clamped)
	}

	// Gap A keeps healing: BOTH batches' frontiers descend with it.
	gaps, _ = s.ListOpenGaps(ctx, "testnet")
	if err := s.CommitHealChunk(ctx, "testnet", gaps[0], 3499, false, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("partial heal A: %v", err)
	}
	for _, id := range []string{"CAAA", "CBBB"} {
		readRow(id)
		if nextTo != 3499 || clamped == nil {
			t.Errorf("mid-heal %s: next_to=%d clamped=%v, want 3499 still clamped", id, nextTo, clamped)
		}
	}

	// A resolves: both close at their target.
	gaps, _ = s.ListOpenGaps(ctx, "testnet")
	if err := s.CommitHealChunk(ctx, "testnet", gaps[0], 999, true, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("resolve A: %v", err)
	}
	for _, id := range []string{"CAAA", "CBBB"} {
		readRow(id)
		if nextTo != 999 || clamped != nil {
			t.Errorf("final %s: next_to=%d clamped=%v, want 999 unclamped", id, nextTo, clamped)
		}
	}
}

// Out-of-order resolution: when the deeper gap resolved first, the later
// batch's registrations have nothing open below at their own resolution
// and must close at their target instead of freezing at the shared floor.
func TestGapResolutionClosesClampWhenNothingOpenBelow(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, backfill, contracts`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := s.UpsertContract(ctx, Contract{
		Network: "testnet", ContractID: "CBBB", Source: SourceAPI, Kinds: []string{KindEvents},
	}); err != nil {
		t.Fatalf("UpsertContract() error = %v", err)
	}
	seedGap(t, s, 1000, 5499)
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO backfill (network, contract_id, target_from, next_to, done, clamped_at)
		VALUES ('testnet', 'CBBB', 1000, 7499, true, 7500)`); err != nil {
		t.Fatalf("seed backfill: %v", err)
	}
	if err := s.RecordGap(ctx, "testnet", 1000, 7499, "batch B"); err != nil {
		t.Fatalf("RecordGap(B) error = %v", err)
	}

	// The deeper gap A resolves FIRST.
	gaps, _ := s.ListOpenGaps(ctx, "testnet")
	deep := gaps[1]
	if deep.From != 1000 {
		t.Fatalf("gaps = %+v, want the deep gap second", gaps)
	}
	if err := s.CommitHealChunk(ctx, "testnet", deep, 999, true, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("resolve deep: %v", err)
	}
	// Now B resolves with nothing open below: CBBB closes at its target.
	gaps, _ = s.ListOpenGaps(ctx, "testnet")
	if err := s.CommitHealChunk(ctx, "testnet", gaps[0], 5499, true, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("resolve B: %v", err)
	}
	var nextTo int64
	var clamped *int64
	if err := s.pool.QueryRow(ctx,
		`SELECT next_to, clamped_at FROM backfill WHERE contract_id = 'CBBB'`).Scan(&nextTo, &clamped); err != nil {
		t.Fatalf("read backfill: %v", err)
	}
	if nextTo != 999 || clamped != nil {
		t.Errorf("CBBB = next_to %d clamped %v, want 999 unclamped (deep history already healed)", nextTo, clamped)
	}
}

func TestCommitHealChunkIsIdempotentOnRecords(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, events, backfill`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	gap := seedGap(t, s, 100, 200)

	ev := testEvent("0000000000000000150-0000000000", "CAAA", "feed", 0)
	ev.ClosedAt = time.Unix(1_700_000_000, 0).UTC()
	if err := s.CommitHealChunk(ctx, "testnet", gap, 99, true, []Event{ev}, nil, nil, nil, nil); err != nil {
		t.Fatalf("CommitHealChunk() error = %v", err)
	}
	// A replayed commit against a resolved gap must fail loudly instead of
	// silently rewriting watermarks.
	err := s.CommitHealChunk(ctx, "testnet", gap, 99, true, []Event{ev}, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("committing into a resolved gap must fail")
	}
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil || n != 1 {
		t.Errorf("events = %d err = %v, want 1 (idempotency key absorbed the replay)", n, err)
	}
}
