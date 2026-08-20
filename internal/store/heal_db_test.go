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
	if err := s.CommitHealChunk(ctx, "testnet", gap, 3499, false, []Event{ev}, nil, nil, nil); err != nil {
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
	if err := s.CommitHealChunk(ctx, "testnet", gaps[0], 999, true, nil, nil, nil, nil); err != nil {
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
	if err := s.CommitHealChunk(ctx, "testnet", gap, 3499, false, nil, nil, nil, nil); err != nil {
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
	if err := s.CommitHealChunk(ctx, "testnet", gaps[0], 999, true, nil, nil, nil, nil); err != nil {
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

func TestCommitHealChunkIsIdempotentOnRecords(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `TRUNCATE gaps, events, backfill`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	gap := seedGap(t, s, 100, 200)

	ev := testEvent("0000000000000000150-0000000000", "CAAA", "feed", 0)
	ev.ClosedAt = time.Unix(1_700_000_000, 0).UTC()
	if err := s.CommitHealChunk(ctx, "testnet", gap, 99, true, []Event{ev}, nil, nil, nil); err != nil {
		t.Fatalf("CommitHealChunk() error = %v", err)
	}
	// A replayed commit against a resolved gap must fail loudly instead of
	// silently rewriting watermarks.
	err := s.CommitHealChunk(ctx, "testnet", gap, 99, true, []Event{ev}, nil, nil, nil)
	if err == nil {
		t.Fatal("committing into a resolved gap must fail")
	}
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil || n != 1 {
		t.Errorf("events = %d err = %v, want 1 (idempotency key absorbed the replay)", n, err)
	}
}
