package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/source"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// fakeChunkChain serves hash-linked, transaction-free ledgers between
// oldest and tip; requests below oldest classify as below-retention.
type fakeChunkChain struct {
	oldest, tip uint32
}

func (f *fakeChunkChain) GetLedgerBatch(_ context.Context, start uint32, limit int) ([]xdr.LedgerCloseMeta, error) {
	if start < f.oldest {
		return nil, fmt.Errorf("ledger %d below oldest %d: %w", start, f.oldest, source.ErrBelowRetention)
	}
	if start > f.tip {
		return nil, fmt.Errorf("ledger %d beyond tip %d: %w", start, f.tip, source.ErrNotYetAvailable)
	}
	end := start + uint32(limit) - 1
	if end > f.tip {
		end = f.tip
	}
	out := make([]xdr.LedgerCloseMeta, 0, end-start+1)
	for seq := start; seq <= end; seq++ {
		out = append(out, xdr.LedgerCloseMeta{
			V: 1,
			V1: &xdr.LedgerCloseMetaV1{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Hash: hashOf(seq),
					Header: xdr.LedgerHeader{
						LedgerSeq:          xdr.Uint32(seq),
						PreviousLedgerHash: hashOf(seq - 1),
						ScpValue:           xdr.StellarValue{CloseTime: xdr.TimePoint(1_700_000_000)},
					},
				},
				// A valid empty tx set: the reader must open these ledgers.
				TxSet: xdr.GeneralizedTransactionSet{
					V:       1,
					V1TxSet: &xdr.TransactionSetV1{},
				},
			},
		})
	}
	return out, nil
}

// fakeBackfillStore keeps backfill rows in memory and records the order of
// gap and chunk writes.
type fakeBackfillStore struct {
	mu    sync.Mutex
	jobs  map[string]store.BackfillJob
	gaps  []string
	order []string // "gap:..." / "chunk:contract:nextTo:done"
}

func newFakeBackfillStore(jobs ...store.BackfillJob) *fakeBackfillStore {
	m := map[string]store.BackfillJob{}
	for _, j := range jobs {
		m[j.Contract.ContractID] = j
	}
	return &fakeBackfillStore{jobs: m}
}

func (f *fakeBackfillStore) ListPendingBackfills(context.Context, string) ([]store.BackfillJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.BackfillJob
	for _, j := range f.jobs {
		if !j.Backfill.Done {
			out = append(out, j)
		}
	}
	return out, nil
}

func (f *fakeBackfillStore) CommitBackfillChunk(_ context.Context, _ string, b store.Backfill, _ []store.Event, _ []store.StateChange, _ []store.Transfer, _ []store.TrustlineChange, _ []store.Movement) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[b.ContractID]
	j.Backfill = b
	f.jobs[b.ContractID] = j
	f.order = append(f.order, fmt.Sprintf("chunk:%s:%d:%v", b.ContractID, b.NextTo, b.Done))
	return nil
}

func (f *fakeBackfillStore) RecordGap(_ context.Context, _ string, from, to uint32, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	gap := fmt.Sprintf("gap:%d:%d", from, to)
	f.gaps = append(f.gaps, gap)
	f.order = append(f.order, gap)
	return nil
}

func (f *fakeBackfillStore) backfill(contractID string) store.Backfill {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.jobs[contractID].Backfill
}

func job(contractID string, targetFrom, nextTo uint32) store.BackfillJob {
	return store.BackfillJob{
		Backfill: store.Backfill{ContractID: contractID, TargetFrom: targetFrom, NextTo: nextTo},
		Contract: store.Contract{
			Network: "testnet", ContractID: contractID, Kinds: []string{store.KindEvents},
		},
	}
}

func newTestBackfiller(src chunkSource, st backfillStore) *Backfiller {
	return NewBackfiller("testnet", "test-pass", src, st,
		nopInstruments{}, slog.New(slog.NewTextHandler(discard{}, nil)))
}

func (nopInstruments) IncBackfillChunks()     {}
func (nopInstruments) AddBackfillLedgers(int) {}

func TestBackfillWalksDescendingChunksToTarget(t *testing.T) {
	src := &fakeChunkChain{oldest: 1, tip: 5000}
	st := newFakeBackfillStore(job("CAAA", 1, 5000))
	b := newTestBackfiller(src, st)

	for i := 0; i < 3; i++ {
		if !b.round(context.Background()) {
			t.Fatalf("round %d did no work", i)
		}
	}
	bf := st.backfill("CAAA")
	if !bf.Done {
		t.Errorf("backfill not done after 3 chunks: %+v", bf)
	}
	want := []string{"chunk:CAAA:3000:false", "chunk:CAAA:1000:false", "chunk:CAAA:0:true"}
	if len(st.order) != 3 {
		t.Fatalf("commits = %v", st.order)
	}
	for i, w := range want {
		if st.order[i] != w {
			t.Errorf("commit %d = %s, want %s", i, st.order[i], w)
		}
	}
	if bf.ClampedAt != nil {
		t.Errorf("full-retention walk must not clamp, got %d", *bf.ClampedAt)
	}
}

func TestBackfillClampsAtRetentionWall(t *testing.T) {
	// The wall (oldest=1200) falls inside the chunk [1001..3000]: the
	// unserved remainder [1..1199] must become one gap, and the servable
	// tail [1200..3000] must still be scanned before the clamp commits.
	src := &fakeChunkChain{oldest: 1200, tip: 5000}
	st := newFakeBackfillStore(job("CAAA", 1, 3000))
	b := newTestBackfiller(src, st)

	if !b.round(context.Background()) {
		t.Fatal("round did no work")
	}
	bf := st.backfill("CAAA")
	if !bf.Done {
		t.Errorf("clamped backfill must be done: %+v", bf)
	}
	if bf.ClampedAt == nil || *bf.ClampedAt != 1200 {
		t.Fatalf("clamped_at = %v, want 1200", bf.ClampedAt)
	}
	if bf.NextTo != 1199 {
		t.Errorf("next_to = %d, want 1199 (coverage starts at the wall)", bf.NextTo)
	}
	if len(st.gaps) != 1 || st.gaps[0] != "gap:1:1199" {
		t.Errorf("gaps = %v, want exactly gap:1:1199", st.gaps)
	}
	// P7: the gap must be persisted before the clamped chunk commits.
	if len(st.order) < 2 || st.order[0] != "gap:1:1199" {
		t.Errorf("write order = %v, want the gap first", st.order)
	}
}

func TestBackfillNothingServableClampsEverything(t *testing.T) {
	src := &fakeChunkChain{oldest: 9000, tip: 9500}
	st := newFakeBackfillStore(job("CAAA", 1, 800))
	b := newTestBackfiller(src, st)

	if !b.round(context.Background()) {
		t.Fatal("round did no work")
	}
	bf := st.backfill("CAAA")
	if !bf.Done || bf.ClampedAt == nil || *bf.ClampedAt != 801 {
		t.Errorf("backfill = %+v, want done with clamped_at 801", bf)
	}
	if len(st.gaps) != 1 || st.gaps[0] != "gap:1:800" {
		t.Errorf("gaps = %v, want the whole range as one gap", st.gaps)
	}
}

func TestBackfillResumesFromWatermark(t *testing.T) {
	src := &fakeChunkChain{oldest: 1, tip: 5000}
	st := newFakeBackfillStore(job("CAAA", 1, 5000))
	b := newTestBackfiller(src, st)

	// One chunk, then simulate a restart with a fresh Backfiller.
	if !b.round(context.Background()) {
		t.Fatal("first round did no work")
	}
	restarted := newTestBackfiller(src, st)
	if !restarted.round(context.Background()) {
		t.Fatal("resumed round did no work")
	}
	bf := st.backfill("CAAA")
	if bf.NextTo != 1000 {
		t.Errorf("next_to after resume = %d, want 1000", bf.NextTo)
	}
}

func TestFindWall(t *testing.T) {
	cases := []struct {
		oldest, lo, hi, want uint32
	}{
		{oldest: 1200, lo: 2, hi: 3000, want: 1200},
		{oldest: 2, lo: 2, hi: 3000, want: 2},
		{oldest: 3000, lo: 2, hi: 3000, want: 3000},
		{oldest: 5000, lo: 2, hi: 3000, want: 3001}, // nothing servable
	}
	for _, tc := range cases {
		src := &fakeChunkChain{oldest: tc.oldest, tip: 10_000}
		b := newTestBackfiller(src, newFakeBackfillStore())
		got, err := b.findWall(context.Background(), tc.lo, tc.hi)
		if err != nil {
			t.Fatalf("findWall(oldest=%d) error = %v", tc.oldest, err)
		}
		if got != tc.want {
			t.Errorf("findWall(oldest=%d, %d..%d) = %d, want %d",
				tc.oldest, tc.lo, tc.hi, got, tc.want)
		}
	}
}

// levelRecorder captures the levels and messages the backfiller logs.
type levelRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *levelRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *levelRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
	return nil
}
func (r *levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *levelRecorder) WithGroup(string) slog.Handler      { return r }

func (r *levelRecorder) worst() slog.Level {
	r.mu.Lock()
	defer r.mu.Unlock()
	worst := slog.LevelDebug
	for _, rec := range r.records {
		if rec.Level > worst {
			worst = rec.Level
		}
	}
	return worst
}

// A fresh registration deliberately anchors its walk past the live cursor,
// so the first chunk asks for ledgers that have not closed yet. That is the
// design working, not a failure: logging it as one trains the operator to
// ignore the line that does mean something. It must also resolve itself
// once the tip advances, without any intervention.
func TestBackfillAnchoredPastTheTipWaitsInsteadOfFailing(t *testing.T) {
	src := &fakeChunkChain{oldest: 1, tip: 4980}
	st := newFakeBackfillStore(job("CAAA", 4000, 5000)) // anchor 20 past the tip
	rec := &levelRecorder{}
	b := NewBackfiller("testnet", "test-pass", src, st, nopInstruments{}, slog.New(rec))

	if b.round(context.Background()) {
		t.Error("a chunk that reaches past the tip did no real work")
	}
	if lvl := rec.worst(); lvl >= slog.LevelWarn {
		t.Errorf("worst log level = %v, want below WARN: waiting for the anchor is expected", lvl)
	}
	if len(st.order) != 0 {
		t.Errorf("nothing may be committed while the anchor is in the future: %v", st.order)
	}

	// The tip catches up: the same job now completes with no intervention.
	src.tip = 5000
	if !b.round(context.Background()) {
		t.Fatal("the walk did not resume once the anchor closed")
	}
	// The span (4000..5000) is under one chunk, so the walk finishes it in
	// a single commit and lands the watermark just below the target.
	if bf := st.backfill("CAAA"); bf.NextTo != 3999 || !bf.Done {
		t.Errorf("backfill = %+v, want the chunk landed and the walk done", bf)
	}
}
