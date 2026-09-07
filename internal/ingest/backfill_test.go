package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/extract"
	"github.com/zkCaleb-dev/sierpe/internal/source"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// fakeChunkChain serves hash-linked, transaction-free ledgers between
// oldest and tip; requests below oldest classify as below-retention. It
// counts batch calls so tests can assert how often a range was fetched.
type fakeChunkChain struct {
	oldest, tip uint32
	calls       int
}

func (f *fakeChunkChain) GetLedgerBatch(_ context.Context, start uint32, limit int) ([]xdr.LedgerCloseMeta, error) {
	f.calls++
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
// gap and chunk writes. failCommits injects per-contract commit failures.
type fakeBackfillStore struct {
	mu          sync.Mutex
	jobs        map[string]store.BackfillJob
	gaps        []string
	order       []string // "gap:..." / "chunk:contract:nextTo:done"
	failCommits map[string]int
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
	if f.failCommits[b.ContractID] > 0 {
		f.failCommits[b.ContractID]--
		return fmt.Errorf("injected commit failure for %s", b.ContractID)
	}
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

// Chunks sit on an absolute 2000-ledger grid: a walk anchored at 5000
// first takes the partial cell [4001..5000], then full cells down to the
// target.
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
	want := []string{"chunk:CAAA:4000:false", "chunk:CAAA:2000:false", "chunk:CAAA:0:true"}
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
	// The wall (oldest=1200) falls inside the grid cell [1..2000]: the
	// first round walks the servable cell [2001..3000]; the second hits
	// retention, records the unserved remainder [1..1199] as one gap, and
	// still scans the servable tail [1200..2000] before the clamp commits.
	src := &fakeChunkChain{oldest: 1200, tip: 5000}
	st := newFakeBackfillStore(job("CAAA", 1, 3000))
	b := newTestBackfiller(src, st)

	for i := 0; i < 2; i++ {
		if !b.round(context.Background()) {
			t.Fatalf("round %d did no work", i)
		}
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
	if len(st.order) != 3 || st.order[1] != "gap:1:1199" {
		t.Errorf("write order = %v, want the gap before the clamped commit", st.order)
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
	if bf.NextTo != 2000 {
		t.Errorf("next_to after resume = %d, want 2000", bf.NextTo)
	}
}

// Two contracts whose next chunk sits in the same grid cell share one
// scan: the range is fetched once, both watermarks land on the cell floor.
func TestBackfillGroupSharesOneScan(t *testing.T) {
	src := &fakeChunkChain{oldest: 1, tip: 5000}
	st := newFakeBackfillStore(job("CAAA", 1, 5000), job("CBBB", 1, 5000))
	b := newTestBackfiller(src, st)

	if !b.round(context.Background()) {
		t.Fatal("round did no work")
	}
	// The partial cell [4001..5000] is 1000 ledgers = 5 batches of 200,
	// fetched once for the whole group — not once per contract.
	if src.calls != 5 {
		t.Errorf("source batch calls = %d, want 5 (one shared scan)", src.calls)
	}
	for _, id := range []string{"CAAA", "CBBB"} {
		if bf := st.backfill(id); bf.NextTo != 4000 {
			t.Errorf("%s next_to = %d, want 4000", id, bf.NextTo)
		}
	}
}

// Registration anchors carry a few ledgers of jitter (each anchors at the
// cursor of its own registration instant). Walks anchored in the same grid
// cell must converge into one group on the very first chunk — exact-range
// grouping kept them one round apart forever, scanning every cell twice.
// Found live on the first smoke of this feature.
func TestBackfillStaggeredAnchorsConvergeInTheFirstCell(t *testing.T) {
	src := &fakeChunkChain{oldest: 1, tip: 5000}
	st := newFakeBackfillStore(job("CAAA", 1, 4993), job("CBBB", 1, 4997))
	b := newTestBackfiller(src, st)

	if !b.round(context.Background()) {
		t.Fatal("round did no work")
	}
	// One scan of [4001..4997] (the cell floor up to the highest member):
	// ceil(997/200) = 5 batches, once for both.
	if src.calls != 5 {
		t.Errorf("source batch calls = %d, want 5 (staggered anchors must share the cell scan)", src.calls)
	}
	for _, id := range []string{"CAAA", "CBBB"} {
		if bf := st.backfill(id); bf.NextTo != 4000 {
			t.Errorf("%s next_to = %d, want 4000 (landed together on the cell floor)", id, bf.NextTo)
		}
	}
	// From here on they are in lockstep: the next round is one shared scan.
	calls := src.calls
	if !b.round(context.Background()) {
		t.Fatal("second round did no work")
	}
	if got := src.calls - calls; got != 10 {
		t.Errorf("second-round batch calls = %d, want 10 (one scan of [2001..4000])", got)
	}
}

// A commit failure isolates to its contract: the rest of the group keeps
// its progress, and the trailing contract retries through the scan cache
// without downloading the range a second time.
func TestBackfillCommitFailureIsolatesAndTrailsOnTheCache(t *testing.T) {
	src := &fakeChunkChain{oldest: 1, tip: 5000}
	st := newFakeBackfillStore(job("CAAA", 1, 5000), job("CBBB", 1, 5000))
	st.failCommits = map[string]int{"CBBB": 1}
	b := newTestBackfiller(src, st)

	if !b.round(context.Background()) {
		t.Fatal("first round did no work")
	}
	if bf := st.backfill("CAAA"); bf.NextTo != 4000 {
		t.Errorf("CAAA next_to = %d, want 4000 (unaffected by the peer failure)", bf.NextTo)
	}
	if bf := st.backfill("CBBB"); bf.NextTo != 5000 {
		t.Errorf("CBBB next_to = %d, want 5000 (failed commit keeps the watermark)", bf.NextTo)
	}
	afterFirst := src.calls

	if !b.round(context.Background()) {
		t.Fatal("second round did no work")
	}
	if bf := st.backfill("CBBB"); bf.NextTo != 4000 {
		t.Errorf("CBBB next_to = %d, want 4000 (retried alone)", bf.NextTo)
	}
	if bf := st.backfill("CAAA"); bf.NextTo != 2000 {
		t.Errorf("CAAA next_to = %d, want 2000 (kept walking)", bf.NextTo)
	}
	// CBBB's retry of [4001..5000] must ride the cache: only CAAA's next
	// cell [2001..4000] (10 batches) may hit the source.
	if got := src.calls - afterFirst; got != 10 {
		t.Errorf("second-round batch calls = %d, want 10 (trailer must not re-download)", got)
	}
}

// A shared scan's rows split by the contract each row belongs to; a group
// member with no rows in the chunk gets an empty partition, never a nil.
func TestPartitionResultSplitsByContract(t *testing.T) {
	res := extract.Result{
		Events: []store.Event{
			{ID: "e1", ContractID: "CAAA"},
			{ID: "e2", ContractID: "CBBB"},
			{ID: "e3", ContractID: "CAAA"},
		},
		Movements:    []store.Movement{{TransferID: "m1", ContractID: "CBBB"}},
		StateChanges: []store.StateChange{{ID: "s1", ContractID: "CAAA"}},
	}
	parts := partitionResult(res)
	if p := parts["CAAA"].orEmpty(); len(p.events) != 2 || len(p.states) != 1 || len(p.movements) != 0 {
		t.Errorf("CAAA partition = %d events, %d states, %d movements", len(p.events), len(p.states), len(p.movements))
	}
	if p := parts["CBBB"].orEmpty(); len(p.events) != 1 || len(p.movements) != 1 {
		t.Errorf("CBBB partition = %d events, %d movements", len(p.events), len(p.movements))
	}
	if p := parts["CNONE"].orEmpty(); p == nil || len(p.events) != 0 {
		t.Errorf("a contract with no rows must get an empty partition, got %+v", p)
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
	// The grid puts the anchor cell at [4001..5000] and the target cuts the
	// final cell to the single ledger 4000, so the walk takes two commits.
	src.tip = 5000
	if !b.round(context.Background()) {
		t.Fatal("the walk did not resume once the anchor closed")
	}
	if bf := st.backfill("CAAA"); bf.NextTo != 4000 || bf.Done {
		t.Errorf("backfill after the anchor cell = %+v, want next_to 4000 and not done", bf)
	}
	if !b.round(context.Background()) {
		t.Fatal("the final cell did not land")
	}
	if bf := st.backfill("CAAA"); bf.NextTo != 3999 || !bf.Done {
		t.Errorf("backfill = %+v, want the walk done just below the target", bf)
	}
}
