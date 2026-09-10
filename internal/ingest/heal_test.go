package ingest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// fakeReplayer replays the same hash-linked fake chain the RPC fake serves,
// optionally corrupting bytes so equivalence fails.
type fakeReplayer struct {
	chain     *fakeChunkChain
	corrupt   bool
	replayErr error
	// hold delays every replay so overlapping workers are observable.
	hold time.Duration
	mu   sync.Mutex
	// ranges records every replay; active and overlaps catch two workers
	// inside the same range at once, which is the bug claiming prevents.
	ranges   [][2]uint32
	active   map[uint32]int
	overlaps int
	peak     int
}

func (f *fakeReplayer) ReplayRange(ctx context.Context, from, to uint32, emit func(xdr.LedgerCloseMeta) error) error {
	f.mu.Lock()
	f.ranges = append(f.ranges, [2]uint32{from, to})
	if f.active == nil {
		f.active = make(map[uint32]int)
	}
	f.active[from]++
	if f.active[from] > 1 {
		f.overlaps++
	}
	inFlight := 0
	for _, n := range f.active {
		inFlight += n
	}
	if inFlight > f.peak {
		f.peak = inFlight
	}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active[from]--
		f.mu.Unlock()
	}()
	if f.hold > 0 {
		select {
		case <-time.After(f.hold):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.replayErr != nil {
		return f.replayErr
	}
	for seq := from; seq <= to; seq++ {
		batch, err := f.chain.GetLedgerBatch(ctx, seq, 1)
		if err != nil {
			return err
		}
		lcm := batch[0]
		if f.corrupt {
			// A different close time changes the bytes without touching the
			// sequence: exactly the kind of divergence the gate must catch.
			lcm.V1.LedgerHeader.Header.ScpValue.CloseTime = 42
		}
		if err := emit(lcm); err != nil {
			return err
		}
	}
	return nil
}

// fakeHealStore keeps gaps in memory and records heal commits.
type fakeHealStore struct {
	mu      sync.Mutex
	gaps    map[string]store.Gap
	cursor  store.Cursor
	commits []string // "gapID:newNextTo:resolved"
}

func (f *fakeHealStore) ListOpenGaps(context.Context, string) ([]store.Gap, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.Gap
	for _, g := range f.gaps {
		out = append(out, g)
	}
	return out, nil
}

func (f *fakeHealStore) CommitHealChunk(_ context.Context, _ string, gap store.Gap, newNextTo uint32, resolved bool,
	_ []store.Event, _ []store.StateChange, _ []store.Transfer, _ []store.TrustlineChange, _ []store.Movement) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, fmt.Sprintf("%s:%d:%v", gap.ID, newNextTo, resolved))
	if resolved {
		delete(f.gaps, gap.ID)
		return nil
	}
	g := f.gaps[gap.ID]
	g.HealNextTo = newNextTo
	f.gaps[gap.ID] = g
	return nil
}

func (f *fakeHealStore) LoadCursor(context.Context, string) (store.Cursor, error) {
	return f.cursor, nil
}

// healInst records the archive states the healer reports.
type healInst struct {
	nopInstruments
	mu           sync.Mutex
	states       []string
	gapsHealed   int
	equivFailed  int
	healedTotals int
}

func (h *healInst) SetArchiveState(s string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.states = append(h.states, s)
}
func (h *healInst) IncGapsHealed() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gapsHealed++
}
func (h *healInst) AddHealedLedgers(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healedTotals += n
}
func (h *healInst) IncEquivalenceFailures() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.equivFailed++
}

func (h *healInst) lastState() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.states) == 0 {
		return ""
	}
	return h.states[len(h.states)-1]
}

func emptyWatch(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New("testnet", emptyLister{})
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("reload: %v", err)
	}
	return reg
}

func newTestHealer(t *testing.T, replayer *fakeReplayer, st *fakeHealStore, inst *healInst) *Healer {
	t.Helper()
	h := NewHealer("testnet", "Test SDF Network ; September 2015",
		replayer, replayer.chain, st, emptyWatch(t), inst,
		slog.New(slog.NewTextHandler(io.Discard, nil)), 2000, 1)
	h.idle = 2 * time.Millisecond
	return h
}

// runUntil runs the healer until cond holds or the deadline passes.
func runUntil(t *testing.T, h *Healer, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.Run(ctx); close(done) }()
	deadline := time.After(5 * time.Second)
	for !cond() {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("condition not reached before deadline")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

func TestHealerHealsGapInChunksAndResolves(t *testing.T) {
	// Chain serves everything; the gap spans 4500 ledgers → 3 chunks.
	chain := &fakeChunkChain{oldest: 1, tip: 20_000}
	replayer := &fakeReplayer{chain: chain}
	st := &fakeHealStore{
		cursor: store.Cursor{Sequence: 20_000},
		gaps: map[string]store.Gap{
			"gap:testnet:1000:5499": {ID: "gap:testnet:1000:5499", From: 1000, To: 5499, HealNextTo: 5499},
		},
	}
	inst := &healInst{}
	h := newTestHealer(t, replayer, st, inst)

	runUntil(t, h, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.gaps) == 0
	})

	st.mu.Lock()
	commits := append([]string(nil), st.commits...)
	st.mu.Unlock()
	want := []string{
		"gap:testnet:1000:5499:3499:false",
		"gap:testnet:1000:5499:1499:false",
		"gap:testnet:1000:5499:999:true",
	}
	if len(commits) != len(want) {
		t.Fatalf("commits = %v, want %v", commits, want)
	}
	for i := range want {
		if commits[i] != want[i] {
			t.Errorf("commit %d = %s, want %s", i, commits[i], want[i])
		}
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.gapsHealed != 1 || inst.healedTotals != 4500 {
		t.Errorf("gapsHealed = %d healedLedgers = %d, want 1/4500", inst.gapsHealed, inst.healedTotals)
	}
	if inst.states[len(inst.states)-1] != ArchiveStateVerified {
		t.Errorf("final state = %s, want verified", inst.states[len(inst.states)-1])
	}
}

func TestHealerZeroChunkSizeUsesDefault(t *testing.T) {
	chain := &fakeChunkChain{oldest: 1, tip: 20_000}
	h := NewHealer("testnet", "Test SDF Network ; September 2015",
		&fakeReplayer{chain: chain}, chain, &fakeHealStore{}, emptyWatch(t), &healInst{},
		slog.New(slog.NewTextHandler(io.Discard, nil)), 0, 0)
	if h.chunk != defaultHealChunkLedgers {
		t.Errorf("chunk = %d, want default %d", h.chunk, defaultHealChunkLedgers)
	}
	if h.workers != 1 {
		t.Errorf("workers = %d, want 1 when unset", h.workers)
	}
}

func TestHealerEquivalenceGateRunsBeforeFirstHeal(t *testing.T) {
	chain := &fakeChunkChain{oldest: 1, tip: 20_000}
	replayer := &fakeReplayer{chain: chain}
	st := &fakeHealStore{
		cursor: store.Cursor{Sequence: 20_000},
		gaps: map[string]store.Gap{
			"gap:testnet:100:150": {ID: "gap:testnet:100:150", From: 100, To: 150, HealNextTo: 150},
		},
	}
	h := newTestHealer(t, replayer, st, &healInst{})

	runUntil(t, h, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.gaps) == 0
	})

	replayer.mu.Lock()
	defer replayer.mu.Unlock()
	if len(replayer.ranges) < 2 {
		t.Fatalf("ranges = %v, want the equivalence sample before the heal", replayer.ranges)
	}
	sample := replayer.ranges[0]
	if sample[1]-sample[0]+1 != equivalenceSample {
		t.Errorf("sample = %v, want %d ledgers", sample, equivalenceSample)
	}
	if (sample[1]+1)%checkpointFrequency != 0 {
		t.Errorf("sample end %d is not a checkpoint ledger", sample[1])
	}
	if heal := replayer.ranges[1]; heal[0] != 100 || heal[1] != 150 {
		t.Errorf("heal range = %v, want [100 150]", heal)
	}
}

func TestHealerDivergentReplayDisablesHealing(t *testing.T) {
	chain := &fakeChunkChain{oldest: 1, tip: 20_000}
	replayer := &fakeReplayer{chain: chain, corrupt: true}
	st := &fakeHealStore{
		cursor: store.Cursor{Sequence: 20_000},
		gaps: map[string]store.Gap{
			"gap:testnet:100:150": {ID: "gap:testnet:100:150", From: 100, To: 150, HealNextTo: 150},
		},
	}
	inst := &healInst{}
	h := newTestHealer(t, replayer, st, inst)

	// A divergent replay must stop the worker on its own.
	done := make(chan struct{})
	go func() { h.Run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("healer kept running after a failed equivalence gate")
	}

	if got := inst.lastState(); got != ArchiveStateFailed {
		t.Errorf("state = %s, want equivalence_failed", got)
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.equivFailed != 1 {
		t.Errorf("equivalence failures = %d, want 1", inst.equivFailed)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.commits) != 0 {
		t.Errorf("commits = %v, want none: unverified data must never land", st.commits)
	}
}

func TestHealerChunkFailureRetries(t *testing.T) {
	chain := &fakeChunkChain{oldest: 1, tip: 20_000}
	replayer := &fakeReplayer{chain: chain}
	st := &fakeHealStore{
		cursor: store.Cursor{Sequence: 20_000},
		gaps: map[string]store.Gap{
			// Below the fake chain's oldest: the replay errors every time.
			"gap:testnet:100:150": {ID: "gap:testnet:100:150", From: 100, To: 150, HealNextTo: 150},
		},
	}
	chain.oldest = 200
	inst := &healInst{}
	h := newTestHealer(t, replayer, st, inst)

	// The gate itself needs served ledgers; oldest=200 keeps the sample fine
	// (near the cursor) while the heal range stays unservable.
	runUntil(t, h, func() bool {
		replayer.mu.Lock()
		defer replayer.mu.Unlock()
		return len(replayer.ranges) >= 3 // gate + at least two heal attempts
	})
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.commits) != 0 {
		t.Errorf("commits = %v, want none while the replay keeps failing", st.commits)
	}
	if got := inst.lastState(); got != ArchiveStateVerified {
		t.Errorf("state = %s, want verified (a failing chunk is transient, not a gate failure)", got)
	}
}

func TestHealerClaimHandsAGapToOneWorkerAtATime(t *testing.T) {
	h := &Healer{inFlight: make(map[string]bool)}
	gaps := []store.Gap{
		{ID: "gap:a", From: 100, To: 200},
		{ID: "gap:b", From: 300, To: 400},
	}

	first, ok := h.claim(gaps)
	if !ok || first.ID != "gap:a" {
		t.Fatalf("first claim = %+v %v, want gap:a", first, ok)
	}
	second, ok := h.claim(gaps)
	if !ok || second.ID != "gap:b" {
		t.Fatalf("second claim = %+v %v, want gap:b: a claimed gap must be skipped", second, ok)
	}
	if _, ok := h.claim(gaps); ok {
		t.Error("third claim succeeded; every gap was already held")
	}

	h.release("gap:a")
	again, ok := h.claim(gaps)
	if !ok || again.ID != "gap:a" {
		t.Errorf("after release, claim = %+v %v, want gap:a back", again, ok)
	}
}

func TestHealerWorkersHealDistinctGapsInParallel(t *testing.T) {
	chain := &fakeChunkChain{oldest: 1, tip: 20_000}
	// Every replay is held briefly, so workers genuinely overlap in time.
	replayer := &fakeReplayer{chain: chain, hold: 20 * time.Millisecond}
	st := &fakeHealStore{
		cursor: store.Cursor{Sequence: 20_000},
		gaps: map[string]store.Gap{
			"gap:testnet:1000:1099": {ID: "gap:testnet:1000:1099", From: 1000, To: 1099, HealNextTo: 1099},
			"gap:testnet:2000:2099": {ID: "gap:testnet:2000:2099", From: 2000, To: 2099, HealNextTo: 2099},
			"gap:testnet:3000:3099": {ID: "gap:testnet:3000:3099", From: 3000, To: 3099, HealNextTo: 3099},
		},
	}
	inst := &healInst{}
	h := NewHealer("testnet", "Test SDF Network ; September 2015",
		replayer, chain, st, emptyWatch(t), inst,
		slog.New(slog.NewTextHandler(io.Discard, nil)), 2000, 3)
	h.idle = 2 * time.Millisecond

	runUntil(t, h, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.gaps) == 0
	})

	replayer.mu.Lock()
	defer replayer.mu.Unlock()
	if replayer.overlaps != 0 {
		t.Errorf("%d replays overlapped on the same range; a gap must be healed by one worker at a time", replayer.overlaps)
	}
	if replayer.peak < 2 {
		t.Errorf("peak concurrent replays = %d, want at least 2: three workers on three gaps should overlap", replayer.peak)
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.gapsHealed != 3 {
		t.Errorf("gapsHealed = %d, want 3", inst.gapsHealed)
	}
}

func TestHealerGateRunsOnceBeforeAnyWorker(t *testing.T) {
	chain := &fakeChunkChain{oldest: 1, tip: 20_000}
	replayer := &fakeReplayer{chain: chain}
	st := &fakeHealStore{
		cursor: store.Cursor{Sequence: 20_000},
		gaps: map[string]store.Gap{
			"gap:testnet:1000:1099": {ID: "gap:testnet:1000:1099", From: 1000, To: 1099, HealNextTo: 1099},
			"gap:testnet:2000:2099": {ID: "gap:testnet:2000:2099", From: 2000, To: 2099, HealNextTo: 2099},
		},
	}
	h := NewHealer("testnet", "Test SDF Network ; September 2015",
		replayer, chain, st, emptyWatch(t), &healInst{},
		slog.New(slog.NewTextHandler(io.Discard, nil)), 2000, 4)
	h.idle = 2 * time.Millisecond

	runUntil(t, h, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.gaps) == 0
	})

	replayer.mu.Lock()
	defer replayer.mu.Unlock()
	// The sample is the very first replay and happens exactly once no
	// matter how many workers follow it.
	sample := replayer.ranges[0]
	if sample[1]-sample[0]+1 != equivalenceSample {
		t.Errorf("first replay = %v, want the %d ledger equivalence sample", sample, equivalenceSample)
	}
	for i, r := range replayer.ranges[1:] {
		if r[1]-r[0]+1 == equivalenceSample && r[0] == sample[0] {
			t.Errorf("replay %d repeated the equivalence sample; the gate must run once per process", i+1)
		}
	}
}
