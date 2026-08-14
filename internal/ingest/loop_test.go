package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/source"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// --- fakes -----------------------------------------------------------------

// fakeChain builds LedgerCloseMeta values with a consistent hash chain so
// continuity checks can be exercised for real.
type fakeChain struct {
	tip        uint32
	passphrase string
	// tamperPrevAt, when set, corrupts the previous-hash of that ledger.
	tamperPrevAt uint32
	calls        int
}

func hashOf(seq uint32) xdr.Hash {
	return sha256.Sum256([]byte(fmt.Sprintf("ledger-%d", seq)))
}

func (f *fakeChain) GetLedger(_ context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	f.calls++
	if seq > f.tip {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("beyond: %w", source.ErrNotYetAvailable)
	}
	prev := hashOf(seq - 1)
	if seq == f.tamperPrevAt {
		prev = sha256.Sum256([]byte("tampered"))
	}
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: hashOf(seq),
				Header: xdr.LedgerHeader{
					LedgerSeq:          xdr.Uint32(seq),
					PreviousLedgerHash: prev,
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(1_700_000_000 + int64(seq)*5),
					},
				},
			},
		},
	}, nil
}

func (f *fakeChain) LatestLedger(context.Context) (uint32, error) { return f.tip, nil }

func (f *fakeChain) VerifyNetwork(_ context.Context, p string) error {
	if p != f.passphrase {
		return fmt.Errorf("network mismatch")
	}
	return nil
}

type fakeStore struct {
	cursor *store.Cursor

	mu        sync.Mutex
	committed []store.LedgerRecord
}

func (s *fakeStore) LoadCursor(context.Context, string) (store.Cursor, error) {
	if s.cursor == nil {
		return store.Cursor{}, store.ErrNoCursor
	}
	return *s.cursor, nil
}

func (s *fakeStore) CommitLedger(_ context.Context, _ string, rec store.LedgerRecord, _ []store.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.committed = append(s.committed, rec)
	return nil
}

// snapshot returns a copy of the committed records, safe to read while the
// loop is still running.
func (s *fakeStore) snapshot() []store.LedgerRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.LedgerRecord(nil), s.committed...)
}

type nopObserver struct{}

func (nopObserver) SetReady(bool)                         {}
func (nopObserver) Observe(uint32, uint32, time.Duration) {}

type nopInstruments struct{}

func (nopInstruments) IncLedgersIngested()         {}
func (nopInstruments) SetTipLag(time.Duration)     {}
func (nopInstruments) ObserveCommit(time.Duration) {}
func (nopInstruments) IncEventsExtracted(int)      {}
func (nopInstruments) IncFailedTxs(int)            {}
func (nopInstruments) IncSuppressedTxs(int)        {}
func (nopInstruments) IncSuppressedEvents(int)     {}

// emptyLister backs a registry that watches nothing; extraction over the
// fake chain ledgers (which carry no transactions) stays a no-op.
type emptyLister struct{}

func (emptyLister) ListContracts(context.Context, string) ([]store.Contract, error) {
	return nil, nil
}

func newTestLoop(src ledgerSource, st cursorStore, startLedger uint32) *Loop {
	return New(
		Config{Network: "testnet", Passphrase: "test-pass", StartLedger: startLedger},
		src, st, registry.New("testnet", emptyLister{}), nopObserver{}, nopInstruments{},
		slog.New(slog.NewTextHandler(discard{}, nil)),
	)
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// --- tests -----------------------------------------------------------------

// The happy path: resume from a cursor, ingest to the tip, commit in order,
// stop when cancelled.
func TestRunResumesAndCommitsInOrder(t *testing.T) {
	src := &fakeChain{tip: 105, passphrase: "test-pass"}
	prev := hashOf(100)
	st := &fakeStore{cursor: &store.Cursor{Sequence: 100, Hash: hex.EncodeToString(prev[:])}}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		// Cancel once the loop has had time to reach the tip and idle.
		for len(st.snapshot()) < 5 {
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
	}()

	if err := newTestLoop(src, st, 0).Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	committed := st.snapshot()
	if len(committed) != 5 {
		t.Fatalf("committed %d ledgers, want 5 (101..105)", len(committed))
	}
	for i, rec := range committed {
		if want := uint32(101 + i); rec.Sequence != want {
			t.Errorf("commit %d: sequence %d, want %d", i, rec.Sequence, want)
		}
	}
}

// Continuity: a ledger whose previous-hash does not match the committed
// chain must stop the loop with a loud error, and nothing may be committed
// at or after the divergence.
func TestRunStopsOnChainDivergence(t *testing.T) {
	src := &fakeChain{tip: 105, passphrase: "test-pass", tamperPrevAt: 103}
	prev := hashOf(100)
	st := &fakeStore{cursor: &store.Cursor{Sequence: 100, Hash: hex.EncodeToString(prev[:])}}

	err := newTestLoop(src, st, 0).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "chain divergence") {
		t.Fatalf("Run() error = %v, want chain divergence", err)
	}
	for _, rec := range st.committed {
		if rec.Sequence >= 103 {
			t.Errorf("ledger %d committed after divergence point", rec.Sequence)
		}
	}
}

// Continuity must also hold on the FIRST ledger after a resume: the stored
// cursor hash is the expected predecessor.
func TestRunStopsWhenResumePredecessorMismatches(t *testing.T) {
	src := &fakeChain{tip: 105, passphrase: "test-pass"}
	st := &fakeStore{cursor: &store.Cursor{Sequence: 100, Hash: "not-the-real-hash"}}

	err := newTestLoop(src, st, 0).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "chain divergence") {
		t.Fatalf("Run() error = %v, want chain divergence on resume", err)
	}
	if len(st.committed) != 0 {
		t.Errorf("committed %d ledgers on a divergent resume, want 0", len(st.committed))
	}
}

// Reset detection: a cursor far beyond the tip is a network reset (or a
// wrong-network source), never something to write through.
func TestRunDetectsNetworkReset(t *testing.T) {
	src := &fakeChain{tip: 1_000, passphrase: "test-pass"}
	st := &fakeStore{cursor: &store.Cursor{Sequence: 4_000_000, Hash: "whatever"}}

	err := newTestLoop(src, st, 0).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "network reset") {
		t.Fatalf("Run() error = %v, want network reset detection", err)
	}
	if len(st.committed) != 0 {
		t.Errorf("committed %d ledgers after reset detection, want 0", len(st.committed))
	}
}

// A wrong-network source must be refused at boot.
func TestRunRefusesWrongNetwork(t *testing.T) {
	src := &fakeChain{tip: 10, passphrase: "other-pass"}
	st := &fakeStore{}

	err := newTestLoop(src, st, 0).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "network mismatch") {
		t.Fatalf("Run() error = %v, want network mismatch", err)
	}
}

// Below-retention without an archive source stops loudly instead of
// skipping silently.
func TestRunStopsBelowRetention(t *testing.T) {
	src := &belowRetentionSource{fakeChain{tip: 105, passphrase: "test-pass"}}
	st := &fakeStore{cursor: nil}

	err := newTestLoop(src, st, 50).Run(context.Background())
	if err == nil || !errors.Is(err, source.ErrBelowRetention) {
		t.Fatalf("Run() error = %v, want ErrBelowRetention", err)
	}
}

type belowRetentionSource struct{ fakeChain }

func (s *belowRetentionSource) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	if seq < 100 {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("old: %w", source.ErrBelowRetention)
	}
	return s.fakeChain.GetLedger(ctx, seq)
}
