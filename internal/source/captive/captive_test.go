package captive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// fakeBackend serves a scripted range and records calls.
type fakeBackend struct {
	prepared   *ledgerbackend.Range
	prepareErr error
	ledgerErr  map[uint32]error
	// misnumber makes GetLedger return a ledger with the wrong sequence.
	misnumber bool
	closed    bool
}

func (f *fakeBackend) PrepareRange(_ context.Context, r ledgerbackend.Range) error {
	f.prepared = &r
	return f.prepareErr
}

func (f *fakeBackend) GetLedger(_ context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	if err := f.ledgerErr[seq]; err != nil {
		return xdr.LedgerCloseMeta{}, err
	}
	out := seq
	if f.misnumber {
		out = seq + 7
	}
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(out)},
			},
		},
	}, nil
}

func (f *fakeBackend) Close() error {
	f.closed = true
	return nil
}

func testSource(be *fakeBackend) *Source {
	return &Source{
		cfg:        Config{Log: slog.New(slog.NewTextHandler(io.Discard, nil))},
		newBackend: func(context.Context) (backend, error) { return be, nil },
	}
}

func TestReplayRangeStreamsInOrder(t *testing.T) {
	be := &fakeBackend{}
	var got []uint32
	err := testSource(be).ReplayRange(context.Background(), 100, 104, func(lcm xdr.LedgerCloseMeta) error {
		got = append(got, lcm.LedgerSequence())
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayRange() error = %v", err)
	}
	if len(got) != 5 || got[0] != 100 || got[4] != 104 {
		t.Errorf("sequences = %v, want 100..104", got)
	}
	if be.prepared == nil {
		t.Fatal("range never prepared")
	}
	if !be.closed {
		t.Error("backend must be closed after the replay")
	}
}

func TestReplayRangeRejectsInvalidBounds(t *testing.T) {
	be := &fakeBackend{}
	src := testSource(be)
	for _, r := range [][2]uint32{{0, 10}, {10, 5}} {
		if err := src.ReplayRange(context.Background(), r[0], r[1], func(xdr.LedgerCloseMeta) error { return nil }); err == nil {
			t.Errorf("range [%d..%d] must be rejected", r[0], r[1])
		}
	}
	if be.prepared != nil {
		t.Error("invalid ranges must never reach the backend")
	}
}

func TestReplayRangeSurfacesPrepareError(t *testing.T) {
	be := &fakeBackend{prepareErr: errors.New("archives unreachable")}
	err := testSource(be).ReplayRange(context.Background(), 100, 110, func(xdr.LedgerCloseMeta) error { return nil })
	if err == nil || !be.closed {
		t.Errorf("prepare error must surface and still close the backend; err = %v closed = %v", err, be.closed)
	}
}

func TestReplayRangeStopsOnEmitError(t *testing.T) {
	be := &fakeBackend{}
	calls := 0
	sentinel := errors.New("stop here")
	err := testSource(be).ReplayRange(context.Background(), 100, 110, func(xdr.LedgerCloseMeta) error {
		calls++
		if calls == 3 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("emit error must propagate untouched, got %v", err)
	}
	if calls != 3 {
		t.Errorf("emit called %d times, want 3 (stop at the error)", calls)
	}
}

func TestReplayRangeDistrustsMisnumberedLedgers(t *testing.T) {
	be := &fakeBackend{misnumber: true}
	err := testSource(be).ReplayRange(context.Background(), 100, 110, func(xdr.LedgerCloseMeta) error { return nil })
	if err == nil {
		t.Fatal("a core producing the wrong ledger must fail the replay (rule 6)")
	}
}

func TestReplayRangeSurfacesLedgerError(t *testing.T) {
	be := &fakeBackend{ledgerErr: map[uint32]error{102: fmt.Errorf("core died")}}
	var got []uint32
	err := testSource(be).ReplayRange(context.Background(), 100, 110, func(lcm xdr.LedgerCloseMeta) error {
		got = append(got, lcm.LedgerSequence())
		return nil
	})
	if err == nil {
		t.Fatal("a mid-range core failure must fail the replay")
	}
	if len(got) != 2 {
		t.Errorf("emitted %d ledgers before the failure, want 2", len(got))
	}
}
