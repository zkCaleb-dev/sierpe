package registry

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// fakeLister returns a fixed contract set (or error) per call.
type fakeLister struct {
	mu        sync.Mutex
	contracts []store.Contract
	err       error
	network   string
}

func (f *fakeLister) ListContracts(_ context.Context, network string) ([]store.Contract, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.network = network
	if f.err != nil {
		return nil, f.err
	}
	return f.contracts, nil
}

func (f *fakeLister) set(contracts []store.Contract, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.contracts, f.err = contracts, err
}

func TestSnapshotEmptyBeforeReload(t *testing.T) {
	r := New("testnet", &fakeLister{})
	snap := r.Snapshot()
	if snap == nil {
		t.Fatal("Snapshot() must never be nil")
	}
	if snap.Len() != 0 {
		t.Errorf("fresh registry Len() = %d, want 0", snap.Len())
	}
	if snap.Watching("CANY") {
		t.Error("fresh registry watches nothing")
	}
}

func TestReloadPublishesContracts(t *testing.T) {
	lister := &fakeLister{contracts: []store.Contract{
		{Network: "testnet", ContractID: "CAAA", Kinds: []string{store.KindEvents}},
		{Network: "testnet", ContractID: "CBBB", Kinds: []string{store.KindEvents}},
	}}
	r := New("testnet", lister)
	if err := r.Reload(context.Background()); err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	if lister.network != "testnet" {
		t.Errorf("Reload queried network %q, want testnet", lister.network)
	}
	snap := r.Snapshot()
	if snap.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", snap.Len())
	}
	if !snap.Watching("CAAA") || !snap.Watching("CBBB") {
		t.Error("expected both contracts watched")
	}
	if c, ok := snap.Get("CAAA"); !ok || c.ContractID != "CAAA" {
		t.Errorf("Get(CAAA) = %+v, %v", c, ok)
	}
	if _, ok := snap.Get("CZZZ"); ok {
		t.Error("Get on unwatched contract must report false")
	}
}

func TestReloadErrorKeepsPreviousSnapshot(t *testing.T) {
	lister := &fakeLister{contracts: []store.Contract{
		{Network: "testnet", ContractID: "CAAA"},
	}}
	r := New("testnet", lister)
	if err := r.Reload(context.Background()); err != nil {
		t.Fatalf("Reload() error = %v", err)
	}

	lister.set(nil, errors.New("db down"))
	if err := r.Reload(context.Background()); err == nil {
		t.Fatal("Reload() with failing store must return the error")
	}
	if !r.Snapshot().Watching("CAAA") {
		t.Error("failed reload must keep the previous snapshot published")
	}
}

func TestReloadRemovesUnregisteredContracts(t *testing.T) {
	lister := &fakeLister{contracts: []store.Contract{
		{Network: "testnet", ContractID: "CAAA"},
		{Network: "testnet", ContractID: "CBBB"},
	}}
	r := New("testnet", lister)
	if err := r.Reload(context.Background()); err != nil {
		t.Fatalf("Reload() error = %v", err)
	}

	lister.set([]store.Contract{{Network: "testnet", ContractID: "CBBB"}}, nil)
	if err := r.Reload(context.Background()); err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	snap := r.Snapshot()
	if snap.Watching("CAAA") {
		t.Error("CAAA was unregistered and must not be watched")
	}
	if !snap.Watching("CBBB") {
		t.Error("CBBB must still be watched")
	}
}

// TestConcurrentSnapshotAndReload exercises the atomic swap under the race
// detector: readers hold immutable snapshots while reloads publish new ones.
func TestConcurrentSnapshotAndReload(t *testing.T) {
	lister := &fakeLister{contracts: []store.Contract{
		{Network: "testnet", ContractID: "CAAA"},
	}}
	r := New("testnet", lister)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snap := r.Snapshot()
					_ = snap.Watching("CAAA")
					_ = snap.Len()
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		if err := r.Reload(context.Background()); err != nil {
			t.Fatalf("Reload() error = %v", err)
		}
	}
	close(stop)
	wg.Wait()
}
