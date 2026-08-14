// Package registry maintains the in-memory set of contracts this instance
// watches: an immutable Snapshot behind an atomic pointer.
//
// The snapshot is rebuilt from the store when registrations change (and at
// boot) and re-read — a single pointer load — once per ledger by the
// ingestion loop. Filtering whole ledgers against it is local and free
// (KNOWLEDGE.md P2): registering one more contract costs zero extra RPC.
package registry

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// lister is the slice of the store the registry consumes (CLAUDE.md rule 5).
type lister interface {
	ListContracts(ctx context.Context, network string) ([]store.Contract, error)
}

// Snapshot is an immutable view of the watched contracts. Nothing mutates a
// Snapshot after Reload publishes it, so it is safe to share across
// goroutines without locks.
type Snapshot struct {
	byID map[string]store.Contract
}

// Watching reports whether data from contractID should be derived.
func (s *Snapshot) Watching(contractID string) bool {
	_, ok := s.byID[contractID]
	return ok
}

// Get returns the registration for contractID, if watched.
func (s *Snapshot) Get(contractID string) (store.Contract, bool) {
	c, ok := s.byID[contractID]
	return c, ok
}

// Len is the number of watched contracts.
func (s *Snapshot) Len() int { return len(s.byID) }

// StaticSnapshot builds a fixed snapshot from explicit contracts. The
// backfill worker uses it to extract for exactly one registration without
// touching the live registry.
func StaticSnapshot(contracts ...store.Contract) *Snapshot {
	byID := make(map[string]store.Contract, len(contracts))
	for _, c := range contracts {
		byID[c.ContractID] = c
	}
	return &Snapshot{byID: byID}
}

// Registry owns the current Snapshot for one network.
type Registry struct {
	network string
	store   lister
	snap    atomic.Pointer[Snapshot]
}

// New returns a Registry holding an empty snapshot; call Reload to populate.
func New(network string, st lister) *Registry {
	r := &Registry{network: network, store: st}
	r.snap.Store(&Snapshot{byID: map[string]store.Contract{}})
	return r
}

// Snapshot returns the current view. Never nil.
func (r *Registry) Snapshot() *Snapshot { return r.snap.Load() }

// Reload rebuilds the snapshot from the store and publishes it atomically.
// On error the previous snapshot stays published: serving a stale set beats
// serving none, and the caller decides whether the error is fatal.
func (r *Registry) Reload(ctx context.Context) error {
	contracts, err := r.store.ListContracts(ctx, r.network)
	if err != nil {
		return fmt.Errorf("registry: reload: %w", err)
	}
	byID := make(map[string]store.Contract, len(contracts))
	for _, c := range contracts {
		byID[c.ContractID] = c
	}
	r.snap.Store(&Snapshot{byID: byID})
	return nil
}
