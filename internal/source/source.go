// Package source defines the seam between ingestion and wherever ledgers
// come from. Implementations live in subpackages (rpc today; captive core
// and datastore later) and are interchangeable via configuration.
//
// Design rules (docs/DESIGN.md §3, CLAUDE.md rule 9): serving authority is
// what the source can actually deliver, never a health endpoint's claim;
// window errors distinguish "not closed yet" (wait) from "below retention"
// (fail fast, record a gap).
package source

import (
	"context"
	"errors"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// Sentinel errors implementations must return so the ingest loop can react
// correctly. Wrap them with %w and context.
var (
	// ErrNotYetAvailable means the requested ledger is beyond the source's
	// current tip. The caller should wait and retry.
	ErrNotYetAvailable = errors.New("ledger not yet available")
	// ErrBelowRetention means the requested ledger is older than what the
	// source can serve. The caller must clamp and record a gap, or switch
	// to an archive-capable source.
	ErrBelowRetention = errors.New("ledger below source retention")
)

// Source delivers ledgers and answers what range it can serve.
type Source interface {
	// GetLedger fetches one closed ledger by sequence.
	GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error)
	// LatestLedger reports the source's current tip.
	LatestLedger(ctx context.Context) (uint32, error)
	// VerifyNetwork fails when the source serves a different network than
	// the given passphrase. Called once at boot; a mismatch is fatal.
	VerifyNetwork(ctx context.Context, passphrase string) error
	Close() error
}

// Info extracts the loop-relevant fields from a ledger's header.
type Info struct {
	Sequence     uint32
	Hash         string
	PreviousHash string
	ClosedAt     time.Time
}

// InfoOf reads sequence, hashes, and close time from a LedgerCloseMeta.
func InfoOf(lcm xdr.LedgerCloseMeta) Info {
	header := lcm.LedgerHeaderHistoryEntry()
	return Info{
		Sequence:     uint32(header.Header.LedgerSeq),
		Hash:         header.Hash.HexString(),
		PreviousHash: header.Header.PreviousLedgerHash.HexString(),
		ClosedAt:     time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC(),
	}
}
