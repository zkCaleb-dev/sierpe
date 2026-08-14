package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/extract"
	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/source"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

const (
	// backfillChunkSize is how many ledgers one atomic chunk covers. An
	// interruption re-scans at most one chunk; idempotent inserts absorb
	// the overlap.
	backfillChunkSize = 2000
	// backfillBatchLimit is how many ledgers to request per source call
	// (the RPC caps getLedgers pagination at 200).
	backfillBatchLimit = 200
	// backfillIdle paces the worker when there is nothing to do.
	backfillIdle = 10 * time.Second
)

// chunkSource is the slice of the source the backfiller consumes: bulk
// ascending reads, no tip semantics.
type chunkSource interface {
	GetLedgerBatch(ctx context.Context, start uint32, limit int) ([]xdr.LedgerCloseMeta, error)
}

// backfillStore is the store slice the backfiller consumes.
type backfillStore interface {
	ListPendingBackfills(ctx context.Context, network string) ([]store.BackfillJob, error)
	CommitBackfillChunk(ctx context.Context, network string, b store.Backfill, events []store.Event) error
	RecordGap(ctx context.Context, network string, from, to uint32, reason string) error
}

// backfillInstruments is the metrics slice the backfiller feeds.
type backfillInstruments interface {
	IncBackfillChunks()
	AddBackfillLedgers(n int)
	IncEventsExtracted(n int)
	IncFailedTxs(n int)
	IncSuppressedTxs(n int)
	IncSuppressedEvents(n int)
}

// Backfiller walks every registered contract's history downward in chunks,
// oldest progress first, one chunk per contract per round. Chunks commit
// atomically (events + watermark); the retention wall clamps the walk with
// one honest gap instead of a silent stop (KNOWLEDGE.md P7, Umbra 4).
type Backfiller struct {
	network    string
	passphrase string
	src        chunkSource
	store      backfillStore
	inst       backfillInstruments
	log        *slog.Logger
}

// NewBackfiller wires a Backfiller. All collaborators are required.
func NewBackfiller(network, passphrase string, src chunkSource, st backfillStore, inst backfillInstruments, log *slog.Logger) *Backfiller {
	return &Backfiller{network: network, passphrase: passphrase, src: src, store: st, inst: inst, log: log}
}

// Run drives backfill until ctx ends. Transient failures idle and retry;
// nothing here may terminate the process (CLAUDE.md rule 10).
func (b *Backfiller) Run(ctx context.Context) {
	for {
		worked := b.round(ctx)
		if ctx.Err() != nil {
			return
		}
		if !worked {
			if !sleepCtx(ctx, backfillIdle) {
				return
			}
		}
	}
}

// round processes at most one chunk per pending contract and reports
// whether any work happened.
func (b *Backfiller) round(ctx context.Context) bool {
	jobs, err := b.store.ListPendingBackfills(ctx, b.network)
	if err != nil {
		if ctx.Err() == nil {
			b.log.Warn("backfill: listing pending work failed", "err", err)
		}
		return false
	}
	worked := false
	for _, job := range jobs {
		if ctx.Err() != nil {
			return worked
		}
		if err := b.processChunk(ctx, job); err != nil {
			if ctx.Err() == nil {
				b.log.Warn("backfill: chunk failed, will retry",
					"contract_id", job.Contract.ContractID, "err", err)
			}
			continue
		}
		worked = true
	}
	return worked
}

// processChunk walks one chunk [chunkFrom .. next_to] for one contract and
// commits its events plus the moved watermark in one transaction. When the
// retention wall cuts into the chunk, the unserved remainder becomes one
// persisted gap BEFORE the servable part is processed (gap first, then the
// work — P7), and the backfill closes clamped.
func (b *Backfiller) processChunk(ctx context.Context, job store.BackfillJob) error {
	bf := job.Backfill
	if bf.NextTo < bf.TargetFrom {
		next := bf
		next.Done = true
		return b.store.CommitBackfillChunk(ctx, b.network, next, nil)
	}
	chunkFrom := bf.TargetFrom
	if span := bf.NextTo - bf.TargetFrom; span >= backfillChunkSize {
		chunkFrom = bf.NextTo - backfillChunkSize + 1
	}
	snap := registry.StaticSnapshot(job.Contract)

	events, stats, err := b.scan(ctx, snap, chunkFrom, bf.NextTo)
	next := bf
	switch {
	case err == nil:
		next.NextTo = chunkFrom - 1 // chunkFrom >= 1: ledger sequences start at 1
		next.Done = chunkFrom <= bf.TargetFrom

	case errors.Is(err, source.ErrBelowRetention):
		// Retention only ever cuts from below: find the real wall by asking
		// getLedgers itself (rule 9), never a health endpoint.
		wall, werr := b.findWall(ctx, chunkFrom+1, bf.NextTo)
		if werr != nil {
			return fmt.Errorf("locating retention wall: %w", werr)
		}
		if err := b.store.RecordGap(ctx, b.network, bf.TargetFrom, wall-1,
			"below source retention during backfill; the archive leg (v1.1) can heal this"); err != nil {
			return err
		}
		next.Done = true
		next.ClampedAt = &wall
		if wall <= bf.NextTo {
			events, stats, err = b.scan(ctx, snap, wall, bf.NextTo)
			if err != nil {
				return fmt.Errorf("scanning above the wall: %w", err)
			}
			next.NextTo = wall - 1
		}
		b.log.Warn("backfill: clamped at retention wall",
			"contract_id", job.Contract.ContractID,
			"unserved_from", bf.TargetFrom, "unserved_to", wall-1, "covered_from", wall)

	default:
		return err
	}

	if err := b.store.CommitBackfillChunk(ctx, b.network, next, events); err != nil {
		return err
	}

	b.inst.IncBackfillChunks()
	b.inst.AddBackfillLedgers(int(bf.NextTo) - int(next.NextTo))
	b.inst.IncEventsExtracted(len(events))
	b.inst.IncFailedTxs(stats.FailedTxs)
	b.inst.IncSuppressedTxs(stats.SuppressedTxs)
	b.inst.IncSuppressedEvents(stats.SuppressedEvents)
	if stats.SuppressedTxs > 0 || stats.SuppressedEvents > 0 {
		b.log.Warn("backfill: suppressed unreadable chain data",
			"contract_id", job.Contract.ContractID,
			"txs", stats.SuppressedTxs, "events", stats.SuppressedEvents)
	}
	b.log.Info("backfill: chunk committed",
		"contract_id", job.Contract.ContractID,
		"from", next.NextTo+1, "to", bf.NextTo, "events", len(events), "done", next.Done)
	return nil
}

// scan fetches and extracts ledgers [from .. to] ascending, verifying hash
// continuity inside the range (rule 6: never trust a source blindly).
func (b *Backfiller) scan(ctx context.Context, snap *registry.Snapshot, from, to uint32) ([]store.Event, extract.Result, error) {
	var events []store.Event
	var stats extract.Result
	var prevHash string

	seq := from
	for seq <= to {
		limit := int(to-seq) + 1
		if limit > backfillBatchLimit {
			limit = backfillBatchLimit
		}
		batch, err := b.src.GetLedgerBatch(ctx, seq, limit)
		if err != nil {
			return nil, stats, err
		}
		for _, lcm := range batch {
			info := source.InfoOf(lcm)
			if info.Sequence != seq {
				return nil, stats, fmt.Errorf("asked for ledger %d, source returned %d", seq, info.Sequence)
			}
			if prevHash != "" && info.PreviousHash != prevHash {
				return nil, stats, fmt.Errorf("hash discontinuity at ledger %d inside backfill chunk", info.Sequence)
			}
			prevHash = info.Hash

			res, err := extract.Events(lcm, b.passphrase, snap)
			if err != nil {
				return nil, stats, fmt.Errorf("extract ledger %d: %w", info.Sequence, err)
			}
			events = append(events, res.Events...)
			stats.FailedTxs += res.FailedTxs
			stats.SuppressedTxs += res.SuppressedTxs
			stats.SuppressedEvents += res.SuppressedEvents
			seq++
		}
	}
	return events, stats, nil
}

// findWall binary-searches [lo .. hi] for the oldest ledger the source will
// actually serve. Returns hi+1 when nothing in the range is served.
func (b *Backfiller) findWall(ctx context.Context, lo, hi uint32) (uint32, error) {
	for lo <= hi {
		mid := lo + (hi-lo)/2
		_, err := b.src.GetLedgerBatch(ctx, mid, 1)
		switch {
		case err == nil:
			if mid == lo {
				return mid, nil
			}
			hi = mid
		case errors.Is(err, source.ErrBelowRetention):
			if mid == hi {
				return hi + 1, nil
			}
			lo = mid + 1
		default:
			return 0, err
		}
	}
	return lo, nil
}
