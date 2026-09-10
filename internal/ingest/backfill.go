package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
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
	CommitBackfillChunk(ctx context.Context, network string, b store.Backfill, events []store.Event, states []store.StateChange, transfers []store.Transfer, trustlines []store.TrustlineChange, movements []store.Movement) error
	RecordGap(ctx context.Context, network string, from, to uint32, reason string) error
}

// backfillInstruments is the metrics slice the backfiller feeds.
type backfillInstruments interface {
	IncBackfillChunks()
	AddBackfillLedgers(n int)
	IncEventsExtracted(n int)
	IncStateChangesExtracted(n int)
	IncTransfersExtracted(n int)
	IncTrustlineChangesExtracted(n int)
	IncMovementsExtracted(n int)
	IncFailedTxs(n int)
	IncSuppressedTxs(n int)
	IncSuppressedEvents(n int)
	IncSuppressedTransfers(n int)
	IncSuppressedTrustlines(n int)
	IncForeignUndecodable(n int)
}

// Backfiller walks every registered contract's history downward in chunks
// aligned to an absolute grid, sharing one scan among the contracts whose
// next chunk is the same range. Each contract still commits its own rows
// and watermark atomically, so a group is a scheduling fact, never a unit
// of failure. Chunks commit atomically (events + watermark); the retention
// wall clamps the walk with one honest gap instead of a silent stop
// (KNOWLEDGE.md P7, Umbra 4).
type Backfiller struct {
	network    string
	passphrase string
	src        chunkSource
	store      backfillStore
	inst       backfillInstruments
	log        *slog.Logger
	// lastScan remembers the one most recent successful scan. A contract
	// whose commit failed trails its group by exactly one grid cell, so
	// every range it asks for next is the range the group scanned last
	// round: the cache turns a split group's trailing walk into zero extra
	// downloads instead of a permanent second copy of the window.
	lastScan *scanCacheEntry
}

// scanCacheEntry is one cached scan: the range, the members it extracted
// for (id plus kinds, so a reconciled registration never reuses rows
// derived under different kinds), and the result.
type scanCacheEntry struct {
	from, to uint32
	members  map[string]string // contract id -> canonical kinds
	res      extract.Result
}

// chunkRange is one grid-aligned scan range; jobs needing the same range
// form one group.
type chunkRange struct {
	from, to uint32
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

// round groups the pending contracts by the exact grid chunk each needs
// next and processes one chunk per group: the range is fetched and
// extracted once for everybody in it, and each contract commits its own
// rows and watermark. A contract walking alone is a group of one — the
// ordinary case — and behaves exactly like a private walk.
func (b *Backfiller) round(ctx context.Context) bool {
	jobs, err := b.store.ListPendingBackfills(ctx, b.network)
	if err != nil {
		if ctx.Err() == nil {
			b.log.Warn("backfill: listing pending work failed", "err", err)
		}
		return false
	}
	worked := false
	// Jobs group by the grid cell of their next chunk (its aligned floor),
	// NOT by exact range: registration anchors carry a few ledgers of
	// jitter, so exact-range grouping would keep same-cell walks one round
	// apart forever. The group scans up to the highest member watermark and
	// everybody lands on the cell floor together; the few ledgers a lower
	// member re-covers are the registration anchor margin, already
	// idempotent by design.
	groups := map[uint32][]store.BackfillJob{}
	tops := map[uint32]uint32{}
	var order []uint32
	for _, job := range jobs {
		bf := job.Backfill
		if bf.NextTo < bf.TargetFrom {
			// Done at birth: the anchor already sits below the target.
			next := bf
			next.Done = true
			if err := b.store.CommitBackfillChunk(ctx, b.network, next, nil, nil, nil, nil, nil); err != nil {
				b.log.Warn("backfill: closing an empty walk failed, will retry",
					"contract_id", job.Contract.ContractID, "err", err)
				continue
			}
			worked = true
			continue
		}
		from := chunkFromFor(bf)
		if _, seen := groups[from]; !seen {
			order = append(order, from)
		}
		groups[from] = append(groups[from], job)
		tops[from] = max(tops[from], bf.NextTo)
	}
	// Highest cell first: a walk trailing another by one grid cell asks for
	// the range scanned immediately before it, so it meets that scan in the
	// cache instead of downloading the cell again.
	//
	// A consequence worth knowing before reading watermarks out of the
	// table: the trailing group holds the higher cell, so it commits FIRST
	// and lands on the watermark the leading group still carries. For the
	// rest of that round the two read as equal. It is a phase of every
	// round, not a merge, and a sample taken inside it looks exactly like
	// one — it fooled the first operator to check.
	sort.Slice(order, func(i, j int) bool { return order[i] > order[j] })
	for _, from := range order {
		if ctx.Err() != nil {
			return worked
		}
		// The group one cell above lands exactly here next round, and the
		// cache only serves contracts the scan extracted for — so extract
		// for them now, while the ledgers are in hand. Without it two
		// groups a cell apart never share a byte: they ask for the same
		// ranges one round apart and miss on membership every time, which
		// is what a batch registered over minutes always produces.
		//
		// Nothing here merges such groups. They collapse into one only by
		// accident, when a round returns false for the leading group and
		// the trailing one advances onto its cell, so a walk cannot be
		// left to converge on its own.
		nextRound := groups[from+backfillChunkSize]
		if b.processGroup(ctx, chunkRange{from: from, to: tops[from]}, groups[from], nextRound) {
			worked = true
		}
	}
	return worked
}

// chunkFromFor aligns a walk's next chunk to the absolute chunk grid, so
// contracts descending through the same region ask for identical ranges no
// matter when each started — which is what makes their scans shareable.
// The target floor still cuts the final chunk short.
func chunkFromFor(bf store.Backfill) uint32 {
	return max(bf.NextTo-(bf.NextTo-1)%backfillChunkSize, bf.TargetFrom)
}

// processGroup scans one grid chunk once for every contract in the group
// and commits each contract's rows and watermark separately. It reports
// whether at least one contract advanced.
//
// ahead are the jobs of the group one cell above, which will ask for this
// exact range next round. They are extracted for and cached but never
// committed here: their own round commits them, off the cache, without
// fetching the cell a second time.
func (b *Backfiller) processGroup(ctx context.Context, rng chunkRange, jobs, ahead []store.BackfillJob) bool {
	contracts := make([]store.Contract, len(jobs))
	for i, j := range jobs {
		contracts[i] = j.Contract
	}
	// partitionResult keys rows by contract, and every commit below takes
	// only its own contract's part, so extracting for more contracts than
	// the group cannot leak a row into the wrong walk.
	scanFor := contracts
	for _, j := range ahead {
		scanFor = append(scanFor, j.Contract)
	}

	res, cached := b.cachedScan(rng, contracts)
	var wall uint32
	clamped := false
	if !cached {
		var err error
		res, err = b.scan(ctx, registry.StaticSnapshot(scanFor...), rng.from, rng.to)
		switch {
		case err == nil:
			b.rememberScan(rng, scanFor, res)

		case errors.Is(err, source.ErrNotYetAvailable):
			// EXPECTED, not a failure: a fresh registration anchors its walk
			// a margin past the live cursor so the ledgers closing during
			// the registry reload belong to somebody. Those ledgers have not
			// closed yet, so the chunk asks for the future and the source
			// rightly says no. It resolves itself as the tip advances;
			// calling it a failure trains the operator to ignore the log
			// line that means something.
			b.log.Info("backfill: waiting for the registration anchor to close",
				"contracts", len(jobs), "anchor", rng.to)
			return false

		case errors.Is(err, source.ErrBelowRetention):
			// Retention only ever cuts from below: find the real wall by
			// asking getLedgers itself (rule 9), never a health endpoint.
			w, werr := b.findWall(ctx, rng.from+1, rng.to)
			if werr != nil {
				b.log.Warn("backfill: locating retention wall failed, will retry", "err", werr)
				return false
			}
			wall, clamped = w, true
			// One gap per distinct target; identical targets collapse into
			// one row through the deterministic gap id. Gaps land BEFORE any
			// clamped chunk commits (gap first, then the work — P7).
			recorded := map[uint32]bool{}
			for _, job := range jobs {
				tf := job.Backfill.TargetFrom
				if recorded[tf] {
					continue
				}
				if err := b.store.RecordGap(ctx, b.network, tf, wall-1,
					"below source retention during backfill; the archive leg can heal this"); err != nil {
					b.log.Warn("backfill: recording gap failed, will retry", "err", err)
					return false
				}
				recorded[tf] = true
			}
			res = extract.Result{}
			if wall <= rng.to {
				res, err = b.scan(ctx, registry.StaticSnapshot(contracts...), wall, rng.to)
				if err != nil {
					b.log.Warn("backfill: scanning above the wall failed, will retry", "err", err)
					return false
				}
			}
			b.log.Warn("backfill: clamped at retention wall",
				"contracts", len(jobs), "unserved_to", wall-1, "covered_from", wall)

		default:
			if ctx.Err() == nil {
				b.log.Warn("backfill: chunk failed, will retry",
					"contracts", len(jobs), "from", rng.from, "to", rng.to, "err", err)
			}
			return false
		}
	}

	parts := partitionResult(res)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].Contract.ContractID < jobs[j].Contract.ContractID
	})
	committed := 0
	for _, job := range jobs {
		bf := job.Backfill
		next := bf
		if clamped {
			w := wall
			next.Done = true
			next.ClampedAt = &w
			if wall <= bf.NextTo {
				next.NextTo = wall - 1
			}
		} else {
			next.NextTo = rng.from - 1 // rng.from >= 1: ledger sequences start at 1
			next.Done = rng.from <= bf.TargetFrom
		}
		p := parts[job.Contract.ContractID].orEmpty()
		if err := b.store.CommitBackfillChunk(ctx, b.network, next,
			p.events, p.states, p.transfers, p.trustlines, p.movements); err != nil {
			// The rest of the group keeps its progress; this contract
			// retries alone next round and rides the scan cache while it
			// trails.
			b.log.Warn("backfill: chunk commit failed, will retry",
				"contract_id", job.Contract.ContractID, "err", err)
			continue
		}
		committed++
	}
	if committed == 0 {
		return false
	}

	// The scan happened once (or not at all, on a cache hit), no matter how
	// many contracts shared it — the instruments say what actually ran.
	if !cached {
		b.inst.IncBackfillChunks()
		scanned := int(rng.to) - int(rng.from) + 1
		if clamped {
			scanned = 0
			if wall <= rng.to {
				scanned = int(rng.to) - int(wall) + 1
			}
		}
		b.inst.AddBackfillLedgers(scanned)
		b.inst.IncEventsExtracted(len(res.Events))
		b.inst.IncStateChangesExtracted(len(res.StateChanges))
		b.inst.IncTransfersExtracted(len(res.Transfers))
		b.inst.IncTrustlineChangesExtracted(len(res.TrustlineChanges))
		b.inst.IncMovementsExtracted(len(res.Movements))
		b.inst.IncFailedTxs(res.FailedTxs)
		b.inst.IncSuppressedTxs(res.SuppressedTxs)
		b.inst.IncSuppressedEvents(res.SuppressedEvents)
		b.inst.IncSuppressedTransfers(res.SuppressedTransfers)
		b.inst.IncSuppressedTrustlines(res.SuppressedTrustlines)
		b.inst.IncForeignUndecodable(res.ForeignUndecodable)
		if res.SuppressedTxs > 0 || res.SuppressedEvents > 0 || res.SuppressedTransfers > 0 || res.SuppressedTrustlines > 0 {
			b.log.Warn("backfill: suppressed unreadable chain data",
				"contracts", len(jobs),
				"txs", res.SuppressedTxs, "events", res.SuppressedEvents,
				"transfers", res.SuppressedTransfers, "trustlines", res.SuppressedTrustlines)
		}
	}
	b.log.Info("backfill: chunk committed",
		"contracts", committed, "of", len(jobs),
		"from", rng.from, "to", rng.to,
		"events", len(res.Events), "state_changes", len(res.StateChanges),
		"transfers", len(res.Transfers), "trustlines", len(res.TrustlineChanges),
		"movements", len(res.Movements),
		"cached", cached)
	return true
}

// cachedScan returns the remembered result when the requested range is
// exactly the last scanned one and every requesting contract was part of
// that scan with the same kinds.
func (b *Backfiller) cachedScan(rng chunkRange, contracts []store.Contract) (extract.Result, bool) {
	c := b.lastScan
	if c == nil || c.from != rng.from || c.to != rng.to {
		return extract.Result{}, false
	}
	for _, ct := range contracts {
		if c.members[ct.ContractID] != canonicalKinds(ct) {
			return extract.Result{}, false
		}
	}
	return c.res, true
}

// rememberScan stores the scan just performed as the one-entry cache.
func (b *Backfiller) rememberScan(rng chunkRange, contracts []store.Contract, res extract.Result) {
	members := make(map[string]string, len(contracts))
	for _, ct := range contracts {
		members[ct.ContractID] = canonicalKinds(ct)
	}
	b.lastScan = &scanCacheEntry{from: rng.from, to: rng.to, members: members, res: res}
}

// canonicalKinds fingerprints a registration's kinds order-independently.
func canonicalKinds(c store.Contract) string {
	kinds := append([]string(nil), c.Kinds...)
	sort.Strings(kinds)
	return strings.Join(kinds, ",")
}

// partitioned is one contract's slice of a shared scan.
type partitioned struct {
	events     []store.Event
	states     []store.StateChange
	transfers  []store.Transfer
	trustlines []store.TrustlineChange
	movements  []store.Movement
}

// partitionResult splits a shared scan's rows by the contract each row
// belongs to; every row type carries its owning contract id.
func partitionResult(res extract.Result) map[string]*partitioned {
	parts := map[string]*partitioned{}
	get := func(id string) *partitioned {
		p := parts[id]
		if p == nil {
			p = &partitioned{}
			parts[id] = p
		}
		return p
	}
	for _, e := range res.Events {
		p := get(e.ContractID)
		p.events = append(p.events, e)
	}
	for _, s := range res.StateChanges {
		p := get(s.ContractID)
		p.states = append(p.states, s)
	}
	for _, t := range res.Transfers {
		p := get(t.ContractID)
		p.transfers = append(p.transfers, t)
	}
	for _, tl := range res.TrustlineChanges {
		p := get(tl.ContractID)
		p.trustlines = append(p.trustlines, tl)
	}
	for _, m := range res.Movements {
		p := get(m.ContractID)
		p.movements = append(p.movements, m)
	}
	return parts
}

// orEmpty returns an empty partition for contracts with no rows in the
// chunk, so commits never dereference a missing map entry.
func (p *partitioned) orEmpty() *partitioned {
	if p == nil {
		return &partitioned{}
	}
	return p
}

// scan fetches and extracts ledgers [from .. to] ascending, verifying hash
// continuity inside the range (rule 6: never trust a source blindly).
func (b *Backfiller) scan(ctx context.Context, snap *registry.Snapshot, from, to uint32) (extract.Result, error) {
	var acc extract.Result
	var prevHash string

	seq := from
	for seq <= to {
		limit := min(int(to-seq)+1, backfillBatchLimit)
		batch, err := b.src.GetLedgerBatch(ctx, seq, limit)
		if err != nil {
			return acc, err
		}
		for _, lcm := range batch {
			info := source.InfoOf(lcm)
			if info.Sequence != seq {
				return acc, fmt.Errorf("asked for ledger %d, source returned %d", seq, info.Sequence)
			}
			if prevHash != "" && info.PreviousHash != prevHash {
				return acc, fmt.Errorf("hash discontinuity at ledger %d inside backfill chunk", info.Sequence)
			}
			prevHash = info.Hash

			res, err := extract.Events(lcm, b.passphrase, snap)
			if err != nil {
				return acc, fmt.Errorf("extract ledger %d: %w", info.Sequence, err)
			}
			acc.Events = append(acc.Events, res.Events...)
			acc.StateChanges = append(acc.StateChanges, res.StateChanges...)
			acc.Transfers = append(acc.Transfers, res.Transfers...)
			acc.TrustlineChanges = append(acc.TrustlineChanges, res.TrustlineChanges...)
			acc.Movements = append(acc.Movements, res.Movements...)
			acc.FailedTxs += res.FailedTxs
			acc.SuppressedTxs += res.SuppressedTxs
			acc.SuppressedEvents += res.SuppressedEvents
			acc.SuppressedTransfers += res.SuppressedTransfers
			acc.SuppressedTrustlines += res.SuppressedTrustlines
			acc.ForeignUndecodable += res.ForeignUndecodable
			seq++
		}
	}
	return acc, nil
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
