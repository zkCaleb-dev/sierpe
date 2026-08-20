package ingest

import (
	"bytes"
	"context"
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
	// healChunkSize is how many ledgers one atomic heal chunk covers.
	healChunkSize = 2000
	// healIdle paces the worker when there are no open gaps.
	healIdle = 60 * time.Second
	// equivalenceSample is how many ledgers the gate compares byte-for-byte
	// between RPC and the captive replay before trusting a single heal.
	equivalenceSample = 8
	// checkpointFrequency is the archives' checkpoint cadence: a bounded
	// captive range must end at or below a published checkpoint ledger.
	checkpointFrequency = 64
)

// Archive states reported in /status.
const (
	ArchiveStateOff        = "off"
	ArchiveStateUnverified = "unverified"
	ArchiveStateVerified   = "verified"
	ArchiveStateFailed     = "equivalence_failed"
)

// healReplayer is the archive-source slice the healer consumes.
type healReplayer interface {
	ReplayRange(ctx context.Context, from, to uint32, emit func(xdr.LedgerCloseMeta) error) error
}

// healStore is the store slice the healer consumes.
type healStore interface {
	ListOpenGaps(ctx context.Context, network string) ([]store.Gap, error)
	CommitHealChunk(ctx context.Context, network string, gap store.Gap, newNextTo uint32, resolved bool,
		events []store.Event, states []store.StateChange, transfers []store.Transfer, trustlines []store.TrustlineChange) error
	LoadCursor(ctx context.Context, network string) (store.Cursor, error)
}

// healInstruments is the metrics slice the healer feeds.
type healInstruments interface {
	IncGapsHealed()
	AddHealedLedgers(n int)
	IncEquivalenceFailures()
	SetArchiveState(state string)
	IncEventsExtracted(n int)
	IncStateChangesExtracted(n int)
	IncTransfersExtracted(n int)
	IncTrustlineChangesExtracted(n int)
	IncFailedTxs(n int)
	IncSuppressedTxs(n int)
	IncSuppressedEvents(n int)
	IncSuppressedTransfers(n int)
	IncSuppressedTrustlines(n int)
}

// watchSource provides the live registry snapshot heals extract against.
type watchSource interface {
	Snapshot() *registry.Snapshot
}

// Healer walks open gaps downward in atomic chunks, replaying the missing
// ranges from the history archives. Before the first heal it proves the
// captive replay byte-equivalent to the RPC on a range both can serve
// (KNOWLEDGE.md P5); a divergent replay disables healing entirely — served
// lies are worse than declared gaps.
type Healer struct {
	network    string
	passphrase string
	archive    healReplayer
	rpc        chunkSource
	store      healStore
	watch      watchSource
	inst       healInstruments
	log        *slog.Logger
	verified   bool
	// idle paces the loop between rounds with nothing to do; a field so
	// tests can shrink it.
	idle time.Duration
}

// NewHealer wires a Healer. All collaborators are required.
func NewHealer(network, passphrase string, archive healReplayer, rpc chunkSource,
	st healStore, watch watchSource, inst healInstruments, log *slog.Logger) *Healer {
	return &Healer{
		network: network, passphrase: passphrase,
		archive: archive, rpc: rpc, store: st, watch: watch, inst: inst, log: log,
		idle: healIdle,
	}
}

// Run drives healing until ctx ends or the equivalence gate fails.
// Transient failures idle and retry; only a proven divergence stops the
// worker — and even that never exits the process (rule 10).
func (h *Healer) Run(ctx context.Context) {
	h.inst.SetArchiveState(ArchiveStateUnverified)
	for {
		if ctx.Err() != nil {
			return
		}
		gaps, err := h.store.ListOpenGaps(ctx, h.network)
		if err != nil {
			if ctx.Err() == nil {
				h.log.Warn("heal: listing gaps failed", "err", err)
			}
			if !sleepCtx(ctx, h.idle) {
				return
			}
			continue
		}
		if len(gaps) == 0 {
			if !sleepCtx(ctx, h.idle) {
				return
			}
			continue
		}

		// The gate runs lazily — no gaps means no core spin-up ever — and
		// exactly once per process: the replay configuration cannot drift
		// while the process lives.
		if !h.verified {
			if err := h.verifyEquivalence(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				h.inst.IncEquivalenceFailures()
				h.inst.SetArchiveState(ArchiveStateFailed)
				h.log.Error("heal: DISABLED, captive replay is not equivalent to the RPC; "+
					"gaps stay recorded rather than filled with unverified data",
					"err", err)
				return
			}
			h.verified = true
			h.inst.SetArchiveState(ArchiveStateVerified)
			h.log.Info("heal: captive replay verified equivalent to the RPC")
		}

		worked := false
		for _, gap := range gaps {
			if ctx.Err() != nil {
				return
			}
			if err := h.healChunk(ctx, gap); err != nil {
				if ctx.Err() == nil {
					h.log.Warn("heal: chunk failed, will retry", "gap", gap.ID, "err", err)
				}
				continue
			}
			worked = true
		}
		if !worked {
			if !sleepCtx(ctx, h.idle) {
				return
			}
		}
	}
}

// healChunk replays one chunk [chunkFrom .. gap.HealNextTo] and commits it
// with the moved watermark in one transaction.
func (h *Healer) healChunk(ctx context.Context, gap store.Gap) error {
	to := gap.HealNextTo
	chunkFrom := gap.From
	if span := to - gap.From; span >= healChunkSize {
		chunkFrom = to - healChunkSize + 1
	}

	snap := h.watch.Snapshot()
	var acc extract.Result
	var prevHash string
	err := h.archive.ReplayRange(ctx, chunkFrom, to, func(lcm xdr.LedgerCloseMeta) error {
		info := source.InfoOf(lcm)
		if prevHash != "" && info.PreviousHash != prevHash {
			return fmt.Errorf("hash discontinuity at ledger %d inside heal chunk", info.Sequence)
		}
		prevHash = info.Hash

		res, err := extract.Events(lcm, h.passphrase, snap)
		if err != nil {
			return fmt.Errorf("extract ledger %d: %w", info.Sequence, err)
		}
		acc.Events = append(acc.Events, res.Events...)
		acc.StateChanges = append(acc.StateChanges, res.StateChanges...)
		acc.Transfers = append(acc.Transfers, res.Transfers...)
		acc.TrustlineChanges = append(acc.TrustlineChanges, res.TrustlineChanges...)
		acc.FailedTxs += res.FailedTxs
		acc.SuppressedTxs += res.SuppressedTxs
		acc.SuppressedEvents += res.SuppressedEvents
		acc.SuppressedTransfers += res.SuppressedTransfers
		acc.SuppressedTrustlines += res.SuppressedTrustlines
		return nil
	})
	if err != nil {
		return err
	}

	newNextTo := chunkFrom - 1
	resolved := chunkFrom <= gap.From
	if err := h.store.CommitHealChunk(ctx, h.network, gap, newNextTo, resolved,
		acc.Events, acc.StateChanges, acc.Transfers, acc.TrustlineChanges); err != nil {
		return err
	}

	h.inst.AddHealedLedgers(int(to) - int(newNextTo))
	if resolved {
		h.inst.IncGapsHealed()
	}
	h.inst.IncEventsExtracted(len(acc.Events))
	h.inst.IncStateChangesExtracted(len(acc.StateChanges))
	h.inst.IncTransfersExtracted(len(acc.Transfers))
	h.inst.IncTrustlineChangesExtracted(len(acc.TrustlineChanges))
	h.inst.IncFailedTxs(acc.FailedTxs)
	h.inst.IncSuppressedTxs(acc.SuppressedTxs)
	h.inst.IncSuppressedEvents(acc.SuppressedEvents)
	h.inst.IncSuppressedTransfers(acc.SuppressedTransfers)
	h.inst.IncSuppressedTrustlines(acc.SuppressedTrustlines)
	h.log.Info("heal: chunk committed",
		"gap", gap.ID, "from", chunkFrom, "to", to,
		"events", len(acc.Events), "state_changes", len(acc.StateChanges),
		"transfers", len(acc.Transfers), "trustlines", len(acc.TrustlineChanges),
		"resolved", resolved)
	return nil
}

// verifyEquivalence replays a recent, RPC-served, checkpoint-aligned range
// through captive core and compares every ledger byte-for-byte against the
// RPC's copy. Anything short of byte equality means the replay
// configuration does not reproduce what live ingestion stored, and no heal
// can be trusted (P5).
func (h *Healer) verifyEquivalence(ctx context.Context) error {
	cur, err := h.store.LoadCursor(ctx, h.network)
	if err != nil {
		return fmt.Errorf("load cursor for the equivalence sample: %w", err)
	}
	if cur.Sequence < 3*checkpointFrequency {
		return fmt.Errorf("cursor %d is too low to pick an equivalence sample", cur.Sequence)
	}
	// The newest checkpoint ledger the archives are guaranteed to have
	// published: checkpoints close at seq ≡ 63 (mod 64), one full cadence
	// behind the cursor to absorb publication delay.
	cp := ((cur.Sequence-checkpointFrequency+1)/checkpointFrequency)*checkpointFrequency - 1
	from := cp - equivalenceSample + 1

	h.log.Info("heal: verifying captive replay against the RPC", "from", from, "to", cp)

	rpcByLedger := make(map[uint32][]byte, equivalenceSample)
	seq := from
	for seq <= cp {
		batch, err := h.rpc.GetLedgerBatch(ctx, seq, int(cp-seq)+1)
		if err != nil {
			return fmt.Errorf("rpc side of the equivalence sample: %w", err)
		}
		for _, lcm := range batch {
			raw, err := lcm.MarshalBinary()
			if err != nil {
				return fmt.Errorf("marshal rpc ledger %d: %w", seq, err)
			}
			rpcByLedger[lcm.LedgerSequence()] = raw
			seq++
		}
	}

	return h.archive.ReplayRange(ctx, from, cp, func(lcm xdr.LedgerCloseMeta) error {
		seq := lcm.LedgerSequence()
		want, ok := rpcByLedger[seq]
		if !ok {
			return fmt.Errorf("equivalence: rpc never served ledger %d", seq)
		}
		got, err := lcm.MarshalBinary()
		if err != nil {
			return fmt.Errorf("marshal replayed ledger %d: %w", seq, err)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf(
				"equivalence: ledger %d differs between captive replay and rpc (%d vs %d bytes); %s",
				seq, len(got), len(want), diffDetail(got, want, lcm, seq))
		}
		return nil
	})
}

// diffDetail names the first component that differs, so an equivalence
// failure is diagnosable from the log line alone.
func diffDetail(got, want []byte, lcm xdr.LedgerCloseMeta, seq uint32) string {
	info := source.InfoOf(lcm)
	i := 0
	for i < len(got) && i < len(want) && got[i] == want[i] {
		i++
	}
	return fmt.Sprintf("first difference at byte %d; replayed hash %s", i, info.Hash)
}
