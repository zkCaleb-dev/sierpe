package ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"sort"
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
		events []store.Event, states []store.StateChange, transfers []store.Transfer, trustlines []store.TrustlineChange, movements []store.Movement) error
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
	IncMovementsExtracted(n int)
	IncFailedTxs(n int)
	IncSuppressedTxs(n int)
	IncSuppressedEvents(n int)
	IncSuppressedTransfers(n int)
	IncSuppressedTrustlines(n int)
	IncForeignUndecodable(n int)
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
		acc.Movements = append(acc.Movements, res.Movements...)
		acc.FailedTxs += res.FailedTxs
		acc.SuppressedTxs += res.SuppressedTxs
		acc.SuppressedEvents += res.SuppressedEvents
		acc.SuppressedTransfers += res.SuppressedTransfers
		acc.SuppressedTrustlines += res.SuppressedTrustlines
		acc.ForeignUndecodable += res.ForeignUndecodable
		return nil
	})
	if err != nil {
		return err
	}

	newNextTo := chunkFrom - 1
	resolved := chunkFrom <= gap.From
	if err := h.store.CommitHealChunk(ctx, h.network, gap, newNextTo, resolved,
		acc.Events, acc.StateChanges, acc.Transfers, acc.TrustlineChanges, acc.Movements); err != nil {
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
	h.inst.IncMovementsExtracted(len(acc.Movements))
	h.inst.IncFailedTxs(acc.FailedTxs)
	h.inst.IncSuppressedTxs(acc.SuppressedTxs)
	h.inst.IncSuppressedEvents(acc.SuppressedEvents)
	h.inst.IncSuppressedTransfers(acc.SuppressedTransfers)
	h.inst.IncSuppressedTrustlines(acc.SuppressedTrustlines)
	h.inst.IncForeignUndecodable(acc.ForeignUndecodable)
	h.log.Info("heal: chunk committed",
		"gap", gap.ID, "from", chunkFrom, "to", to,
		"events", len(acc.Events), "state_changes", len(acc.StateChanges),
		"transfers", len(acc.Transfers), "trustlines", len(acc.TrustlineChanges),
		"movements", len(acc.Movements),
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

	rpcByLedger := make(map[uint32]xdr.LedgerCloseMeta, equivalenceSample)
	seq := from
	for seq <= cp {
		batch, err := h.rpc.GetLedgerBatch(ctx, seq, int(cp-seq)+1)
		if err != nil {
			return fmt.Errorf("rpc side of the equivalence sample: %w", err)
		}
		for _, lcm := range batch {
			rpcByLedger[lcm.LedgerSequence()] = lcm
			seq++
		}
	}

	return h.archive.ReplayRange(ctx, from, cp, func(lcm xdr.LedgerCloseMeta) error {
		seq := lcm.LedgerSequence()
		rpcLcm, ok := rpcByLedger[seq]
		if !ok {
			return fmt.Errorf("equivalence: rpc never served ledger %d", seq)
		}
		// Transaction meta is not part of consensus, and two runs of the
		// SAME core build can emit it with cosmetic differences. Both were
		// proven live against testnet, and both are normalized away so the
		// comparison stays byte-strict on everything Sierpe can ever
		// store:
		//   - diagnostic events differ run to run (debug output; never
		//     stored, never extracted) — stripped on both sides;
		//   - the interleaving of ledger-entry-change units inside one
		//     operation varies (same set, different order) — each unit (a
		//     single CREATED/RESTORED, or a STATE plus its UPDATED/REMOVED)
		//     is kept intact and units are sorted canonically.
		normalizeForEquivalence(&lcm)
		normalizeForEquivalence(&rpcLcm)
		want, err := rpcLcm.MarshalBinary()
		if err != nil {
			return fmt.Errorf("marshal rpc ledger %d: %w", seq, err)
		}
		got, err := lcm.MarshalBinary()
		if err != nil {
			return fmt.Errorf("marshal replayed ledger %d: %w", seq, err)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf(
				"equivalence: ledger %d differs between captive replay and rpc (%d vs %d bytes); %s",
				seq, len(got), len(want), diffDetail(got, want, lcm, rpcLcm))
		}
		return nil
	})
}

// normalizeForEquivalence rewrites the non-consensus, run-to-run unstable
// parts of a ledger's meta into a canonical form (see the call site for the
// rationale). Consensus data is never altered — only debug output is
// dropped and change ordering is made deterministic.
func normalizeForEquivalence(lcm *xdr.LedgerCloseMeta) {
	normalizeMeta := func(meta *xdr.TransactionMeta) {
		switch meta.V {
		case 3:
			if meta.V3 == nil {
				return
			}
			if meta.V3.SorobanMeta != nil {
				meta.V3.SorobanMeta.DiagnosticEvents = nil
			}
			meta.V3.TxChangesBefore = canonicalizeChanges(meta.V3.TxChangesBefore)
			meta.V3.TxChangesAfter = canonicalizeChanges(meta.V3.TxChangesAfter)
			for i := range meta.V3.Operations {
				meta.V3.Operations[i].Changes = canonicalizeChanges(meta.V3.Operations[i].Changes)
			}
		case 4:
			if meta.V4 == nil {
				return
			}
			meta.V4.DiagnosticEvents = nil
			meta.V4.TxChangesBefore = canonicalizeChanges(meta.V4.TxChangesBefore)
			meta.V4.TxChangesAfter = canonicalizeChanges(meta.V4.TxChangesAfter)
			for i := range meta.V4.Operations {
				meta.V4.Operations[i].Changes = canonicalizeChanges(meta.V4.Operations[i].Changes)
			}
		}
	}
	switch lcm.V {
	case 1:
		if lcm.V1 != nil {
			for i := range lcm.V1.TxProcessing {
				tx := &lcm.V1.TxProcessing[i]
				tx.FeeProcessing = canonicalizeChanges(tx.FeeProcessing)
				normalizeMeta(&tx.TxApplyProcessing)
			}
		}
	case 2:
		if lcm.V2 != nil {
			for i := range lcm.V2.TxProcessing {
				tx := &lcm.V2.TxProcessing[i]
				tx.FeeProcessing = canonicalizeChanges(tx.FeeProcessing)
				tx.PostTxApplyFeeProcessing = canonicalizeChanges(tx.PostTxApplyFeeProcessing)
				normalizeMeta(&tx.TxApplyProcessing)
			}
		}
	}
}

// canonicalizeChanges sorts a change vector into a deterministic order
// while preserving its semantic units: a STATE change and the UPDATED or
// REMOVED that follows it travel together, and CREATED/RESTORED entries
// travel alone. Within-vector unit order carries no meaning (core itself
// emits it differently run to run); unit contents are untouched.
func canonicalizeChanges(changes xdr.LedgerEntryChanges) xdr.LedgerEntryChanges {
	if len(changes) < 2 {
		return changes
	}
	type unit struct {
		raw []byte
		chs []xdr.LedgerEntryChange
	}
	units := make([]unit, 0, len(changes))
	for i := 0; i < len(changes); i++ {
		u := []xdr.LedgerEntryChange{changes[i]}
		if changes[i].Type == xdr.LedgerEntryChangeTypeLedgerEntryState && i+1 < len(changes) {
			switch changes[i+1].Type {
			case xdr.LedgerEntryChangeTypeLedgerEntryUpdated, xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
				u = append(u, changes[i+1])
				i++
			}
		}
		var buf bytes.Buffer
		if _, err := xdr.Marshal(&buf, u); err != nil {
			return changes // cannot canonicalize; compare as-is
		}
		units = append(units, unit{raw: buf.Bytes(), chs: u})
	}
	sort.Slice(units, func(i, j int) bool { return bytes.Compare(units[i].raw, units[j].raw) < 0 })
	out := make(xdr.LedgerEntryChanges, 0, len(changes))
	for _, u := range units {
		out = append(out, u.chs...)
	}
	return out
}

// diffDetail names the first component that differs, so an equivalence
// failure is diagnosable from the log line alone.
func diffDetail(got, want []byte, replayed, rpc xdr.LedgerCloseMeta) string {
	i := 0
	for i < len(got) && i < len(want) && got[i] == want[i] {
		i++
	}
	return fmt.Sprintf("first difference at byte %d; %s; replayed hash %s",
		i, structuredDiff(replayed, rpc), source.InfoOf(replayed).Hash)
}

// structuredDiff walks both metas transaction by transaction and names the
// first differing component, because "byte 19624 differs" is not something
// an operator can act on.
func structuredDiff(replayed, rpc xdr.LedgerCloseMeta) string {
	marshal := func(v interface{}) []byte {
		var buf bytes.Buffer
		if _, err := xdr.Marshal(&buf, v); err != nil {
			return nil
		}
		return buf.Bytes()
	}
	if replayed.V != rpc.V {
		return fmt.Sprintf("meta version %d vs %d", replayed.V, rpc.V)
	}
	if replayed.V != 2 || replayed.V2 == nil || rpc.V2 == nil {
		return fmt.Sprintf("structural diff only decodes V2 meta (got V%d)", replayed.V)
	}
	a, b := replayed.V2, rpc.V2
	if !bytes.Equal(marshal(a.Ext), marshal(b.Ext)) {
		return "ledger close meta ext differs"
	}
	if !bytes.Equal(marshal(a.LedgerHeader), marshal(b.LedgerHeader)) {
		return "ledger header differs"
	}
	if !bytes.Equal(marshal(a.TxSet), marshal(b.TxSet)) {
		return "tx set differs"
	}
	if len(a.TxProcessing) != len(b.TxProcessing) {
		return fmt.Sprintf("tx count %d vs %d", len(a.TxProcessing), len(b.TxProcessing))
	}
	for i := range a.TxProcessing {
		at, bt := &a.TxProcessing[i], &b.TxProcessing[i]
		if !bytes.Equal(marshal(at.Result), marshal(bt.Result)) {
			return fmt.Sprintf("tx %d result differs", i)
		}
		if !bytes.Equal(marshal(at.FeeProcessing), marshal(bt.FeeProcessing)) {
			return fmt.Sprintf("tx %d fee processing differs", i)
		}
		if !bytes.Equal(marshal(at.PostTxApplyFeeProcessing), marshal(bt.PostTxApplyFeeProcessing)) {
			return fmt.Sprintf("tx %d post-apply fee processing differs", i)
		}
		if !bytes.Equal(marshal(at.TxApplyProcessing), marshal(bt.TxApplyProcessing)) {
			detail := ""
			am, aok := at.TxApplyProcessing.GetV4()
			bm, bok := bt.TxApplyProcessing.GetV4()
			if aok && bok {
				switch {
				case !bytes.Equal(marshal(am.Events), marshal(bm.Events)):
					detail = ": tx events"
				case !bytes.Equal(marshal(am.DiagnosticEvents), marshal(bm.DiagnosticEvents)):
					detail = ": diagnostic events"
				case !bytes.Equal(marshal(am.Operations), marshal(bm.Operations)):
					detail = ": operation meta"
					if len(am.Operations) != len(bm.Operations) {
						detail = fmt.Sprintf(": operation count %d vs %d", len(am.Operations), len(bm.Operations))
						break
					}
					for op := range am.Operations {
						ao, bo := &am.Operations[op], &bm.Operations[op]
						switch {
						case !bytes.Equal(marshal(ao.Changes), marshal(bo.Changes)):
							detail = fmt.Sprintf(": op %d ledger entry changes; replay=%s rpc=%s",
								op,
								base64.StdEncoding.EncodeToString(marshal(ao.Changes)),
								base64.StdEncoding.EncodeToString(marshal(bo.Changes)))
						case !bytes.Equal(marshal(ao.Events), marshal(bo.Events)):
							detail = fmt.Sprintf(": op %d events (%d vs %d)", op, len(ao.Events), len(bo.Events))
						case !bytes.Equal(marshal(ao.Ext), marshal(bo.Ext)):
							detail = fmt.Sprintf(": op %d meta ext", op)
						default:
							continue
						}
						break
					}
				case !bytes.Equal(marshal(am.TxChangesBefore), marshal(bm.TxChangesBefore)),
					!bytes.Equal(marshal(am.TxChangesAfter), marshal(bm.TxChangesAfter)):
					detail = ": tx changes"
				case !bytes.Equal(marshal(am.SorobanMeta), marshal(bm.SorobanMeta)):
					detail = ": soroban meta"
				default:
					detail = ": meta ext"
				}
			}
			return fmt.Sprintf("tx %d apply meta differs%s", i, detail)
		}
	}
	if !bytes.Equal(marshal(a.UpgradesProcessing), marshal(b.UpgradesProcessing)) {
		return "upgrades differ"
	}
	if !bytes.Equal(marshal(a.ScpInfo), marshal(b.ScpInfo)) {
		return "scp info differs"
	}
	if a.TotalByteSizeOfLiveSorobanState != b.TotalByteSizeOfLiveSorobanState {
		return fmt.Sprintf("total live soroban state size %d vs %d",
			a.TotalByteSizeOfLiveSorobanState, b.TotalByteSizeOfLiveSorobanState)
	}
	if !bytes.Equal(marshal(a.EvictedKeys), marshal(b.EvictedKeys)) {
		return "evicted keys differ"
	}
	return "difference outside the decoded components"
}
