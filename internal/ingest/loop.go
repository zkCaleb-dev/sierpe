// Package ingest runs the single-writer ledger loop: resume from the
// persisted cursor, verify chain continuity, commit one ledger at a time,
// follow the tip.
//
// Failure doctrine (CLAUDE.md rule 10): transient errors back off and retry
// forever; only integrity violations (hash divergence, network reset) and
// configuration errors terminate the loop.
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
	// tipPollInterval paces polling when the loop is at the tip; Stellar
	// closes a ledger roughly every 5 seconds.
	tipPollInterval = 2 * time.Second
	// backoffMin/Max bound the transient-error retry schedule.
	backoffMin = time.Second
	backoffMax = 30 * time.Second
	// resetSlack is how far the cursor may sit above the source tip before
	// the loop declares a network reset instead of a lagging endpoint.
	// Testnet resets move the tip back by millions; failover skew is tiny.
	resetSlack = 17_280 // ~one day of ledgers
	// readyLagThreshold is the tip distance below which /ready reports 200.
	readyLagThreshold = 6
)

// ledgerSource is the slice of source.Source the loop consumes (small,
// consumer-side interface; CLAUDE.md rule 5).
type ledgerSource interface {
	GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error)
	LatestLedger(ctx context.Context) (uint32, error)
	VerifyNetwork(ctx context.Context, passphrase string) error
}

// cursorStore is the slice of the store the loop needs.
type cursorStore interface {
	LoadCursor(ctx context.Context, network string) (store.Cursor, error)
	CommitLedger(ctx context.Context, network string, rec store.LedgerRecord, events []store.Event, states []store.StateChange) error
}

// snapshotter hands the loop the current watched-contracts view. Reading it
// is a pointer load; the loop re-reads it once per ledger (KNOWLEDGE.md P2).
type snapshotter interface {
	Snapshot() *registry.Snapshot
}

// observer receives loop progress; the health package implements it.
type observer interface {
	SetReady(bool)
	Observe(cursor, latest uint32, tipLag time.Duration)
}

// instruments is the metrics slice the loop feeds.
type instruments interface {
	IncLedgersIngested()
	SetTipLag(time.Duration)
	ObserveCommit(time.Duration)
	IncEventsExtracted(n int)
	IncStateChangesExtracted(n int)
	IncFailedTxs(n int)
	IncSuppressedTxs(n int)
	IncSuppressedEvents(n int)
}

// Config parameterizes a Loop.
type Config struct {
	Network     string // "testnet" | "mainnet"
	Passphrase  string
	StartLedger uint32 // 0 = resume from cursor, or start at tip on a fresh db
}

// Loop is the single-writer ingestion engine.
type Loop struct {
	cfg   Config
	src   ledgerSource
	store cursorStore
	watch snapshotter
	obs   observer
	inst  instruments
	log   *slog.Logger

	// lastKnownTip caches the most recent tip seen while polling; it is
	// informational (feeds /status), never an ingestion decision.
	lastKnownTip uint32
}

// New wires a Loop. All collaborators are required.
func New(cfg Config, src ledgerSource, st cursorStore, watch snapshotter, obs observer, inst instruments, log *slog.Logger) *Loop {
	return &Loop{cfg: cfg, src: src, store: st, watch: watch, obs: obs, inst: inst, log: log}
}

// Run drives ingestion until ctx is cancelled or an integrity violation is
// found. The returned error is nil only on context cancellation.
func (l *Loop) Run(ctx context.Context) error {
	if err := l.src.VerifyNetwork(ctx, l.cfg.Passphrase); err != nil {
		return fmt.Errorf("ingest: %w", err)
	}

	next, expectedPrev, err := l.resumePoint(ctx)
	if err != nil {
		return err
	}
	l.log.Info("ingestion starting", "network", l.cfg.Network, "next_ledger", next)

	backoff := backoffMin
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		lcm, err := l.src.GetLedger(ctx, next)
		switch {
		case err == nil:
			// fallthrough to processing below
		case errors.Is(err, source.ErrNotYetAvailable):
			l.markTipReached(ctx, next)
			if !sleepCtx(ctx, tipPollInterval) {
				return nil
			}
			continue
		case errors.Is(err, source.ErrBelowRetention):
			// M0 has no archive leg to fall back to: stop loudly rather
			// than silently skip a range (P7 — a gap must be a decision).
			return fmt.Errorf("ingest: ledger %d is below source retention and no archive source is configured; set START_LEDGER inside the window or wait for the archive leg (v1.1): %w", next, err)
		default:
			if ctx.Err() != nil {
				return nil
			}
			l.log.Warn("transient source error, backing off", "ledger", next, "backoff", backoff, "err", err)
			if !sleepCtx(ctx, backoff) {
				return nil
			}
			backoff = min(backoff*2, backoffMax)
			continue
		}
		backoff = backoffMin

		info := source.InfoOf(lcm)
		if info.Sequence != next {
			return fmt.Errorf("ingest: asked for ledger %d, source returned %d — refusing to continue", next, info.Sequence)
		}
		// Chain continuity is permanent, not one-shot (KNOWLEDGE.md, TW
		// lesson M4): every ledger must extend the one we committed.
		if expectedPrev != "" && info.PreviousHash != expectedPrev {
			return fmt.Errorf(
				"ingest: chain divergence at ledger %d: committed hash %s but the source says its predecessor is %s — this indicates a fork, a corrupted source, or a network reset; refusing to write",
				info.Sequence, expectedPrev, info.PreviousHash,
			)
		}

		// Extraction filters the whole ledger against the current snapshot
		// (a pointer load per ledger — KNOWLEDGE.md P2). A ledger that does
		// not parse is treated as transient: a retry may be served by a
		// healthier endpoint in the pool.
		extracted, err := extract.Events(lcm, l.cfg.Passphrase, l.watch.Snapshot())
		if err != nil {
			l.log.Warn("extraction failed, backing off", "ledger", next, "err", err)
			if !sleepCtx(ctx, backoff) {
				return nil
			}
			backoff = min(backoff*2, backoffMax)
			continue
		}
		l.inst.IncFailedTxs(extracted.FailedTxs)
		l.inst.IncSuppressedTxs(extracted.SuppressedTxs)
		l.inst.IncSuppressedEvents(extracted.SuppressedEvents)
		if extracted.SuppressedTxs > 0 || extracted.SuppressedEvents > 0 {
			l.log.Warn("suppressed unreadable chain data",
				"ledger", next, "txs", extracted.SuppressedTxs, "events", extracted.SuppressedEvents)
		}

		start := time.Now()
		rec := store.LedgerRecord{
			Sequence:     info.Sequence,
			Hash:         info.Hash,
			PreviousHash: info.PreviousHash,
			ClosedAt:     info.ClosedAt,
		}
		if err := l.store.CommitLedger(ctx, l.cfg.Network, rec, extracted.Events, extracted.StateChanges); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			l.log.Warn("commit failed, backing off", "ledger", next, "err", err)
			if !sleepCtx(ctx, backoff) {
				return nil
			}
			backoff = min(backoff*2, backoffMax)
			continue
		}
		l.inst.ObserveCommit(time.Since(start))
		l.inst.IncLedgersIngested()
		l.inst.IncEventsExtracted(len(extracted.Events))
		l.inst.IncStateChangesExtracted(len(extracted.StateChanges))

		tipLag := time.Since(info.ClosedAt)
		l.inst.SetTipLag(tipLag)
		l.obs.Observe(info.Sequence, maxU32(info.Sequence, l.lastKnownTip), tipLag)

		expectedPrev = info.Hash
		next++
	}
}

// resumePoint decides where ingestion starts and which predecessor hash the
// first ledger must match.
func (l *Loop) resumePoint(ctx context.Context) (next uint32, expectedPrev string, err error) {
	cursor, err := l.store.LoadCursor(ctx, l.cfg.Network)
	switch {
	case errors.Is(err, store.ErrNoCursor):
		latest, lerr := l.src.LatestLedger(ctx)
		if lerr != nil {
			return 0, "", fmt.Errorf("ingest: cannot determine tip for first start: %w", lerr)
		}
		start := l.cfg.StartLedger
		if start == 0 {
			start = latest
			l.log.Info("fresh database, starting at the tip", "ledger", start)
		} else {
			l.log.Info("fresh database, starting at configured ledger", "ledger", start)
		}
		// First ledger after a fresh start has no committed predecessor to
		// check against; continuity arms from the second ledger on.
		return start, "", nil

	case err != nil:
		return 0, "", fmt.Errorf("ingest: load cursor: %w", err)
	}

	latest, lerr := l.src.LatestLedger(ctx)
	if lerr == nil && cursor.Sequence > latest+resetSlack {
		return 0, "", fmt.Errorf(
			"ingest: cursor %d is far beyond the source tip %d — this looks like a network reset (testnet resets quarterly) or a source serving the wrong network; refusing to write. Runbook: verify NETWORK, then reset the database if the network really was reset",
			cursor.Sequence, latest,
		)
	}
	l.log.Info("resuming from cursor", "ledger", cursor.Sequence)
	return cursor.Sequence + 1, cursor.Hash, nil
}

// markTipReached flips readiness when the loop has caught up.
func (l *Loop) markTipReached(ctx context.Context, next uint32) {
	latest, err := l.src.LatestLedger(ctx)
	if err != nil {
		return
	}
	l.lastKnownTip = latest
	caughtUp := next+readyLagThreshold >= latest
	l.obs.SetReady(caughtUp)
	if next > 0 {
		l.obs.Observe(next-1, latest, 0)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func maxU32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
