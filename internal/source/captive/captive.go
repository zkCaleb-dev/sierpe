// Package captive replays history-archive ranges through a captive
// stellar-core subprocess: the archive leg of the pipeline (docs/DESIGN.md
// §5). It serves bounded ranges below RPC retention; it has no tip and no
// live mode.
//
// Non-negotiable (KNOWLEDGE.md P5): the replay toml enables the unified
// event semantics RPC serves (EMIT_CLASSIC_EVENTS + BACKFILL_STELLAR_ASSET_
// EVENTS), and the consumer must gate the first use behind an equivalence
// check against an RPC-served range before trusting a single replayed row.
package captive

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/source"
)

// Config wires a replay source. All fields are required except
// StoragePath (defaults to the OS temp dir upstream in boot config).
type Config struct {
	BinaryPath  string
	Passphrase  string
	ArchiveURLs []string
	StoragePath string
	Log         *slog.Logger
}

// backend is the slice of ledgerbackend.CaptiveStellarCore a replay
// consumes (CLAUDE.md rule 5); faked in tests, real in production.
type backend interface {
	PrepareRange(ctx context.Context, r ledgerbackend.Range) error
	GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error)
	Close() error
}

// Source replays bounded archive ranges. Each replay spawns a fresh
// captive core, streams the range, and tears the process down: replays
// are occasional gap-healing work, not a resident service.
type Source struct {
	cfg        Config
	newBackend func(ctx context.Context) (backend, error)
}

// New validates the configuration eagerly — building the captive-core toml
// is where bad archive URLs and binary paths surface — so a misconfigured
// archive leg fails at boot, not at the first heal (rule 12).
func New(cfg Config) (*Source, error) {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	toml, err := ledgerbackend.NewCaptiveCoreToml(ledgerbackend.CaptiveCoreTomlParams{
		NetworkPassphrase:  cfg.Passphrase,
		HistoryArchiveURLs: cfg.ArchiveURLs,
		CoreBinaryPath:     cfg.BinaryPath,
		// Match the event semantics RPC serves (DESIGN §5): unified events
		// for every operation, backfilled for pre-protocol-22 SAC history.
		EmitUnifiedEvents:                 true,
		EmitUnifiedEventsBeforeProtocol22: true,
	})
	if err != nil {
		return nil, fmt.Errorf("captive: build core config: %w", err)
	}

	// Core process output goes through the SDK logger at warn level: the
	// replay progress Sierpe cares about is logged by the consumer, not by
	// core's own chatter.
	coreLog := supportlog.New()
	coreLog.SetLevel(supportlog.WarnLevel)

	s := &Source{cfg: cfg}
	s.newBackend = func(ctx context.Context) (backend, error) {
		return ledgerbackend.NewCaptive(ledgerbackend.CaptiveCoreConfig{
			BinaryPath:         cfg.BinaryPath,
			NetworkPassphrase:  cfg.Passphrase,
			HistoryArchiveURLs: cfg.ArchiveURLs,
			StoragePath:        filepath.Join(cfg.StoragePath, "sierpe-captive"),
			Toml:               toml,
			UserAgent:          "sierpe",
			Log:                coreLog.WithField("subsystem", "captive-core"),
			Context:            ctx,
		})
	}
	return s, nil
}

// ReplayRange streams ledgers [from .. to] ascending through emit. The
// range must be at or below a checkpoint the archives serve; a range the
// archives cannot satisfy surfaces as an error from the prepare phase.
// The captive core process lives exactly as long as the call.
func (s *Source) ReplayRange(ctx context.Context, from, to uint32, emit func(xdr.LedgerCloseMeta) error) error {
	if from == 0 || to < from {
		return fmt.Errorf("captive: invalid range [%d..%d]", from, to)
	}
	be, err := s.newBackend(ctx)
	if err != nil {
		return fmt.Errorf("captive: start core: %w", err)
	}
	defer be.Close()

	s.cfg.Log.Info("captive: preparing archive range", "from", from, "to", to)
	if err := be.PrepareRange(ctx, ledgerbackend.BoundedRange(from, to)); err != nil {
		return fmt.Errorf("captive: prepare range [%d..%d]: %w", from, to, err)
	}

	for seq := from; seq <= to; seq++ {
		lcm, err := be.GetLedger(ctx, seq)
		if err != nil {
			return fmt.Errorf("captive: replay ledger %d: %w", seq, err)
		}
		if got := source.InfoOf(lcm).Sequence; got != seq {
			return fmt.Errorf("captive: asked for ledger %d, core produced %d", seq, got)
		}
		if err := emit(lcm); err != nil {
			return err
		}
	}
	return nil
}
