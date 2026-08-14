// Command sierpe is the self-hosted Stellar contract indexer.
//
// Subcommands (see docs/DESIGN.md §3):
//
//	run       start the full appliance: ingestion + operational API (default)
//	serve     start the operational API only, without the ingestion engine
//	replay    re-ingest a ledger range beside the live process (not yet built)
//	rederive  rebuild derived tables from stored raw data (not yet built)
//	reseed    rebuild the contract watchlist from the database (not yet built)
//	version   print build information
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zkCaleb-dev/sierpe/internal/admin"
	"github.com/zkCaleb-dev/sierpe/internal/api"
	"github.com/zkCaleb-dev/sierpe/internal/config"
	"github.com/zkCaleb-dev/sierpe/internal/health"
	"github.com/zkCaleb-dev/sierpe/internal/ingest"
	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/source/rpc"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// version is stamped at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	cmd := "run"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	var err error
	switch cmd {
	case "version":
		fmt.Printf("sierpe %s\n", version)
		return
	case "run":
		err = run(log, true)
	case "serve":
		err = run(log, false)
	case "replay", "rederive", "reseed":
		fmt.Fprintf(os.Stderr, "sierpe %s: %q is not implemented yet (see docs/DESIGN.md milestones)\n", version, cmd)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "sierpe: unknown command %q\nusage: sierpe [run|serve|replay|rederive|reseed|version]\n", cmd)
		os.Exit(2)
	}
	if err != nil {
		log.Error("sierpe terminated", "err", err)
		os.Exit(1)
	}
}

// run boots the appliance. With ingestion disabled it serves only the
// operational surface (the serve mode; KNOWLEDGE.md P24 / Ponder pattern).
func run(log *slog.Logger, withIngestion bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	log.Info("configuration loaded", "config", cfg.Redacted(), "version", version)

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	log.Info("database ready")

	// The registry snapshot is what ingestion filters against; boot with the
	// persisted registrations so a restart never ingests blind.
	reg := registry.New(string(cfg.Network), st)
	if err := reg.Reload(ctx); err != nil {
		return err
	}

	// Both modes need the RPC pool: ingestion pulls ledgers from it and the
	// admin API classifies contracts through it.
	src, err := rpc.New(cfg.RPCURLs)
	if err != nil {
		return err
	}
	defer src.Close()
	if !withIngestion {
		// The ingestion loop verifies the network itself; serve mode must
		// not classify against the wrong chain either.
		if err := src.VerifyNetwork(ctx, cfg.Network.Passphrase()); err != nil {
			return err
		}
	}

	metrics := health.NewMetrics()
	state := &health.State{}
	mux := http.NewServeMux()
	health.NewServer(version, string(cfg.Network), state, metrics).Register(mux)
	admin.NewServer(string(cfg.Network), cfg.AdminToken, st, st, reg, registry.NewClassifier(src), log).Register(mux)
	api.NewServer(string(cfg.Network), st, st, log).Register(mux)

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	httpErr := make(chan error, 1)
	go func() {
		log.Info("http listening", "port", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	// Slow-moving status feeders (gaps, pending backfills) refresh in the
	// background.
	go feedStatus(ctx, st, state, metrics, string(cfg.Network))
	// Periodic reload converges the snapshot when a mutation happened in
	// another instance (serve beside run) or its post-mutation reload failed.
	go reloadRegistry(ctx, reg, log)

	if !withIngestion {
		log.Info("serve mode: ingestion disabled")
		select {
		case <-ctx.Done():
			return nil
		case err := <-httpErr:
			return err
		}
	}

	go feedFailovers(ctx, src, state, metrics)

	// The backfill worker walks registered history downward beside the live
	// loop; they never touch the same ledgers (backfill ends where the
	// registration cursor anchored it).
	backfiller := ingest.NewBackfiller(
		string(cfg.Network), cfg.Network.Passphrase(), src, st, metrics, log,
	)
	go backfiller.Run(ctx)

	loop := ingest.New(
		ingest.Config{
			Network:     string(cfg.Network),
			Passphrase:  cfg.Network.Passphrase(),
			StartLedger: cfg.StartLedger,
		},
		src, st, reg, state, metrics, log,
	)

	loopErr := make(chan error, 1)
	go func() { loopErr <- loop.Run(ctx) }()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
		return <-loopErr
	case err := <-loopErr:
		return err
	case err := <-httpErr:
		return err
	}
}

// reloadRegistry keeps the watched-contracts snapshot converged with the
// database even when the in-process post-mutation reload is not the one that
// saw the change.
func reloadRegistry(ctx context.Context, reg *registry.Registry, log *slog.Logger) {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := reg.Reload(ctx); err != nil {
				log.Warn("periodic registry reload failed", "err", err)
			}
		}
	}
}

// feedStatus refreshes the database-backed status fields periodically.
func feedStatus(ctx context.Context, st *store.Store, state *health.State, metrics *health.Metrics, network string) {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		if n, err := st.OpenGaps(ctx, network); err == nil {
			state.SetOpenGaps(n)
			metrics.OpenGaps.Set(float64(n))
		}
		if n, err := st.CountPendingBackfills(ctx, network); err == nil {
			state.SetPendingBackfills(n)
			metrics.BackfillPending.Set(float64(n))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// feedFailovers mirrors the source pool's failover count into status/metrics.
func feedFailovers(ctx context.Context, src *rpc.Client, state *health.State, metrics *health.Metrics) {
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			now := src.Failovers()
			state.SetFailovers(now)
			for ; last < now; last++ {
				metrics.SourceFailovers.Inc()
			}
		}
	}
}
