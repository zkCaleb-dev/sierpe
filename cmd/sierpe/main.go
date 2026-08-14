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

	"github.com/zkCaleb-dev/sierpe/internal/config"
	"github.com/zkCaleb-dev/sierpe/internal/health"
	"github.com/zkCaleb-dev/sierpe/internal/ingest"
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

	metrics := health.NewMetrics()
	state := &health.State{}
	mux := http.NewServeMux()
	health.NewServer(version, string(cfg.Network), state, metrics).Register(mux)

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

	// Slow-moving status feeders (gaps count) refresh in the background.
	go feedStatus(ctx, st, state, string(cfg.Network))

	if !withIngestion {
		log.Info("serve mode: ingestion disabled")
		select {
		case <-ctx.Done():
			return nil
		case err := <-httpErr:
			return err
		}
	}

	src, err := rpc.New(cfg.RPCURLs)
	if err != nil {
		return err
	}
	defer src.Close()
	go feedFailovers(ctx, src, state, metrics)

	loop := ingest.New(
		ingest.Config{
			Network:     string(cfg.Network),
			Passphrase:  cfg.Network.Passphrase(),
			StartLedger: cfg.StartLedger,
		},
		src, st, state, metrics, log,
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

// feedStatus refreshes the database-backed status fields periodically.
func feedStatus(ctx context.Context, st *store.Store, state *health.State, network string) {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		if n, err := st.OpenGaps(ctx, network); err == nil {
			state.SetOpenGaps(n)
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
