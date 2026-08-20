// Package health exposes Sierpe's operational surface: liveness, readiness,
// human-readable status, and Prometheus metrics.
//
// Semantics follow KNOWLEDGE.md P19: /health answers 200 the moment the
// process is up; /ready answers 200 only when ingestion is at the tip (503
// during backfill/catch-up) so orchestrators can gate traffic on it.
package health

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is Sierpe's Prometheus instrument set. One instance per process.
// Every metric is documented in docs/METRICS.md; keep both in sync.
type Metrics struct {
	LedgersIngested      prometheus.Counter
	TipLagSeconds        prometheus.Gauge
	SourceFailovers      prometheus.Counter
	CommitSeconds        prometheus.Histogram
	OpenGaps             prometheus.Gauge
	EventsExtracted      prometheus.Counter
	StateChanges         prometheus.Counter
	Transfers            prometheus.Counter
	TrustlineChanges     prometheus.Counter
	FailedTxs            prometheus.Counter
	SuppressedTxs        prometheus.Counter
	SuppressedEvents     prometheus.Counter
	SuppressedTransfers  prometheus.Counter
	SuppressedTrustlines prometheus.Counter
	BackfillChunks       prometheus.Counter
	BackfillLedgers      prometheus.Counter
	BackfillPending      prometheus.Gauge
	registry             *prometheus.Registry
}

// NewMetrics builds and registers the instrument set on a private registry
// (no default-registry pollution, no accidental double registration).
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)
	return &Metrics{
		LedgersIngested: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_ledgers_ingested_total",
			Help: "Ledgers committed since process start.",
		}),
		TipLagSeconds: factory.NewGauge(prometheus.GaugeOpts{
			Name: "sierpe_tip_lag_seconds",
			Help: "Age of the last committed ledger relative to wall clock.",
		}),
		SourceFailovers: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_source_failovers_total",
			Help: "Times the ledger source pool switched endpoints.",
		}),
		CommitSeconds: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "sierpe_commit_duration_seconds",
			Help:    "Time to commit one ledger (cursor + data, one transaction).",
			Buckets: prometheus.DefBuckets,
		}),
		OpenGaps: factory.NewGauge(prometheus.GaugeOpts{
			Name: "sierpe_open_gaps",
			Help: "Unresolved coverage gaps recorded in the database.",
		}),
		EventsExtracted: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_events_extracted_total",
			Help: "Events from watched contracts committed to the store.",
		}),
		StateChanges: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_state_changes_extracted_total",
			Help: "Contract-data changes from watched contracts committed to the store.",
		}),
		Transfers: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_transfers_extracted_total",
			Help: "Token transfers decoded from watched contracts and committed to the store.",
		}),
		TrustlineChanges: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_trustline_changes_extracted_total",
			Help: "Classic trustline changes of watched SAC assets committed to the store.",
		}),
		FailedTxs: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_failed_txs_skipped_total",
			Help: "Failed transactions skipped during extraction (their events never happened). Routine.",
		}),
		SuppressedTxs: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_suppressed_txs_total",
			Help: "Transactions dropped because their meta was unreadable or panicked mid-decode. Alert if nonzero.",
		}),
		SuppressedEvents: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_suppressed_events_total",
			Help: "Events dropped because their XDR could not be re-encoded. Alert if nonzero.",
		}),
		SuppressedTransfers: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_suppressed_transfers_total",
			Help: "Events that named a token movement but did not decode as one; the raw event still lands. Alert if nonzero.",
		}),
		SuppressedTrustlines: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_suppressed_trustlines_total",
			Help: "Watched trustline changes that could not be read. Alert if nonzero.",
		}),
		BackfillChunks: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_backfill_chunks_total",
			Help: "Backfill chunks committed since process start.",
		}),
		BackfillLedgers: factory.NewCounter(prometheus.CounterOpts{
			Name: "sierpe_backfill_ledgers_scanned_total",
			Help: "Ledgers covered by committed backfill chunks.",
		}),
		BackfillPending: factory.NewGauge(prometheus.GaugeOpts{
			Name: "sierpe_backfill_pending",
			Help: "Registered contracts whose backfill has not finished.",
		}),
		registry: reg,
	}
}

// The ingest loop consumes these through its own small interface
// (CLAUDE.md rule 5); the methods keep prometheus types out of the loop.

// IncLedgersIngested counts one committed ledger.
func (m *Metrics) IncLedgersIngested() { m.LedgersIngested.Inc() }

// SetTipLag records the age of the last committed ledger.
func (m *Metrics) SetTipLag(d time.Duration) { m.TipLagSeconds.Set(d.Seconds()) }

// ObserveCommit records one ledger-commit duration.
func (m *Metrics) ObserveCommit(d time.Duration) { m.CommitSeconds.Observe(d.Seconds()) }

// IncEventsExtracted counts events committed for watched contracts.
func (m *Metrics) IncEventsExtracted(n int) { m.EventsExtracted.Add(float64(n)) }

// IncStateChangesExtracted counts state changes committed for watched
// contracts.
func (m *Metrics) IncStateChangesExtracted(n int) { m.StateChanges.Add(float64(n)) }

// IncFailedTxs counts failed transactions skipped by extraction.
func (m *Metrics) IncFailedTxs(n int) { m.FailedTxs.Add(float64(n)) }

// IncSuppressedTxs counts transactions dropped as unreadable.
func (m *Metrics) IncSuppressedTxs(n int) { m.SuppressedTxs.Add(float64(n)) }

// IncSuppressedEvents counts events dropped as unencodable.
func (m *Metrics) IncSuppressedEvents(n int) { m.SuppressedEvents.Add(float64(n)) }

// IncTransfersExtracted counts token transfers committed for watched
// contracts.
func (m *Metrics) IncTransfersExtracted(n int) { m.Transfers.Add(float64(n)) }

// IncSuppressedTransfers counts movement-named events that did not decode
// as transfers.
func (m *Metrics) IncSuppressedTransfers(n int) { m.SuppressedTransfers.Add(float64(n)) }

// IncTrustlineChangesExtracted counts trustline changes committed for
// watched SAC assets.
func (m *Metrics) IncTrustlineChangesExtracted(n int) { m.TrustlineChanges.Add(float64(n)) }

// IncSuppressedTrustlines counts watched trustline changes dropped as
// unreadable.
func (m *Metrics) IncSuppressedTrustlines(n int) { m.SuppressedTrustlines.Add(float64(n)) }

// IncBackfillChunks counts one committed backfill chunk.
func (m *Metrics) IncBackfillChunks() { m.BackfillChunks.Inc() }

// AddBackfillLedgers counts ledgers covered by committed chunks.
func (m *Metrics) AddBackfillLedgers(n int) { m.BackfillLedgers.Add(float64(n)) }

// Status is the snapshot served at /status. Fields are set atomically by the
// ingest loop through State.
type Status struct {
	Version          string    `json:"version"`
	Network          string    `json:"network"`
	StartedAt        time.Time `json:"started_at"`
	Ready            bool      `json:"ready"`
	CursorSequence   uint32    `json:"cursor_sequence"`
	LatestKnown      uint32    `json:"latest_known_ledger"`
	TipLagSeconds    float64   `json:"tip_lag_seconds"`
	OpenGaps         int64     `json:"open_gaps"`
	SourceFailovers  int64     `json:"source_failovers"`
	PendingBackfills int64     `json:"pending_backfills"`
}

// State holds the mutable pieces of Status, safe for concurrent update.
type State struct {
	ready            atomic.Bool
	cursor           atomic.Uint32
	latestKnown      atomic.Uint32
	tipLagMilli      atomic.Int64
	openGaps         atomic.Int64
	failovers        atomic.Int64
	pendingBackfills atomic.Int64
}

// SetReady flips readiness; the loop calls it when it reaches the tip and
// clears it if it falls into catch-up again.
func (s *State) SetReady(v bool) { s.ready.Store(v) }

// Observe records the loop's current view after a commit or a tip poll.
func (s *State) Observe(cursor, latest uint32, tipLag time.Duration) {
	s.cursor.Store(cursor)
	s.latestKnown.Store(latest)
	s.tipLagMilli.Store(tipLag.Milliseconds())
}

// SetOpenGaps, SetFailovers, and SetPendingBackfills feed the slower-moving
// counters.
func (s *State) SetOpenGaps(n int64)         { s.openGaps.Store(n) }
func (s *State) SetFailovers(n int64)        { s.failovers.Store(n) }
func (s *State) SetPendingBackfills(n int64) { s.pendingBackfills.Store(n) }

// Server wires the operational endpoints onto an http.ServeMux.
type Server struct {
	version   string
	network   string
	startedAt time.Time
	state     *State
	metrics   *Metrics
}

// NewServer builds the health surface for one process.
func NewServer(version, network string, state *State, metrics *Metrics) *Server {
	return &Server{
		version:   version,
		network:   network,
		startedAt: time.Now().UTC(),
		state:     state,
		metrics:   metrics,
	}
}

// Register mounts /health, /ready, /status, and /metrics on mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		if s.state.ready.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "catching up", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.snapshot())
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.metrics.registry, promhttp.HandlerOpts{}))
}

func (s *Server) snapshot() Status {
	return Status{
		Version:          s.version,
		Network:          s.network,
		StartedAt:        s.startedAt,
		Ready:            s.state.ready.Load(),
		CursorSequence:   s.state.cursor.Load(),
		LatestKnown:      s.state.latestKnown.Load(),
		TipLagSeconds:    float64(s.state.tipLagMilli.Load()) / 1000,
		OpenGaps:         s.state.openGaps.Load(),
		SourceFailovers:  s.state.failovers.Load(),
		PendingBackfills: s.state.pendingBackfills.Load(),
	}
}
