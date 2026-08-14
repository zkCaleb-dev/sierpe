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
type Metrics struct {
	LedgersIngested prometheus.Counter
	TipLagSeconds   prometheus.Gauge
	SourceFailovers prometheus.Counter
	CommitSeconds   prometheus.Histogram
	OpenGaps        prometheus.Gauge
	registry        *prometheus.Registry
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

// Status is the snapshot served at /status. Fields are set atomically by the
// ingest loop through State.
type Status struct {
	Version         string    `json:"version"`
	Network         string    `json:"network"`
	StartedAt       time.Time `json:"started_at"`
	Ready           bool      `json:"ready"`
	CursorSequence  uint32    `json:"cursor_sequence"`
	LatestKnown     uint32    `json:"latest_known_ledger"`
	TipLagSeconds   float64   `json:"tip_lag_seconds"`
	OpenGaps        int64     `json:"open_gaps"`
	SourceFailovers int64     `json:"source_failovers"`
}

// State holds the mutable pieces of Status, safe for concurrent update.
type State struct {
	ready       atomic.Bool
	cursor      atomic.Uint32
	latestKnown atomic.Uint32
	tipLagMilli atomic.Int64
	openGaps    atomic.Int64
	failovers   atomic.Int64
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

// SetOpenGaps and SetFailovers feed the slower-moving counters.
func (s *State) SetOpenGaps(n int64)  { s.openGaps.Store(n) }
func (s *State) SetFailovers(n int64) { s.failovers.Store(n) }

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
		Version:         s.version,
		Network:         s.network,
		StartedAt:       s.startedAt,
		Ready:           s.state.ready.Load(),
		CursorSequence:  s.state.cursor.Load(),
		LatestKnown:     s.state.latestKnown.Load(),
		TipLagSeconds:   float64(s.state.tipLagMilli.Load()) / 1000,
		OpenGaps:        s.state.openGaps.Load(),
		SourceFailovers: s.state.failovers.Load(),
	}
}
