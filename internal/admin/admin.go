// Package admin implements the authenticated admin surface (CLAUDE.md rule
// 11): always authenticated, every mutation idempotent, reconciling desired
// state rather than emitting commands.
//
// M1 surface: contract registration. POST /v1/contracts registers or
// reconciles a contract; DELETE /v1/contracts/{id} unregisters it. Deleting
// stops following and backfilling but keeps already-indexed data, so a
// re-register resumes where it left off.
package admin

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// maxBodyBytes bounds admin request bodies; registration payloads are tiny.
const maxBodyBytes = 64 << 10

// contractStore is the slice of the store the admin API consumes.
type contractStore interface {
	UpsertContract(ctx context.Context, c store.Contract) (store.Contract, error)
	DeleteContract(ctx context.Context, network, contractID string) (bool, error)
}

// backfillPlanner anchors a registration's history walk (CLAUDE.md rule 5:
// small consumer-side interfaces, one per concern).
type backfillPlanner interface {
	LoadCursor(ctx context.Context, network string) (store.Cursor, error)
	EnsureBackfill(ctx context.Context, network, contractID string, targetFrom, nextTo uint32) error
}

// reloader republishes the registry snapshot after a mutation.
type reloader interface {
	Reload(ctx context.Context) error
}

// classifier resolves a contract id to its on-chain classification.
type classifier interface {
	Classify(ctx context.Context, contractID string) (registry.Classification, error)
}

// Server holds the admin API dependencies.
type Server struct {
	network    string
	token      string
	store      contractStore
	planner    backfillPlanner
	registry   reloader
	classifier classifier
	log        *slog.Logger
}

// NewServer wires the admin API. All collaborators are required.
func NewServer(network, token string, st contractStore, planner backfillPlanner, reg reloader, cls classifier, log *slog.Logger) *Server {
	return &Server{network: network, token: token, store: st, planner: planner, registry: reg, classifier: cls, log: log}
}

// Register mounts the admin routes onto mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.Handle("POST /v1/contracts", s.auth(s.handleRegister))
	mux.Handle("DELETE /v1/contracts/{id}", s.auth(s.handleDelete))
}

// auth admits requests carrying the admin bearer token. The comparison is
// constant-time over fixed-length digests so neither content nor length of
// the expected token leaks through timing.
func (s *Server) auth(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || !tokenEqual(token, s.token) {
			writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		next(w, r)
	})
}

func tokenEqual(got, want string) bool {
	hg := sha256.Sum256([]byte(got))
	hw := sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(hg[:], hw[:]) == 1
}

// registerRequest is the POST /v1/contracts body.
type registerRequest struct {
	ContractID string   `json:"contract_id"`
	Kinds      []string `json:"kinds"`
	// From is the backfill origin: absent or "genesis" walks history as far
	// back as sources allow; a ledger number stops there.
	From json.RawMessage `json:"from"`
}

// parseFrom interprets the backfill origin. Ledger sequences start at 1, so
// genesis is target 1; the retention clamp keeps the promise honest when
// sources cannot reach it.
func parseFrom(raw json.RawMessage) (uint32, error) {
	if len(raw) == 0 {
		return 1, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if s == "genesis" {
			return 1, nil
		}
		return 0, fmt.Errorf("from %q is not supported (use genesis or a ledger number)", s)
	}
	var n uint64
	if err := json.Unmarshal(raw, &n); err != nil || n == 0 || n > math.MaxUint32 {
		return 0, fmt.Errorf("from %s must be genesis or a positive ledger number", raw)
	}
	return uint32(n), nil
}

// contractResponse is the public shape of a registration.
type contractResponse struct {
	ContractID     string          `json:"contract_id"`
	Network        string          `json:"network"`
	Source         string          `json:"source"`
	Kinds          []string        `json:"kinds"`
	Classification json.RawMessage `json:"classification"`
	RegisteredAt   time.Time       `json:"registered_at"`
}

func toResponse(c store.Contract) contractResponse {
	return contractResponse{
		ContractID:     c.ContractID,
		Network:        c.Network,
		Source:         c.Source,
		Kinds:          c.Kinds,
		Classification: c.Classification,
		RegisteredAt:   c.RegisteredAt,
	}
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	// An unknown field is almost certainly a typo (kind vs kinds); rejecting
	// it beats silently ignoring intent (CLAUDE.md rule 12 spirit).
	dec.DisallowUnknownFields()

	var req registerRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	if !strkey.IsValidContractAddress(req.ContractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("contract_id %q is not a valid contract address (C... strkey)", req.ContractID))
		return
	}
	targetFrom, err := parseFrom(req.From)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	kinds := req.Kinds
	if len(kinds) == 0 {
		// D1 default: derive everything v1 knows how to derive.
		kinds = []string{store.KindEvents, store.KindState}
	}
	for _, k := range kinds {
		if k != store.KindEvents && k != store.KindState {
			writeError(w, http.StatusBadRequest,
				fmt.Sprintf("kind %q is not supported (supported: %s, %s)", k, store.KindEvents, store.KindState))
			return
		}
	}

	// Classification happens before the row exists: a contract that is not
	// on chain is a caller mistake, not a registration (D1 — kinds default
	// follows what the chain says the contract is).
	cls, err := s.classifier.Classify(r.Context(), req.ContractID)
	switch {
	case errors.Is(err, registry.ErrContractNotFound):
		writeError(w, http.StatusNotFound,
			fmt.Sprintf("contract %s does not exist on %s", req.ContractID, s.network))
		return
	case err != nil:
		s.log.Error("classification failed", "contract_id", req.ContractID, "err", err)
		writeError(w, http.StatusBadGateway,
			"could not read the contract from the chain; retry when the RPC is reachable")
		return
	}
	clsJSON, err := json.Marshal(cls)
	if err != nil {
		s.log.Error("classification marshal failed", "contract_id", req.ContractID, "err", err)
		writeError(w, http.StatusInternalServerError, "registration failed; see server logs")
		return
	}

	saved, err := s.store.UpsertContract(r.Context(), store.Contract{
		Network:        s.network,
		ContractID:     req.ContractID,
		Source:         store.SourceAPI,
		Kinds:          kinds,
		Classification: clsJSON,
	})
	if err != nil {
		s.log.Error("contract registration failed", "contract_id", req.ContractID, "err", err)
		writeError(w, http.StatusInternalServerError, "registration failed; see server logs")
		return
	}
	// Anchor the history walk at the current cursor: live ingestion covers
	// everything after it, the backfill worker walks everything before it
	// down to the target.
	nextTo := uint32(0)
	switch cursor, err := s.planner.LoadCursor(r.Context(), s.network); {
	case err == nil:
		nextTo = cursor.Sequence
	case errors.Is(err, store.ErrNoCursor):
		s.log.Warn("no ingestion cursor yet; history before this registration will not be backfilled",
			"contract_id", saved.ContractID)
	default:
		s.log.Error("cursor read failed during registration", "contract_id", saved.ContractID, "err", err)
		writeError(w, http.StatusInternalServerError, "registration failed; see server logs")
		return
	}
	if err := s.planner.EnsureBackfill(r.Context(), s.network, saved.ContractID, targetFrom, nextTo); err != nil {
		s.log.Error("backfill planning failed", "contract_id", saved.ContractID, "err", err)
		writeError(w, http.StatusInternalServerError, "registration failed; see server logs")
		return
	}

	s.reloadRegistry(r.Context())
	s.log.Info("contract registered", "contract_id", saved.ContractID,
		"kinds", saved.Kinds, "type", cls.Type, "method", cls.Method,
		"events", len(cls.Events), "backfill_from", targetFrom, "backfill_to", nextTo)
	writeJSON(w, http.StatusOK, toResponse(saved))
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !strkey.IsValidContractAddress(id) {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("contract id %q is not a valid contract address (C... strkey)", id))
		return
	}
	existed, err := s.store.DeleteContract(r.Context(), s.network, id)
	if err != nil {
		s.log.Error("contract unregistration failed", "contract_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "unregistration failed; see server logs")
		return
	}
	s.reloadRegistry(r.Context())
	s.log.Info("contract unregistered", "contract_id", id, "existed", existed)
	w.WriteHeader(http.StatusNoContent)
}

// reloadRegistry refreshes the snapshot after a committed mutation. Failure
// is logged, not surfaced: the row is durable and the periodic reload will
// converge the snapshot.
func (s *Server) reloadRegistry(ctx context.Context) {
	if err := s.registry.Reload(ctx); err != nil {
		s.log.Warn("registry reload after mutation failed; periodic reload will catch up", "err", err)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
