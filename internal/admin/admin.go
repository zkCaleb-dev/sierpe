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
	registry   reloader
	classifier classifier
	log        *slog.Logger
}

// NewServer wires the admin API. All collaborators are required.
func NewServer(network, token string, st contractStore, reg reloader, cls classifier, log *slog.Logger) *Server {
	return &Server{network: network, token: token, store: st, registry: reg, classifier: cls, log: log}
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
	kinds := req.Kinds
	if len(kinds) == 0 {
		kinds = []string{store.KindEvents}
	}
	for _, k := range kinds {
		if k != store.KindEvents {
			writeError(w, http.StatusBadRequest,
				fmt.Sprintf("kind %q is not supported (supported: %s)", k, store.KindEvents))
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
	s.reloadRegistry(r.Context())
	s.log.Info("contract registered", "contract_id", saved.ContractID,
		"kinds", saved.Kinds, "type", cls.Type, "method", cls.Method, "events", len(cls.Events))
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
