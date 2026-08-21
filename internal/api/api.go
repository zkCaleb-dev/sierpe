// Package api serves Sierpe's public v1 read surface. The events endpoint
// is a compatible superset of the getEvents v2 proposal (decision D7,
// stellar#1872): positional topic filters, opaque cursors that encode the
// whole query, and scan-status honesty — an empty page always says whether
// it means "nothing happened" or "not indexed yet" (KNOWLEDGE.md P7, P17).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
)

// Scan statuses, per the getEvents v2 vocabulary (stellar#1872).
const (
	scanHasMore           = "HAS_MORE"
	scanWaitingForLedgers = "WAITING_FOR_LEDGERS"
	scanOldestReached     = "OLDEST_REACHED"
	scanComplete          = "COMPLETE"
)

// eventReader is the store slice the events endpoint consumes.
type eventReader interface {
	QueryEvents(ctx context.Context, network string, q store.EventQuery) ([]store.Event, bool, error)
	LoadCursor(ctx context.Context, network string) (store.Cursor, error)
}

// contractReader is the store slice the contract endpoints consume.
type contractReader interface {
	GetContract(ctx context.Context, network, contractID string) (store.Contract, error)
	ListContracts(ctx context.Context, network string) ([]store.Contract, error)
	GetBackfill(ctx context.Context, network, contractID string) (store.Backfill, error)
	EventCountsByName(ctx context.Context, network, contractID string) (map[string]int64, error)
	CountStateEntries(ctx context.Context, network, contractID string) (int64, error)
}

// Server holds the public API dependencies.
type Server struct {
	network   string
	events    eventReader
	contracts contractReader
	log       *slog.Logger
}

// NewServer wires the public API. All collaborators are required.
func NewServer(network string, events eventReader, contracts contractReader, log *slog.Logger) *Server {
	return &Server{network: network, events: events, contracts: contracts, log: log}
}

// Register mounts the public routes onto mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/contracts", s.handleContractList)
	mux.HandleFunc("GET /v1/contracts/{id}", s.handleContract)
	mux.HandleFunc("GET /v1/contracts/{id}/events", s.handleEvents)
}

// --- events -----------------------------------------------------------------

// coverageInfo declares which part of the request the response can vouch
// for. A range Sierpe cannot vouch for is stated, never implied (rule 7).
type coverageInfo struct {
	// Kind names the data kind this declaration is about: coverage is a
	// property of (contract, kind), never of a contract alone — a kind the
	// registration does not derive has no history at all, however finished
	// the contract's walk looks.
	Kind                string  `json:"kind"`
	KindDerived         bool    `json:"kindDerived"`
	RequestedFromLedger uint32  `json:"requestedFromLedger"`
	RequestedToLedger   *uint32 `json:"requestedToLedger"` // null = follow the tip
	IndexedFromLedger   uint32  `json:"indexedFromLedger"`
	IndexedToLedger     uint32  `json:"indexedToLedger"`
	BackfillPending     bool    `json:"backfillPending"`
	ClampedAt           *uint32 `json:"clampedAt,omitempty"`
}

type eventRecord struct {
	ID                       string   `json:"id"`
	Type                     string   `json:"type"`
	Ledger                   uint32   `json:"ledger"`
	LedgerClosedAt           string   `json:"ledgerClosedAt"`
	ContractID               string   `json:"contractId"`
	Topic                    []string `json:"topic"`
	Value                    string   `json:"value"`
	InSuccessfulContractCall bool     `json:"inSuccessfulContractCall"`
	TxHash                   string   `json:"txHash"`
	TxIndex                  int32    `json:"txIndex"`
	OpIndex                  int32    `json:"opIndex"`
	EventName                string   `json:"eventName,omitempty"`
}

type eventsResponse struct {
	Events       []eventRecord `json:"events"`
	Cursor       string        `json:"cursor"`
	ScanStatus   string        `json:"scanStatus"`
	Coverage     coverageInfo  `json:"coverage"`
	LatestLedger uint32        `json:"latestLedger"`
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	contract, ok := s.knownContract(w, r)
	if !ok {
		return
	}
	contractID := contract.ContractID

	q, err := s.buildQuery(r, contractID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	events, hasMore, err := s.events.QueryEvents(r.Context(), s.network, q)
	if err != nil {
		s.serverError(w, "event query failed", err)
		return
	}

	cursorSeq := uint32(0)
	if cur, err := s.events.LoadCursor(r.Context(), s.network); err == nil {
		cursorSeq = cur.Sequence
	} else if !errors.Is(err, store.ErrNoCursor) {
		s.serverError(w, "cursor read failed", err)
		return
	}
	coverage := s.coverage(r.Context(), contract, store.KindEvents, q, cursorSeq)

	afterID := q.AfterID
	if len(events) > 0 {
		afterID = events[len(events)-1].ID
	}

	resp := eventsResponse{
		Events:       make([]eventRecord, 0, len(events)),
		Cursor:       encodeCursor(s.network, q, afterID),
		ScanStatus:   scanStatus(hasMore, q, coverage, cursorSeq),
		Coverage:     coverage,
		LatestLedger: cursorSeq,
	}
	for _, e := range events {
		resp.Events = append(resp.Events, eventRecord{
			ID:                       e.ID,
			Type:                     "contract",
			Ledger:                   e.LedgerSequence,
			LedgerClosedAt:           e.ClosedAt.UTC().Format(time.RFC3339),
			ContractID:               e.ContractID,
			Topic:                    e.Topics,
			Value:                    e.ValueXDR,
			InSuccessfulContractCall: true, // failed txs are never indexed
			TxHash:                   e.TxHash,
			TxIndex:                  e.TxIndex,
			OpIndex:                  e.OpIndex,
			EventName:                e.EventName,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// buildQuery assembles the effective query from either the cursor (which
// encodes everything) or the request parameters — never a mix: a cursor
// must honor the bounds it was minted with.
func (s *Server) buildQuery(r *http.Request, contractID string) (store.EventQuery, error) {
	params := r.URL.Query()

	if cursor := params.Get("cursor"); cursor != "" {
		for _, banned := range []string{"topic0", "topic1", "topic2", "topic3", "startLedger", "endLedger", "type"} {
			if params.Has(banned) {
				return store.EventQuery{}, fmt.Errorf(
					"cursor cannot be combined with %s: the cursor already encodes the whole query", banned)
			}
		}
		return decodeCursor(s.network, contractID, cursor)
	}

	q := store.EventQuery{ContractID: contractID, FromLedger: 1, Limit: defaultLimit}

	if t := params.Get("type"); t != "" && t != "contract" {
		return q, fmt.Errorf("type %q is not indexed (M1 indexes contract events only)", t)
	}
	if raw := params.Get("startLedger"); raw != "" {
		v, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || v == 0 {
			return q, fmt.Errorf("startLedger %q must be a positive ledger sequence", raw)
		}
		q.FromLedger = uint32(v)
	}
	if raw := params.Get("endLedger"); raw != "" {
		v, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || v == 0 {
			return q, fmt.Errorf("endLedger %q must be a positive ledger sequence", raw)
		}
		q.ToLedger = uint32(v)
		if q.ToLedger < q.FromLedger {
			return q, fmt.Errorf("endLedger %d is below startLedger %d", q.ToLedger, q.FromLedger)
		}
	}
	if raw := params.Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 || v > maxLimit {
			return q, fmt.Errorf("limit %q must be between 1 and %d", raw, maxLimit)
		}
		q.Limit = v
	}
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("topic%d", i)
		raw := params.Get(name)
		if raw == "" {
			continue
		}
		var scval xdr.ScVal
		if err := xdr.SafeUnmarshalBase64(raw, &scval); err != nil {
			return q, fmt.Errorf("%s is not a valid base64 ScVal: %v", name, err)
		}
		topic := raw
		q.Topics[i] = &topic
	}
	return q, nil
}

// coverage derives the vouchable range from backfill state and the live
// cursor (docs/DESIGN.md §4: coverage is computed from persisted state,
// never guessed).
func (s *Server) coverage(ctx context.Context, contract store.Contract, kind string, q store.EventQuery, cursorSeq uint32) coverageInfo {
	cov := coverageInfo{
		Kind:                kind,
		KindDerived:         contract.HasKind(kind),
		RequestedFromLedger: q.FromLedger,
		IndexedToLedger:     cursorSeq,
	}
	if q.ToLedger > 0 {
		to := q.ToLedger
		cov.RequestedToLedger = &to
		if to < cursorSeq {
			cov.IndexedToLedger = to
		}
	}
	if !cov.KindDerived {
		// Nothing was ever derived for this kind: an empty page here means
		// "not indexed", not "never happened", and the response has to say
		// which (rule 7).
		cov.IndexedFromLedger = 0
		cov.IndexedToLedger = 0
		return cov
	}

	bf, err := s.contracts.GetBackfill(ctx, s.network, contract.ContractID)
	switch {
	case errors.Is(err, store.ErrNoBackfill):
		// Registration without a walk (should not happen); vouch for nothing
		// below the tip rather than guessing.
		cov.IndexedFromLedger = cursorSeq
	case err != nil:
		cov.IndexedFromLedger = cursorSeq
	case !bf.CoversKind(kind):
		// The kind was added after this walk ran, and the walk has not been
		// reopened yet: below the anchor nothing was derived for it.
		cov.IndexedFromLedger = cursorSeq
		cov.BackfillPending = true
	default:
		cov.IndexedFromLedger = bf.NextTo + 1
		cov.BackfillPending = !bf.Done
		cov.ClampedAt = bf.ClampedAt
	}
	if cov.IndexedFromLedger < q.FromLedger {
		cov.IndexedFromLedger = q.FromLedger
	}
	return cov
}

// coverageByKind declares coverage for every kind the registration derives,
// in a stable order so a client can diff two responses.
func (s *Server) coverageByKind(ctx context.Context, contract store.Contract, cursorSeq uint32) []coverageInfo {
	q := store.EventQuery{ContractID: contract.ContractID, FromLedger: 1}
	out := make([]coverageInfo, 0, len(contract.Kinds))
	for _, kind := range []string{
		store.KindEvents, store.KindState, store.KindTransfers, store.KindTrustlines,
		store.KindMovements,
	} {
		if contract.HasKind(kind) {
			out = append(out, s.coverage(ctx, contract, kind, q, cursorSeq))
		}
	}
	return out
}

// scanStatus reports how the scan of the requested range ended, with the
// getEvents v2 vocabulary. Priority: more rows now > more rows later >
// unreachable history > complete.
func scanStatus(hasMore bool, q store.EventQuery, cov coverageInfo, cursorSeq uint32) string {
	switch {
	case hasMore:
		return scanHasMore
	case !cov.KindDerived:
		// Nothing was ever derived for this kind, so the page is empty
		// because it was never filled — the one thing COMPLETE must never
		// be allowed to mean (rule 7). OLDEST_REACHED is the vocabulary's
		// "there is history here this instance cannot serve", which is
		// exactly the situation, and it keeps the getEvents v2 status set
		// intact (decision D7).
		return scanOldestReached
	case q.ToLedger == 0 || q.ToLedger > cursorSeq:
		return scanWaitingForLedgers
	case q.FromLedger < cov.IndexedFromLedger:
		return scanOldestReached
	default:
		return scanComplete
	}
}

// --- contract detail --------------------------------------------------------

type contractDetail struct {
	ContractID     string          `json:"contract_id"`
	Network        string          `json:"network"`
	Source         string          `json:"source"`
	Kinds          []string        `json:"kinds"`
	Classification json.RawMessage `json:"classification"`
	RegisteredAt   string          `json:"registered_at"`
	// Coverage carries one declaration per registered kind: a contract does
	// not have a coverage, its (contract, kind) pairs do.
	Coverage []coverageInfo   `json:"coverage"`
	Events   contractEventAgg `json:"events"`
	State    contractStateAgg `json:"state"`
}

type contractEventAgg struct {
	Total  int64            `json:"total"`
	ByName map[string]int64 `json:"by_name"`
}

type contractStateAgg struct {
	Entries int64 `json:"entries"`
}

func (s *Server) handleContract(w http.ResponseWriter, r *http.Request) {
	contract, ok := s.knownContract(w, r)
	if !ok {
		return
	}
	contractID := contract.ContractID

	cursorSeq := uint32(0)
	if cur, err := s.events.LoadCursor(r.Context(), s.network); err == nil {
		cursorSeq = cur.Sequence
	}
	counts, err := s.contracts.EventCountsByName(r.Context(), s.network, contractID)
	if err != nil {
		s.serverError(w, "event counts failed", err)
		return
	}
	var total int64
	for _, n := range counts {
		total += n
	}
	stateEntries, err := s.contracts.CountStateEntries(r.Context(), s.network, contractID)
	if err != nil {
		s.serverError(w, "state counts failed", err)
		return
	}

	detail := contractDetail{
		ContractID:     contract.ContractID,
		Network:        contract.Network,
		Source:         contract.Source,
		Kinds:          contract.Kinds,
		Classification: contract.Classification,
		RegisteredAt:   contract.RegisteredAt.UTC().Format(time.RFC3339),
		Coverage:       s.coverageByKind(r.Context(), contract, cursorSeq),
		Events:         contractEventAgg{Total: total, ByName: counts},
		State:          contractStateAgg{Entries: stateEntries},
	}
	writeJSON(w, http.StatusOK, detail)
}

// contractSummary is one row of the registration listing: identity and
// classification without the per-contract aggregation queries (fetch the
// detail endpoint for coverage and counts).
type contractSummary struct {
	ContractID     string          `json:"contract_id"`
	Network        string          `json:"network"`
	Source         string          `json:"source"`
	Kinds          []string        `json:"kinds"`
	Classification json.RawMessage `json:"classification"`
	RegisteredAt   string          `json:"registered_at"`
}

type contractListResponse struct {
	Contracts []contractSummary `json:"contracts"`
}

func (s *Server) handleContractList(w http.ResponseWriter, r *http.Request) {
	contracts, err := s.contracts.ListContracts(r.Context(), s.network)
	if err != nil {
		s.serverError(w, "contract listing failed", err)
		return
	}
	resp := contractListResponse{Contracts: make([]contractSummary, 0, len(contracts))}
	for _, c := range contracts {
		resp.Contracts = append(resp.Contracts, contractSummary{
			ContractID:     c.ContractID,
			Network:        c.Network,
			Source:         c.Source,
			Kinds:          c.Kinds,
			Classification: c.Classification,
			RegisteredAt:   c.RegisteredAt.UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- plumbing ---------------------------------------------------------------

func (s *Server) serverError(w http.ResponseWriter, msg string, err error) {
	s.log.Error(msg, "err", err)
	writeError(w, http.StatusInternalServerError, "internal error; see server logs")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
