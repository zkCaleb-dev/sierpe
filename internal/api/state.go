package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// stateReader is the store slice the state endpoints consume.
type stateReader interface {
	QueryStateEntries(ctx context.Context, network string, q store.StateQuery) ([]store.StateEntry, bool, error)
	QueryStateChanges(ctx context.Context, network string, q store.StateQuery) ([]store.StateChange, bool, error)
}

// RegisterState mounts the state routes onto mux; kept separate from
// Register so main wires it explicitly when the state reader exists.
func (s *Server) RegisterState(mux *http.ServeMux, state stateReader) {
	mux.HandleFunc("GET /v1/contracts/{id}/state", func(w http.ResponseWriter, r *http.Request) {
		s.handleStateSnapshot(w, r, state)
	})
	mux.HandleFunc("GET /v1/contracts/{id}/state/history", func(w http.ResponseWriter, r *http.Request) {
		s.handleStateHistory(w, r, state)
	})
}

// --- snapshot ---------------------------------------------------------------

type stateEntryRecord struct {
	Key                string `json:"key"`
	Durability         string `json:"durability"`
	Value              string `json:"value"`
	LastModifiedLedger uint32 `json:"lastModifiedLedger"`
	ClosedAt           string `json:"closedAt"`
}

type stateSnapshotResponse struct {
	Entries      []stateEntryRecord `json:"entries"`
	Cursor       string             `json:"cursor,omitempty"`
	Coverage     coverageInfo       `json:"coverage"`
	LatestLedger uint32             `json:"latestLedger"`
}

func (s *Server) handleStateSnapshot(w http.ResponseWriter, r *http.Request, state stateReader) {
	contractID, ok := s.knownContract(w, r)
	if !ok {
		return
	}

	q := store.StateQuery{ContractID: contractID, Limit: defaultLimit}
	params := r.URL.Query()
	if cursor := params.Get("cursor"); cursor != "" {
		for _, banned := range []string{"key", "limit"} {
			if params.Has(banned) {
				writeError(w, http.StatusBadRequest, fmt.Sprintf(
					"cursor cannot be combined with %s: the cursor already encodes the whole query", banned))
				return
			}
		}
		decoded, err := decodeStateCursor(s.network, contractID, kindSnapshot, cursor)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		q = decoded
	} else {
		var err error
		if q.KeyXDR, err = keyParam(params.Get("key")); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if q.Limit, err = limitParam(params.Get("limit")); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	entries, hasMore, err := state.QueryStateEntries(r.Context(), s.network, q)
	if err != nil {
		s.serverError(w, "state query failed", err)
		return
	}
	cursorSeq := s.cursorSequence(r.Context())

	resp := stateSnapshotResponse{
		Entries: make([]stateEntryRecord, 0, len(entries)),
		Coverage: s.coverage(r.Context(), contractID,
			store.EventQuery{ContractID: contractID, FromLedger: 1}, cursorSeq),
		LatestLedger: cursorSeq,
	}
	for _, e := range entries {
		resp.Entries = append(resp.Entries, stateEntryRecord{
			Key:                e.KeyXDR,
			Durability:         e.Durability,
			Value:              e.ValueXDR,
			LastModifiedLedger: e.LastLedger,
			ClosedAt:           e.ClosedAt.UTC().Format(time.RFC3339),
		})
	}
	if hasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		q.AfterKey, q.AfterDurability = last.KeyXDR, last.Durability
		resp.Cursor = encodeStateCursor(s.network, kindSnapshot, q)
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- history ----------------------------------------------------------------

type stateChangeRecord struct {
	ID             string `json:"id"`
	Ledger         uint32 `json:"ledger"`
	LedgerClosedAt string `json:"ledgerClosedAt"`
	ContractID     string `json:"contractId"`
	Key            string `json:"key"`
	Durability     string `json:"durability"`
	ChangeType     string `json:"changeType"`
	Pre            string `json:"pre,omitempty"`
	Post           string `json:"post,omitempty"`
	TxHash         string `json:"txHash"`
	TxIndex        int32  `json:"txIndex"`
	OpIndex        int32  `json:"opIndex"`
}

type stateHistoryResponse struct {
	Changes      []stateChangeRecord `json:"changes"`
	Cursor       string              `json:"cursor"`
	ScanStatus   string              `json:"scanStatus"`
	Coverage     coverageInfo        `json:"coverage"`
	LatestLedger uint32              `json:"latestLedger"`
}

func (s *Server) handleStateHistory(w http.ResponseWriter, r *http.Request, state stateReader) {
	contractID, ok := s.knownContract(w, r)
	if !ok {
		return
	}

	q := store.StateQuery{ContractID: contractID, FromLedger: 1, Limit: defaultLimit}
	params := r.URL.Query()
	if cursor := params.Get("cursor"); cursor != "" {
		for _, banned := range []string{"key", "limit", "startLedger", "endLedger"} {
			if params.Has(banned) {
				writeError(w, http.StatusBadRequest, fmt.Sprintf(
					"cursor cannot be combined with %s: the cursor already encodes the whole query", banned))
				return
			}
		}
		decoded, err := decodeStateCursor(s.network, contractID, kindHistory, cursor)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		q = decoded
	} else {
		var err error
		if q.KeyXDR, err = keyParam(params.Get("key")); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if q.Limit, err = limitParam(params.Get("limit")); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if raw := params.Get("startLedger"); raw != "" {
			v, err := strconv.ParseUint(raw, 10, 32)
			if err != nil || v == 0 {
				writeError(w, http.StatusBadRequest,
					fmt.Sprintf("startLedger %q must be a positive ledger sequence", raw))
				return
			}
			q.FromLedger = uint32(v)
		}
		if raw := params.Get("endLedger"); raw != "" {
			v, err := strconv.ParseUint(raw, 10, 32)
			if err != nil || v == 0 || uint32(v) < q.FromLedger {
				writeError(w, http.StatusBadRequest,
					fmt.Sprintf("endLedger %q must be a ledger sequence at or above startLedger", raw))
				return
			}
			q.ToLedger = uint32(v)
		}
	}

	changes, hasMore, err := state.QueryStateChanges(r.Context(), s.network, q)
	if err != nil {
		s.serverError(w, "state history query failed", err)
		return
	}
	cursorSeq := s.cursorSequence(r.Context())
	coverage := s.coverage(r.Context(), contractID,
		store.EventQuery{ContractID: contractID, FromLedger: q.FromLedger, ToLedger: q.ToLedger}, cursorSeq)

	if len(changes) > 0 {
		q.AfterID = changes[len(changes)-1].ID
	}
	resp := stateHistoryResponse{
		Changes: make([]stateChangeRecord, 0, len(changes)),
		Cursor:  encodeStateCursor(s.network, kindHistory, q),
		ScanStatus: scanStatus(hasMore,
			store.EventQuery{FromLedger: q.FromLedger, ToLedger: q.ToLedger}, coverage, cursorSeq),
		Coverage:     coverage,
		LatestLedger: cursorSeq,
	}
	for _, c := range changes {
		resp.Changes = append(resp.Changes, stateChangeRecord{
			ID:             c.ID,
			Ledger:         c.LedgerSequence,
			LedgerClosedAt: c.ClosedAt.UTC().Format(time.RFC3339),
			ContractID:     c.ContractID,
			Key:            c.KeyXDR,
			Durability:     c.Durability,
			ChangeType:     c.ChangeType,
			Pre:            c.PreXDR,
			Post:           c.PostXDR,
			TxHash:         c.TxHash,
			TxIndex:        c.TxIndex,
			OpIndex:        c.OpIndex,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- shared helpers ---------------------------------------------------------

// knownContract validates the path id and 404s unregistered contracts.
func (s *Server) knownContract(w http.ResponseWriter, r *http.Request) (string, bool) {
	contractID := r.PathValue("id")
	if !strkey.IsValidContractAddress(contractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("contract id %q is not a valid contract address (C... strkey)", contractID))
		return "", false
	}
	if _, err := s.contracts.GetContract(r.Context(), s.network, contractID); err != nil {
		if errors.Is(err, store.ErrNoContract) {
			writeError(w, http.StatusNotFound,
				fmt.Sprintf("contract %s is not registered; register it via POST /v1/contracts", contractID))
			return "", false
		}
		s.serverError(w, "contract lookup failed", err)
		return "", false
	}
	return contractID, true
}

// cursorSequence reads the live cursor, tolerating a fresh database.
func (s *Server) cursorSequence(ctx context.Context) uint32 {
	cur, err := s.events.LoadCursor(ctx, s.network)
	if err != nil {
		return 0
	}
	return cur.Sequence
}

func keyParam(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	var scval xdr.ScVal
	if err := xdr.SafeUnmarshalBase64(raw, &scval); err != nil {
		return "", fmt.Errorf("key is not a valid base64 ScVal: %v", err)
	}
	return raw, nil
}

func limitParam(raw string) (int, error) {
	if raw == "" {
		return defaultLimit, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 || v > maxLimit {
		return 0, fmt.Errorf("limit %q must be between 1 and %d", raw, maxLimit)
	}
	return v, nil
}
