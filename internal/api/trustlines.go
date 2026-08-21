package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// trustlinesReader is the store slice the trustline endpoints consume.
type trustlinesReader interface {
	QueryTrustlineEntries(ctx context.Context, network string, q store.TrustlineQuery) ([]store.TrustlineEntry, bool, error)
	QueryTrustlineChanges(ctx context.Context, network string, q store.TrustlineQuery) ([]store.TrustlineChange, bool, error)
}

// RegisterTrustlines mounts the trustline routes onto mux; kept separate
// from Register so main wires it explicitly when the reader exists.
func (s *Server) RegisterTrustlines(mux *http.ServeMux, trustlines trustlinesReader) {
	mux.HandleFunc("GET /v1/contracts/{id}/trustlines", func(w http.ResponseWriter, r *http.Request) {
		s.handleTrustlineSnapshot(w, r, trustlines)
	})
	mux.HandleFunc("GET /v1/contracts/{id}/trustlines/history", func(w http.ResponseWriter, r *http.Request) {
		s.handleTrustlineHistory(w, r, trustlines)
	})
}

// --- snapshot ---------------------------------------------------------------

type trustlineEntryRecord struct {
	AccountID          string `json:"accountId"`
	Asset              string `json:"asset"`
	Balance            string `json:"balance"`
	Limit              string `json:"limit"`
	Flags              uint32 `json:"flags"`
	LastModifiedLedger uint32 `json:"lastModifiedLedger"`
	ClosedAt           string `json:"closedAt"`
}

type trustlineSnapshotResponse struct {
	Trustlines   []trustlineEntryRecord `json:"trustlines"`
	Cursor       string                 `json:"cursor,omitempty"`
	Coverage     coverageInfo           `json:"coverage"`
	LatestLedger uint32                 `json:"latestLedger"`
}

func (s *Server) handleTrustlineSnapshot(w http.ResponseWriter, r *http.Request, trustlines trustlinesReader) {
	contract, ok := s.knownContract(w, r)
	if !ok {
		return
	}
	contractID := contract.ContractID

	q := store.TrustlineQuery{ContractID: contractID, Limit: defaultLimit}
	params := r.URL.Query()
	if cursor := params.Get("cursor"); cursor != "" {
		for _, banned := range []string{"account", "limit"} {
			if params.Has(banned) {
				writeError(w, http.StatusBadRequest, fmt.Sprintf(
					"cursor cannot be combined with %s: the cursor already encodes the whole query", banned))
				return
			}
		}
		decoded, err := decodeTrustlinesCursor(s.network, contractID, kindTrustlines, cursor)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		q = decoded
	} else {
		q.AccountID = params.Get("account")
		var err error
		if q.Limit, err = limitParam(params.Get("limit")); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	entries, hasMore, err := trustlines.QueryTrustlineEntries(r.Context(), s.network, q)
	if err != nil {
		s.serverError(w, "trustlines query failed", err)
		return
	}
	cursorSeq := s.cursorSequence(r.Context())

	resp := trustlineSnapshotResponse{
		Trustlines: make([]trustlineEntryRecord, 0, len(entries)),
		Coverage: s.coverage(r.Context(), contract, store.KindTrustlines,
			store.EventQuery{ContractID: contractID, FromLedger: 1}, cursorSeq),
		LatestLedger: cursorSeq,
	}
	for _, e := range entries {
		resp.Trustlines = append(resp.Trustlines, trustlineEntryRecord{
			AccountID:          e.AccountID,
			Asset:              e.Asset,
			Balance:            int64String(e.Balance),
			Limit:              int64String(e.Limit),
			Flags:              e.Flags,
			LastModifiedLedger: e.LastLedger,
			ClosedAt:           e.ClosedAt.UTC().Format(time.RFC3339),
		})
	}
	if hasMore && len(entries) > 0 {
		q.AfterAccount = entries[len(entries)-1].AccountID
		resp.Cursor = encodeTrustlinesCursor(s.network, kindTrustlines, q)
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- history ----------------------------------------------------------------

type trustlineChangeRecord struct {
	ID             string `json:"id"`
	Ledger         uint32 `json:"ledger"`
	LedgerClosedAt string `json:"ledgerClosedAt"`
	ContractID     string `json:"contractId"`
	AccountID      string `json:"accountId"`
	Asset          string `json:"asset"`
	ChangeType     string `json:"changeType"`
	PreBalance     string `json:"preBalance,omitempty"`
	PostBalance    string `json:"postBalance,omitempty"`
	PreLimit       string `json:"preLimit,omitempty"`
	PostLimit      string `json:"postLimit,omitempty"`
	Flags          uint32 `json:"flags"`
	TxHash         string `json:"txHash"`
	TxIndex        int32  `json:"txIndex"`
	OpIndex        int32  `json:"opIndex"`
}

type trustlineHistoryResponse struct {
	Changes      []trustlineChangeRecord `json:"changes"`
	Cursor       string                  `json:"cursor"`
	ScanStatus   string                  `json:"scanStatus"`
	Coverage     coverageInfo            `json:"coverage"`
	LatestLedger uint32                  `json:"latestLedger"`
}

func (s *Server) handleTrustlineHistory(w http.ResponseWriter, r *http.Request, trustlines trustlinesReader) {
	contract, ok := s.knownContract(w, r)
	if !ok {
		return
	}
	contractID := contract.ContractID

	q := store.TrustlineQuery{ContractID: contractID, FromLedger: 1, Limit: defaultLimit}
	params := r.URL.Query()
	if cursor := params.Get("cursor"); cursor != "" {
		for _, banned := range []string{"account", "limit", "startLedger", "endLedger"} {
			if params.Has(banned) {
				writeError(w, http.StatusBadRequest, fmt.Sprintf(
					"cursor cannot be combined with %s: the cursor already encodes the whole query", banned))
				return
			}
		}
		decoded, err := decodeTrustlinesCursor(s.network, contractID, kindTrustlineHistory, cursor)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		q = decoded
	} else {
		q.AccountID = params.Get("account")
		var err error
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

	changes, hasMore, err := trustlines.QueryTrustlineChanges(r.Context(), s.network, q)
	if err != nil {
		s.serverError(w, "trustline history query failed", err)
		return
	}
	cursorSeq := s.cursorSequence(r.Context())
	coverage := s.coverage(r.Context(), contract, store.KindTrustlines,
		store.EventQuery{ContractID: contractID, FromLedger: q.FromLedger, ToLedger: q.ToLedger}, cursorSeq)

	if len(changes) > 0 {
		q.AfterID = changes[len(changes)-1].ID
	}
	resp := trustlineHistoryResponse{
		Changes: make([]trustlineChangeRecord, 0, len(changes)),
		Cursor:  encodeTrustlinesCursor(s.network, kindTrustlineHistory, q),
		ScanStatus: scanStatus(hasMore,
			store.EventQuery{FromLedger: q.FromLedger, ToLedger: q.ToLedger}, coverage, cursorSeq),
		Coverage:     coverage,
		LatestLedger: cursorSeq,
	}
	for _, c := range changes {
		resp.Changes = append(resp.Changes, trustlineChangeRecord{
			ID:             c.ID,
			Ledger:         c.LedgerSequence,
			LedgerClosedAt: c.ClosedAt.UTC().Format(time.RFC3339),
			ContractID:     c.ContractID,
			AccountID:      c.AccountID,
			Asset:          c.Asset,
			ChangeType:     c.ChangeType,
			PreBalance:     int64String(c.PreBalance),
			PostBalance:    int64String(c.PostBalance),
			PreLimit:       int64String(c.PreLimit),
			PostLimit:      int64String(c.PostLimit),
			Flags:          c.Flags,
			TxHash:         c.TxHash,
			TxIndex:        c.TxIndex,
			OpIndex:        c.OpIndex,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// int64String renders a nullable stroop value; nil becomes "" (omitted).
func int64String(v *int64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatInt(*v, 10)
}
