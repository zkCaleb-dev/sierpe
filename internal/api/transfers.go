package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// transfersReader is the store slice the transfers endpoint consumes.
type transfersReader interface {
	QueryTransfers(ctx context.Context, network string, q store.TransferQuery) ([]store.Transfer, bool, error)
}

// RegisterTransfers mounts the transfers route onto mux; kept separate from
// Register so main wires it explicitly when the transfers reader exists.
func (s *Server) RegisterTransfers(mux *http.ServeMux, transfers transfersReader) {
	mux.HandleFunc("GET /v1/contracts/{id}/transfers", func(w http.ResponseWriter, r *http.Request) {
		s.handleTransfers(w, r, transfers)
	})
}

type transferRecord struct {
	ID             string `json:"id"`
	Ledger         uint32 `json:"ledger"`
	LedgerClosedAt string `json:"ledgerClosedAt"`
	ContractID     string `json:"contractId"`
	TransferType   string `json:"transferType"`
	From           string `json:"from,omitempty"`
	To             string `json:"to,omitempty"`
	ToMuxedID      string `json:"toMuxedId,omitempty"`
	Amount         string `json:"amount"`
	Asset          string `json:"asset,omitempty"`
	TxHash         string `json:"txHash"`
	TxIndex        int32  `json:"txIndex"`
	OpIndex        int32  `json:"opIndex"`
}

type transfersResponse struct {
	Transfers    []transferRecord `json:"transfers"`
	Cursor       string           `json:"cursor"`
	ScanStatus   string           `json:"scanStatus"`
	Coverage     coverageInfo     `json:"coverage"`
	LatestLedger uint32           `json:"latestLedger"`
}

func (s *Server) handleTransfers(w http.ResponseWriter, r *http.Request, transfers transfersReader) {
	contract, ok := s.knownContract(w, r)
	if !ok {
		return
	}
	contractID := contract.ContractID

	q := store.TransferQuery{ContractID: contractID, FromLedger: 1, Limit: defaultLimit}
	params := r.URL.Query()
	if cursor := params.Get("cursor"); cursor != "" {
		for _, banned := range []string{"account", "from", "to", "type", "limit", "startLedger", "endLedger"} {
			if params.Has(banned) {
				writeError(w, http.StatusBadRequest, fmt.Sprintf(
					"cursor cannot be combined with %s: the cursor already encodes the whole query", banned))
				return
			}
		}
		decoded, err := decodeTransfersCursor(s.network, contractID, cursor)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		q = decoded
	} else {
		q.Account = params.Get("account")
		q.From = params.Get("from")
		q.To = params.Get("to")
		if q.Account != "" && (q.From != "" || q.To != "") {
			writeError(w, http.StatusBadRequest,
				"account matches either side and cannot be combined with from or to")
			return
		}
		if t := params.Get("type"); t != "" {
			switch t {
			case store.TransferTypeTransfer, store.TransferTypeMint,
				store.TransferTypeBurn, store.TransferTypeClawback:
				q.TransferType = t
			default:
				writeError(w, http.StatusBadRequest, fmt.Sprintf(
					"type %q is not supported (supported: transfer, mint, burn, clawback)", t))
				return
			}
		}
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

	rows, hasMore, err := transfers.QueryTransfers(r.Context(), s.network, q)
	if err != nil {
		s.serverError(w, "transfers query failed", err)
		return
	}
	cursorSeq := s.cursorSequence(r.Context())
	coverage := s.coverage(r.Context(), contract, store.KindTransfers,
		store.EventQuery{ContractID: contractID, FromLedger: q.FromLedger, ToLedger: q.ToLedger}, cursorSeq)

	if len(rows) > 0 {
		q.AfterID = rows[len(rows)-1].ID
	}
	resp := transfersResponse{
		Transfers: make([]transferRecord, 0, len(rows)),
		Cursor:    encodeTransfersCursor(s.network, q),
		ScanStatus: scanStatus(hasMore,
			store.EventQuery{FromLedger: q.FromLedger, ToLedger: q.ToLedger}, coverage, cursorSeq),
		Coverage:     coverage,
		LatestLedger: cursorSeq,
	}
	for _, t := range rows {
		resp.Transfers = append(resp.Transfers, transferRecord{
			ID:             t.ID,
			Ledger:         t.LedgerSequence,
			LedgerClosedAt: t.ClosedAt.UTC().Format(time.RFC3339),
			ContractID:     t.ContractID,
			TransferType:   t.TransferType,
			From:           t.FromAddress,
			To:             t.ToAddress,
			ToMuxedID:      t.ToMuxedID,
			Amount:         t.Amount,
			Asset:          t.Asset,
			TxHash:         t.TxHash,
			TxIndex:        t.TxIndex,
			OpIndex:        t.OpIndex,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}
