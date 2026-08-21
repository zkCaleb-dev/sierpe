package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// movementReader is the store slice the movements endpoint consumes.
type movementReader interface {
	QueryMovements(ctx context.Context, network string, q store.MovementQuery) ([]store.Movement, bool, error)
}

// RegisterMovements mounts the movements route onto mux.
func (s *Server) RegisterMovements(mux *http.ServeMux, movements movementReader) {
	mux.HandleFunc("GET /v1/contracts/{id}/movements", func(w http.ResponseWriter, r *http.Request) {
		s.handleMovements(w, r, movements)
	})
}

type movementRecord struct {
	// ID is the source transfer's id, which is also its event id: the same
	// movement seen from both sides of a self-transfer shares it, so a
	// client keys on (id, role).
	ID              string `json:"id"`
	Role            string `json:"role"`
	TokenContractID string `json:"tokenContractId"`
	TransferType    string `json:"transferType"`
	Counterparty    string `json:"counterparty,omitempty"`
	Amount          string `json:"amount"`
	Ledger          uint32 `json:"ledger"`
	LedgerClosedAt  string `json:"ledgerClosedAt"`
}

type movementsResponse struct {
	Movements    []movementRecord `json:"movements"`
	Cursor       string           `json:"cursor"`
	ScanStatus   string           `json:"scanStatus"`
	Coverage     coverageInfo     `json:"coverage"`
	LatestLedger uint32           `json:"latestLedger"`
	// Note states what this resource is and is not. Movements are derived
	// from token-transfer events naming the contract, so value that arrives
	// without such an event is invisible here — which makes the running
	// total an inflow record, never a balance.
	Note string `json:"note"`
}

const movementsNote = "Movements are token transfers this contract took part in, derived from " +
	"the emitting token's events. Value can also arrive without any SEP-41 transfer event " +
	"(custom tokens, contracts crediting internal balances), so this is not a balance."

func (s *Server) handleMovements(w http.ResponseWriter, r *http.Request, movements movementReader) {
	contract, ok := s.knownContract(w, r)
	if !ok {
		return
	}
	contractID := contract.ContractID

	q := store.MovementQuery{ContractID: contractID, FromLedger: 1, Limit: defaultLimit}
	params := r.URL.Query()
	if cursor := params.Get("cursor"); cursor != "" {
		for _, banned := range []string{"role", "token", "type", "startLedger", "endLedger", "limit"} {
			if params.Has(banned) {
				writeError(w, http.StatusBadRequest, fmt.Sprintf(
					"cursor cannot be combined with %s: the cursor already encodes the whole query", banned))
				return
			}
		}
		decoded, err := decodeMovementsCursor(s.network, contractID, cursor)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		q = decoded
	} else {
		var err error
		if q.Limit, err = limitParam(params.Get("limit")); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		switch role := params.Get("role"); role {
		case "", store.RoleSender, store.RoleRecipient:
			q.Role = role
		default:
			writeError(w, http.StatusBadRequest, fmt.Sprintf(
				"role %q is not supported (use %s or %s; omit for both)",
				role, store.RoleSender, store.RoleRecipient))
			return
		}
		switch t := params.Get("type"); t {
		case "", store.TransferTypeTransfer, store.TransferTypeMint,
			store.TransferTypeBurn, store.TransferTypeClawback:
			q.Type = t
		default:
			writeError(w, http.StatusBadRequest, fmt.Sprintf(
				"type %q is not a token movement (use transfer, mint, burn or clawback)", t))
			return
		}
		if token := params.Get("token"); token != "" {
			if !strkey.IsValidContractAddress(token) {
				writeError(w, http.StatusBadRequest, fmt.Sprintf(
					"token %q is not a valid contract address (C... strkey); "+
						"movements identify an asset by its contract, never by its asset string", token))
				return
			}
			q.Token = token
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

	rows, hasMore, err := movements.QueryMovements(r.Context(), s.network, q)
	if err != nil {
		s.serverError(w, "movement query failed", err)
		return
	}
	cursorSeq := s.cursorSequence(r.Context())
	coverage := s.coverage(r.Context(), contract, store.KindMovements,
		store.EventQuery{ContractID: contractID, FromLedger: q.FromLedger, ToLedger: q.ToLedger}, cursorSeq)

	if len(rows) > 0 {
		last := rows[len(rows)-1]
		q.AfterID, q.AfterRole = last.TransferID, last.Role
	}
	resp := movementsResponse{
		Movements: make([]movementRecord, 0, len(rows)),
		Cursor:    encodeMovementsCursor(s.network, q),
		ScanStatus: scanStatus(hasMore,
			store.EventQuery{FromLedger: q.FromLedger, ToLedger: q.ToLedger}, coverage, cursorSeq),
		Coverage:     coverage,
		LatestLedger: cursorSeq,
		Note:         movementsNote,
	}
	for _, m := range rows {
		resp.Movements = append(resp.Movements, movementRecord{
			ID:              m.TransferID,
			Role:            m.Role,
			TokenContractID: m.TokenContractID,
			TransferType:    m.TransferType,
			Counterparty:    m.Counterparty,
			Amount:          m.Amount,
			Ledger:          m.LedgerSequence,
			LedgerClosedAt:  m.ClosedAt.UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, resp)
}
