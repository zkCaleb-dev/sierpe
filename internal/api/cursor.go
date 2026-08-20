package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// cursorPayload is the opaque page cursor: it encodes the ENTIRE query
// (bounds, filters, limit) plus the resume position, so a cursor never
// expires and always honors the bounds it was minted with (decision D7,
// stellar#1872 cursor philosophy). Network and contract are included so a
// cursor cannot be replayed against a different dataset.
type cursorPayload struct {
	V          int        `json:"v"`
	Network    string     `json:"n"`
	ContractID string     `json:"c"`
	Topics     [4]*string `json:"t"`
	FromLedger uint32     `json:"f"`
	ToLedger   uint32     `json:"e"`
	Limit      int        `json:"l"`
	AfterID    string     `json:"a"`
}

const cursorVersion = 1

func encodeCursor(network string, q store.EventQuery, afterID string) string {
	payload := cursorPayload{
		V:          cursorVersion,
		Network:    network,
		ContractID: q.ContractID,
		Topics:     q.Topics,
		FromLedger: q.FromLedger,
		ToLedger:   q.ToLedger,
		Limit:      q.Limit,
		AfterID:    afterID,
	}
	raw, _ := json.Marshal(payload) // fixed shape; cannot fail
	return base64.RawURLEncoding.EncodeToString(raw)
}

// State cursor kinds: a snapshot cursor resumes a (key, durability) walk, a
// history cursor resumes a change-id walk. Encoded in the payload so a
// cursor can never be replayed against the wrong endpoint.
const (
	kindSnapshot         = "state"
	kindHistory          = "state_history"
	kindTransfers        = "transfers"
	kindTrustlines       = "trustlines"
	kindTrustlineHistory = "trustlines_history"
)

// trustlinesCursorPayload is the opaque cursor for both trustline endpoints.
type trustlinesCursorPayload struct {
	V            int    `json:"v"`
	Kind         string `json:"k"`
	Network      string `json:"n"`
	ContractID   string `json:"c"`
	AccountID    string `json:"acc,omitempty"`
	FromLedger   uint32 `json:"f,omitempty"`
	ToLedger     uint32 `json:"e,omitempty"`
	Limit        int    `json:"l"`
	AfterID      string `json:"a,omitempty"`
	AfterAccount string `json:"aa,omitempty"`
}

func encodeTrustlinesCursor(network, kind string, q store.TrustlineQuery) string {
	payload := trustlinesCursorPayload{
		V:            cursorVersion,
		Kind:         kind,
		Network:      network,
		ContractID:   q.ContractID,
		AccountID:    q.AccountID,
		FromLedger:   q.FromLedger,
		ToLedger:     q.ToLedger,
		Limit:        q.Limit,
		AfterID:      q.AfterID,
		AfterAccount: q.AfterAccount,
	}
	raw, _ := json.Marshal(payload) // fixed shape; cannot fail
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeTrustlinesCursor(network, contractID, kind, cursor string) (store.TrustlineQuery, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return store.TrustlineQuery{}, fmt.Errorf("cursor is not valid: %w", err)
	}
	var p trustlinesCursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return store.TrustlineQuery{}, fmt.Errorf("cursor is not valid: %w", err)
	}
	if p.V != cursorVersion {
		return store.TrustlineQuery{}, fmt.Errorf("cursor version %d is not supported", p.V)
	}
	if p.Kind != kind {
		return store.TrustlineQuery{}, fmt.Errorf("cursor belongs to a different endpoint")
	}
	if p.Network != network || p.ContractID != contractID {
		return store.TrustlineQuery{}, fmt.Errorf("cursor belongs to a different contract or network")
	}
	return store.TrustlineQuery{
		ContractID:   p.ContractID,
		AccountID:    p.AccountID,
		FromLedger:   p.FromLedger,
		ToLedger:     p.ToLedger,
		Limit:        p.Limit,
		AfterID:      p.AfterID,
		AfterAccount: p.AfterAccount,
	}, nil
}

// transfersCursorPayload is the opaque cursor for the transfers endpoint.
type transfersCursorPayload struct {
	V            int    `json:"v"`
	Kind         string `json:"k"`
	Network      string `json:"n"`
	ContractID   string `json:"c"`
	Account      string `json:"acc,omitempty"`
	From         string `json:"from,omitempty"`
	To           string `json:"to,omitempty"`
	TransferType string `json:"tt,omitempty"`
	FromLedger   uint32 `json:"f,omitempty"`
	ToLedger     uint32 `json:"e,omitempty"`
	Limit        int    `json:"l"`
	AfterID      string `json:"a,omitempty"`
}

func encodeTransfersCursor(network string, q store.TransferQuery) string {
	payload := transfersCursorPayload{
		V:            cursorVersion,
		Kind:         kindTransfers,
		Network:      network,
		ContractID:   q.ContractID,
		Account:      q.Account,
		From:         q.From,
		To:           q.To,
		TransferType: q.TransferType,
		FromLedger:   q.FromLedger,
		ToLedger:     q.ToLedger,
		Limit:        q.Limit,
		AfterID:      q.AfterID,
	}
	raw, _ := json.Marshal(payload) // fixed shape; cannot fail
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeTransfersCursor(network, contractID, cursor string) (store.TransferQuery, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return store.TransferQuery{}, fmt.Errorf("cursor is not valid: %w", err)
	}
	var p transfersCursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return store.TransferQuery{}, fmt.Errorf("cursor is not valid: %w", err)
	}
	if p.V != cursorVersion {
		return store.TransferQuery{}, fmt.Errorf("cursor version %d is not supported", p.V)
	}
	if p.Kind != kindTransfers {
		return store.TransferQuery{}, fmt.Errorf("cursor belongs to a different endpoint")
	}
	if p.Network != network || p.ContractID != contractID {
		return store.TransferQuery{}, fmt.Errorf("cursor belongs to a different contract or network")
	}
	return store.TransferQuery{
		ContractID:   p.ContractID,
		Account:      p.Account,
		From:         p.From,
		To:           p.To,
		TransferType: p.TransferType,
		FromLedger:   p.FromLedger,
		ToLedger:     p.ToLedger,
		Limit:        p.Limit,
		AfterID:      p.AfterID,
	}, nil
}

// stateCursorPayload is the opaque cursor for both state endpoints.
type stateCursorPayload struct {
	V          int    `json:"v"`
	Kind       string `json:"k"`
	Network    string `json:"n"`
	ContractID string `json:"c"`
	KeyXDR     string `json:"key,omitempty"`
	FromLedger uint32 `json:"f,omitempty"`
	ToLedger   uint32 `json:"e,omitempty"`
	Limit      int    `json:"l"`
	AfterID    string `json:"a,omitempty"`
	AfterKey   string `json:"ak,omitempty"`
	AfterDur   string `json:"ad,omitempty"`
}

func encodeStateCursor(network, kind string, q store.StateQuery) string {
	payload := stateCursorPayload{
		V:          cursorVersion,
		Kind:       kind,
		Network:    network,
		ContractID: q.ContractID,
		KeyXDR:     q.KeyXDR,
		FromLedger: q.FromLedger,
		ToLedger:   q.ToLedger,
		Limit:      q.Limit,
		AfterID:    q.AfterID,
		AfterKey:   q.AfterKey,
		AfterDur:   q.AfterDurability,
	}
	raw, _ := json.Marshal(payload) // fixed shape; cannot fail
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeStateCursor(network, contractID, kind, cursor string) (store.StateQuery, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return store.StateQuery{}, fmt.Errorf("cursor is not valid: %w", err)
	}
	var p stateCursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return store.StateQuery{}, fmt.Errorf("cursor is not valid: %w", err)
	}
	if p.V != cursorVersion {
		return store.StateQuery{}, fmt.Errorf("cursor version %d is not supported", p.V)
	}
	if p.Kind != kind {
		return store.StateQuery{}, fmt.Errorf("cursor belongs to a different endpoint")
	}
	if p.Network != network || p.ContractID != contractID {
		return store.StateQuery{}, fmt.Errorf("cursor belongs to a different contract or network")
	}
	return store.StateQuery{
		ContractID:      p.ContractID,
		KeyXDR:          p.KeyXDR,
		FromLedger:      p.FromLedger,
		ToLedger:        p.ToLedger,
		Limit:           p.Limit,
		AfterID:         p.AfterID,
		AfterKey:        p.AfterKey,
		AfterDurability: p.AfterDur,
	}, nil
}

func decodeCursor(network, contractID, cursor string) (store.EventQuery, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return store.EventQuery{}, fmt.Errorf("cursor is not valid: %w", err)
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return store.EventQuery{}, fmt.Errorf("cursor is not valid: %w", err)
	}
	if p.V != cursorVersion {
		return store.EventQuery{}, fmt.Errorf("cursor version %d is not supported", p.V)
	}
	if p.Network != network || p.ContractID != contractID {
		return store.EventQuery{}, fmt.Errorf("cursor belongs to a different contract or network")
	}
	return store.EventQuery{
		ContractID: p.ContractID,
		Topics:     p.Topics,
		FromLedger: p.FromLedger,
		ToLedger:   p.ToLedger,
		Limit:      p.Limit,
		AfterID:    p.AfterID,
	}, nil
}
