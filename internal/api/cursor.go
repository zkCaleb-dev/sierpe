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
	kindSnapshot = "state"
	kindHistory  = "state_history"
)

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
