package api

import (
	"testing"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// A cursor is opaque, NOT trusted: it comes back from the client and anyone
// can mint one. A negative limit reaches the store as out[:limit] and
// panics the request goroutine; an oversized one silently bypasses
// maxLimit. Every decoder has to refuse both before returning a query.
func TestCursorsRejectLimitsNoHandlerWouldMint(t *testing.T) {
	for _, limit := range []int{-1, 0, maxLimit + 1} {
		t.Run("events", func(t *testing.T) {
			c := encodeCursor("testnet", store.EventQuery{ContractID: registered, Limit: limit}, "")
			if _, err := decodeCursor("testnet", registered, c); err == nil {
				t.Errorf("limit %d accepted", limit)
			}
		})
		t.Run("state", func(t *testing.T) {
			c := encodeStateCursor("testnet", kindSnapshot, store.StateQuery{ContractID: registered, Limit: limit})
			if _, err := decodeStateCursor("testnet", registered, kindSnapshot, c); err == nil {
				t.Errorf("limit %d accepted", limit)
			}
		})
		t.Run("transfers", func(t *testing.T) {
			c := encodeTransfersCursor("testnet", store.TransferQuery{ContractID: registered, Limit: limit})
			if _, err := decodeTransfersCursor("testnet", registered, c); err == nil {
				t.Errorf("limit %d accepted", limit)
			}
		})
		t.Run("trustlines", func(t *testing.T) {
			c := encodeTrustlinesCursor("testnet", kindTrustlines, store.TrustlineQuery{ContractID: registered, Limit: limit})
			if _, err := decodeTrustlinesCursor("testnet", registered, kindTrustlines, c); err == nil {
				t.Errorf("limit %d accepted", limit)
			}
		})
		t.Run("movements", func(t *testing.T) {
			c := encodeMovementsCursor("testnet", store.MovementQuery{ContractID: registered, Limit: limit})
			if _, err := decodeMovementsCursor("testnet", registered, c); err == nil {
				t.Errorf("limit %d accepted", limit)
			}
		})
	}
}
