// Package extract turns whole-ledger XDR into Sierpe's canonical records:
// pure functions, meta in and records out (docs/DESIGN.md §6), with
// systematic distrust of chain data (CLAUDE.md rule 6) — failed transactions
// are skipped and counted, SDK XDR access sits behind a recover frontier,
// and nothing suppressed goes uncounted.
package extract

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// Result carries the extracted events plus the distrust counters (rule 6:
// everything suppressed is counted, never silently dropped).
type Result struct {
	Events []store.Event
	// FailedTxs counts transactions skipped because they did not succeed;
	// their events never happened. Routine, expected traffic.
	FailedTxs int
	// SuppressedTxs counts transactions whose meta could not be read or
	// panicked mid-decode. Should be zero; nonzero means lost data and is
	// exposed as a metric worth alerting on.
	SuppressedTxs int
	// SuppressedEvents counts individual events dropped because their XDR
	// could not be re-encoded. Same alarm semantics as SuppressedTxs.
	SuppressedEvents int
}

// Events extracts every event emitted by watched contracts in one ledger.
// An error means the ledger itself could not be opened (malformed meta from
// the source) — the caller retries; per-transaction damage never fails the
// ledger, it is counted instead.
func Events(lcm xdr.LedgerCloseMeta, passphrase string, watch *registry.Snapshot) (Result, error) {
	var res Result
	if watch.Len() == 0 {
		return res, nil
	}
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(passphrase, lcm)
	if err != nil {
		return res, fmt.Errorf("extract: open ledger %d: %w", lcm.LedgerSequence(), err)
	}

	header := lcm.LedgerHeaderHistoryEntry().Header
	seq := uint32(header.LedgerSeq)
	closedAt := time.Unix(int64(header.ScpValue.CloseTime), 0).UTC()

	for {
		tx, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// A transaction the reader cannot even hand over: count it and
			// keep going — the reader advances past it on its own.
			res.SuppressedTxs++
			continue
		}
		txEvents(tx, seq, closedAt, watch, &res)
	}
	return res, nil
}

// txEvents appends one transaction's watched events to res. The recover
// frontier lives here: SDK XDR getters panic on nil unions, and one hostile
// transaction must cost exactly one transaction (counted), not the ledger.
func txEvents(tx ingest.LedgerTransaction, seq uint32, closedAt time.Time, watch *registry.Snapshot, res *Result) {
	defer func() {
		if r := recover(); r != nil {
			res.SuppressedTxs++
		}
	}()

	// The SDK streams failed transactions too; their events describe things
	// that did not happen (KNOWLEDGE.md P12).
	if !tx.Successful() {
		res.FailedTxs++
		return
	}
	events, err := tx.GetTransactionEvents()
	if err != nil {
		res.SuppressedTxs++
		return
	}

	txHash := tx.Result.TransactionHash.HexString()
	prefix := toid.New(int32(seq), int32(tx.Index), 0).ToInt64()
	// The event index runs across every operation event in the transaction,
	// counted before filtering so ids stay stable regardless of what we
	// store (getEvents id semantics, decision D7).
	eventIndex := int32(-1)

	for opIndex, opEvents := range events.OperationEvents {
		for _, ev := range opEvents {
			eventIndex++
			if ev.Type != xdr.ContractEventTypeContract || ev.ContractId == nil {
				continue
			}
			contractID, err := strkey.Encode(strkey.VersionByteContract, ev.ContractId[:])
			if err != nil {
				res.SuppressedEvents++
				continue
			}
			contract, watched := watch.Get(contractID)
			if !watched || !contract.HasKind(store.KindEvents) {
				continue
			}
			record, err := buildEvent(ev, contractID, seq, closedAt, txHash, int32(tx.Index), int32(opIndex), eventIndex, prefix)
			if err != nil {
				res.SuppressedEvents++
				continue
			}
			res.Events = append(res.Events, record)
		}
	}
}

// buildEvent renders one ContractEvent into the canonical envelope row (D3).
func buildEvent(ev xdr.ContractEvent, contractID string, seq uint32, closedAt time.Time,
	txHash string, txIndex, opIndex, eventIndex int32, prefix int64) (store.Event, error) {

	rawXDR, err := xdr.MarshalBase64(ev)
	if err != nil {
		return store.Event{}, fmt.Errorf("extract: encode event: %w", err)
	}
	body, ok := ev.Body.GetV0()
	if !ok {
		return store.Event{}, fmt.Errorf("extract: unsupported event body version %d", ev.Body.V)
	}
	topics := make([]string, 0, len(body.Topics))
	for _, t := range body.Topics {
		b64, err := xdr.MarshalBase64(t)
		if err != nil {
			return store.Event{}, fmt.Errorf("extract: encode topic: %w", err)
		}
		topics = append(topics, b64)
	}
	value, err := xdr.MarshalBase64(body.Data)
	if err != nil {
		return store.Event{}, fmt.Errorf("extract: encode event data: %w", err)
	}

	var name string
	if len(body.Topics) > 0 {
		if sym, ok := body.Topics[0].GetSym(); ok {
			name = string(sym)
		}
	}

	return store.Event{
		ID:             fmt.Sprintf("%019d-%010d", prefix, eventIndex),
		ContractID:     contractID,
		LedgerSequence: seq,
		ClosedAt:       closedAt,
		TxHash:         txHash,
		TxIndex:        txIndex,
		OpIndex:        opIndex,
		EventIndex:     eventIndex,
		EventName:      name,
		Topics:         topics,
		ValueXDR:       value,
		RawXDR:         rawXDR,
	}, nil
}
