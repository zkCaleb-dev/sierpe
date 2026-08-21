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

// scope is the per-ledger decision of what the registry makes worth doing,
// computed once from the snapshot instead of re-derived per transaction.
type scope struct {
	// assets memoizes asset-to-SAC derivation; non-nil only when some
	// registration derives trustlines.
	assets *assetContracts
	// wantMovements is true when some registration derives movements, which
	// is the only derivation that inspects events from unwatched emitters.
	wantMovements bool
}

// Result carries the extracted records plus the distrust counters (rule 6:
// everything suppressed is counted, never silently dropped).
type Result struct {
	Events           []store.Event
	StateChanges     []store.StateChange
	Transfers        []store.Transfer
	TrustlineChanges []store.TrustlineChange
	Movements        []store.Movement
	// FailedTxs counts transactions skipped because they did not succeed;
	// their events and state changes never happened. Routine traffic.
	FailedTxs int
	// SuppressedTxs counts transactions whose meta could not be read or
	// panicked mid-decode. Should be zero; nonzero means lost data and is
	// exposed as a metric worth alerting on.
	SuppressedTxs int
	// SuppressedEvents counts individual records dropped because their XDR
	// could not be re-encoded. Same alarm semantics as SuppressedTxs.
	SuppressedEvents int
	// SuppressedTransfers counts events that named a token movement but did
	// not decode as one. The raw event row still lands, so nothing is lost —
	// but a nonzero value means the decoder no longer matches what the
	// network emits, and that is worth alerting on.
	SuppressedTransfers int
	// SuppressedTrustlines counts watched trustline changes that could not
	// be read. Same alarm semantics as SuppressedEvents.
	SuppressedTrustlines int
	// ForeignUndecodable counts movement-named events that did not decode,
	// emitted by token contracts whose transfers this instance does not
	// index. Expected to be nonzero on an open network — anyone can emit an
	// event called "transfer" carrying anything, aimed at any address — so
	// it is reported but never alerted on, which is what keeps the
	// suppression counters above meaningful. The flip side is real: a
	// genuinely non-standard token moving value into a watched contract
	// lands in this same bucket, and the endpoint documents that movements
	// are not a balance for exactly that reason.
	ForeignUndecodable int
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

	// What the registry makes worth doing is decided once per ledger, not
	// per transaction: when nothing derives trustlines or movements, those
	// passes cost nothing at all.
	sc := scope{wantMovements: watch.AnyKind(store.KindMovements)}
	if watch.AnyKind(store.KindTrustlines) {
		sc.assets = newAssetContracts(passphrase)
	}

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
		processTx(tx, seq, closedAt, watch, sc, &res)
	}
	return res, nil
}

// processTx appends one transaction's watched events and state changes to
// res. The recover frontier lives here: SDK XDR getters panic on nil
// unions, and one hostile transaction must cost exactly one transaction
// (counted), not the ledger.
func processTx(tx ingest.LedgerTransaction, seq uint32, closedAt time.Time, watch *registry.Snapshot, sc scope, res *Result) {
	defer func() {
		if r := recover(); r != nil {
			res.SuppressedTxs++
		}
	}()

	// The SDK streams failed transactions too; their events and state
	// changes describe things that did not happen (KNOWLEDGE.md P12).
	if !tx.Successful() {
		res.FailedTxs++
		return
	}
	eventsOK := txEvents(tx, seq, closedAt, watch, sc, res)
	stateOK := txLedgerChanges(tx, seq, closedAt, watch, sc.assets, res)
	// One broken transaction is one suppression, however many extractors
	// tripped over it.
	if !eventsOK || !stateOK {
		res.SuppressedTxs++
	}
}

// txEvents appends the transaction's watched contract events; ok=false
// means the meta could not be read.
func txEvents(tx ingest.LedgerTransaction, seq uint32, closedAt time.Time, watch *registry.Snapshot, sc scope, res *Result) bool {
	events, err := tx.GetTransactionEvents()
	if err != nil {
		return false
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
			if watched && contract.HasKind(store.KindEvents) {
				record, err := buildEvent(ev, contractID, seq, closedAt, txHash, int32(tx.Index), int32(opIndex), eventIndex, prefix)
				if err != nil {
					res.SuppressedEvents++
				} else {
					res.Events = append(res.Events, record)
				}
			}

			// Token movements are the one derivation NOT gated on the
			// emitter being watched: a payment into a watched contract is
			// emitted by the asset's own SAC, which the operator has no
			// reason to register. Everything below therefore runs for
			// foreign tokens too, so each guard is ordered cheapest-first.
			emitsTransfers := watched && contract.HasKind(store.KindTransfers)
			if !emitsTransfers && !sc.wantMovements {
				continue
			}
			body, ok := ev.Body.GetV0()
			if !ok || len(body.Topics) == 0 {
				continue
			}
			sym, ok := body.Topics[0].GetSym()
			if !ok {
				continue
			}
			transferType, isMovement := transferEventNames[string(sym)]
			if !isMovement {
				continue
			}
			wantMovements := sc.wantMovements && participatesWatched(body, watch)
			if !emitsTransfers && !wantMovements {
				continue
			}

			record, err := buildTransfer(body, transferType, contractID, seq, closedAt, txHash, int32(tx.Index), int32(opIndex), eventIndex, prefix)
			if err != nil {
				// A watched emitter failing to decode is counted data loss
				// and alerts; a foreign token doing so is a decoder that
				// does not know that token's dialect, which is routine on
				// an open network and must not mute the real alarm.
				if emitsTransfers {
					res.SuppressedTransfers++
				} else {
					res.ForeignUndecodable++
				}
				continue
			}
			if emitsTransfers {
				res.Transfers = append(res.Transfers, record)
			}
			if wantMovements {
				res.Movements = append(res.Movements, movementsOf(record, watch)...)
			}
		}
	}
	return true
}

// txLedgerChanges appends the transaction's watched contract-data changes
// and, when any registration wants them, its watched trustline changes.
// The change index counts every change in the transaction — before any
// filtering — so ids stay stable no matter what is derived.
//
// Removals here come from the ledger meta itself (an explicit REMOVED
// change), never derived from an absence, so the mass-absence plausibility
// guard (P12) does not apply on this path. ok=false means the meta could
// not be read.
func txLedgerChanges(tx ingest.LedgerTransaction, seq uint32, closedAt time.Time, watch *registry.Snapshot, assets *assetContracts, res *Result) bool {
	changes, err := tx.GetChanges()
	if err != nil {
		return false
	}
	txHash := tx.Result.TransactionHash.HexString()
	prefix := toid.New(int32(seq), int32(tx.Index), 0).ToInt64()

	for i, ch := range changes {
		switch ch.Type {
		case xdr.LedgerEntryTypeContractData:
			stateChange(ch, seq, closedAt, txHash, int32(tx.Index), int32(i), prefix, watch, res)
		case xdr.LedgerEntryTypeTrustline:
			if assets != nil {
				trustline(ch, seq, closedAt, txHash, int32(tx.Index), int32(i), prefix, watch, assets, res)
			}
		}
	}
	return true
}

// stateChange derives one watched contract-data change into res.
func stateChange(ch ingest.Change, seq uint32, closedAt time.Time, txHash string,
	txIndex, changeIndex int32, prefix int64, watch *registry.Snapshot, res *Result) {

	entry := ch.Post
	if entry == nil {
		entry = ch.Pre
	}
	if entry == nil {
		return // neither side present: nothing attributable
	}
	data, ok := entry.Data.GetContractData()
	if !ok || data.Contract.ContractId == nil {
		return
	}
	contractID, err := strkey.Encode(strkey.VersionByteContract, data.Contract.ContractId[:])
	if err != nil {
		res.SuppressedEvents++
		return
	}
	contract, watched := watch.Get(contractID)
	if !watched || !contract.HasKind(store.KindState) {
		return
	}
	record, err := buildStateChange(ch, data, contractID, seq, closedAt, txHash, txIndex, changeIndex, prefix)
	if err != nil {
		res.SuppressedEvents++
		return
	}
	res.StateChanges = append(res.StateChanges, record)
}

// trustline derives one classic trustline change when the SAC wrapping its
// asset is watched with the trustlines kind. Pool-share trustlines have no
// SAC and are skipped.
func trustline(ch ingest.Change, seq uint32, closedAt time.Time, txHash string,
	txIndex, changeIndex int32, prefix int64, watch *registry.Snapshot, assets *assetContracts, res *Result) {

	entry := ch.Post
	if entry == nil {
		entry = ch.Pre
	}
	if entry == nil {
		return
	}
	tl, ok := entry.Data.GetTrustLine()
	if !ok || tl.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
		return
	}
	asset := tl.Asset.ToAsset()
	contractID, err := assets.contractIDFor(asset)
	if err != nil {
		res.SuppressedTrustlines++
		return
	}
	contract, watched := watch.Get(contractID)
	if !watched || !contract.HasKind(store.KindTrustlines) {
		return
	}
	record, err := buildTrustlineChange(ch, tl, contractID, asset.StringCanonical(),
		seq, closedAt, txHash, txIndex, changeIndex, prefix)
	if err != nil {
		res.SuppressedTrustlines++
		return
	}
	res.TrustlineChanges = append(res.TrustlineChanges, record)
}

// buildStateChange renders one ledger-entry change into a history row.
func buildStateChange(ch ingest.Change, data xdr.ContractDataEntry, contractID string,
	seq uint32, closedAt time.Time, txHash string, txIndex, changeIndex int32, prefix int64) (store.StateChange, error) {

	keyXDR, err := xdr.MarshalBase64(data.Key)
	if err != nil {
		return store.StateChange{}, fmt.Errorf("extract: encode state key: %w", err)
	}

	var changeType string
	switch ch.ChangeType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		changeType = "created"
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		changeType = "updated"
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		changeType = "removed"
	case xdr.LedgerEntryChangeTypeLedgerEntryRestored:
		changeType = "restored"
	default:
		return store.StateChange{}, fmt.Errorf("extract: unsupported change type %d", ch.ChangeType)
	}

	valueOf := func(e *xdr.LedgerEntry) (string, error) {
		if e == nil {
			return "", nil
		}
		d, ok := e.Data.GetContractData()
		if !ok {
			return "", fmt.Errorf("extract: change side lost its contract data")
		}
		return xdr.MarshalBase64(d.Val)
	}
	pre, err := valueOf(ch.Pre)
	if err != nil {
		return store.StateChange{}, err
	}
	post, err := valueOf(ch.Post)
	if err != nil {
		return store.StateChange{}, err
	}

	durability := "persistent"
	if data.Durability == xdr.ContractDataDurabilityTemporary {
		durability = "temporary"
	}

	return store.StateChange{
		ID:             fmt.Sprintf("%019d-%010d", prefix, changeIndex),
		ContractID:     contractID,
		LedgerSequence: seq,
		ClosedAt:       closedAt,
		TxHash:         txHash,
		TxIndex:        txIndex,
		OpIndex:        int32(ch.OperationIndex),
		ChangeIndex:    changeIndex,
		ChangeType:     changeType,
		KeyXDR:         keyXDR,
		Durability:     durability,
		PreXDR:         pre,
		PostXDR:        post,
	}, nil
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
