package extract

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// transferEventNames maps a topic0 symbol to the stored transfer type. Any
// event outside this set is simply not a token movement — skipping it is
// routine, never a suppression.
var transferEventNames = map[string]string{
	"transfer": store.TransferTypeTransfer,
	"mint":     store.TransferTypeMint,
	"burn":     store.TransferTypeBurn,
	"clawback": store.TransferTypeClawback,
}

// buildTransfer decodes one SEP-41 token event into a transfer row. It
// accepts both topic generations the network has emitted — the classic SAC
// shapes (admin present on mint/clawback, trailing SEP-0011 asset string)
// and the CAP-67 unified shapes (map-valued data carrying amount plus an
// optional destination muxed id) — by mapping address positions per type
// instead of hardcoding one layout. An event that names a movement but does
// not decode as one is hostile or unknown data: the caller counts it as a
// suppressed transfer (CLAUDE.md rule 6) and the raw event row still lands.
func buildTransfer(body xdr.ContractEventV0, transferType, contractID string,
	seq uint32, closedAt time.Time, txHash string, txIndex, opIndex, eventIndex int32, prefix int64) (store.Transfer, error) {

	// Addresses in topic order, plus the trailing SEP-0011 asset when the
	// emitter is a SAC. Any other topic type past topic0 is unexpected.
	var addrs []string
	var asset string
	for i, t := range body.Topics[1:] {
		switch t.Type {
		case xdr.ScValTypeScvAddress:
			s, err := t.MustAddress().String()
			if err != nil {
				return store.Transfer{}, fmt.Errorf("extract: transfer topic %d address: %w", i+1, err)
			}
			addrs = append(addrs, s)
		case xdr.ScValTypeScvString:
			asset = string(t.MustStr())
		default:
			return store.Transfer{}, fmt.Errorf("extract: transfer topic %d has type %v", i+1, t.Type)
		}
	}

	var from, to string
	switch transferType {
	case store.TransferTypeTransfer:
		if len(addrs) != 2 {
			return store.Transfer{}, fmt.Errorf("extract: transfer event with %d addresses", len(addrs))
		}
		from, to = addrs[0], addrs[1]
	case store.TransferTypeMint:
		// Classic SAC: [mint, admin, to]; unified: [mint, to].
		if len(addrs) == 0 {
			return store.Transfer{}, fmt.Errorf("extract: mint event with no addresses")
		}
		to = addrs[len(addrs)-1]
	case store.TransferTypeBurn:
		// Both generations: [burn, from].
		if len(addrs) == 0 {
			return store.Transfer{}, fmt.Errorf("extract: burn event with no addresses")
		}
		from = addrs[0]
	case store.TransferTypeClawback:
		// Classic SAC: [clawback, admin, from]; unified: [clawback, from].
		if len(addrs) == 0 {
			return store.Transfer{}, fmt.Errorf("extract: clawback event with no addresses")
		}
		from = addrs[len(addrs)-1]
	}

	amt, muxedID, err := decodeTransferData(body.Data)
	if err != nil {
		return store.Transfer{}, err
	}

	return store.Transfer{
		ID:             fmt.Sprintf("%019d-%010d", prefix, eventIndex),
		ContractID:     contractID,
		LedgerSequence: seq,
		ClosedAt:       closedAt,
		TxHash:         txHash,
		TxIndex:        txIndex,
		OpIndex:        opIndex,
		EventIndex:     eventIndex,
		TransferType:   transferType,
		FromAddress:    from,
		ToAddress:      to,
		ToMuxedID:      muxedID,
		Amount:         amt,
		Asset:          asset,
	}, nil
}

// decodeTransferData reads the event value: a bare i128 amount (classic) or
// a CAP-67 map holding amount plus an optional destination muxed id.
func decodeTransferData(v xdr.ScVal) (amt, muxedID string, err error) {
	switch v.Type {
	case xdr.ScValTypeScvI128:
		return i128Amount(v.MustI128())
	case xdr.ScValTypeScvMap:
		if v.Map == nil || *v.Map == nil {
			return "", "", fmt.Errorf("extract: transfer data map is empty")
		}
		for _, entry := range **v.Map {
			key, ok := mapKey(entry.Key)
			if !ok {
				return "", "", fmt.Errorf("extract: transfer data map key has type %v", entry.Key.Type)
			}
			switch key {
			case "amount":
				if entry.Val.Type != xdr.ScValTypeScvI128 {
					return "", "", fmt.Errorf("extract: transfer amount has type %v", entry.Val.Type)
				}
				if amt, _, err = i128Amount(entry.Val.MustI128()); err != nil {
					return "", "", err
				}
			case "to_muxed_id":
				if muxedID, err = muxedIDString(entry.Val); err != nil {
					return "", "", err
				}
			default:
				return "", "", fmt.Errorf("extract: transfer data map has unknown key %q", key)
			}
		}
		if amt == "" {
			return "", "", fmt.Errorf("extract: transfer data map has no amount")
		}
		return amt, muxedID, nil
	default:
		return "", "", fmt.Errorf("extract: transfer data has type %v", v.Type)
	}
}

// i128Amount renders a token amount in raw units. A negative amount is not a
// movement any token protocol emits: hostile data, suppressed.
func i128Amount(parts xdr.Int128Parts) (string, string, error) {
	s := amount.String128Raw(parts)
	if strings.HasPrefix(s, "-") {
		return "", "", fmt.Errorf("extract: negative transfer amount %s", s)
	}
	return s, "", nil
}

// mapKey reads a CAP-67 map key, which the protocol writes as a symbol but
// tolerant decoding also accepts as a string.
func mapKey(v xdr.ScVal) (string, bool) {
	switch v.Type {
	case xdr.ScValTypeScvSymbol:
		return string(v.MustSym()), true
	case xdr.ScValTypeScvString:
		return string(v.MustStr()), true
	default:
		return "", false
	}
}

// muxedIDString renders the CAP-67 destination muxed id: u64 as decimal,
// bytes as hex, string verbatim.
func muxedIDString(v xdr.ScVal) (string, error) {
	switch v.Type {
	case xdr.ScValTypeScvU64:
		return fmt.Sprintf("%d", uint64(v.MustU64())), nil
	case xdr.ScValTypeScvBytes:
		return hex.EncodeToString(v.MustBytes()), nil
	case xdr.ScValTypeScvString:
		return string(v.MustStr()), nil
	default:
		return "", fmt.Errorf("extract: to_muxed_id has type %v", v.Type)
	}
}
