package extract

import (
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// assetContracts memoizes asset → SAC contract id within one ledger, so a
// payment-heavy ledger hashes each distinct asset once instead of once per
// trustline change.
type assetContracts struct {
	passphrase string
	byAsset    map[string]string
}

func newAssetContracts(passphrase string) *assetContracts {
	return &assetContracts{passphrase: passphrase, byAsset: map[string]string{}}
}

// contractIDFor derives the SAC contract address wrapping the asset.
func (a *assetContracts) contractIDFor(asset xdr.Asset) (string, error) {
	key := asset.StringCanonical()
	if id, ok := a.byAsset[key]; ok {
		return id, nil
	}
	raw, err := asset.ContractID(a.passphrase)
	if err != nil {
		return "", fmt.Errorf("extract: derive contract id for asset %s: %w", key, err)
	}
	id, err := strkey.Encode(strkey.VersionByteContract, raw[:])
	if err != nil {
		return "", fmt.Errorf("extract: encode contract id for asset %s: %w", key, err)
	}
	a.byAsset[key] = id
	return id, nil
}

// buildTrustlineChange renders one trustline entry change into a history
// row attributed to contractID.
func buildTrustlineChange(ch ingest.Change, tl xdr.TrustLineEntry, contractID, asset string,
	seq uint32, closedAt time.Time, txHash string, txIndex, changeIndex int32, prefix int64) (store.TrustlineChange, error) {

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
		return store.TrustlineChange{}, fmt.Errorf("extract: unsupported trustline change type %d", ch.ChangeType)
	}

	account, err := tl.AccountId.GetAddress()
	if err != nil {
		return store.TrustlineChange{}, fmt.Errorf("extract: trustline account: %w", err)
	}

	side := func(e *xdr.LedgerEntry) (balance, limit *int64, flags uint32, ok bool) {
		if e == nil {
			return nil, nil, 0, false
		}
		t, found := e.Data.GetTrustLine()
		if !found {
			return nil, nil, 0, false
		}
		b, l := int64(t.Balance), int64(t.Limit)
		return &b, &l, uint32(t.Flags), true
	}
	preBalance, preLimit, preFlags, hasPre := side(ch.Pre)
	postBalance, postLimit, postFlags, hasPost := side(ch.Post)
	if !hasPre && !hasPost {
		return store.TrustlineChange{}, fmt.Errorf("extract: trustline change with no readable side")
	}
	flags := postFlags
	if !hasPost {
		flags = preFlags
	}

	return store.TrustlineChange{
		ID:             fmt.Sprintf("%019d-%010d", prefix, changeIndex),
		ContractID:     contractID,
		AccountID:      account,
		Asset:          asset,
		LedgerSequence: seq,
		ClosedAt:       closedAt,
		TxHash:         txHash,
		TxIndex:        txIndex,
		OpIndex:        int32(ch.OperationIndex),
		ChangeIndex:    changeIndex,
		ChangeType:     changeType,
		PreBalance:     preBalance,
		PostBalance:    postBalance,
		PreLimit:       preLimit,
		PostLimit:      postLimit,
		Flags:          flags,
	}, nil
}
