package extract

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

const tlIssuer = "GA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVSGZ"
const tlHolder = "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"

// tlAsset is the credit asset every trustline test observes.
func tlAsset(t *testing.T) xdr.Asset {
	t.Helper()
	asset, err := xdr.NewCreditAsset("USDA", tlIssuer)
	if err != nil {
		t.Fatalf("credit asset: %v", err)
	}
	return asset
}

// tlSACID derives the SAC contract address wrapping tlAsset on testnet.
func tlSACID(t *testing.T) string {
	t.Helper()
	raw, err := tlAsset(t).ContractID(passphrase)
	if err != nil {
		t.Fatalf("asset contract id: %v", err)
	}
	id, err := strkey.Encode(strkey.VersionByteContract, raw[:])
	if err != nil {
		t.Fatalf("encode contract id: %v", err)
	}
	return id
}

func trustlineEntry(t *testing.T, holder string, balance, limit int64, flags uint32) xdr.LedgerEntry {
	t.Helper()
	var account xdr.AccountId
	if err := account.SetAddress(holder); err != nil {
		t.Fatalf("holder address: %v", err)
	}
	tla := tlAsset(t).ToTrustLineAsset()
	return xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: account,
				Asset:     tla,
				Balance:   xdr.Int64(balance),
				Limit:     xdr.Int64(limit),
				Flags:     xdr.Uint32(flags),
			},
		},
	}
}

func trustlineRemoved(t *testing.T, pre xdr.LedgerEntry) []xdr.LedgerEntryChange {
	t.Helper()
	tl, ok := pre.Data.GetTrustLine()
	if !ok {
		t.Fatal("pre entry is not a trustline")
	}
	key := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeTrustline,
		TrustLine: &xdr.LedgerKeyTrustLine{
			AccountId: tl.AccountId,
			Asset:     tl.Asset,
		},
	}
	return []xdr.LedgerEntryChange{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: &pre},
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &key},
	}
}

func runChanges(t *testing.T, kinds []string, watchedContract string, changes []xdr.LedgerEntryChange) Result {
	t.Helper()
	snap := watchingKinds(t, watchedContract, kinds...)
	lcm := buildLCM(t, fixtureTx{
		envelope: sorobanEnvelope(t, 1),
		success:  true,
		meta:     metaV3WithChanges(changes),
	})
	res, err := Events(lcm, passphrase, snap)
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	return res
}

func TestTrustlineCreatedExtracted(t *testing.T) {
	sac := tlSACID(t)
	res := runChanges(t, []string{store.KindTrustlines}, sac,
		changeCreated(trustlineEntry(t, tlHolder, 500, 1000, 1)))
	if len(res.TrustlineChanges) != 1 {
		t.Fatalf("TrustlineChanges = %d, want 1 (suppressed %d)", len(res.TrustlineChanges), res.SuppressedTrustlines)
	}
	c := res.TrustlineChanges[0]
	if c.ContractID != sac || c.AccountID != tlHolder || c.Asset != "USDA:"+tlIssuer {
		t.Errorf("attribution = %+v", c)
	}
	if c.ChangeType != "created" || c.PreBalance != nil || c.PostBalance == nil || *c.PostBalance != 500 {
		t.Errorf("balances = %+v", c)
	}
	if c.PostLimit == nil || *c.PostLimit != 1000 || c.Flags != 1 {
		t.Errorf("limit/flags = %+v", c)
	}
}

func TestTrustlineUpdatedCarriesBothSides(t *testing.T) {
	sac := tlSACID(t)
	res := runChanges(t, []string{store.KindTrustlines}, sac,
		changeUpdated(trustlineEntry(t, tlHolder, 500, 1000, 1), trustlineEntry(t, tlHolder, 750, 1000, 1)))
	if len(res.TrustlineChanges) != 1 {
		t.Fatalf("TrustlineChanges = %d, want 1", len(res.TrustlineChanges))
	}
	c := res.TrustlineChanges[0]
	if c.ChangeType != "updated" || *c.PreBalance != 500 || *c.PostBalance != 750 {
		t.Errorf("change = %+v", c)
	}
}

func TestTrustlineRemovedKeepsPreSide(t *testing.T) {
	sac := tlSACID(t)
	res := runChanges(t, []string{store.KindTrustlines}, sac,
		trustlineRemoved(t, trustlineEntry(t, tlHolder, 0, 1000, 1)))
	if len(res.TrustlineChanges) != 1 {
		t.Fatalf("TrustlineChanges = %d, want 1", len(res.TrustlineChanges))
	}
	c := res.TrustlineChanges[0]
	if c.ChangeType != "removed" || c.PostBalance != nil || c.PreBalance == nil || c.Flags != 1 {
		t.Errorf("change = %+v", c)
	}
}

func TestTrustlineIgnoredWithoutKind(t *testing.T) {
	sac := tlSACID(t)
	res := runChanges(t, []string{store.KindEvents, store.KindState}, sac,
		changeCreated(trustlineEntry(t, tlHolder, 500, 1000, 1)))
	if len(res.TrustlineChanges) != 0 || res.SuppressedTrustlines != 0 {
		t.Errorf("changes = %d suppressed = %d, want 0/0",
			len(res.TrustlineChanges), res.SuppressedTrustlines)
	}
}

func TestTrustlineOfUnwatchedAssetIgnored(t *testing.T) {
	// Watch a different contract with the trustlines kind: the USDA change
	// must not be attributed to it.
	res := runChanges(t, []string{store.KindTrustlines}, watchedID,
		changeCreated(trustlineEntry(t, tlHolder, 500, 1000, 1)))
	if len(res.TrustlineChanges) != 0 || res.SuppressedTrustlines != 0 {
		t.Errorf("changes = %d suppressed = %d, want 0/0",
			len(res.TrustlineChanges), res.SuppressedTrustlines)
	}
}
