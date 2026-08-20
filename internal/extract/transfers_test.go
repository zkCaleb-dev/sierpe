package extract

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

const (
	fromG = "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	toG   = "GA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVSGZ"
	asset = "native"
)

// --- ScVal builders ----------------------------------------------------------

func accountAddr(t *testing.T, g string) xdr.ScVal {
	t.Helper()
	var aid xdr.AccountId
	if err := aid.SetAddress(g); err != nil {
		t.Fatalf("set address %s: %v", g, err)
	}
	addr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &aid}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &addr}
}

func str(s string) xdr.ScVal {
	v := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &v}
}

func i128(hi int64, lo uint64) xdr.ScVal {
	parts := xdr.Int128Parts{Hi: xdr.Int64(hi), Lo: xdr.Uint64(lo)}
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &parts}
}

func u64(v uint64) xdr.ScVal {
	u := xdr.Uint64(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
}

func scMap(t *testing.T, pairs ...[2]xdr.ScVal) xdr.ScVal {
	t.Helper()
	m := make(xdr.ScMap, 0, len(pairs))
	for _, p := range pairs {
		m = append(m, xdr.ScMapEntry{Key: p[0], Val: p[1]})
	}
	pm := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &pm}
}

// tokenEvent is contractEvent with caller-controlled data.
func tokenEvent(t *testing.T, contract string, data xdr.ScVal, topics ...xdr.ScVal) xdr.ContractEvent {
	t.Helper()
	ev := contractEvent(t, contract, topics...)
	ev.Body.V0.Data = data
	return ev
}

func extractTransfers(t *testing.T, snap *registry.Snapshot, events ...xdr.ContractEvent) Result {
	t.Helper()
	lcm := buildLCM(t, fixtureTx{
		envelope: sorobanEnvelope(t, 1),
		success:  true,
		meta:     metaV3WithEvents(events),
	})
	res, err := Events(lcm, passphrase, snap)
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	return res
}

// --- tests -------------------------------------------------------------------

func TestTransfersClassicSACShape(t *testing.T) {
	snap := watchingKinds(t, watchedID, store.KindTransfers)
	res := extractTransfers(t, snap,
		tokenEvent(t, watchedID, i128(0, 5000),
			symbol("transfer"), accountAddr(t, fromG), accountAddr(t, toG), str(asset)),
	)
	if len(res.Transfers) != 1 {
		t.Fatalf("Transfers = %d, want 1", len(res.Transfers))
	}
	tr := res.Transfers[0]
	if tr.TransferType != store.TransferTypeTransfer {
		t.Errorf("TransferType = %q", tr.TransferType)
	}
	if tr.FromAddress != fromG || tr.ToAddress != toG {
		t.Errorf("from/to = %q/%q", tr.FromAddress, tr.ToAddress)
	}
	if tr.Amount != "5000" {
		t.Errorf("Amount = %q, want 5000", tr.Amount)
	}
	if tr.Asset != asset {
		t.Errorf("Asset = %q, want %q", tr.Asset, asset)
	}
	if tr.ContractID != watchedID {
		t.Errorf("ContractID = %q", tr.ContractID)
	}
}

func TestTransfersCAP67MapWithMuxedID(t *testing.T) {
	snap := watchingKinds(t, watchedID, store.KindTransfers)
	data := scMap(t,
		[2]xdr.ScVal{symbol("amount"), i128(0, 42)},
		[2]xdr.ScVal{symbol("to_muxed_id"), u64(777)},
	)
	res := extractTransfers(t, snap,
		tokenEvent(t, watchedID, data,
			symbol("transfer"), accountAddr(t, fromG), accountAddr(t, toG), str(asset)),
	)
	if len(res.Transfers) != 1 {
		t.Fatalf("Transfers = %d, want 1 (suppressed %d)", len(res.Transfers), res.SuppressedTransfers)
	}
	tr := res.Transfers[0]
	if tr.Amount != "42" {
		t.Errorf("Amount = %q, want 42", tr.Amount)
	}
	if tr.ToMuxedID != "777" {
		t.Errorf("ToMuxedID = %q, want 777", tr.ToMuxedID)
	}
}

func TestTransfersLargeAmountIsExact(t *testing.T) {
	snap := watchingKinds(t, watchedID, store.KindTransfers)
	// hi=1, lo=0 → 2^64 = 18446744073709551616.
	res := extractTransfers(t, snap,
		tokenEvent(t, watchedID, i128(1, 0),
			symbol("transfer"), accountAddr(t, fromG), accountAddr(t, toG)),
	)
	if len(res.Transfers) != 1 {
		t.Fatalf("Transfers = %d, want 1", len(res.Transfers))
	}
	if got := res.Transfers[0].Amount; got != "18446744073709551616" {
		t.Errorf("Amount = %q, want 18446744073709551616", got)
	}
}

func TestTransfersAddressPositionsPerType(t *testing.T) {
	snap := watchingKinds(t, watchedID, store.KindTransfers)
	cases := []struct {
		name     string
		topics   []xdr.ScVal
		wantFrom string
		wantTo   string
	}{
		{"mint classic admin+to", []xdr.ScVal{symbol("mint"), accountAddr(t, fromG), accountAddr(t, toG), str(asset)}, "", toG},
		{"mint unified to only", []xdr.ScVal{symbol("mint"), accountAddr(t, toG), str(asset)}, "", toG},
		{"burn", []xdr.ScVal{symbol("burn"), accountAddr(t, fromG), str(asset)}, fromG, ""},
		{"clawback classic admin+from", []xdr.ScVal{symbol("clawback"), accountAddr(t, fromG), accountAddr(t, toG), str(asset)}, toG, ""},
		{"clawback unified from only", []xdr.ScVal{symbol("clawback"), accountAddr(t, toG), str(asset)}, toG, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := extractTransfers(t, snap, tokenEvent(t, watchedID, i128(0, 9), tc.topics...))
			if len(res.Transfers) != 1 {
				t.Fatalf("Transfers = %d, want 1 (suppressed %d)", len(res.Transfers), res.SuppressedTransfers)
			}
			tr := res.Transfers[0]
			if tr.FromAddress != tc.wantFrom || tr.ToAddress != tc.wantTo {
				t.Errorf("from/to = %q/%q, want %q/%q", tr.FromAddress, tr.ToAddress, tc.wantFrom, tc.wantTo)
			}
		})
	}
}

func TestTransfersHostileDataIsSuppressedNotStored(t *testing.T) {
	snap := watchingKinds(t, watchedID, store.KindEvents, store.KindTransfers)
	cases := []struct {
		name  string
		event xdr.ContractEvent
	}{
		{"negative amount", tokenEvent(t, watchedID, i128(-1, 0),
			symbol("transfer"), accountAddr(t, fromG), accountAddr(t, toG))},
		{"transfer with one address", tokenEvent(t, watchedID, i128(0, 5),
			symbol("transfer"), accountAddr(t, fromG))},
		{"mint with no addresses", tokenEvent(t, watchedID, i128(0, 5), symbol("mint"))},
		{"map without amount", tokenEvent(t, watchedID,
			scMap(t, [2]xdr.ScVal{symbol("to_muxed_id"), u64(1)}),
			symbol("transfer"), accountAddr(t, fromG), accountAddr(t, toG))},
		{"map with unknown key", tokenEvent(t, watchedID,
			scMap(t,
				[2]xdr.ScVal{symbol("amount"), i128(0, 5)},
				[2]xdr.ScVal{symbol("mystery"), u64(1)}),
			symbol("transfer"), accountAddr(t, fromG), accountAddr(t, toG))},
		{"non-scval-amount data", tokenEvent(t, watchedID, str("nope"),
			symbol("transfer"), accountAddr(t, fromG), accountAddr(t, toG))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := extractTransfers(t, snap, tc.event)
			if len(res.Transfers) != 0 {
				t.Errorf("Transfers = %d, want 0", len(res.Transfers))
			}
			if res.SuppressedTransfers != 1 {
				t.Errorf("SuppressedTransfers = %d, want 1", res.SuppressedTransfers)
			}
			// The raw event is never a casualty of a failed derivation.
			if len(res.Events) != 1 {
				t.Errorf("Events = %d, want 1", len(res.Events))
			}
		})
	}
}

func TestTransfersNonMovementEventIsRoutine(t *testing.T) {
	snap := watchingKinds(t, watchedID, store.KindTransfers)
	res := extractTransfers(t, snap,
		tokenEvent(t, watchedID, i128(0, 5), symbol("approve"), accountAddr(t, fromG)),
	)
	if len(res.Transfers) != 0 || res.SuppressedTransfers != 0 {
		t.Errorf("Transfers = %d, SuppressedTransfers = %d, want 0/0",
			len(res.Transfers), res.SuppressedTransfers)
	}
}

func TestTransfersRespectKindGating(t *testing.T) {
	ev := func() xdr.ContractEvent {
		return tokenEvent(t, watchedID, i128(0, 5),
			symbol("transfer"), accountAddr(t, fromG), accountAddr(t, toG))
	}

	eventsOnly := extractTransfers(t, watchingKinds(t, watchedID, store.KindEvents), ev())
	if len(eventsOnly.Transfers) != 0 || len(eventsOnly.Events) != 1 {
		t.Errorf("events-only: transfers %d events %d, want 0/1",
			len(eventsOnly.Transfers), len(eventsOnly.Events))
	}

	transfersOnly := extractTransfers(t, watchingKinds(t, watchedID, store.KindTransfers), ev())
	if len(transfersOnly.Transfers) != 1 || len(transfersOnly.Events) != 0 {
		t.Errorf("transfers-only: transfers %d events %d, want 1/0",
			len(transfersOnly.Transfers), len(transfersOnly.Events))
	}
}

func TestTransfersShareEventIdentity(t *testing.T) {
	snap := watchingKinds(t, watchedID, store.KindEvents, store.KindTransfers)
	res := extractTransfers(t, snap,
		tokenEvent(t, watchedID, i128(0, 5),
			symbol("transfer"), accountAddr(t, fromG), accountAddr(t, toG)),
	)
	if len(res.Events) != 1 || len(res.Transfers) != 1 {
		t.Fatalf("events %d transfers %d, want 1/1", len(res.Events), len(res.Transfers))
	}
	if res.Events[0].ID != res.Transfers[0].ID {
		t.Errorf("ids diverge: event %q transfer %q", res.Events[0].ID, res.Transfers[0].ID)
	}
}
