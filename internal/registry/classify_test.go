package registry

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func decodeB64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

func contractScAddress(t *testing.T, contractID string) xdr.ScAddress {
	t.Helper()
	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		t.Fatalf("decode contract id: %v", err)
	}
	var cid xdr.ContractId
	copy(cid[:], raw)
	return xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &cid,
	}
}

const testContract = "CBF64DEOVQAXJFBSNGFEUT2AH4H7K5JBY3ZYJ5GVEINMNSDISWRG5N3F"

// fakeEntries serves canned ledger entries keyed by base64 LedgerKey.
type fakeEntries struct {
	byKey map[string]string
	err   error
}

func (f *fakeEntries) GetLedgerEntry(_ context.Context, keyB64 string) (string, bool, error) {
	if f.err != nil {
		return "", false, f.err
	}
	xdrB64, ok := f.byKey[keyB64]
	return xdrB64, ok, nil
}

// --- fixture builders: real XDR, synthetic wasm ---

func instanceEntryB64(t *testing.T, executable xdr.ContractExecutable) string {
	t.Helper()
	data := xdr.LedgerEntryData{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.ContractDataEntry{
			Contract: contractScAddress(t, testContract),
			Key:      xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
			Val: xdr.ScVal{
				Type:     xdr.ScValTypeScvContractInstance,
				Instance: &xdr.ScContractInstance{Executable: executable},
			},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}
	out, err := xdr.MarshalBase64(data)
	if err != nil {
		t.Fatalf("marshal instance entry: %v", err)
	}
	return out
}

func codeEntryB64(t *testing.T, hash xdr.Hash, wasm []byte) string {
	t.Helper()
	data := xdr.LedgerEntryData{
		Type:         xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.ContractCodeEntry{Hash: hash, Code: wasm},
	}
	out, err := xdr.MarshalBase64(data)
	if err != nil {
		t.Fatalf("marshal code entry: %v", err)
	}
	return out
}

// buildWasm wraps a payload in a minimal valid wasm module with one custom
// section carrying the given name.
func buildWasm(t *testing.T, sectionName string, payload []byte) []byte {
	t.Helper()
	section := binary.AppendUvarint(nil, uint64(len(sectionName)))
	section = append(section, sectionName...)
	section = append(section, payload...)

	wasm := append([]byte{}, wasmMagic...)
	wasm = append(wasm, 1, 0, 0, 0) // version
	wasm = append(wasm, 0)          // custom section id
	wasm = binary.AppendUvarint(wasm, uint64(len(section)))
	return append(wasm, section...)
}

func specEntryBytes(t *testing.T, entries ...xdr.ScSpecEntry) []byte {
	t.Helper()
	var out []byte
	for _, e := range entries {
		b64, err := xdr.MarshalBase64(e)
		if err != nil {
			t.Fatalf("marshal spec entry: %v", err)
		}
		raw, err := decodeB64(b64)
		if err != nil {
			t.Fatalf("decode spec entry: %v", err)
		}
		out = append(out, raw...)
	}
	return out
}

func eventEntry(name string) xdr.ScSpecEntry {
	return xdr.ScSpecEntry{
		Kind:    xdr.ScSpecEntryKindScSpecEntryEventV0,
		EventV0: &xdr.ScSpecEventV0{Name: xdr.ScSymbol(name)},
	}
}

func functionEntry(name string) xdr.ScSpecEntry {
	return xdr.ScSpecEntry{
		Kind:       xdr.ScSpecEntryKindScSpecEntryFunctionV0,
		FunctionV0: &xdr.ScSpecFunctionV0{Name: xdr.ScSymbol(name)},
	}
}

// wasmFixture wires instance + code entries for a wasm contract whose code
// is the given bytes, and returns the ready fake.
func wasmFixture(t *testing.T, wasm []byte) *fakeEntries {
	t.Helper()
	var hash xdr.Hash
	hash[0] = 0xAB
	instKey, err := instanceLedgerKeyB64(testContract)
	if err != nil {
		t.Fatalf("instance key: %v", err)
	}
	codeKey, err := codeLedgerKeyB64(hash)
	if err != nil {
		t.Fatalf("code key: %v", err)
	}
	return &fakeEntries{byKey: map[string]string{
		instKey: instanceEntryB64(t, xdr.ContractExecutable{
			Type:     xdr.ContractExecutableTypeContractExecutableWasm,
			WasmHash: &hash,
		}),
		codeKey: codeEntryB64(t, hash, wasm),
	}}
}

// --- tests ---

func TestClassifySACByExecutable(t *testing.T) {
	instKey, err := instanceLedgerKeyB64(testContract)
	if err != nil {
		t.Fatalf("instance key: %v", err)
	}
	fake := &fakeEntries{byKey: map[string]string{
		instKey: instanceEntryB64(t, xdr.ContractExecutable{
			Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
		}),
	}}
	cls, err := NewClassifier(fake).Classify(context.Background(), testContract)
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}
	if cls.Type != TypeSAC || cls.Method != MethodSACBuiltin {
		t.Errorf("cls = %+v, want sac/sac_builtin", cls)
	}
	if !reflect.DeepEqual(cls.Events, sacEvents) {
		t.Errorf("events = %v, want the protocol SAC set", cls.Events)
	}
}

func TestClassifyWasmSpecEvents(t *testing.T) {
	spec := specEntryBytes(t,
		eventEntry("Transfer"),
		eventEntry("FeeUpdated"),
		functionEntry("mint"),
	)
	fake := wasmFixture(t, buildWasm(t, "contractspecv0", spec))

	cls, err := NewClassifier(fake).Classify(context.Background(), testContract)
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}
	if cls.Type != TypeWasm || cls.Method != MethodSpecEvents {
		t.Errorf("cls = %+v, want wasm/spec_events", cls)
	}
	if !reflect.DeepEqual(cls.Events, []string{"transfer", "fee_updated"}) {
		t.Errorf("events = %v, want declared events normalized", cls.Events)
	}
	if cls.WasmHash == "" {
		t.Error("wasm classification must carry the code hash")
	}
}

func TestClassifyWasmFunctionFallback(t *testing.T) {
	spec := specEntryBytes(t, functionEntry("transfer"), functionEntry("set_admin"))
	fake := wasmFixture(t, buildWasm(t, "contractspecv0", spec))

	cls, err := NewClassifier(fake).Classify(context.Background(), testContract)
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}
	if cls.Method != MethodFunctionNames {
		t.Errorf("method = %s, want function_names", cls.Method)
	}
	if !reflect.DeepEqual(cls.Events, []string{"transfer", "set_admin"}) {
		t.Errorf("events = %v", cls.Events)
	}
}

func TestClassifyWasmDegradesToOpaque(t *testing.T) {
	cases := []struct {
		name string
		wasm []byte
	}{
		{"not wasm at all", []byte("definitely not wasm")},
		{"no spec section", buildWasm(t, "othersection", []byte{1, 2, 3})},
		{"corrupt spec stream", buildWasm(t, "contractspecv0", []byte{0xFF, 0xFF, 0xFF})},
		{"truncated wasm", []byte{0x00, 0x61, 0x73, 0x6d, 1, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := wasmFixture(t, tc.wasm)
			cls, err := NewClassifier(fake).Classify(context.Background(), testContract)
			if err != nil {
				t.Fatalf("Classify() must degrade, not fail: %v", err)
			}
			if cls.Type != TypeWasm || cls.Method != MethodOpaque {
				t.Errorf("cls = %+v, want wasm/opaque", cls)
			}
			if len(cls.Events) != 0 {
				t.Errorf("opaque classification must declare no events, got %v", cls.Events)
			}
		})
	}
}

func TestClassifyMissingCodeEntryIsOpaque(t *testing.T) {
	fake := wasmFixture(t, nil)
	// Remove the code entry: the instance points at code the source no
	// longer serves.
	for k, v := range fake.byKey {
		var data xdr.LedgerEntryData
		if err := xdr.SafeUnmarshalBase64(v, &data); err == nil && data.Type == xdr.LedgerEntryTypeContractCode {
			delete(fake.byKey, k)
		}
	}
	cls, err := NewClassifier(fake).Classify(context.Background(), testContract)
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}
	if cls.Type != TypeWasm || cls.Method != MethodOpaque {
		t.Errorf("cls = %+v, want wasm/opaque", cls)
	}
}

func TestClassifyContractNotFound(t *testing.T) {
	fake := &fakeEntries{byKey: map[string]string{}}
	_, err := NewClassifier(fake).Classify(context.Background(), testContract)
	if !errors.Is(err, ErrContractNotFound) {
		t.Errorf("err = %v, want ErrContractNotFound", err)
	}
}

func TestClassifySourceErrorPropagates(t *testing.T) {
	fake := &fakeEntries{err: errors.New("rpc down")}
	_, err := NewClassifier(fake).Classify(context.Background(), testContract)
	if err == nil || errors.Is(err, ErrContractNotFound) {
		t.Errorf("err = %v, want a transport error", err)
	}
}

func TestClassifyMalformedInstanceEntry(t *testing.T) {
	instKey, err := instanceLedgerKeyB64(testContract)
	if err != nil {
		t.Fatalf("instance key: %v", err)
	}
	fake := &fakeEntries{byKey: map[string]string{instKey: "bm90IHhkcg=="}}
	if _, err := NewClassifier(fake).Classify(context.Background(), testContract); err == nil {
		t.Error("malformed instance entry must be an error, not a silent guess")
	}
}

func TestToSnakeCase(t *testing.T) {
	cases := map[string]string{
		"Transfer":    "transfer",
		"FeeUpdated":  "fee_updated",
		"NFTMinted":   "nft_minted",
		"set_admin":   "set_admin",
		"transfer":    "transfer",
		"AuctionBid2": "auction_bid2",
		"":            "",
	}
	for in, want := range cases {
		if got := toSnakeCase(in); got != want {
			t.Errorf("toSnakeCase(%q) = %q, want %q", in, got, want)
		}
	}
}
