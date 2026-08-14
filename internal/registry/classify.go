package registry

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ErrContractNotFound reports that a contract has no instance entry on chain.
var ErrContractNotFound = errors.New("registry: contract not found on chain")

// Classification types.
const (
	TypeWasm     = "wasm"     // custom contract backed by uploaded wasm
	TypeSAC      = "sac"      // built-in Stellar Asset Contract
	TypeExternal = "external" // executable referencing another contract
)

// Classification methods: how the event names were discovered.
const (
	MethodSpecEvents    = "spec_events"    // declared events in contractspecv0
	MethodFunctionNames = "function_names" // fallback: exported function names
	MethodSACBuiltin    = "sac_builtin"    // the SAC event set is protocol-defined
	MethodOpaque        = "opaque"         // spec absent or unreadable
)

// sacEvents is the protocol-defined event set of the Stellar Asset Contract
// (CAP-46-6): classification by executable, no spec involved.
var sacEvents = []string{
	"approve", "burn", "clawback", "mint", "set_admin", "set_authorized", "transfer",
}

// Classification is what registration learns about a contract from the chain
// (docs/DESIGN.md §6). It is advisory — it names the events we expect to see
// — and is stored verbatim on the contract row and reported by the API.
type Classification struct {
	Type     string   `json:"type"`
	WasmHash string   `json:"wasm_hash,omitempty"`
	Events   []string `json:"events"`
	Method   string   `json:"method"`
}

// entryReader is the slice of the RPC client classification consumes
// (CLAUDE.md rule 5). found=false means the entry does not exist on chain.
type entryReader interface {
	GetLedgerEntry(ctx context.Context, keyB64 string) (xdrB64 string, found bool, err error)
}

// Classifier resolves a contract id to its Classification by reading the
// contract instance (and, for wasm contracts, the code entry) on chain.
type Classifier struct {
	entries entryReader
}

// NewClassifier wires a Classifier over a ledger-entry reader.
func NewClassifier(entries entryReader) *Classifier {
	return &Classifier{entries: entries}
}

// Classify reads the contract's on-chain executable and derives its event
// names. Wasm contracts declare events in the contractspecv0 section
// (normalized CamelCase to snake_case); when the spec declares none, exported
// function names are the declared fallback; an unreadable spec degrades to
// MethodOpaque rather than failing registration. Only a contract with no
// instance on chain is an error (ErrContractNotFound).
//
// The whole call sits behind a recover frontier: SDK XDR getters panic on
// nil unions and chain data is never trusted (CLAUDE.md rule 6).
func (c *Classifier) Classify(ctx context.Context, contractID string) (cls Classification, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("registry: classify %s: recovered from malformed chain data: %v", contractID, r)
		}
	}()

	keyB64, err := instanceLedgerKeyB64(contractID)
	if err != nil {
		return cls, err
	}
	entryB64, found, err := c.entries.GetLedgerEntry(ctx, keyB64)
	if err != nil {
		return cls, fmt.Errorf("registry: classify %s: %w", contractID, err)
	}
	if !found {
		return cls, fmt.Errorf("%w: %s", ErrContractNotFound, contractID)
	}

	var data xdr.LedgerEntryData
	if err := xdr.SafeUnmarshalBase64(entryB64, &data); err != nil {
		return cls, fmt.Errorf("registry: classify %s: decode instance entry: %w", contractID, err)
	}
	contractData, ok := data.GetContractData()
	if !ok {
		return cls, fmt.Errorf("registry: classify %s: instance key returned a %s entry", contractID, data.Type)
	}
	instance, ok := contractData.Val.GetInstance()
	if !ok {
		return cls, fmt.Errorf("registry: classify %s: instance value has type %s", contractID, contractData.Val.Type)
	}

	switch instance.Executable.Type {
	case xdr.ContractExecutableTypeContractExecutableStellarAsset:
		return Classification{Type: TypeSAC, Events: sacEvents, Method: MethodSACBuiltin}, nil
	case xdr.ContractExecutableTypeContractExecutableWasm:
		hash, ok := instance.Executable.GetWasmHash()
		if !ok {
			return cls, fmt.Errorf("registry: classify %s: wasm executable without hash", contractID)
		}
		return c.classifyWasm(ctx, contractID, hash)
	default:
		// External refs (and future executable types) have no spec of their
		// own; stay honest about not knowing their events.
		return Classification{Type: TypeExternal, Events: []string{}, Method: MethodOpaque}, nil
	}
}

// classifyWasm fetches the code entry and mines its contract spec.
func (c *Classifier) classifyWasm(ctx context.Context, contractID string, hash xdr.Hash) (Classification, error) {
	cls := Classification{Type: TypeWasm, WasmHash: hash.HexString(), Events: []string{}, Method: MethodOpaque}

	keyB64, err := codeLedgerKeyB64(hash)
	if err != nil {
		return cls, err
	}
	entryB64, found, err := c.entries.GetLedgerEntry(ctx, keyB64)
	if err != nil {
		return cls, fmt.Errorf("registry: classify %s: fetch code entry: %w", contractID, err)
	}
	if !found {
		// Instance points at code the chain no longer serves: suspicious but
		// not a registration blocker; declare the spec opaque.
		return cls, nil
	}
	var data xdr.LedgerEntryData
	if err := xdr.SafeUnmarshalBase64(entryB64, &data); err != nil {
		return cls, nil
	}
	code, ok := data.GetContractCode()
	if !ok {
		return cls, nil
	}
	spec, ok := contractSpecSection(code.Code)
	if !ok {
		return cls, nil
	}

	events, functions, ok := specNames(spec)
	if !ok {
		return cls, nil
	}
	switch {
	case len(events) > 0:
		cls.Events, cls.Method = events, MethodSpecEvents
	case len(functions) > 0:
		cls.Events, cls.Method = functions, MethodFunctionNames
	}
	return cls, nil
}

// specNames decodes a contractspecv0 payload (a bare stream of ScSpecEntry)
// and returns declared event names and exported function names, both
// normalized to snake_case. ok=false means the stream did not decode.
func specNames(spec []byte) (events, functions []string, ok bool) {
	r := bytes.NewReader(spec)
	for r.Len() > 0 {
		var entry xdr.ScSpecEntry
		if _, err := xdr.Unmarshal(r, &entry); err != nil {
			// The loop only runs with bytes pending, so any error here —
			// including EOF — means a truncated or corrupt stream.
			return nil, nil, false
		}
		if ev, isEvent := entry.GetEventV0(); isEvent {
			events = append(events, toSnakeCase(string(ev.Name)))
		}
		if fn, isFn := entry.GetFunctionV0(); isFn {
			functions = append(functions, toSnakeCase(string(fn.Name)))
		}
	}
	return events, functions, true
}

// wasmMagic opens every wasm module: \0asm followed by a 4-byte version.
var wasmMagic = []byte{0x00, 0x61, 0x73, 0x6d}

// contractSpecSection walks the wasm section table looking for the custom
// section named contractspecv0. Bounds are checked at every step: the bytes
// come from the chain and are not trusted (CLAUDE.md rule 6).
func contractSpecSection(wasm []byte) ([]byte, bool) {
	if len(wasm) < 8 || !bytes.Equal(wasm[:4], wasmMagic) {
		return nil, false
	}
	rest := wasm[8:]
	for len(rest) > 0 {
		sectionID := rest[0]
		rest = rest[1:]
		size, n := binary.Uvarint(rest)
		if n <= 0 || size > uint64(len(rest)-n) {
			return nil, false
		}
		section := rest[n : n+int(size)]
		rest = rest[n+int(size):]

		if sectionID != 0 { // 0 = custom section
			continue
		}
		nameLen, m := binary.Uvarint(section)
		if m <= 0 || nameLen > uint64(len(section)-m) {
			return nil, false
		}
		name := section[m : m+int(nameLen)]
		if string(name) == "contractspecv0" {
			return section[m+int(nameLen):], true
		}
	}
	return nil, false
}

// instanceLedgerKeyB64 builds the base64 LedgerKey of a contract's instance
// entry (the persistent ScvLedgerKeyContractInstance datum).
func instanceLedgerKeyB64(contractID string) (string, error) {
	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return "", fmt.Errorf("registry: decode contract id %s: %w", contractID, err)
	}
	var cid xdr.ContractId
	copy(cid[:], raw)
	key := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &cid,
			},
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}
	out, err := xdr.MarshalBase64(key)
	if err != nil {
		return "", fmt.Errorf("registry: marshal instance key: %w", err)
	}
	return out, nil
}

// codeLedgerKeyB64 builds the base64 LedgerKey of a contract code entry.
func codeLedgerKeyB64(hash xdr.Hash) (string, error) {
	key := xdr.LedgerKey{
		Type:         xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{Hash: hash},
	}
	out, err := xdr.MarshalBase64(key)
	if err != nil {
		return "", fmt.Errorf("registry: marshal code key: %w", err)
	}
	return out, nil
}

// toSnakeCase normalizes a spec name (CamelCase struct names, already-snake
// function names) to snake_case. Acronym runs stay together: NFTMinted
// becomes nft_minted.
func toSnakeCase(s string) string {
	runes := []rune(s)
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i, r := range runes {
		if unicode.IsUpper(r) {
			prevLower := i > 0 && (unicode.IsLower(runes[i-1]) || unicode.IsDigit(runes[i-1]))
			nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if i > 0 && (prevLower || nextLower) {
				b.WriteRune('_')
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
