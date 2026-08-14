package extract

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

const (
	passphrase    = network.TestNetworkPassphrase
	watchedID     = "CBF64DEOVQAXJFBSNGFEUT2AH4H7K5JBY3ZYJ5GVEINMNSDISWRG5N3F"
	testLedgerSeq = 12345
	testCloseTime = 1_700_000_000
)

// --- fixture builders -------------------------------------------------------

func contractIDOf(t *testing.T, strkeyID string) xdr.ContractId {
	t.Helper()
	raw, err := strkey.Decode(strkey.VersionByteContract, strkeyID)
	if err != nil {
		t.Fatalf("decode contract id: %v", err)
	}
	var cid xdr.ContractId
	copy(cid[:], raw)
	return cid
}

func symbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func contractEvent(t *testing.T, contract string, topics ...xdr.ScVal) xdr.ContractEvent {
	t.Helper()
	cid := contractIDOf(t, contract)
	val := xdr.Int64(7)
	return xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &cid,
		Body: xdr.ContractEventBody{
			V:  0,
			V0: &xdr.ContractEventV0{Topics: topics, Data: xdr.ScVal{Type: xdr.ScValTypeScvI64, I64: &val}},
		},
	}
}

// sorobanEnvelope builds a minimal InvokeHostFunction envelope so the SDK
// treats the transaction as Soroban.
func sorobanEnvelope(t *testing.T, seqNum int64) xdr.TransactionEnvelope {
	t.Helper()
	var source xdr.MuxedAccount
	if err := source.SetEd25519Address("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"); err != nil {
		t.Fatalf("set source: %v", err)
	}
	return xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: source,
				Fee:           100,
				SeqNum:        xdr.SequenceNumber(seqNum),
				// The SDK detects Soroban transactions by this ext, not by
				// the operation type.
				Ext: xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}},
				Operations: []xdr.Operation{{
					Body: xdr.OperationBody{
						Type: xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
							HostFunction: xdr.HostFunction{
								Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
								InvokeContract: &xdr.InvokeContractArgs{
									ContractAddress: xdr.ScAddress{
										Type:       xdr.ScAddressTypeScAddressTypeContract,
										ContractId: func() *xdr.ContractId { c := contractIDOf(t, watchedID); return &c }(),
									},
									FunctionName: "test",
								},
							},
						},
					},
				}},
			},
		},
	}
}

func txResult(hash xdr.Hash, success bool) xdr.TransactionResultPair {
	code := xdr.TransactionResultCodeTxSuccess
	if !success {
		code = xdr.TransactionResultCodeTxFailed
	}
	opResults := []xdr.OperationResult{}
	return xdr.TransactionResultPair{
		TransactionHash: hash,
		Result: xdr.TransactionResult{
			Result: xdr.TransactionResultResult{Code: code, Results: &opResults},
		},
	}
}

func metaV3WithEvents(events []xdr.ContractEvent) xdr.TransactionMeta {
	return xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			SorobanMeta: &xdr.SorobanTransactionMeta{Events: events},
		},
	}
}

// fixtureTx couples one envelope with its result and meta.
type fixtureTx struct {
	envelope xdr.TransactionEnvelope
	success  bool
	meta     xdr.TransactionMeta
}

// buildLCM assembles a V1 LedgerCloseMeta whose transaction hashes are real
// (computed with the network passphrase) so the SDK reader accepts them.
func buildLCM(t *testing.T, txs ...fixtureTx) xdr.LedgerCloseMeta {
	t.Helper()
	envelopes := make([]xdr.TransactionEnvelope, 0, len(txs))
	processing := make([]xdr.TransactionResultMeta, 0, len(txs))
	for _, tx := range txs {
		hash, err := network.HashTransactionInEnvelope(tx.envelope, passphrase)
		if err != nil {
			t.Fatalf("hash envelope: %v", err)
		}
		envelopes = append(envelopes, tx.envelope)
		processing = append(processing, xdr.TransactionResultMeta{
			Result:            txResult(hash, tx.success),
			TxApplyProcessing: tx.meta,
		})
	}
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: testLedgerSeq,
					ScpValue:  xdr.StellarValue{CloseTime: testCloseTime},
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					Phases: []xdr.TransactionPhase{{
						V: 0,
						V0Components: &[]xdr.TxSetComponent{{
							Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
							TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
								Txs: envelopes,
							},
						}},
					}},
				},
			},
			TxProcessing: processing,
		},
	}
}

// watching builds a snapshot that watches the given contracts with the
// events kind.
func watching(t *testing.T, ids ...string) *registry.Snapshot {
	t.Helper()
	contracts := make([]store.Contract, 0, len(ids))
	for _, id := range ids {
		contracts = append(contracts, store.Contract{
			Network: "testnet", ContractID: id, Kinds: []string{store.KindEvents},
		})
	}
	reg := registry.New("testnet", staticLister(contracts))
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("reload: %v", err)
	}
	return reg.Snapshot()
}

type staticLister []store.Contract

func (s staticLister) ListContracts(context.Context, string) ([]store.Contract, error) {
	return s, nil
}

// --- tests ------------------------------------------------------------------

func TestEventsExtractsWatchedContract(t *testing.T) {
	lcm := buildLCM(t, fixtureTx{
		envelope: sorobanEnvelope(t, 1),
		success:  true,
		meta: metaV3WithEvents([]xdr.ContractEvent{
			contractEvent(t, watchedID, symbol("transfer"), symbol("extra")),
		}),
	})

	res, err := Events(lcm, passphrase, watching(t, watchedID))
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if len(res.Events) != 1 {
		t.Fatalf("extracted %d events, want 1", len(res.Events))
	}
	ev := res.Events[0]
	if ev.ContractID != watchedID {
		t.Errorf("contract = %s", ev.ContractID)
	}
	if ev.EventName != "transfer" {
		t.Errorf("event name = %q, want transfer", ev.EventName)
	}
	if ev.LedgerSequence != testLedgerSeq {
		t.Errorf("ledger = %d, want %d", ev.LedgerSequence, testLedgerSeq)
	}
	if !ev.ClosedAt.Equal(time.Unix(testCloseTime, 0).UTC()) {
		t.Errorf("closed_at = %v", ev.ClosedAt)
	}
	if ev.TxIndex != 1 || ev.EventIndex != 0 || ev.OpIndex != 0 {
		t.Errorf("position = tx %d op %d event %d", ev.TxIndex, ev.OpIndex, ev.EventIndex)
	}
	if len(ev.Topics) != 2 || ev.ValueXDR == "" || ev.RawXDR == "" {
		t.Errorf("envelope incomplete: %+v", ev)
	}
	if !strings.Contains(ev.ID, "-") || len(ev.ID) != 30 {
		t.Errorf("id %q must be the padded toid-eventindex form", ev.ID)
	}
	if ev.TxHash == "" {
		t.Error("tx hash must be set")
	}
}

func TestEventsIgnoresUnwatchedContract(t *testing.T) {
	other := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	lcm := buildLCM(t, fixtureTx{
		envelope: sorobanEnvelope(t, 1),
		success:  true,
		meta: metaV3WithEvents([]xdr.ContractEvent{
			contractEvent(t, other, symbol("transfer")),
		}),
	})

	res, err := Events(lcm, passphrase, watching(t, watchedID))
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if len(res.Events) != 0 {
		t.Errorf("extracted %d events from an unwatched contract", len(res.Events))
	}
}

func TestEventsSkipsAndCountsFailedTx(t *testing.T) {
	lcm := buildLCM(t,
		fixtureTx{
			envelope: sorobanEnvelope(t, 1),
			success:  false,
			meta: metaV3WithEvents([]xdr.ContractEvent{
				contractEvent(t, watchedID, symbol("transfer")),
			}),
		},
		fixtureTx{
			envelope: sorobanEnvelope(t, 2),
			success:  true,
			meta: metaV3WithEvents([]xdr.ContractEvent{
				contractEvent(t, watchedID, symbol("mint")),
			}),
		},
	)

	res, err := Events(lcm, passphrase, watching(t, watchedID))
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if res.FailedTxs != 1 {
		t.Errorf("failed txs = %d, want 1", res.FailedTxs)
	}
	if len(res.Events) != 1 || res.Events[0].EventName != "mint" {
		t.Errorf("events = %+v, want only the successful tx event", res.Events)
	}
}

func TestEventsCountsUnreadableTxMeta(t *testing.T) {
	// Meta version 99 does not exist: GetTransactionEvents must fail and the
	// transaction must be counted, not lost silently.
	lcm := buildLCM(t, fixtureTx{
		envelope: sorobanEnvelope(t, 1),
		success:  true,
		meta:     xdr.TransactionMeta{V: 99},
	})

	res, err := Events(lcm, passphrase, watching(t, watchedID))
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if res.SuppressedTxs != 1 {
		t.Errorf("suppressed txs = %d, want 1", res.SuppressedTxs)
	}
	if len(res.Events) != 0 {
		t.Errorf("events = %d, want 0", len(res.Events))
	}
}

func TestEventsIgnoresDiagnosticTypeAndKeepsIndexes(t *testing.T) {
	diag := contractEvent(t, watchedID, symbol("noise"))
	diag.Type = xdr.ContractEventTypeDiagnostic
	lcm := buildLCM(t, fixtureTx{
		envelope: sorobanEnvelope(t, 1),
		success:  true,
		meta: metaV3WithEvents([]xdr.ContractEvent{
			diag,
			contractEvent(t, watchedID, symbol("transfer")),
		}),
	})

	res, err := Events(lcm, passphrase, watching(t, watchedID))
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if len(res.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(res.Events))
	}
	// The skipped event still consumed index 0: ids must stay stable no
	// matter what we filter (D7 id semantics).
	if res.Events[0].EventIndex != 1 {
		t.Errorf("event index = %d, want 1", res.Events[0].EventIndex)
	}
}

func TestEventsRespectsKinds(t *testing.T) {
	// Watched contract whose registration derives no events (D1: config
	// controls what is derived, not what is fetched).
	contracts := []store.Contract{{
		Network: "testnet", ContractID: watchedID, Kinds: []string{},
	}}
	reg := registry.New("testnet", staticLister(contracts))
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("reload: %v", err)
	}

	lcm := buildLCM(t, fixtureTx{
		envelope: sorobanEnvelope(t, 1),
		success:  true,
		meta: metaV3WithEvents([]xdr.ContractEvent{
			contractEvent(t, watchedID, symbol("transfer")),
		}),
	})
	res, err := Events(lcm, passphrase, reg.Snapshot())
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if len(res.Events) != 0 {
		t.Errorf("a registration without the events kind must derive nothing, got %d", len(res.Events))
	}
}

func TestEventsEmptyWatchIsNoOp(t *testing.T) {
	lcm := buildLCM(t, fixtureTx{
		envelope: sorobanEnvelope(t, 1),
		success:  true,
		meta: metaV3WithEvents([]xdr.ContractEvent{
			contractEvent(t, watchedID, symbol("transfer")),
		}),
	})
	res, err := Events(lcm, passphrase, registry.New("testnet", staticLister(nil)).Snapshot())
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if len(res.Events) != 0 || res.FailedTxs != 0 {
		t.Errorf("empty watch must extract nothing, got %+v", res)
	}
}
