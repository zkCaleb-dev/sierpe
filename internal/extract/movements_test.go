package extract

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// otherID is a second contract address, used as the token emitting a
// transfer into the watched contract.
const otherID = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

func contractAddr(t *testing.T, id string) xdr.ScVal {
	t.Helper()
	cid := contractIDOf(t, id)
	addr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &cid}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &addr}
}

func TestMovementsCaptureAPaymentFromAnUnwatchedToken(t *testing.T) {
	// The exact shape of a wallet paying a contract: the ASSET's SAC emits
	// the transfer, and that SAC is not registered. The old behaviour lost
	// this event entirely.
	snap := watchingKinds(t, watchedID, store.KindMovements)
	res := extractTransfers(t, snap,
		tokenEvent(t, otherID, i128(0, 690000000),
			symbol("transfer"), accountAddr(t, fromG), contractAddr(t, watchedID), str(asset)),
	)
	if len(res.Transfers) != 0 {
		t.Errorf("the emitting token is not registered: no transfer row is owed, got %d", len(res.Transfers))
	}
	if len(res.Movements) != 1 {
		t.Fatalf("movements = %d, want 1", len(res.Movements))
	}
	m := res.Movements[0]
	if m.ContractID != watchedID {
		t.Errorf("movement must belong to the watched participant, got %s", m.ContractID)
	}
	if m.TokenContractID != otherID {
		t.Errorf("token contract = %s, want the emitter %s", m.TokenContractID, otherID)
	}
	if m.Role != store.RoleRecipient || m.Counterparty != fromG {
		t.Errorf("role/counterparty = %s/%s", m.Role, m.Counterparty)
	}
	if m.Amount != "690000000" {
		t.Errorf("amount = %s", m.Amount)
	}
}

func TestMovementsRecordBothDirections(t *testing.T) {
	snap := watchingKinds(t, watchedID, store.KindMovements)
	out := extractTransfers(t, snap,
		tokenEvent(t, otherID, i128(0, 25),
			symbol("transfer"), contractAddr(t, watchedID), accountAddr(t, toG)),
	)
	if len(out.Movements) != 1 || out.Movements[0].Role != store.RoleSender {
		t.Fatalf("outbound movement = %+v", out.Movements)
	}
	if out.Movements[0].Counterparty != toG {
		t.Errorf("counterparty = %s, want the recipient", out.Movements[0].Counterparty)
	}
}

func TestMovementsSelfTransferYieldsBothRoles(t *testing.T) {
	// One event, two attributions: this is why role is part of the identity.
	snap := watchingKinds(t, watchedID, store.KindMovements)
	res := extractTransfers(t, snap,
		tokenEvent(t, otherID, i128(0, 7),
			symbol("transfer"), contractAddr(t, watchedID), contractAddr(t, watchedID)),
	)
	if len(res.Movements) != 2 {
		t.Fatalf("self transfer movements = %d, want 2", len(res.Movements))
	}
	roles := map[string]bool{res.Movements[0].Role: true, res.Movements[1].Role: true}
	if !roles[store.RoleSender] || !roles[store.RoleRecipient] {
		t.Errorf("roles = %v, want both", roles)
	}
	if res.Movements[0].TransferID != res.Movements[1].TransferID {
		t.Error("both attributions describe one event and must share its id")
	}
}

func TestMovementsIgnoreUnrelatedContracts(t *testing.T) {
	// A transfer between two parties that are not the watched contract must
	// not be stored, even though the watcher wants movements.
	snap := watchingKinds(t, watchedID, store.KindMovements)
	res := extractTransfers(t, snap,
		tokenEvent(t, otherID, i128(0, 5),
			symbol("transfer"), accountAddr(t, fromG), accountAddr(t, toG)),
	)
	if len(res.Movements) != 0 {
		t.Errorf("movements = %d, want 0: neither side is watched", len(res.Movements))
	}
}

func TestMovementsRequireTheKind(t *testing.T) {
	snap := watchingKinds(t, watchedID, store.KindEvents, store.KindTransfers)
	res := extractTransfers(t, snap,
		tokenEvent(t, otherID, i128(0, 5),
			symbol("transfer"), accountAddr(t, fromG), contractAddr(t, watchedID)),
	)
	if len(res.Movements) != 0 {
		t.Errorf("movements must be opt-in, got %d", len(res.Movements))
	}
}

func TestMovementsAndTransfersCoexistOnOneEvent(t *testing.T) {
	// The emitter is watched for transfers AND is itself the recipient with
	// movements on: one event, one transfer row, one movement row.
	snap := watchingKinds(t, watchedID, store.KindTransfers, store.KindMovements)
	res := extractTransfers(t, snap,
		tokenEvent(t, watchedID, i128(0, 5),
			symbol("transfer"), accountAddr(t, fromG), contractAddr(t, watchedID)),
	)
	if len(res.Transfers) != 1 {
		t.Fatalf("transfers = %d, want 1", len(res.Transfers))
	}
	if len(res.Movements) != 1 || res.Movements[0].Role != store.RoleRecipient {
		t.Fatalf("movements = %+v, want one recipient row", res.Movements)
	}
	if res.Movements[0].TransferID != res.Transfers[0].ID {
		t.Error("the movement must carry the transfer's identity")
	}
}

func TestMovementsCountForeignUndecodableSeparately(t *testing.T) {
	// A foreign token emitting a malformed movement must not touch the
	// alerting suppression counter: on an open network anyone can emit an
	// event called transfer carrying anything.
	snap := watchingKinds(t, watchedID, store.KindMovements)
	res := extractTransfers(t, snap,
		tokenEvent(t, otherID, i128(-1, 0),
			symbol("transfer"), accountAddr(t, fromG), contractAddr(t, watchedID)),
	)
	if res.SuppressedTransfers != 0 {
		t.Errorf("a foreign token must not raise the alerting counter, got %d", res.SuppressedTransfers)
	}
	if res.ForeignUndecodable != 1 {
		t.Errorf("ForeignUndecodable = %d, want 1", res.ForeignUndecodable)
	}
	if len(res.Movements) != 0 {
		t.Errorf("nothing decodable, nothing stored; got %d", len(res.Movements))
	}
}

func TestMovementsCostNothingWhenNobodyWantsThem(t *testing.T) {
	// The ledger-scope gate: with no registration asking for movements, an
	// event from an unwatched emitter is not decoded at all.
	snap := watchingKinds(t, watchedID, store.KindEvents)
	res := extractTransfers(t, snap,
		tokenEvent(t, otherID, i128(0, 5),
			symbol("transfer"), accountAddr(t, fromG), contractAddr(t, watchedID)),
	)
	if len(res.Movements) != 0 || res.ForeignUndecodable != 0 || len(res.Events) != 0 {
		t.Errorf("unwatched emitter must be skipped entirely: %+v", res)
	}
}
