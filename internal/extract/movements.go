package extract

import (
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/store"
)

// participatesWatched reports whether any address topic of a movement event
// names a contract registered for movements — the cheap pre-check that runs
// before the full transfer decode.
//
// Guard order is load-bearing. On mainnet only ~0.6% of token transfers name
// a contract on either side (the rest move between accounts and liquidity
// pools), so testing the ScAddress TYPE first discards almost everything
// before any base32/CRC16 encoding happens. Reversing these two lines makes
// every instance that watches movements pay strkey encoding for every token
// transfer on the network.
func participatesWatched(body xdr.ContractEventV0, watch *registry.Snapshot) bool {
	for _, t := range body.Topics[1:] {
		if t.Type != xdr.ScValTypeScvAddress {
			continue
		}
		addr, ok := t.GetAddress()
		if !ok || addr.Type != xdr.ScAddressTypeScAddressTypeContract {
			continue
		}
		cid := addr.MustContractId()
		id, err := strkey.Encode(strkey.VersionByteContract, cid[:])
		if err != nil {
			continue
		}
		if c, watched := watch.Get(id); watched && c.HasKind(store.KindMovements) {
			return true
		}
	}
	return false
}

// movementsOf turns one decoded transfer into the attributions owed to the
// contracts that took part in it. A self-transfer yields two rows for one
// event — one per role — which is why role is part of the identity.
func movementsOf(t store.Transfer, watch *registry.Snapshot) []store.Movement {
	var out []store.Movement
	add := func(owner, role, counterparty string) {
		if owner == "" {
			return
		}
		c, watched := watch.Get(owner)
		if !watched || !c.HasKind(store.KindMovements) {
			return
		}
		out = append(out, store.Movement{
			ContractID:      owner,
			TransferID:      t.ID,
			Role:            role,
			TokenContractID: t.ContractID, // the emitter: the asset's identity
			TransferType:    t.TransferType,
			Counterparty:    counterparty,
			Amount:          t.Amount,
			LedgerSequence:  t.LedgerSequence,
			ClosedAt:        t.ClosedAt,
		})
	}
	add(t.FromAddress, store.RoleSender, t.ToAddress)
	add(t.ToAddress, store.RoleRecipient, t.FromAddress)
	return out
}
