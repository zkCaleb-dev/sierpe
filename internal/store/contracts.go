package store

import (
	"context"
	"fmt"
	"time"
)

// SourceAPI marks a contract registered through the admin API. The value
// "config" is reserved for config-seeded rows, which win over api rows.
const SourceAPI = "api"

// KindEvents is the only data kind M1 derives; state (M2) and transfers
// (v1.1) join this list later.
const KindEvents = "events"

// Contract is a registered contract: one row of hot config (P21).
type Contract struct {
	Network    string
	ContractID string
	Source     string
	Kinds      []string
	// Classification is the raw JSON produced by the on-chain spec
	// classifier; nil until classification runs.
	Classification []byte
	RegisteredAt   time.Time
}

// UpsertContract registers a contract or reconciles an existing registration
// to the desired state (admin doctrine: reconcile sets, idempotent mutations
// — CLAUDE.md rule 11). Kinds and source follow the new request;
// classification is only overwritten when the new row carries one;
// registered_at is preserved. Returns the stored row.
func (s *Store) UpsertContract(ctx context.Context, c Contract) (Contract, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO contracts (network, contract_id, source, kinds, classification)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (network, contract_id) DO UPDATE
		SET source         = EXCLUDED.source,
		    kinds          = EXCLUDED.kinds,
		    classification = COALESCE(EXCLUDED.classification, contracts.classification)
		RETURNING network, contract_id, source, kinds, classification, registered_at`,
		c.Network, c.ContractID, c.Source, c.Kinds, c.Classification,
	)
	var out Contract
	if err := row.Scan(&out.Network, &out.ContractID, &out.Source, &out.Kinds,
		&out.Classification, &out.RegisteredAt); err != nil {
		return Contract{}, fmt.Errorf("store: upsert contract: %w", err)
	}
	return out, nil
}

// DeleteContract removes a registration. Idempotent: deleting an absent
// contract is a no-op; the bool reports whether a row existed.
func (s *Store) DeleteContract(ctx context.Context, network, contractID string) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM contracts WHERE network = $1 AND contract_id = $2`,
		network, contractID,
	)
	if err != nil {
		return false, fmt.Errorf("store: delete contract: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ListContracts returns every registration for a network in stable order.
func (s *Store) ListContracts(ctx context.Context, network string) ([]Contract, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT network, contract_id, source, kinds, classification, registered_at
		FROM contracts
		WHERE network = $1
		ORDER BY registered_at, contract_id`,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list contracts: %w", err)
	}
	defer rows.Close()

	var out []Contract
	for rows.Next() {
		var c Contract
		if err := rows.Scan(&c.Network, &c.ContractID, &c.Source, &c.Kinds,
			&c.Classification, &c.RegisteredAt); err != nil {
			return nil, fmt.Errorf("store: scan contract: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list contracts: %w", err)
	}
	return out, nil
}
