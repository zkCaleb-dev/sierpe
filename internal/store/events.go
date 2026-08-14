package store

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

// Event is one extracted contract event in the canonical envelope shape
// (decision D3): identity and meta columns plus the raw XDR everything can
// be rebuilt from.
type Event struct {
	ID             string // {toid}-{event_index}, zero-padded, sortable
	ContractID     string
	LedgerSequence uint32
	ClosedAt       time.Time
	TxHash         string
	TxIndex        int32 // 1-based application order
	OpIndex        int32
	EventIndex     int32  // position within the transaction
	EventName      string // topic0 symbol when decodable; empty otherwise
	Topics         []string
	ValueXDR       string
	RawXDR         string
}

// HasKind reports whether a contract registration derives the given kind.
func (c Contract) HasKind(kind string) bool {
	return slices.Contains(c.Kinds, kind)
}

// insertEvents batches the ledger's events into the open transaction. The
// idempotency key absorbs replays: conflicting rows are left untouched.
func insertEvents(ctx context.Context, tx pgx.Tx, network string, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, e := range events {
		topic := func(i int) *string {
			if i < len(e.Topics) {
				return &e.Topics[i]
			}
			return nil
		}
		var name *string
		if e.EventName != "" {
			name = &e.EventName
		}
		batch.Queue(`
			INSERT INTO events (
				network, id, contract_id, ledger_sequence, closed_at,
				tx_hash, tx_index, op_index, event_index, event_name,
				topic0, topic1, topic2, topic3, topics, value_xdr, raw_xdr
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
			ON CONFLICT ON CONSTRAINT events_idempotency DO NOTHING`,
			network, e.ID, e.ContractID, int64(e.LedgerSequence), e.ClosedAt,
			e.TxHash, e.TxIndex, e.OpIndex, e.EventIndex, name,
			topic(0), topic(1), topic(2), topic(3), e.Topics, e.ValueXDR, e.RawXDR,
		)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range events {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("store: insert event: %w", err)
		}
	}
	return nil
}

// EventCountsByName aggregates stored events per event name for one
// contract; unnamed events group under the empty string.
func (s *Store) EventCountsByName(ctx context.Context, network, contractID string) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(event_name, ''), count(*)
		FROM events
		WHERE network = $1 AND contract_id = $2
		GROUP BY 1`,
		network, contractID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: count events: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var name string
		var n int64
		if err := rows.Scan(&name, &n); err != nil {
			return nil, fmt.Errorf("store: scan event count: %w", err)
		}
		out[name] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: count events: %w", err)
	}
	return out, nil
}
