package store

import (
	"context"
	"encoding/json"
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
			topic(0), topic(1), topic(2), topic(3), jsonText(e.Topics), e.ValueXDR, e.RawXDR,
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

// EventQuery selects events for the public API. Zero values widen: nil
// topics are wildcards, ToLedger 0 is unbounded, AfterID empty starts at
// the beginning.
type EventQuery struct {
	ContractID string
	// Topics filter by exact base64 ScVal per position (getEvents v2
	// semantics: AND across positions, omitted = wildcard).
	Topics     [4]*string
	FromLedger uint32
	ToLedger   uint32
	AfterID    string
	Limit      int
}

// QueryEvents returns up to Limit events in chain order (id ascending) and
// whether more rows match beyond the page.
func (s *Store) QueryEvents(ctx context.Context, network string, q EventQuery) ([]Event, bool, error) {
	sql := `
		SELECT id, contract_id, ledger_sequence, closed_at, tx_hash,
		       tx_index, op_index, event_index, COALESCE(event_name, ''),
		       topics, value_xdr, raw_xdr
		FROM events
		WHERE network = $1 AND contract_id = $2 AND ledger_sequence >= $3`
	args := []any{network, q.ContractID, int64(q.FromLedger)}
	if q.ToLedger > 0 {
		args = append(args, int64(q.ToLedger))
		sql += fmt.Sprintf(" AND ledger_sequence <= $%d", len(args))
	}
	if q.AfterID != "" {
		args = append(args, q.AfterID)
		sql += fmt.Sprintf(" AND id > $%d", len(args))
	}
	for i, topic := range q.Topics {
		if topic != nil {
			args = append(args, *topic)
			sql += fmt.Sprintf(" AND topic%d = $%d", i, len(args))
		}
	}
	// One extra row answers has-more without a second query.
	args = append(args, q.Limit+1)
	sql += fmt.Sprintf(" ORDER BY id LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, false, fmt.Errorf("store: query events: %w", err)
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		var seq int64
		if err := rows.Scan(&e.ID, &e.ContractID, &seq, &e.ClosedAt, &e.TxHash,
			&e.TxIndex, &e.OpIndex, &e.EventIndex, &e.EventName,
			&e.Topics, &e.ValueXDR, &e.RawXDR); err != nil {
			return nil, false, fmt.Errorf("store: scan event: %w", err)
		}
		e.LedgerSequence = uint32(seq)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("store: query events: %w", err)
	}
	hasMore := len(out) > q.Limit
	if hasMore {
		out = out[:q.Limit]
	}
	return out, hasMore, nil
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

// jsonText serializes a value for a jsonb parameter EXPLICITLY. Under the
// extended protocol pgx knows the target column is jsonb and encodes a Go
// slice as JSON on its own; under the simple protocol (what a user must
// select to sit behind a transaction-mode pooler without prepared-statement
// support) it has no type information and sends a Go slice as a Postgres
// array literal, which jsonb rejects. Sending the JSON text ourselves works
// identically in both modes, so the choice of protocol stops being a
// correctness question.
func jsonText(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		// Topics are []string: Marshal cannot fail on them. Keep the
		// signature total anyway rather than panic inside a batch.
		return "[]"
	}
	return string(raw)
}
