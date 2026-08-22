package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Sierpe must work under pgx's simple protocol, because that is what a
// user has to select to run behind a transaction-mode pooler that does not
// support prepared statements (older PgBouncer, some managed poolers). The
// trap is jsonb: the extended protocol lets pgx encode Go values as JSON
// from the column's type, the simple protocol has no such hint and sends
// slices as array literals and []byte as bytea — both rejected by jsonb.
// Found by running the suite with default_query_exec_mode=simple_protocol.
func TestStoreWritesJSONBUnderSimpleProtocol(t *testing.T) {
	base := openTestStore(t)
	_ = base // ensures the migration ran and skips without a database
	url := testDatabaseURL(t)
	sep := "?"
	if strings.Contains(url, "?") {
		sep = "&"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := Open(ctx, url+sep+"default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("Open(simple_protocol) error = %v", err)
	}
	defer s.Close()
	if _, err := s.pool.Exec(ctx, `TRUNCATE events, cursor, ledger_hashes, contracts CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// contracts.classification is jsonb, written from []byte.
	c := Contract{
		Network: "testnet", ContractID: "CSIMPLE", Source: SourceAPI,
		Kinds: []string{KindEvents}, Classification: []byte(`{"type":"wasm","events":["transfer"]}`),
	}
	if _, err := s.UpsertContract(ctx, c); err != nil {
		t.Fatalf("UpsertContract() under simple protocol: %v", err)
	}

	// events.topics is jsonb, written from []string.
	rec := LedgerRecord{Sequence: 100, Hash: "aa", PreviousHash: "99", ClosedAt: time.Now().UTC()}
	ev := Event{
		ID: "0000000000000000100-0000000000", ContractID: "CSIMPLE", LedgerSequence: 100,
		ClosedAt: rec.ClosedAt, TxHash: "deadbeef", Topics: []string{"AAAADwAAAAh0cmFuc2Zlcg==", "AAAAEg=="},
		ValueXDR: "AAAAAQ==", RawXDR: "AAAA",
	}
	if err := s.CommitLedger(ctx, "testnet", rec, []Event{ev}, nil, nil, nil, nil); err != nil {
		t.Fatalf("CommitLedger() under simple protocol: %v", err)
	}
	rows, _, err := s.QueryEvents(ctx, "testnet", EventQuery{ContractID: "CSIMPLE", FromLedger: 1, Limit: 10})
	if err != nil || len(rows) != 1 || len(rows[0].Topics) != 2 {
		t.Fatalf("read back: rows=%d err=%v", len(rows), err)
	}
}
