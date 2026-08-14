// Package store owns Sierpe's Postgres schema: migrations, ingestion state,
// and (from M1 on) the indexed data itself.
//
// The invariant everything else depends on lives here: the cursor and the
// ledger's data commit in the same transaction (CLAUDE.md rule 1). If that
// transaction fails, the ledger never happened; if it commits, it happened
// exactly once.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// hashRingSize bounds the chain-continuity table; anything older adds no
// safety (continuity only needs the immediate predecessor plus forensics).
const hashRingSize = 1000

// Store is a handle to Sierpe's database.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to Postgres and verifies the connection.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("store: parse database url: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

// Migrate applies embedded migrations in filename order, recording each in
// schema_migrations. Concurrent instances serialize on an advisory lock so
// a fleet can restart safely.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			filename   text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("store: ensure schema_migrations: %w", err)
	}

	names, err := MigrationNames()
	if err != nil {
		return err
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Advisory lock: one migrator at a time, released on commit/rollback.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('sierpe_migrations'))`); err != nil {
			return fmt.Errorf("store: migration lock: %w", err)
		}
		for _, name := range names {
			var exists bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)`, name,
			).Scan(&exists); err != nil {
				return fmt.Errorf("store: check migration %s: %w", name, err)
			}
			if exists {
				continue
			}
			sql, err := migrationsFS.ReadFile("migrations/" + name)
			if err != nil {
				return fmt.Errorf("store: read migration %s: %w", name, err)
			}
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("store: apply migration %s: %w", name, err)
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (filename) VALUES ($1)`, name,
			); err != nil {
				return fmt.Errorf("store: record migration %s: %w", name, err)
			}
		}
		return nil
	})
}

// MigrationNames lists embedded migrations in apply order.
func MigrationNames() ([]string, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("store: read migrations dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

// Cursor is the persisted ingestion position.
type Cursor struct {
	Sequence uint32
	Hash     string
}

// ErrNoCursor is returned by LoadCursor on a fresh database.
var ErrNoCursor = errors.New("store: no cursor for network")

// LoadCursor returns the last committed position for a network.
func (s *Store) LoadCursor(ctx context.Context, network string) (Cursor, error) {
	var c Cursor
	var seq int64
	err := s.pool.QueryRow(ctx,
		`SELECT last_sequence, last_hash FROM cursor WHERE network = $1`, network,
	).Scan(&seq, &c.Hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, ErrNoCursor
	}
	if err != nil {
		return c, fmt.Errorf("store: load cursor: %w", err)
	}
	c.Sequence = uint32(seq)
	return c, nil
}

// LedgerRecord is what CommitLedger persists per ledger in M0. From M1 on it
// grows a Batch of extracted records committed in the same transaction.
type LedgerRecord struct {
	Sequence     uint32
	Hash         string
	PreviousHash string
	ClosedAt     time.Time
}

// CommitLedger atomically advances the cursor and records chain continuity.
// This is THE transaction: when extraction lands (M1), its writes join here.
func (s *Store) CommitLedger(ctx context.Context, network string, rec LedgerRecord) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO cursor (network, last_sequence, last_hash, updated_at)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (network) DO UPDATE
			SET last_sequence = EXCLUDED.last_sequence,
			    last_hash     = EXCLUDED.last_hash,
			    updated_at    = now()
			WHERE cursor.last_sequence < EXCLUDED.last_sequence`,
			network, int64(rec.Sequence), rec.Hash,
		); err != nil {
			return fmt.Errorf("store: advance cursor: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO ledger_hashes (sequence, hash, previous_hash, closed_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (sequence) DO NOTHING`,
			int64(rec.Sequence), rec.Hash, rec.PreviousHash, rec.ClosedAt,
		); err != nil {
			return fmt.Errorf("store: record ledger hash: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM ledger_hashes WHERE sequence < $1`,
			int64(rec.Sequence)-hashRingSize,
		); err != nil {
			return fmt.Errorf("store: prune hash ring: %w", err)
		}
		return nil
	})
}

// RecordGap persists an unserved range before it is skipped. Idempotent:
// the deterministic id makes re-recording on restart a no-op.
func (s *Store) RecordGap(ctx context.Context, network string, from, to uint32, reason string) error {
	id := fmt.Sprintf("gap:%s:%d:%d", network, from, to)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO gaps (id, network, from_sequence, to_sequence, reason)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO NOTHING`,
		id, network, int64(from), int64(to), reason,
	)
	if err != nil {
		return fmt.Errorf("store: record gap: %w", err)
	}
	return nil
}

// OpenGaps counts unresolved gaps for /status and metrics.
func (s *Store) OpenGaps(ctx context.Context, network string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM gaps WHERE network = $1 AND resolved_at IS NULL`, network,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: count gaps: %w", err)
	}
	return n, nil
}
