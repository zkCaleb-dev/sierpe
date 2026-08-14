-- M0: ingestion state. The application owns this schema entirely
-- (docs/DESIGN.md §4); users interact through the API, never these tables.

CREATE TABLE cursor (
    network        text PRIMARY KEY,
    last_sequence  bigint      NOT NULL,
    last_hash      text        NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Recent chain-continuity ring: lets a restart verify PreviousLedgerHash
-- and detect testnet resets / forks (CLAUDE.md rule 10 fatality class).
CREATE TABLE ledger_hashes (
    sequence       bigint PRIMARY KEY,
    hash           text        NOT NULL,
    previous_hash  text        NOT NULL,
    closed_at      timestamptz NOT NULL,  -- business clock
    ingested_at    timestamptz NOT NULL DEFAULT now()  -- operational clock
);

-- Gaps are first-class data (KNOWLEDGE.md P7): persisted before any range
-- is skipped, resolved explicitly, never implied.
CREATE TABLE gaps (
    id             text PRIMARY KEY,  -- deterministic: gap:{network}:{from}:{to}
    network        text        NOT NULL,
    from_sequence  bigint      NOT NULL,
    to_sequence    bigint      NOT NULL,
    reason         text        NOT NULL,
    recorded_at    timestamptz NOT NULL DEFAULT now(),
    resolved_at    timestamptz,
    UNIQUE (network, from_sequence, to_sequence)
);
