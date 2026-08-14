-- M2: contract state (docs/DESIGN.md §4, decision D8): full change history
-- with provenance plus a cheap current snapshot.
--
-- The snapshot is convergence-safe under out-of-order writes: every upsert
-- is guarded by last_ledger, so the descending backfill can replay old
-- history without ever overwriting newer state, and removals leave
-- tombstones (a deleted row still carries its last_ledger) so stale
-- re-inserts lose the comparison instead of resurrecting dead entries.

CREATE TABLE state_changes (
    network         text        NOT NULL,
    -- {toid}-{change_index}, zero-padded: lexicographic order equals chain
    -- order, same shape as event ids.
    id              text        NOT NULL,
    contract_id     text        NOT NULL,
    ledger_sequence bigint      NOT NULL,
    closed_at       timestamptz NOT NULL,  -- business clock
    tx_hash         text        NOT NULL,
    tx_index        int         NOT NULL,
    op_index        int         NOT NULL,
    change_index    int         NOT NULL,  -- position among the tx changes
    change_type     text        NOT NULL,  -- created | updated | removed | restored
    key_xdr         text        NOT NULL,  -- base64 ScVal entry key
    durability      text        NOT NULL,  -- temporary | persistent
    pre_xdr         text,                  -- base64 ScVal value before (null on create)
    post_xdr        text,                  -- base64 ScVal value after (null on remove)
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (network, id),
    CONSTRAINT state_changes_idempotency UNIQUE (network, contract_id, tx_hash, change_index)
);

CREATE INDEX state_changes_by_contract ON state_changes (network, contract_id, id);
CREATE INDEX state_changes_by_key ON state_changes (network, contract_id, key_xdr, id);

CREATE TABLE state_entries (
    network     text        NOT NULL,
    contract_id text        NOT NULL,
    key_xdr     text        NOT NULL,
    durability  text        NOT NULL,
    value_xdr   text,                  -- null when deleted (tombstone)
    deleted     boolean     NOT NULL DEFAULT false,
    last_ledger bigint      NOT NULL,  -- ordering guard for the upsert
    closed_at   timestamptz NOT NULL,  -- business clock of the last change
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (network, contract_id, key_xdr, durability)
);
