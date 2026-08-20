-- v1.1: classic trustlines of SAC-wrapped assets (docs/DESIGN.md D5).
-- A trustline change is attributed to the SAC contract whose asset it
-- trusts (the contract id is derived locally from the asset, so watching
-- trustlines costs zero extra RPC). Native XLM has no trustlines: the kind
-- only observes issued classic assets. Balances and limits are int64
-- stroop-scale values straight from the ledger entry.

CREATE TABLE trustline_changes (
    network         text        NOT NULL,
    -- {toid}-{change_index}, zero-padded: same identity scheme as state
    -- changes (the index counts every change in the transaction).
    id              text        NOT NULL,
    contract_id     text        NOT NULL,  -- the SAC wrapping the asset
    account_id      text        NOT NULL,  -- the trustline holder
    asset           text        NOT NULL,  -- SEP-0011 canonical CODE:ISSUER
    ledger_sequence bigint      NOT NULL,
    closed_at       timestamptz NOT NULL,  -- business clock
    tx_hash         text        NOT NULL,
    tx_index        int         NOT NULL,
    op_index        int         NOT NULL,
    change_index    int         NOT NULL,
    change_type     text        NOT NULL,  -- created | updated | removed | restored
    pre_balance     bigint,                -- null on create
    post_balance    bigint,                -- null on remove
    pre_limit       bigint,
    post_limit      bigint,
    -- Authorization flags of the surviving side (post, or pre on remove).
    flags           int         NOT NULL,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (network, id),
    CONSTRAINT trustline_changes_idempotency UNIQUE (network, contract_id, tx_hash, change_index)
);

CREATE INDEX trustline_changes_by_contract ON trustline_changes (network, contract_id, id);
CREATE INDEX trustline_changes_by_account ON trustline_changes (network, contract_id, account_id, id);

-- Current snapshot, convergence-safe exactly like state_entries: the
-- last_ledger guard keeps descending backfill from overwriting newer rows,
-- and removals leave tombstones so stale re-inserts lose the comparison.
CREATE TABLE trustline_entries (
    network     text        NOT NULL,
    contract_id text        NOT NULL,
    account_id  text        NOT NULL,
    asset       text        NOT NULL,
    balance     bigint,                -- null when deleted (tombstone)
    trust_limit bigint,
    flags       int         NOT NULL DEFAULT 0,
    deleted     boolean     NOT NULL DEFAULT false,
    last_ledger bigint      NOT NULL,
    closed_at   timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (network, contract_id, account_id)
);
