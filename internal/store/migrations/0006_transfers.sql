-- v1.1: token transfers derived from SEP-41 token events (docs/DESIGN.md D5).
-- A transfer row is a decoded view of one stored event: same identity
-- ({toid}-{event_index}) and same idempotency key, so the raw event and its
-- derived transfer can always be joined and the derivation re-run offline.
-- Amounts are i128 held in numeric: exact, never floated, and never summed
-- across contracts by the API (read-model lesson: mixed scales lie).

CREATE TABLE transfers (
    network         text        NOT NULL,
    -- {toid}-{event_index}, zero-padded: identical to the source event id.
    id              text        NOT NULL,
    contract_id     text        NOT NULL,  -- the token contract that emitted it
    ledger_sequence bigint      NOT NULL,
    closed_at       timestamptz NOT NULL,  -- business clock
    tx_hash         text        NOT NULL,
    tx_index        int         NOT NULL,
    op_index        int         NOT NULL,
    event_index     int         NOT NULL,
    transfer_type   text        NOT NULL,  -- transfer | mint | burn | clawback
    from_address    text,                  -- null on mint
    to_address      text,                  -- null on burn and clawback
    -- CAP-67 destination muxed id from the event data map, when present:
    -- u64 as decimal text, bytes as hex, string verbatim.
    to_muxed_id     text,
    amount          numeric     NOT NULL,  -- i128, raw token units, no scaling
    -- SEP-0011 asset string from the trailing SAC topic; null for custom
    -- SEP-41 tokens that do not carry it.
    asset           text,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (network, id),
    CONSTRAINT transfers_idempotency UNIQUE (network, contract_id, tx_hash, event_index)
);

-- The canonical read path: one contract, walked in chain order.
CREATE INDEX transfers_by_contract ON transfers (network, contract_id, id);
-- Account activity lookups, both directions.
CREATE INDEX transfers_by_from ON transfers (network, contract_id, from_address, id);
CREATE INDEX transfers_by_to ON transfers (network, contract_id, to_address, id);
