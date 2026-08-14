-- M1: extracted contract events (docs/DESIGN.md §4, decision D3).
-- Raw XDR is kept beside the decoded columns so every derived form can be
-- rebuilt offline (KNOWLEDGE.md P13). closed_at is the business clock;
-- ingested_at is operational and never appears in business queries or sorts
-- (CLAUDE.md rule 8).

CREATE TABLE events (
    network         text        NOT NULL,
    -- {toid}-{event_index}, zero-padded: lexicographic order equals chain
    -- order, and the shape matches getEvents event ids (decision D7).
    id              text        NOT NULL,
    contract_id     text        NOT NULL,
    ledger_sequence bigint      NOT NULL,
    closed_at       timestamptz NOT NULL,
    tx_hash         text        NOT NULL,
    tx_index        int         NOT NULL,  -- 1-based application order in the ledger
    op_index        int         NOT NULL,  -- operation that emitted the event
    event_index     int         NOT NULL,  -- position within the transaction
    event_name      text,                  -- topic0 symbol when decodable
    topic0          text,                  -- base64 ScVal filter columns (D7)
    topic1          text,
    topic2          text,
    topic3          text,
    topics          jsonb       NOT NULL,  -- every topic, base64 ScVal
    value_xdr       text        NOT NULL,  -- base64 ScVal event data
    raw_xdr         text        NOT NULL,  -- full ContractEvent, base64
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (network, id),
    -- Idempotency key (D3): re-ingesting a ledger is a no-op, never a dupe.
    CONSTRAINT events_idempotency UNIQUE (network, contract_id, tx_hash, event_index)
);

-- The canonical read path: one contract, walked in chain order.
CREATE INDEX events_by_contract ON events (network, contract_id, id);
