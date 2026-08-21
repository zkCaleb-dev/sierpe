-- v1.5: movements — token transfers a registered contract PARTICIPATES in,
-- as sender or recipient, regardless of which contract emitted the event.
--
-- The `transfers` table answers "what did this token contract emit"; this
-- one answers "what moved in or out of my contract". Both are needed and
-- they are not the same question: a payment to an escrow is emitted by the
-- asset's SAC, so it belongs to that SAC's transfer stream AND to the
-- escrow's movement stream.
--
-- One physical transfer row, N attributions. Attributing by duplicating
-- transfer rows is impossible anyway: transfers.id is {toid}-{event_index},
-- identical no matter who the row is about, so a second row collides on the
-- primary key, aborts the ledger transaction and stalls ingestion (rule 1).
--
-- role is part of the key because a self-transfer (from == to == C) is one
-- event that legitimately produces two attributions.

CREATE TABLE movements (
    network           text        NOT NULL,
    -- The WATCHER: the registered contract this movement is about.
    contract_id       text        NOT NULL,
    -- transfers.id of the same event; the two rows always join.
    transfer_id       text        NOT NULL,
    role              text        NOT NULL,  -- sender | recipient
    -- The token contract that emitted the event. THIS is the asset's
    -- identity; the SEP-0011 asset string on the transfer row is
    -- attacker-choosable for custom tokens and must never be joined on.
    token_contract_id text        NOT NULL,
    transfer_type     text        NOT NULL,  -- transfer | mint | burn | clawback
    counterparty      text,                  -- the other side, when there is one
    amount            numeric     NOT NULL,  -- i128, raw token units
    ledger_sequence   bigint      NOT NULL,
    closed_at         timestamptz NOT NULL,  -- business clock (rule 8)
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (network, contract_id, transfer_id, role)
);

-- The canonical read path: one contract's movements in chain order.
CREATE INDEX movements_by_contract
    ON movements (network, contract_id, transfer_id);
-- Filtering one contract's movements by the token that moved.
CREATE INDEX movements_by_token
    ON movements (network, contract_id, token_contract_id, transfer_id);
