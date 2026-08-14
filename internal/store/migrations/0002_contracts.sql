-- M1: registered contracts — the hot config that survives redeploys
-- (docs/DESIGN.md §7, KNOWLEDGE.md P21). A row controls what gets derived
-- and stored for that contract, never what gets fetched (CLAUDE.md rule 2).

CREATE TABLE contracts (
    network        text        NOT NULL,
    contract_id    text        NOT NULL,
    -- Where the registration came from: 'api' today; 'config' reserved for
    -- config-seeded rows (which win over api rows when both exist).
    source         text        NOT NULL DEFAULT 'api',
    -- Which data kinds are derived for this contract (D1). Default follows
    -- the auto-classification; overridable at registration.
    kinds          text[]      NOT NULL DEFAULT '{events}',
    -- Result of on-chain spec classification (filled by the classifier).
    classification jsonb,
    registered_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (network, contract_id)
);
