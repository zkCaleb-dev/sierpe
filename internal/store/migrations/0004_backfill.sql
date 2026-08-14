-- M1: descending backfill state, one row per registered contract.
-- Coverage is derived, never guessed: a contract's indexed range is
-- [next_to + 1 .. cursor] while the worker walks next_to down towards
-- target_from in chunks, each chunk committed atomically with its events.
-- clamped_at records the retention wall when history below it could not be
-- served (the matching gap row is persisted before the clamp is committed —
-- KNOWLEDGE.md P7).

CREATE TABLE backfill (
    network      text        NOT NULL,
    contract_id  text        NOT NULL,
    target_from  bigint      NOT NULL,  -- oldest ledger the user asked for
    next_to      bigint      NOT NULL,  -- upper bound of the next chunk (descending)
    done         boolean     NOT NULL DEFAULT false,
    clamped_at   bigint,                -- oldest ledger actually served, when clamped
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (network, contract_id)
);
