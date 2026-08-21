-- Coverage is per (contract, kind), not per contract.
--
-- A backfill walk derives whatever kinds the registration carried while it
-- ran. Adding a kind to an existing registration therefore leaves history
-- underived for that kind while the row still reads done — and the API,
-- which derives coverage from this row alone, would declare COMPLETE over
-- ledgers it never looked at for that kind. That is a rule 7 violation
-- reachable by a one-word edit, so the row now records what it covered.
--
-- Existing rows are seeded from the registration's current kinds: those
-- walks ran with exactly those kinds (nothing could add a kind before this
-- migration existed without also reopening nothing, which is the bug being
-- fixed — see the release notes).

ALTER TABLE backfill ADD COLUMN covered_kinds text[] NOT NULL DEFAULT '{}';

UPDATE backfill b
SET covered_kinds = c.kinds
FROM contracts c
WHERE b.network = c.network AND b.contract_id = c.contract_id;
