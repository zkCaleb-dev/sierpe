-- v1.2: gap healing watermark for the archive leg (docs/DESIGN.md §5).
-- The healer walks a gap downward in atomic chunks, exactly like backfill.
-- Progress lives in its own column so the gap's identity (id and original
-- range) never mutates: heal_next_to is the highest ledger still MISSING.
-- NULL means untouched (everything up to to_sequence is missing); a value
-- below from_sequence means fully healed, recorded by resolved_at.

ALTER TABLE gaps ADD COLUMN heal_next_to bigint;
