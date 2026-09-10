-- v1.10: sparse heal (docs/SPARSE-HEAL.md).
-- A gap is a promise that a range is missing. Splitting one lets the healer
-- replay the stretches where a registered contract could have produced a row
-- and leave the rest open and declared, so no heal watermark ever descends
-- through a range that was never read.
--
-- 'replay'   : the healer owns this range and will replay it.
-- 'deferred' : deliberately not replayed. It says what was decided, never
--              that the range is empty; the gap stays open and declared.

ALTER TABLE gaps ADD COLUMN heal_mode text NOT NULL DEFAULT 'replay'
    CHECK (heal_mode IN ('replay', 'deferred'));
