-- Migration 108: a new pipeline starts with its object-storage layout undecided.
--
-- pipelines.storage_layout_version (added by 106)
--   0 = undecided: the pipeline has not written to object storage yet. The
--       orchestrator decides the layout the first time it starts the sink for
--       the pipeline (executor/object_layout_v2.go) and stores 1 or 2.
--   1 = the layout every pipeline used before layout v2
--       (<conn prefix>/<pipeline id>/...). A pipeline that already wrote files
--       stays here until a reload, so its folders never change shape mid-run.
--   2 = layout v2 (<conn prefix>/<pipeline prefix>/<db>/[<schema>/]<table>/...).
--
-- Only the default changes. Every existing row keeps its value (1), so no live
-- pipeline moves to a new folder shape. Re-running this file is a no-op: SET
-- DEFAULT is idempotent and the CHECK is dropped before it is re-added.

ALTER TABLE pipelines
    ALTER COLUMN storage_layout_version SET DEFAULT 0;

ALTER TABLE pipelines
    DROP CONSTRAINT IF EXISTS pipelines_storage_layout_version_check;

ALTER TABLE pipelines
    ADD CONSTRAINT pipelines_storage_layout_version_check
        CHECK (storage_layout_version IN (0, 1, 2));
