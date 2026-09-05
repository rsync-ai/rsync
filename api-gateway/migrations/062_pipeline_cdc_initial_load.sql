-- 061_pipeline_cdc_initial_load.sql
-- Hybrid CDC: choose how the historical (initial) load is performed for a CDC pipeline.
--
-- Why: Debezium's built-in initial snapshot is slow, non-resumable, lock-prone, and
-- for large tables it pins PostgreSQL WAL / holds a long MySQL read view. The executor
-- already has a fast, parallel, resumable, MinIO-staged batch data plane. This column
-- lets a CDC pipeline use the BATCH path for the historical load and Debezium ONLY for
-- incremental changes after a captured source position (the "position-anchored handoff").
--
-- Values:
--   'debezium' (or NULL) — current behavior: Debezium performs the initial snapshot.
--   'batch'              — hybrid: batch loads history, Debezium streams changes from P.
--
-- Only meaningful when the pipeline's effective sync_mode = 'cdc'. NULL preserves the
-- existing default behavior, so this change is backward compatible.
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS cdc_initial_load character varying(20);

ALTER TABLE pipelines DROP CONSTRAINT IF EXISTS pipelines_cdc_initial_load_check;
ALTER TABLE pipelines ADD CONSTRAINT pipelines_cdc_initial_load_check
    CHECK (cdc_initial_load IS NULL OR cdc_initial_load IN ('debezium', 'batch'));

COMMENT ON COLUMN pipelines.cdc_initial_load IS 'How a CDC pipeline loads historical data: ''debezium'' (default snapshot) or ''batch'' (hybrid batch backfill + position-anchored CDC handoff). NULL = debezium.';
