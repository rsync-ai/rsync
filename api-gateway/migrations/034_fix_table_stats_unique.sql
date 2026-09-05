-- Migration 034: Fix pipeline_run_table_stats uniqueness for UPSERT
--
-- Postgres ON CONFLICT cannot target expression indexes directly.
-- We normalize by enforcing execution_id NOT NULL and using a regular unique index:
--   UNIQUE(pipeline_id, execution_id, qualified_name)
--
-- For CDC table stats, emitters should set execution_id=pipeline_id (stable per pipeline).

BEGIN;

-- Backfill existing rows with NULL execution_id (CDC) to pipeline_id
UPDATE pipeline_run_table_stats
SET execution_id = pipeline_id
WHERE execution_id IS NULL;

-- Drop the expression-based unique index (cannot be used by ON CONFLICT column list)
DROP INDEX IF EXISTS idx_pipeline_run_table_stats_unique;

-- Enforce NOT NULL so (pipeline_id, execution_id, qualified_name) is truly unique
ALTER TABLE pipeline_run_table_stats
ALTER COLUMN execution_id SET NOT NULL;

-- Recreate as normal unique index for ON CONFLICT (pipeline_id, execution_id, qualified_name)
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_run_table_stats_unique
ON pipeline_run_table_stats(pipeline_id, execution_id, qualified_name);

COMMIT;

