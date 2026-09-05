-- Migration 035: Add read_rows to pipeline_run_table_stats
--
-- We track both source read rows and destination written rows.
-- Existing inserted_rows is treated as "written rows" (destination-acknowledged).
--
-- Note: migration runner wraps in a transaction; do not add BEGIN/COMMIT.

ALTER TABLE pipeline_run_table_stats
ADD COLUMN IF NOT EXISTS read_rows BIGINT NULL;

