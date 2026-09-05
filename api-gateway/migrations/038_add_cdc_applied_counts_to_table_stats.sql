-- Migration 038: Add applied CDC counters to pipeline_run_table_stats
--
-- We already store CDC "captured" counts (inserts/updates/deletes/total_events/last_event_ts)
-- from Debezium events. This migration adds destination "applied" counters so the UI can
-- show DMS-like "incoming vs applied" per table.
--
-- Applied counts are incremented only after successful destination writes (kafka-mcp-sink ACK),
-- and can be projected from TABLE_STATS events emitted by kafka-mcp-sink.
--
-- Note: migration runner wraps in a transaction; do not add BEGIN/COMMIT.

ALTER TABLE pipeline_run_table_stats
ADD COLUMN IF NOT EXISTS applied_inserts BIGINT NULL,
ADD COLUMN IF NOT EXISTS applied_updates BIGINT NULL,
ADD COLUMN IF NOT EXISTS applied_deletes BIGINT NULL,
ADD COLUMN IF NOT EXISTS applied_total_events BIGINT NULL,
ADD COLUMN IF NOT EXISTS last_applied_ts TIMESTAMPTZ NULL;

-- Optional: index for "freshness" dashboards
CREATE INDEX IF NOT EXISTS idx_pipeline_run_table_stats_cdc_last_applied
ON pipeline_run_table_stats(last_applied_ts DESC)
WHERE mode = 'cdc';

