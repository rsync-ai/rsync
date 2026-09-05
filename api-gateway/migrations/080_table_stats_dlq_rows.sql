-- Migration 080: Add dlq_rows to pipeline_run_table_stats
--
-- A CDC record the destination genuinely cannot accept is parked in the sink's DLQ,
-- its Kafka offset is committed, and the worker moves on (per-row isolation,
-- kafka-sink-worker main.go flushBatchPerRow). That is the correct runtime behavior --
-- one poison row must not condemn the batch -- but until now the count died in sink
-- metrics: the row was never counted as captured and never reported as lost, so the
-- captured-vs-applied pair that exists to detect loss reconciled perfectly while the
-- source held one more row than the destination, with Failed 0 and a green badge.
--
-- dlq_rows is the missing term. It is deliberately NOT folded into read_rows or
-- inserted_rows: those mean "what landed", and adding a lost row to either would
-- restore the very reconciliation that hid the loss.
--
-- Note: migration runner wraps in a transaction; do not add BEGIN/COMMIT.

ALTER TABLE pipeline_run_table_stats
ADD COLUMN IF NOT EXISTS dlq_rows BIGINT NOT NULL DEFAULT 0;

-- Partial index: the only interesting query is "which tables are shedding rows?",
-- which is a vanishing fraction of rows in this table.
CREATE INDEX IF NOT EXISTS idx_pipeline_run_table_stats_dlq
ON pipeline_run_table_stats(pipeline_id, execution_id)
WHERE dlq_rows > 0;
