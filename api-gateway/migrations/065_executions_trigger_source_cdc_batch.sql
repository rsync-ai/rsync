-- 065_executions_trigger_source_cdc_batch.sql
-- Allow 'cdc' and 'batch' as trigger_source values on the executions table.
--
-- Bug this fixes: migration 024 added
--     CHECK (trigger_source IN ('manual', 'scheduled'))
-- but the kafka-mcp-sink worker best-effort-inserts an executions row with
-- trigger_source='cdc' (kafka-sink-worker/main.go ensureExecutionRowPG) so the
-- transform_execution_logs FK to executions can bind for CDC writes
-- (execution_id == pipeline_id). That INSERT violated the check constraint:
--     pq: new row for relation "executions" violates check constraint
--         "executions_trigger_source_check"
-- The INSERT is ON CONFLICT DO NOTHING + logged as a warning, so it did not
-- crash the sink — but the executions bookkeeping row was never created, which
-- can later orphan transform_execution_logs and skews execution history.
--
-- Fix: widen the allowed set to include the two pipeline execution modes the
-- system actually triggers programmatically — 'cdc' (streaming sink) and
-- 'batch' (one-time/initial load) — alongside the existing user/schedule values.
-- The constraint name is the one Postgres auto-assigned to 024's inline CHECK.

ALTER TABLE executions DROP CONSTRAINT IF EXISTS executions_trigger_source_check;

ALTER TABLE executions
    ADD CONSTRAINT executions_trigger_source_check
    CHECK (trigger_source IN ('manual', 'scheduled', 'cdc', 'batch'));

COMMENT ON COLUMN executions.trigger_source IS 'How this execution was triggered: manual (user clicked run), scheduled (Temporal schedule), cdc (kafka-mcp-sink streaming write), or batch (one-time/initial load).';
