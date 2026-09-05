-- Migration 090: make a CDC pipeline's logs joinable to its stats
--
-- pipeline_run_table_stats.execution_id is NOT the execution id the orchestrator
-- minted for a CDC run. The sink forces execution_id = pipeline_id on the CDC lane
-- (parseCDCMessage), and event_projector.go re-asserts the same normalization, so
-- that the captured-side counters (orchestrator cdcstats agent) and the applied-side
-- counters (kafka-mcp-sink) upsert into ONE row. That convention is load-bearing:
-- migration 034 makes (pipeline_id, execution_id, qualified_name) unique, and two
-- hard FKs hang off the matching executions row (transform_execution_logs, migration
-- 045; pipeline_batch_acks, migration 043).
--
-- The cost is that the id every sink log line carries -- the real orchestration
-- execution -- appears in no stats row at all, so answering "which run produced these
-- numbers?" for a CDC pipeline requires knowing the convention exists. This column
-- records it.
--
-- ADDED, never substituted, for the same reason migration 089 added the destination
-- name instead of correcting qualified_name: execution_id is the cross-producer
-- conflict key. Rewriting it would stop the two producers colliding and split every
-- CDC table into two half-filled rows instead of correcting a label.
--
-- NULL means "the emitter had nothing to add": the batch lane (where execution_id
-- already IS the orchestration id), a sink that was never told one, or a sink older
-- than this change. All three must leave a stored value intact -- see the COALESCE in
-- the projector's ON CONFLICT clause, which exists because the cdcstats agent upserts
-- the same row on every tick and does not know this id.
--
-- Note: our migration runner already wraps each migration in a transaction, so this
-- file must not include BEGIN/COMMIT statements.

ALTER TABLE pipeline_run_table_stats
ADD COLUMN IF NOT EXISTS orchestration_execution_id UUID NULL;

COMMENT ON COLUMN pipeline_run_table_stats.orchestration_execution_id IS
'The execution id the orchestrator minted for this run, for correlation only. Differs from execution_id on the CDC lane, where execution_id is forced to pipeline_id so two producers share one row. NULL on batch (nothing to add) and on pre-090 sinks.';

-- Partial: only CDC rows ever carry a value, and the query this serves is
-- "find the stats for orchestration execution X".
CREATE INDEX IF NOT EXISTS idx_table_stats_orchestration_execution
ON pipeline_run_table_stats (orchestration_execution_id)
WHERE orchestration_execution_id IS NOT NULL;
