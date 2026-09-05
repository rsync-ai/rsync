-- Migration 089: record WHERE the rows landed, alongside where they came from
--
-- pipeline_run_table_stats.schema_name / qualified_name describe the table as the
-- emitter saw it, which for CDC is the SOURCE-side name: the sink overwrites the
-- table with "<source schema>.<source table>" from the Debezium envelope, so a
-- MySQL->Postgres pipeline whose rows landed in rsync_verify_cdc reported schema
-- "pipeline_test" (the MySQL source database). Reading the stats to answer "where
-- did my data go?" returned the wrong half of the pipeline.
--
-- The destination name is ADDED, not substituted, because qualified_name is a
-- cross-producer correlation key: migration 034 makes
-- (pipeline_id, execution_id, qualified_name) unique, and TWO producers upsert the
-- same row -- the kafka-mcp-sink (applied counters) and the orchestrator's cdcstats
-- agent (captured counters, named from payload.source.schema). Rewriting
-- qualified_name on one of them would stop the collision and split every CDC table
-- into two half-filled rows instead of correcting a label.
--
-- NULL means "the emitter could not name a destination namespace" -- an object-storage
-- destination (where db_or_schema is a bronze-layout path segment holding the SOURCE
-- schema), a pipeline with no user-selected namespace, or a sink older than this
-- change. It is deliberately distinguishable from an empty string.
--
-- Note: our migration runner already wraps each migration in a transaction, so
-- this file must not include BEGIN/COMMIT statements.

ALTER TABLE pipeline_run_table_stats
ADD COLUMN IF NOT EXISTS destination_schema TEXT NULL,
ADD COLUMN IF NOT EXISTS destination_qualified_name TEXT NULL;

COMMENT ON COLUMN pipeline_run_table_stats.destination_schema IS
'Destination database/schema the rows were written into. NULL when the emitter cannot name one (object-storage destination, no configured namespace, or a pre-089 sink). schema_name remains the SOURCE-side name.';

COMMENT ON COLUMN pipeline_run_table_stats.destination_qualified_name IS
'destination_schema.table_name. NULL under the same conditions as destination_schema.';
