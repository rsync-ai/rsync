-- Migration 033: Pipeline run table statistics
--
-- DMS-like per-table statistics for batch and CDC pipelines.
-- Batch: inserts, bytes, files per table.
-- CDC: inserts/updates/deletes per table from Debezium events.
--
-- Note: our migration runner already wraps each migration in a transaction, so
-- this file must not include BEGIN/COMMIT statements.

CREATE TABLE IF NOT EXISTS pipeline_run_table_stats (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    execution_id UUID NULL,
    
    -- Table identification
    schema_name TEXT NULL,
    table_name TEXT NOT NULL,
    qualified_name TEXT NOT NULL, -- schema.table or table (for queries/joins)
    
    -- Mode (batch vs cdc)
    mode TEXT NOT NULL CHECK (mode IN ('batch', 'cdc')),
    
    -- Status (lifecycle)
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'degraded')),
    
    -- Batch counters (null for CDC)
    inserted_rows BIGINT NULL,
    bytes_read BIGINT NULL,
    files_written BIGINT NULL,
    
    -- CDC counters (null for batch)
    inserts BIGINT NULL,
    updates BIGINT NULL,
    deletes BIGINT NULL,
    total_events BIGINT NULL,
    last_event_ts TIMESTAMPTZ NULL,
    
    -- Metadata
    started_at TIMESTAMPTZ NULL,
    completed_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Primary lookup: get all tables for a pipeline/execution
CREATE INDEX IF NOT EXISTS idx_pipeline_run_table_stats_pipeline_exec
ON pipeline_run_table_stats(pipeline_id, execution_id, qualified_name);

-- Execution-scoped queries (most common)
CREATE INDEX IF NOT EXISTS idx_pipeline_run_table_stats_exec_updated
ON pipeline_run_table_stats(execution_id, updated_at DESC)
WHERE execution_id IS NOT NULL;

-- Pipeline-scoped queries (aggregate across runs)
CREATE INDEX IF NOT EXISTS idx_pipeline_run_table_stats_pipeline_updated
ON pipeline_run_table_stats(pipeline_id, updated_at DESC);

-- Search by table name
CREATE INDEX IF NOT EXISTS idx_pipeline_run_table_stats_table_name
ON pipeline_run_table_stats(table_name);

-- CDC freshness queries
CREATE INDEX IF NOT EXISTS idx_pipeline_run_table_stats_cdc_last_event
ON pipeline_run_table_stats(last_event_ts DESC)
WHERE mode = 'cdc';

-- Unique constraint: one row per (pipeline, execution, table)
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_run_table_stats_unique
ON pipeline_run_table_stats(pipeline_id, COALESCE(execution_id::text, 'NULL'), qualified_name);
