-- Schema baselines: per-pipeline snapshot of each SELECTED source table's shape.
-- Written by the executor batch path (backend-orchestrator/internal/agents/executor/
-- schema_drift.go) after a successful DiscoverSchema, then diffed against the live
-- discovery on the next run to detect drift for the drift-detect -> approve loop.
-- Entirely gated behind RSYNC_SCHEMA_DRIFT_ENABLED on the writer side; this table is
-- additive and inert until that flag is on. Mirrors the conventions of
-- 051_schema_evolution.sql (no explicit BEGIN/COMMIT; the runner wraps each file).

CREATE TABLE IF NOT EXISTS schema_baselines (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id   UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    schema_name   TEXT NOT NULL DEFAULT '',          -- '' when the connector omits schema
    table_name    TEXT NOT NULL,
    baseline      JSONB NOT NULL,                     -- TableMetadata JSON: columns(name/type/nullable/is_primary_key) + primary_keys
    source_type   TEXT,                               -- connector type at capture (e.g. postgresql, mysql)
    execution_id  TEXT,                               -- TEXT (not UUID): the run id is best-effort and may be empty
    captured_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, schema_name, table_name)
);

CREATE INDEX IF NOT EXISTS idx_schema_baselines_pipeline ON schema_baselines(pipeline_id);
