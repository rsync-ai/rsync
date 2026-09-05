-- Pre-flight Assessment Service (Pillar 1).
--
-- Pattern stolen from AWS DMS Premigration Assessment Runs: every CDC
-- pipeline should be assessed before it starts. The assessment checks
-- source DB configuration (wal_level, binlog_format, replication grants,
-- primary keys, etc.) and produces a structured result that:
--   1. Blocks pipeline start if STRICT_PREFLIGHT=true and any check fails
--   2. Warns the user with copy-pasteable fixes (via the StructuredError
--      envelope) when checks fail or warn
--   3. Persists for audit + retry (re-running an assessment is cheap)
--
-- Each row is one assessment run. A pipeline can have many runs over time
-- (initial check, retry after the user fixed wal_level, etc.). The latest
-- run determines whether the pipeline is currently allowed to start.

CREATE TABLE IF NOT EXISTS pipeline_assessments (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    -- Status of the overall run.
    --   pending     — created, not yet executed
    --   running     — executor is checking
    --   passed      — all checks passed (or only info-level findings)
    --   warning     — one or more warning checks (pipeline can run, with caveats)
    --   failed      — one or more error checks (pipeline blocked by STRICT_PREFLIGHT)
    --   error       — the assessment itself crashed (DB unreachable, etc.)
    --   cancelled   — operator cancelled before completion
    status          TEXT NOT NULL DEFAULT 'pending',
    -- Source connector type at time of assessment. Captured separately from
    -- the pipeline row because the user might change connectors and we want
    -- the audit trail to be honest about what was assessed when.
    source_type     TEXT NOT NULL,
    source_connection_id  UUID,  -- nullable for pre-creation dry-runs
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    -- checks_json is an array of per-check records:
    --   [{code, severity, passed, message, remediation:{steps[], sql_to_run[], doc_url, estimated_minutes}}]
    -- Stored as JSONB so the UI can render without joining a per-check table —
    -- assessments are small (~10-25 checks per run) and read together.
    checks_json     JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Summary counts for cheap filtering / dashboards without unpacking JSON.
    passed_count    INTEGER NOT NULL DEFAULT 0,
    warning_count   INTEGER NOT NULL DEFAULT 0,
    failed_count    INTEGER NOT NULL DEFAULT 0,
    error_count     INTEGER NOT NULL DEFAULT 0,
    -- When STRICT_PREFLIGHT=true, this flag controls whether the pipeline
    -- can transition created→running. Persisted so re-running the same
    -- assessment row idempotently has a clear "is this still authoritative".
    blocks_start    BOOLEAN NOT NULL DEFAULT FALSE,
    -- Who triggered this run — for audit when assessments are run via API.
    triggered_by    UUID REFERENCES users(id) ON DELETE SET NULL
);

-- The most common query: "what's the latest assessment for this pipeline?"
CREATE INDEX IF NOT EXISTS idx_pipeline_assessments_pipeline_latest
    ON pipeline_assessments(pipeline_id, started_at DESC);

-- Status-filtered queries for the admin dashboard ("which pipelines have
-- failed assessments?").
CREATE INDEX IF NOT EXISTS idx_pipeline_assessments_status
    ON pipeline_assessments(status, started_at DESC);

COMMENT ON TABLE pipeline_assessments IS
'Pre-flight assessment runs (Pillar 1). Each row is one execution of the SourceAssessor for a pipeline. The latest row per pipeline is authoritative for whether the pipeline is allowed to start when STRICT_PREFLIGHT=true.';

COMMENT ON COLUMN pipeline_assessments.checks_json IS
'JSONB array of {code, severity, passed, message, remediation}. Mirrors the per-check shape returned by the SourceAssessor Go interface. Small enough to read inline (no per-check table needed).';
