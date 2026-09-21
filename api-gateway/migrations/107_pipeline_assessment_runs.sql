-- 107_pipeline_assessment_runs.sql
-- History behind the pipeline Assessment tab.
--
-- One row per pre-migration assessment the gateway builds: a manual "Run
-- assessment", the gate inside RunPipeline, or the periodic re-check of a
-- running CDC pipeline. The tab lists these rows and diffs each run against
-- the one before it to tell the owner, once, about a new Critical or High
-- issue.
--
-- How this differs from 057 pipeline_assessments: that table is written by
-- the orchestrator and holds only ITS source-readiness checks. This one holds
-- the report the user actually sees — those checks merged with the gateway's
-- per-table findings (primary keys, destination auto-create) and destination
-- namespace probes, each graded Critical / High / Medium / Low.

CREATE TABLE IF NOT EXISTS pipeline_assessment_runs (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id   UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    -- manual    — "Run assessment" on the tab (POST /pipelines/:id/assess)
    -- run_gate  — the check RunPipeline performs before dispatching a run
    -- scheduled — the periodic re-check of a running CDC pipeline
    trigger       TEXT NOT NULL CHECK (trigger IN ('manual', 'run_gate', 'scheduled')),
    -- NULL for scheduled runs, and after the user who ran it is deleted.
    triggered_by  UUID REFERENCES users(id) ON DELETE SET NULL,
    -- TRUE when a Critical issue would stop the pipeline from starting.
    blocking      BOOLEAN NOT NULL DEFAULT FALSE,
    -- Failed + warning checks per level, for the history list without
    -- unpacking the report.
    critical_count INTEGER NOT NULL DEFAULT 0,
    high_count     INTEGER NOT NULL DEFAULT 0,
    medium_count   INTEGER NOT NULL DEFAULT 0,
    low_count      INTEGER NOT NULL DEFAULT 0,
    passed_count   INTEGER NOT NULL DEFAULT 0,
    -- The full AssessmentReport, including its graded "checks" list.
    report        JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The tab's only queries: latest run, and the history newest first.
CREATE INDEX IF NOT EXISTS idx_pipeline_assessment_runs_pipeline_created
    ON pipeline_assessment_runs (pipeline_id, created_at DESC);
