-- Migration 086: append-only run history for saved-query models.
--
-- Migration 085 gave saved_queries three last_run_* columns. Those answer "how did
-- the most recent run go?" and nothing else — each run overwrites its predecessor.
-- That is enough for a status dot in a list and useless for the question a schedule
-- actually raises: a tick fired at 03:00 while nobody was watching, and by morning
-- the only surviving evidence is a green badge from the 04:00 tick that succeeded.
-- A schedule the user cannot audit is a schedule the user cannot trust.
--
-- So: one row per attempt, written on the shared outcome path that both the manual
-- and the scheduled caller already funnel through. The last_run_* columns stay —
-- they are the list view's cheap denormalized read, and dropping them would turn
-- every row render into a correlated subquery.
--
-- No workspace_id column, matching saved_query_schedules (085) and
-- pipeline_run_events (022): tenancy is inherited through saved_query_id, and every
-- read path reaches this table by joining a saved_queries row that the caller has
-- already been authorized against. A duplicated workspace_id would be a second
-- source of truth that can drift from the first.

CREATE TABLE IF NOT EXISTS saved_query_runs (
    run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    saved_query_id UUID NOT NULL REFERENCES saved_queries(id) ON DELETE CASCADE,

    -- NULL for a manual run. ON DELETE SET NULL rather than CASCADE: detaching a
    -- schedule is explicitly documented as leaving the target table alone, so it
    -- must leave the evidence of what that schedule did alone too.
    schedule_id UUID REFERENCES saved_query_schedules(schedule_id) ON DELETE SET NULL,

    -- Which door the run came through. Kept separate from schedule_id being NULL
    -- because that column goes NULL when a schedule is deleted, and a run that was
    -- once scheduled must not retroactively read as a human action.
    trigger_source TEXT NOT NULL CHECK (trigger_source IN ('manual', 'scheduled')),

    -- Mirrors modelRunResult.Status. 'skipped' is a refusal (wrong statement class,
    -- missing target, unusable connection) and is NOT a failure of the SQL.
    status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'skipped')),

    statement_class TEXT NOT NULL DEFAULT '',
    target_table TEXT NOT NULL DEFAULT '',
    rows_affected BIGINT,
    error TEXT,

    -- Non-empty only when the run tripped a condition that will still be true next
    -- tick, which is what turns a failure into an auto-pause. Stored so the UI can
    -- explain WHY a schedule stopped itself without reconstructing it from logs.
    auto_pause_reason TEXT,

    -- The identity the statement executed as: the caller for a manual run, the
    -- schedule's run_as_user_id for a scheduled one.
    ran_as_user_id UUID,

    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The per-query history panel: newest attempt first, for one query.
CREATE INDEX IF NOT EXISTS idx_saved_query_runs_query_finished
ON saved_query_runs(saved_query_id, finished_at DESC);

-- The cross-workspace "what broke overnight" sweep, which filters on outcome before
-- it filters on query.
CREATE INDEX IF NOT EXISTS idx_saved_query_runs_status_finished
ON saved_query_runs(status, finished_at DESC)
WHERE status != 'succeeded';

COMMENT ON TABLE saved_query_runs IS 'Append-only attempt history for saved-query models; the auditable counterpart to the denormalized saved_queries.last_run_* columns';
COMMENT ON COLUMN saved_query_runs.trigger_source IS 'manual|scheduled — durable even after schedule_id is nulled by a schedule delete';
COMMENT ON COLUMN saved_query_runs.auto_pause_reason IS 'Set when the outcome was a stable condition that auto-paused the schedule; empty for a retryable SQL failure';
COMMENT ON COLUMN saved_query_runs.ran_as_user_id IS 'Identity the statement executed as; for scheduled runs this is the schedule run_as_user_id resolved at fire time';
