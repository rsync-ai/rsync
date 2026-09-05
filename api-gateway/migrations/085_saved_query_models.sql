-- Migration 085: turn a saved query into a MODEL — a query that materializes to a
-- table on a schedule.
--
-- Two objects, deliberately not one:
--
--   1. Materialization is an ATTRIBUTE of the saved query (columns below). A model
--      is "this query, written to this table."
--   2. A schedule is its own durable object with its own lifecycle and a 1:1 mapping
--      to a Temporal Schedule ID — exactly the shape migration 023 established for
--      pipeline_schedules. Mirroring it keeps one schedule pattern in the codebase
--      instead of two.
--
-- The run-as identity lives on the SCHEDULE, not the query: a schedule is the only
-- thing that fires with no human present, so it is the only thing that needs a
-- stored identity to run as. A manual run already has the caller's session.

-- ---------------------------------------------------------------------------
-- Part A — model columns on saved_queries
-- ---------------------------------------------------------------------------

-- 'none' keeps every existing row a plain saved query. 'table' is CREATE TABLE AS.
-- 'incremental' is deliberately ABSENT: it needs a merge key and a watermark, which
-- is real design work, and shipping the enum value before the behaviour would let a
-- user select a mode that silently does a full rebuild.
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS materialization TEXT NOT NULL DEFAULT 'none';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'saved_queries_materialization_check'
    ) THEN
        ALTER TABLE saved_queries
            ADD CONSTRAINT saved_queries_materialization_check
            CHECK (materialization IN ('none', 'table'));
    END IF;
END $$;

-- Destination identifier, either "table" or "schema.table". Validated in Go against a
-- strict identifier pattern before it is ever interpolated into SQL — identifiers
-- cannot be bound as query parameters, so this column is an injection surface and the
-- application layer is the only place that can guard it.
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS target_table TEXT;

-- target_owned records that a run of THIS model successfully created target_table.
--
-- It is the entire safety argument for the replace path. Run 1 issues a bare
-- CREATE TABLE <target> AS <sql> with no IF NOT EXISTS, so the database itself
-- rejects the run when the name is already taken — rsync never has to ask "who owns
-- this table?", the engine answers it. Only after that create succeeds does this flip
-- true, and only then may a later run issue DROP TABLE. So a DROP is only ever aimed
-- at a table this model provably created.
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS target_owned BOOLEAN NOT NULL DEFAULT FALSE;

-- Outcome of the most recent run, for the list UI. last_run_at already exists (084).
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS last_run_status TEXT;
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS last_run_error TEXT;

-- A materialized model must name its destination. Enforced here rather than only in
-- Go so a direct DB write cannot produce a model that runs with a NULL target.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'saved_queries_target_table_required'
    ) THEN
        ALTER TABLE saved_queries
            ADD CONSTRAINT saved_queries_target_table_required
            CHECK (materialization = 'none' OR NULLIF(TRIM(target_table), '') IS NOT NULL);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Part B — saved_query_schedules (mirrors pipeline_schedules, migration 023)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS saved_query_schedules (
    schedule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    saved_query_id UUID NOT NULL REFERENCES saved_queries(id) ON DELETE CASCADE,

    schedule_type TEXT NOT NULL CHECK (schedule_type IN ('cron', 'interval')),
    schedule_spec JSONB NOT NULL,

    temporal_schedule_id TEXT UNIQUE NOT NULL,

    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'deleted')),

    -- The identity every unattended fire re-resolves. NOT a snapshot of permissions:
    -- the run path reads this user's CURRENT workspace role at each fire and
    -- auto-pauses if it no longer clears the statement's required role. Storing the
    -- role here instead would make offboarding invisible to the scheduler.
    run_as_user_id UUID NOT NULL,

    created_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- A human pause (operator clicked Pause) is distinct from a machine pause (the
    -- run-as identity stopped clearing the bar). Two columns, because collapsing them
    -- would let a resume click silently clear a security stop.
    paused_at TIMESTAMPTZ,
    paused_reason TEXT,
    auto_paused_at TIMESTAMPTZ,
    auto_paused_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_sq_schedules_query ON saved_query_schedules(saved_query_id);
CREATE INDEX IF NOT EXISTS idx_sq_schedules_status ON saved_query_schedules(status);
CREATE INDEX IF NOT EXISTS idx_sq_schedules_temporal ON saved_query_schedules(temporal_schedule_id);

-- One live schedule per saved query, matching pipeline_schedules' V1 rule.
CREATE UNIQUE INDEX IF NOT EXISTS idx_sq_schedules_unique_query
ON saved_query_schedules(saved_query_id)
WHERE status != 'deleted';

DROP TRIGGER IF EXISTS update_saved_query_schedules_updated_at ON saved_query_schedules;
CREATE TRIGGER update_saved_query_schedules_updated_at
BEFORE UPDATE ON saved_query_schedules
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE saved_query_schedules IS 'Durable schedules for saved-query models, managed via the Temporal Schedules API (mirrors pipeline_schedules)';
COMMENT ON COLUMN saved_query_schedules.run_as_user_id IS 'Identity each unattended fire re-resolves against workspace_members; never a permission snapshot';
COMMENT ON COLUMN saved_query_schedules.auto_paused_reason IS 'Set by the run path when the run-as identity no longer clears the statement class; distinct from an operator pause';
COMMENT ON COLUMN saved_queries.target_owned IS 'True once a run of this model created target_table; the sole precondition for the replace path issuing DROP TABLE';
