-- Migration 095: rebuild a model when a pipeline finishes, instead of at a time.
--
-- Migration 085 gave a model a clock. A clock is the wrong instrument for the thing
-- users actually asked for. The model reads tables that a pipeline writes, so the
-- honest trigger is "when that pipeline finishes", and the only way to say that with
-- a cron is to guess a time far enough after the pipeline usually lands. That guess
-- is wrong in both directions and neither failure announces itself: too early and the
-- model rebuilds from yesterday's data and reports success, too late and the dashboard
-- is stale for hours the user paid for by padding the gap.
--
-- So schedule_type gains a third value that is not a cadence at all. Everything else
-- about the row is unchanged: same run-as identity re-resolved per fire, same
-- auto-pause, same one-live-schedule-per-query rule. Only the thing that wakes it up
-- is different.
--
-- Consequence of that last rule (idx_sq_schedules_unique_query, 085): a model has a
-- clock OR an upstream, never both. That is deliberate for V1 — two independent
-- triggers on one target table race each other into the same DROP/CREATE, and the
-- advisory lock in acquireModelRunLock would turn that race into a silently skipped
-- rebuild rather than an error anyone sees.

-- ---------------------------------------------------------------------------
-- Part A — the new schedule_type
-- ---------------------------------------------------------------------------

-- Named inline in 085's CREATE TABLE, so Postgres generated this name.
ALTER TABLE saved_query_schedules
    DROP CONSTRAINT IF EXISTS saved_query_schedules_schedule_type_check;

ALTER TABLE saved_query_schedules
    ADD CONSTRAINT saved_query_schedules_schedule_type_check
    CHECK (schedule_type IN ('cron', 'interval', 'after_pipeline'));

-- The upstream. ON DELETE CASCADE, and the alternative is worse than it looks:
-- SET NULL would leave an after_pipeline row with no pipeline to fire from, which
-- the CHECK below forbids — so Postgres would refuse the pipeline DELETE itself,
-- and deleting a pipeline would fail with a constraint error naming a saved query
-- the operator has never heard of. CASCADE keeps the pipeline deletable; the model
-- survives and simply shows as unscheduled, which is what it is.
ALTER TABLE saved_query_schedules
    ADD COLUMN IF NOT EXISTS trigger_pipeline_id UUID REFERENCES pipelines(id) ON DELETE CASCADE;

-- ---------------------------------------------------------------------------
-- Part B — the two shapes, kept from wearing each other's columns
-- ---------------------------------------------------------------------------
--
-- 085 declared temporal_schedule_id NOT NULL because every schedule was a Temporal
-- schedule. An event trigger has none — nothing is registered with Temporal, because
-- nothing is waiting for a time. Rather than drop the NOT NULL outright and lose the
-- guarantee for the rows that DO need it, the requirement moves into a conditional
-- constraint: still mandatory for a clock, forbidden for an upstream.
--
-- Writing it as one constraint per direction (rather than one big OR) means a
-- violation names which half is wrong.

ALTER TABLE saved_query_schedules
    ALTER COLUMN temporal_schedule_id DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'saved_query_schedules_clock_needs_temporal'
    ) THEN
        ALTER TABLE saved_query_schedules
            ADD CONSTRAINT saved_query_schedules_clock_needs_temporal
            CHECK (
                schedule_type = 'after_pipeline'
                OR (temporal_schedule_id IS NOT NULL AND trigger_pipeline_id IS NULL)
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'saved_query_schedules_trigger_needs_pipeline'
    ) THEN
        ALTER TABLE saved_query_schedules
            ADD CONSTRAINT saved_query_schedules_trigger_needs_pipeline
            CHECK (
                schedule_type != 'after_pipeline'
                OR (trigger_pipeline_id IS NOT NULL AND temporal_schedule_id IS NULL)
            );
    END IF;
END $$;

-- The fire path's only lookup: given the pipeline that just completed, which live
-- triggers point at it? Partial on both columns because the overwhelming majority of
-- rows are clock schedules with a NULL here, and a paused trigger must not fire.
CREATE INDEX IF NOT EXISTS idx_sq_schedules_trigger_pipeline
ON saved_query_schedules(trigger_pipeline_id)
WHERE trigger_pipeline_id IS NOT NULL AND status = 'active';

-- ---------------------------------------------------------------------------
-- Part C — run history has a third door
-- ---------------------------------------------------------------------------
--
-- 086 named the two ways a run could start. 'triggered' is genuinely a third and not
-- a flavour of 'scheduled': the audit question a user asks of an event-driven model
-- is "which pipeline run caused this rebuild", and collapsing it into 'scheduled'
-- would answer "a clock did", which is false.

ALTER TABLE saved_query_runs
    DROP CONSTRAINT IF EXISTS saved_query_runs_trigger_source_check;

ALTER TABLE saved_query_runs
    ADD CONSTRAINT saved_query_runs_trigger_source_check
    CHECK (trigger_source IN ('manual', 'scheduled', 'triggered'));

COMMENT ON COLUMN saved_query_schedules.trigger_pipeline_id IS 'Upstream pipeline for schedule_type=after_pipeline; NULL for cron/interval, enforced both ways by CHECK';
COMMENT ON COLUMN saved_query_schedules.temporal_schedule_id IS 'Temporal Schedule ID for cron/interval; NULL for after_pipeline, which is woken by an event rather than a clock';
