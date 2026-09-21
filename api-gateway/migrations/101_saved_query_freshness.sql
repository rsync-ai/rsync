-- Migration 101: a freshness deadline on a model, and a durable record of every
-- time one was missed.
--
-- Every trigger this subsystem has fires on something HAPPENING: a pipeline
-- completing (095), another model finishing (100), a clock ticking (085). None of
-- them can fire on something NOT happening, and that is the failure this table
-- exists for. A model whose last producer was deleted keeps an active schedule with
-- an empty upstream set, never fires, never auto-pauses, and renders on the
-- schedules page as "After an upstream runs" -- so it reads as scheduled forever
-- while its table rots. Nothing in the product can currently say so.
--
-- The deadline lives on saved_queries, not on saved_query_schedules, because
-- freshness is a property of the TABLE rather than of the mechanism that happens to
-- maintain it. Detaching a schedule is precisely the moment a target starts going
-- stale unnoticed, and a deadline that vanished with the schedule would go quiet
-- exactly then. It also means a model with no schedule at all can still declare one.
--
-- Breaches are rows rather than a boolean on saved_queries for the reason migration
-- 086 gave for run history: a model that goes stale for six hours every night and
-- recovers by morning is invisible to anything computed at read time. By the time
-- someone looks it is either still stale, with no record of since when, or it
-- recovered and nobody ever knew. The row carries the timestamp; a flag cannot.
--
-- No workspace_id column, matching saved_query_schedules (085) and saved_query_runs
-- (086): tenancy is inherited through saved_query_id, and every read path reaches
-- this table by joining a saved_queries row the caller is already authorized against.


-- How old this model's table is allowed to get, in seconds. NULL means nobody has
-- made a promise about it, which is the default and stays the default -- this
-- feature is opt-in per model, because a deadline nobody chose would turn every
-- unscheduled saved query in the workspace into an alert on the day it shipped.
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS freshness_deadline_seconds INTEGER;

-- 60 seconds floor, equal to the sweep interval that checks this column. A deadline
-- shorter than the gap between two checks cannot be honoured BY THE CHECKER, so every
-- breach it produced would be a report about how often we look rather than about the
-- data. 31536000 ceiling (one year) catches a caller who sent milliseconds -- a
-- deadline of 21600000 "seconds" would silently never fire, which is the same class of
-- silent-wrong-direction bug the trigger docs warn about: wrong in the quiet direction.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'saved_queries_freshness_deadline_range'
    ) THEN
        ALTER TABLE saved_queries
            ADD CONSTRAINT saved_queries_freshness_deadline_range
            CHECK (freshness_deadline_seconds IS NULL
                   OR (freshness_deadline_seconds >= 60
                       AND freshness_deadline_seconds <= 31536000));
    END IF;
END $$;

COMMENT ON COLUMN saved_queries.freshness_deadline_seconds IS
'Opt-in staleness budget for a materialized model, measured from its last SUCCEEDED run; NULL means no promise';


CREATE TABLE IF NOT EXISTS saved_query_freshness_breaches (
    breach_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    saved_query_id UUID NOT NULL REFERENCES saved_queries(id) ON DELETE CASCADE,

    -- The deadline as it stood when the breach opened. Stored rather than read back
    -- through saved_queries because editing the deadline must not rewrite history:
    -- a breach against a 1-hour promise stays a breach against a 1-hour promise even
    -- after someone widens it to a day to stop the alerts.
    deadline_seconds INTEGER NOT NULL,

    -- WHY nothing rebuilt it. This is the load-bearing column: "stale" alone sends an
    -- operator to look at a model, and the cause tells them whether anything is
    -- actually broken. 'schedule_paused' is a decision somebody made and is not a
    -- fault; 'overdue' means an active schedule should have fired and did not;
    -- 'no_upstreams' is the empty-set hole above, which no other signal in the
    -- product can currently distinguish from a healthy event-triggered model.
    cause TEXT NOT NULL CHECK (cause IN (
        'overdue', 'no_schedule', 'schedule_paused', 'schedule_auto_paused', 'no_upstreams'
    )),

    -- The instant staleness was measured from: the finish of the last SUCCEEDED run,
    -- or the model's creation when there has never been one. Deliberately NOT
    -- saved_queries.last_run_at -- that column is stamped on every outcome including
    -- failures, so a model whose rebuild fails hourly would read as an hour fresh
    -- forever, which is the exact shape of silent staleness this table exists to end.
    reference_at TIMESTAMPTZ NOT NULL,

    -- True when the model has never had a succeeded run, so reference_at is its
    -- created_at. Kept as its own column because the two cases read very differently
    -- to an operator: "stale since 03:00" is a pipeline that stopped, "never built"
    -- is a model that was configured and then forgotten.
    never_succeeded BOOLEAN NOT NULL DEFAULT FALSE,

    -- How far past the deadline the model was when the breach opened. Not maintained
    -- as it ages -- current staleness is derivable from reference_at, and a row that
    -- rewrites itself every sweep is no longer a record of an event.
    stale_seconds BIGINT NOT NULL,

    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- NULL while the model is still stale.
    --
    -- The three ways a breach ends are kept apart on purpose, because only one of
    -- them means the data got better. 'rebuilt' is a succeeded run landing, and is
    -- the only resolution that says the table is current. 'deadline_widened' is the
    -- same stale table under a promise somebody relaxed until it fit -- a legitimate
    -- action, and one that must never be recorded as a fix, or the way to clear an
    -- alert becomes moving the goalposts and the history would agree. It is told
    -- apart from a rebuild by comparing reference_at: a rebuild moves it forward, a
    -- widened deadline leaves it exactly where it was. 'no_longer_tracked' is the
    -- promise going away entirely (deadline cleared, or the model no longer
    -- materialized), which must close the breach too or it sits open forever against
    -- a model nobody is measuring any more.
    resolved_at TIMESTAMPTZ,
    resolution TEXT CHECK (resolution IN ('rebuilt', 'deadline_widened', 'no_longer_tracked'))
);

-- At most one OPEN breach per model. This is the deduplication, and it is the
-- database's job rather than the sweep's: the sweep runs every few minutes against a
-- model that may stay stale for days, so an application-side check would be a
-- read-then-write race that quietly accumulates thousands of rows for one outage.
CREATE UNIQUE INDEX IF NOT EXISTS idx_sq_freshness_one_open_per_query
ON saved_query_freshness_breaches(saved_query_id)
WHERE resolved_at IS NULL;

-- The workspace-wide "what is stale right now" read, newest first.
CREATE INDEX IF NOT EXISTS idx_sq_freshness_open
ON saved_query_freshness_breaches(detected_at DESC)
WHERE resolved_at IS NULL;

-- The per-model history panel: every breach this model has ever had, open or closed.
CREATE INDEX IF NOT EXISTS idx_sq_freshness_by_query
ON saved_query_freshness_breaches(saved_query_id, detected_at DESC);

-- The sweep's reference-point lookup: the newest succeeded run for one model. 086's
-- idx_saved_query_runs_status_finished is partial on status != 'succeeded', which is
-- exactly the rows this query does not want.
CREATE INDEX IF NOT EXISTS idx_saved_query_runs_succeeded
ON saved_query_runs(saved_query_id, finished_at DESC)
WHERE status = 'succeeded';

COMMENT ON TABLE saved_query_freshness_breaches IS
'One row per interval a model spent past its freshness deadline; open while resolved_at IS NULL';
COMMENT ON COLUMN saved_query_freshness_breaches.cause IS
'Why nothing rebuilt it: overdue|no_schedule|schedule_paused|schedule_auto_paused|no_upstreams -- recorded at detection and never rewritten';
COMMENT ON COLUMN saved_query_freshness_breaches.reference_at IS
'Finish of the last SUCCEEDED run, or the model created_at when there has never been one; never saved_queries.last_run_at, which counts failures';
