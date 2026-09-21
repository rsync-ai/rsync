-- Migration 100: fan-in for model triggers, and a model that can wake another model.
--
-- Migration 095 gave a model exactly one upstream: a single pipeline, held in
-- saved_query_schedules.trigger_pipeline_id. Two things were wrong with that shape.
--
-- A model reading tables from three pipelines could be wired to one of them. The asset
-- graph already reports the other two as uncovered upstreams -- producers a model
-- depends on that do not refresh it -- and no configuration existed that could close
-- the gap. And a model could never be woken by another model, so a staging model
-- feeding a reporting model had to be joined by a cron guess about when the first one
-- finishes.
--
-- Both fall out of the same fix: the upstream set moves to a child table, and the
-- schedule row stays what it always was, the model's one wake-up policy. That keeps
-- idx_sq_schedules_unique_query (085) intact, so every route that addresses a schedule
-- by saved-query id keeps working unchanged, and it keeps one refresh workflow per
-- model in the adapter -- which is what makes a burst of upstream completions coalesce
-- into one rebuild instead of N.
--
-- Correction to 095's header, recorded here rather than by editing it. That file
-- explains the one-upstream rule by saying two triggers on one target table "race each
-- other into the same DROP/CREATE". They do not: acquireModelRunLock takes a Postgres
-- advisory lock keyed on the model before any DDL runs, so a second concurrent rebuild
-- is skipped, not interleaved. The real cost of fan-in is a DROPPED rebuild, which the
-- refresh workflow's coalescing absorbs. 095 stays as written; a migration is a record
-- of what ran, not a document to keep current.
--
-- What this migration gives up. 095 could state "an event trigger has an upstream" as a
-- CHECK, because the upstream was a column on the same row. A cross-table requirement
-- is not expressible as a CHECK, so that rule is now enforced in three weaker places:
-- the schedule row and its upstream rows are written in one transaction, the create and
-- update handlers refuse an empty set, and the fire-time lookup requires an upstream row
-- to exist before it will run anything. The last one is the one that matters, because it
-- is the only one a row written by some future path cannot get around.

-- ---------------------------------------------------------------------------
-- Part A -- the upstream set
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS saved_query_schedule_upstreams (
    -- uuid_generate_v4 rather than gen_random_uuid, matching the table this one hangs
    -- off (085 saved_query_schedules).
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    schedule_id UUID NOT NULL
        REFERENCES saved_query_schedules(schedule_id) ON DELETE CASCADE,

    -- Stored rather than inferred from which of the two id columns is set. A reader
    -- that has to work that out gets it wrong once and then reports a model trigger as
    -- a pipeline trigger forever; the CHECK below is what keeps the two in step.
    upstream_kind TEXT NOT NULL CHECK (upstream_kind IN ('pipeline', 'model')),

    -- ON DELETE CASCADE on both, for the reason 095 gives for the column it replaces:
    -- SET NULL would leave a row that no longer names anything, and RESTRICT would make
    -- deleting a pipeline fail with a constraint error naming a saved query the operator
    -- has never heard of. CASCADE keeps the producer deletable; the model survives and
    -- simply stops being woken by it.
    upstream_pipeline_id UUID REFERENCES pipelines(id) ON DELETE CASCADE,
    upstream_saved_query_id UUID REFERENCES saved_queries(id) ON DELETE CASCADE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT saved_query_schedule_upstreams_kind_matches_column CHECK (
        (upstream_kind = 'pipeline'
             AND upstream_pipeline_id IS NOT NULL
             AND upstream_saved_query_id IS NULL)
        OR
        (upstream_kind = 'model'
             AND upstream_saved_query_id IS NOT NULL
             AND upstream_pipeline_id IS NULL)
    )
);

-- Identity. Two partial indexes rather than one over both columns, because a UNIQUE
-- over (schedule_id, a, b) treats every NULL as distinct and would happily admit the
-- same upstream twice.
CREATE UNIQUE INDEX IF NOT EXISTS idx_sq_upstreams_unique_pipeline
ON saved_query_schedule_upstreams(schedule_id, upstream_pipeline_id)
WHERE upstream_pipeline_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_sq_upstreams_unique_model
ON saved_query_schedule_upstreams(schedule_id, upstream_saved_query_id)
WHERE upstream_saved_query_id IS NOT NULL;

-- The fire path reads this table BY upstream -- "this pipeline just finished, which
-- models wake up?" -- which is the opposite direction from every other reader.
CREATE INDEX IF NOT EXISTS idx_sq_upstreams_by_pipeline
ON saved_query_schedule_upstreams(upstream_pipeline_id)
WHERE upstream_pipeline_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sq_upstreams_by_model
ON saved_query_schedule_upstreams(upstream_saved_query_id)
WHERE upstream_saved_query_id IS NOT NULL;

-- And the cycle check walks it BY schedule, downstream from a model.
CREATE INDEX IF NOT EXISTS idx_sq_upstreams_by_schedule
ON saved_query_schedule_upstreams(schedule_id);

-- ---------------------------------------------------------------------------
-- Part B -- carry 095's single upstream across
-- ---------------------------------------------------------------------------

-- Guarded on the column still existing so the file is safe to re-run after Part D has
-- dropped it. ON CONFLICT DO NOTHING makes a second pass a no-op rather than a
-- duplicate-key failure.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'saved_query_schedules'
          AND column_name = 'trigger_pipeline_id'
    ) THEN
        INSERT INTO saved_query_schedule_upstreams
            (schedule_id, upstream_kind, upstream_pipeline_id)
        SELECT s.schedule_id, 'pipeline', s.trigger_pipeline_id
        FROM saved_query_schedules s
        WHERE s.trigger_pipeline_id IS NOT NULL
        ON CONFLICT DO NOTHING;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Part C -- one event schedule_type, named for what it now means
-- ---------------------------------------------------------------------------
--
-- 'after_pipeline' stops being true the moment an upstream can be a model, and a value
-- that lies is worse than a rename: every reader that branches on it keeps compiling
-- and keeps reading correctly right up until someone believes the name.
--
-- Every constraint that mentions the old value comes off BEFORE the UPDATE, not after.
-- 095's clock_needs_temporal reads "schedule_type = 'after_pipeline' OR
-- temporal_schedule_id IS NOT NULL", so renaming the value out from under it turns
-- every existing event trigger -- which has no Temporal id by construction -- into a
-- violation, and the UPDATE itself would fail.

ALTER TABLE saved_query_schedules
    DROP CONSTRAINT IF EXISTS saved_query_schedules_schedule_type_check;
ALTER TABLE saved_query_schedules
    DROP CONSTRAINT IF EXISTS saved_query_schedules_clock_needs_temporal;
ALTER TABLE saved_query_schedules
    DROP CONSTRAINT IF EXISTS saved_query_schedules_trigger_needs_pipeline;
ALTER TABLE saved_query_schedules
    DROP CONSTRAINT IF EXISTS saved_query_schedules_event_has_no_temporal;

UPDATE saved_query_schedules
SET schedule_type = 'after_upstream'
WHERE schedule_type = 'after_pipeline';

ALTER TABLE saved_query_schedules
    ADD CONSTRAINT saved_query_schedules_schedule_type_check
    CHECK (schedule_type IN ('cron', 'interval', 'after_upstream'));

-- 095's two pairing CHECKs, restated. The clock half is unchanged in meaning. The event
-- half loses the "has an upstream" clause it used to carry -- that requirement now lives
-- across two tables, see the header -- and keeps only the part still expressible here:
-- an event trigger registers nothing with Temporal, so it has no schedule id. Neither
-- mentions trigger_pipeline_id, so both survive Part D.
ALTER TABLE saved_query_schedules
    ADD CONSTRAINT saved_query_schedules_clock_needs_temporal
    CHECK (schedule_type = 'after_upstream' OR temporal_schedule_id IS NOT NULL);

ALTER TABLE saved_query_schedules
    ADD CONSTRAINT saved_query_schedules_event_has_no_temporal
    CHECK (schedule_type != 'after_upstream' OR temporal_schedule_id IS NULL);

-- ---------------------------------------------------------------------------
-- Part D -- retire the single-upstream column
-- ---------------------------------------------------------------------------

-- Dropped rather than kept as a "primary upstream" beside the child table. Two places
-- to read the same fact is how the two drift, and the one that drifts silently is the
-- one the fire path does not use.
DROP INDEX IF EXISTS idx_sq_schedules_trigger_pipeline;

ALTER TABLE saved_query_schedules
    DROP COLUMN IF EXISTS trigger_pipeline_id;

COMMENT ON TABLE saved_query_schedule_upstreams IS
    'Producers that wake one saved-query model schedule (schedule_type=after_upstream); many rows per schedule is fan-in, and upstream_kind=model is a model-to-model trigger';
COMMENT ON COLUMN saved_query_schedule_upstreams.upstream_kind IS
    'pipeline or model; must agree with which of upstream_pipeline_id / upstream_saved_query_id is set';
COMMENT ON COLUMN saved_query_schedules.temporal_schedule_id IS
    'Temporal Schedule ID for cron/interval; NULL for after_upstream, which is woken by an event rather than a clock';
