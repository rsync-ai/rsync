-- Migration 104: why a triggered model ran, why it did not, and fan-in that waits.
--
-- saved_query_runs (086) recorded THAT a model rebuilt and whether it worked. For a
-- rebuild woken by an upstream (095/100) it could say only "triggered": not which
-- upstream, not which of that upstream's runs, not how deep in the chain, and not how
-- many upstream completions the rebuild absorbed. And when an upstream failed, or the
-- chain hit its depth bound, the models below it got no row at all, so their history
-- read exactly like "nothing happened".
--
-- Provenance columns (all nullable; a manual or clock run leaves them empty):
--   upstream_kind / upstream_id  the pipeline or model whose completion woke this run.
--                                No FK: the upstream may be deleted later, and the run
--                                history must still say what woke it.
--   upstream_run_id              the upstream model's own saved_query_runs row, so a
--                                chain can be walked back hop by hop. SET NULL when
--                                that row goes (its model was deleted).
--   origin_execution_id          the pipeline execution at the root of the chain, when
--                                the chain started at a pipeline. TEXT because it is
--                                carried through Temporal as a string.
--   trigger_depth                hops from the root (1 = woken directly by the root).
--   coalesced_count              upstream completions this one rebuild absorbed.
--
-- skip_reason: a 'skipped' row now also records a model that was deliberately NOT
-- rebuilt, not only a runner refusal. It is set only on those rows (named CHECK below):
--   upstream_failed / upstream_skipped  an upstream in the chain did not succeed.
--   chain_depth_exceeded                the chain hit maxTriggerChainDepth.
--   waiting_on_upstreams                upstream_policy='all' and a sibling is stale.
-- Skip rows do not stamp saved_queries.last_run_*, and freshness reads only succeeded
-- rows, so neither is moved by this.
--
-- saved_query_schedules.upstream_policy: 'any' (the 100 behaviour, and the default so
-- every existing schedule is unchanged) rebuilds on each upstream completion; 'all'
-- rebuilds only once every upstream has succeeded since this model last rebuilt, which
-- is what stops a fan-in model reading a sibling that has not refreshed yet.
--
-- The CHECKs are added NOT VALID and validated after COMMIT. Added plainly, each one scans
-- saved_query_runs while holding the ACCESS EXCLUSIVE lock the ALTER took, so every run
-- write waits for the scan; VALIDATE CONSTRAINT scans under SHARE UPDATE EXCLUSIVE, which
-- does not block reads or writes. The file manages its own transaction for that (the runner
-- detects "BEGIN;"), and every statement is re-runnable, so a failed validation leaves the
-- migration unrecorded and the next start applies it again from the top.

BEGIN;

ALTER TABLE saved_query_runs
    ADD COLUMN IF NOT EXISTS upstream_kind TEXT,
    ADD COLUMN IF NOT EXISTS upstream_id UUID,
    ADD COLUMN IF NOT EXISTS upstream_run_id UUID REFERENCES saved_query_runs(run_id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS origin_execution_id TEXT,
    ADD COLUMN IF NOT EXISTS trigger_depth INT,
    ADD COLUMN IF NOT EXISTS coalesced_count INT,
    ADD COLUMN IF NOT EXISTS skip_reason TEXT;

ALTER TABLE saved_query_runs DROP CONSTRAINT IF EXISTS saved_query_runs_upstream_kind_check;
ALTER TABLE saved_query_runs ADD CONSTRAINT saved_query_runs_upstream_kind_check
    CHECK (upstream_kind IS NULL OR upstream_kind IN ('pipeline', 'model')) NOT VALID;

ALTER TABLE saved_query_runs DROP CONSTRAINT IF EXISTS saved_query_runs_skip_reason_check;
ALTER TABLE saved_query_runs ADD CONSTRAINT saved_query_runs_skip_reason_check
    CHECK (skip_reason IS NULL OR skip_reason IN
        ('upstream_failed', 'upstream_skipped', 'chain_depth_exceeded', 'waiting_on_upstreams')) NOT VALID;

ALTER TABLE saved_query_runs DROP CONSTRAINT IF EXISTS saved_query_runs_skip_reason_only_when_skipped;
ALTER TABLE saved_query_runs ADD CONSTRAINT saved_query_runs_skip_reason_only_when_skipped
    CHECK (skip_reason IS NULL OR status = 'skipped') NOT VALID;

CREATE INDEX IF NOT EXISTS idx_saved_query_runs_upstream_run
    ON saved_query_runs (upstream_run_id) WHERE upstream_run_id IS NOT NULL;

ALTER TABLE saved_query_schedules
    ADD COLUMN IF NOT EXISTS upstream_policy TEXT NOT NULL DEFAULT 'any';

ALTER TABLE saved_query_schedules DROP CONSTRAINT IF EXISTS saved_query_schedules_upstream_policy_check;
ALTER TABLE saved_query_schedules ADD CONSTRAINT saved_query_schedules_upstream_policy_check
    CHECK (upstream_policy IN ('any', 'all')) NOT VALID;

COMMIT;

ALTER TABLE saved_query_runs VALIDATE CONSTRAINT saved_query_runs_upstream_kind_check;
ALTER TABLE saved_query_runs VALIDATE CONSTRAINT saved_query_runs_skip_reason_check;
ALTER TABLE saved_query_runs VALIDATE CONSTRAINT saved_query_runs_skip_reason_only_when_skipped;
ALTER TABLE saved_query_schedules VALIDATE CONSTRAINT saved_query_schedules_upstream_policy_check;
