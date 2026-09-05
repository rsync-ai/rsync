-- 082_heal_reeligibility_index.sql
--
-- Index maintenance for the heal sweep's corrected eligibility predicate.
--
-- Migration 053 added heal_attempted_at plus a partial index on
-- `WHERE heal_attempted_at IS NULL`, because that was the whole eligibility rule:
-- the healer looked at an execution once and never again.
--
-- That rule assumed one executions row per run. Batch honours it; CDC does not —
-- a CDC pipeline mints a single row and reuses it for the stream's entire
-- lifetime, re-stamping status/end_time in place. Since markHealed only ever
-- writes heal_attempted_at = NOW() and nothing ever clears it, the first heal of a
-- CDC pipeline was also its last. Observed on production (pipeline a9d7f773,
-- execution a8de91d4): heal_attempted_at 2026-07-16 18:52, end_time 2026-07-30
-- 11:59, status 'failed' — it failed again 14 days after its only heal attempt and
-- could never be picked up again.
--
-- heal/worker.go sweepCandidatesQuery now asks "has this row failed SINCE the last
-- time we looked at it":
--
--     heal_attempted_at IS NULL
--     OR (heal_attempted_at < end_time AND heal_attempted_at < NOW() - cooldown)
--
-- 053's index no longer covers that set. The NOW() term is not immutable so it
-- cannot live in an index predicate, but it is only a rate limit — indexing the
-- eligibility half is what matters, and the planner applies the cooldown as a
-- cheap filter on the (small) result.
--
-- The status list and the end_time IS NOT NULL term are folded in too, so this one
-- partial index answers the whole sweep. Ordered by end_time DESC to match the
-- query's ORDER BY, letting the LIMIT terminate the scan early.
BEGIN;

-- Superseded: its predicate now matches the wrong row set, and leaving it would
-- mislead the next reader into thinking IS NULL is still the eligibility rule.
DROP INDEX IF EXISTS idx_executions_heal_attempted;

CREATE INDEX IF NOT EXISTS idx_executions_heal_pending
  ON executions (end_time DESC)
  WHERE status IN (
          'failed', 'error',
          'silent_drop_detected', 'silent_partial_drop_detected',
          'credential_check_failed'
        )
    AND end_time IS NOT NULL
    AND (heal_attempted_at IS NULL OR heal_attempted_at < end_time);

COMMIT;
