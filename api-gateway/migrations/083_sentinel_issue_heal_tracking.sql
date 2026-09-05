-- Migration 083: Heal tracking on sentinel_active_issues
--
-- Until now the healer read this table nowhere. The Sentinel detected, wrote a
-- row, and nothing downstream ever looked at it — detection and healing were two
-- systems that happened to live in the same process. The heal sweep only ever
-- queried `executions`, and only rows with `end_time IS NOT NULL`, so a pipeline
-- that was actively broken but still marked running was invisible to it until
-- the 4-hour zombie sweep noticed.
--
-- This adds the same marker `executions` already carries (migration 053), for
-- the same reason: without it the issue-driven sweep would re-diagnose every
-- open issue on every 60-second poll.
--
-- Deliberately mirrors the executions semantics rather than inventing new ones:
-- the marker records that THIS occurrence was looked at, not that the issue is
-- retired. `last_occurrence` advancing past the stamp re-admits the issue, so a
-- problem that keeps happening keeps earning a fresh look.
BEGIN;

ALTER TABLE sentinel_active_issues
  ADD COLUMN IF NOT EXISTS heal_attempted_at TIMESTAMPTZ;

-- Partial index matching the sweep predicate: unresolved issues nobody has
-- looked at yet are the common case, and the index stays small because rows
-- leave it as soon as they are processed.
CREATE INDEX IF NOT EXISTS idx_sentinel_issues_heal_pending
  ON sentinel_active_issues (last_occurrence DESC)
  WHERE resolved_at IS NULL AND heal_attempted_at IS NULL;

COMMIT;
