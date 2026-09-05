-- Migration 079: Backfill read_rows for pre-#670 CDC table-stats rows
--
-- Context
-- -------
-- The Usage page's "Rows read (source)" reads pipeline_run_table_stats.read_rows.
-- The event projector's CDC (applied-producer) branch only began populating
-- read_rows in #670 (event_projector.go, commit 229eaa4b, 2026-07-22 13:14 UTC).
-- Its sibling inserted_rows ("Rows written") has been populated since #131
-- (commit c2c6bf73, 2026-06-04). So any CDC stream that finished BEFORE the #670
-- deploy has inserted_rows set but read_rows NULL, and the Usage page shows
-- "Rows read = 0" for it while "Rows written" is correct. read_rows upserts
-- monotonically (GREATEST) and a finished/dead stream emits no new events, so
-- these rows can never self-heal. Batch pipelines are unaffected (the batch
-- branch has always populated read_rows).
--
-- Fix
-- ---
-- For CDC rows that never received a read_rows value, backfill it from the best
-- available event count. The sink emits read_rows = total_events
-- (kafka-sink-worker main.go:5001), and that same total lands in
-- applied_total_events on the projector side (event_projector.go:751-754), so
-- applied_total_events is the exact value #670 would have written. Fall back to
-- captured total_events, then to the captured ops sum (inserts+updates+deletes).
-- This reproduces the post-#670 result and keeps backfilled rows consistent with
-- rows projected after the fix.
--
-- Scope / safety
-- --------------
--   * mode = 'cdc'          -> never touches batch rows.
--   * read_rows IS NULL     -> never overwrites a real value, including a
--                              legitimate post-#670 read_rows = 0.
--   * derived count > 0     -> no pointless NULL->0 writes.
-- Idempotent: once read_rows is set the row no longer matches, so re-running is
-- a no-op. This is a data-only migration; there is no schema change to reverse.
--
-- Note: migration runner wraps in a transaction; do not add BEGIN/COMMIT.

UPDATE pipeline_run_table_stats
SET read_rows = COALESCE(
        applied_total_events,
        total_events,
        COALESCE(inserts, 0) + COALESCE(updates, 0) + COALESCE(deletes, 0)
    )
WHERE mode = 'cdc'
  AND read_rows IS NULL
  AND COALESCE(
        applied_total_events,
        total_events,
        COALESCE(inserts, 0) + COALESCE(updates, 0) + COALESCE(deletes, 0)
    ) > 0;
