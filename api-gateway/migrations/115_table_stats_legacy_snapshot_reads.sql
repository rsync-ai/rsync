-- 115_table_stats_legacy_snapshot_reads.sql
--
-- Move the snapshot reads a pre-114 producer counted as inserts out of CDC
-- table stats rows written before migration 114.
--
-- Why: migration 114 counts snapshot reads (Debezium op 'r') apart from inserts,
-- but only for rows counted after it. A row counted before it keeps every read in
-- `inserts` (captured) and `applied_inserts` (applied) -- prod, 2026-09-25: Captured
-- Inserts 51,600 on a pipeline whose change stream held no insert at all. The
-- projector's one-time split (event_projector.go) never moves them: the stats
-- consumer seeds its counters from the stored row, so its first event carries the
-- inflated inserts back and GREATEST keeps them; and an idle table sends no event.
--
-- The count to move comes from the delivery ledger, pipeline_batch_acks, which
-- is never pruned and records each delivered event's op -- the same ledger the
-- sink seeds its applied counters from at start (cdc_counter_ledger.go).
--
-- legacy_snapshot_reads  reads still inside `inserts`. Readers show
--                        inserts - legacy_snapshot_reads as inserts and add it to
--                        snapshot_rows (table_stats.go, usage.go). `inserts` itself
--                        is not lowered: the stats consumer would write the seeded,
--                        inflated value straight back. NULL = nothing to move.
--
-- The applied side is corrected in place: the sink seeds applied_inserts from the
-- ledger's creates alone, so the lowered value is the one it reports next.
--
-- snapshot_rows / applied_snapshot_rows are set non-NULL on the rows moved, which
-- turns off the projector's one-time split for them -- it would take the same
-- reads out a second time.
--
-- total_events, applied_total_events and inserted_rows are not touched: reads are
-- events, and the backlog (total_events - applied_total_events) stays as it was.
--
-- Idempotent: a row once moved has legacy_snapshot_reads set and is skipped. Only
-- rows created before 114 ran are touched; a fresh install has none.

ALTER TABLE pipeline_run_table_stats
    ADD COLUMN IF NOT EXISTS legacy_snapshot_reads BIGINT NULL;

WITH ledger AS (
    SELECT pipeline_id, execution_id, table_name,
           COUNT(*) FILTER (WHERE LOWER(TRIM(cdc_op)) = 'r') AS reads,
           COUNT(*) FILTER (WHERE LOWER(TRIM(cdc_op)) = 'c') AS creates
    FROM pipeline_batch_acks
    WHERE storage_type = 'cdc' AND last_error IS NULL
    GROUP BY pipeline_id, execution_id, table_name
)
UPDATE pipeline_run_table_stats s SET
    -- Reads the captured side has not counted apart yet, never more than it counted.
    legacy_snapshot_reads = LEAST(COALESCE(s.inserts, 0), GREATEST(l.reads - COALESCE(s.snapshot_rows, 0), 0)),
    snapshot_rows = COALESCE(s.snapshot_rows, 0),
    -- Applied side: already split when applied_snapshot_rows is set.
    applied_inserts = CASE
        WHEN s.applied_snapshot_rows IS NULL
        THEN GREATEST(COALESCE(s.applied_inserts, 0) - l.reads, l.creates)
        ELSE s.applied_inserts
    END,
    applied_snapshot_rows = COALESCE(s.applied_snapshot_rows, l.reads)
FROM ledger l
WHERE s.mode = 'cdc'
  AND l.pipeline_id = s.pipeline_id
  AND l.execution_id = s.execution_id
  AND l.table_name = s.qualified_name
  AND l.reads > 0
  AND s.legacy_snapshot_reads IS NULL
  AND s.created_at < (
      SELECT applied_at FROM schema_migrations
      WHERE version = '114_table_stats_snapshot_rows.sql'
  );
