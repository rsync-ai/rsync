-- 114_table_stats_snapshot_rows.sql
--
-- Count CDC snapshot reads apart from inserts in pipeline_run_table_stats.
--
-- Why: Debezium emits every row a snapshot reads (the initial load and every
-- re-snapshot) as op 'r'. Both the stats consumer and the sink counted 'r' as an
-- insert, so each re-snapshot added the whole table to "Inserts" again — a
-- 4,000-row table showed 8,000 inserts after one re-snapshot, and nothing
-- explained the gap.
--
-- snapshot_rows          rows read by snapshots, counted from Kafka by the
--                        stats consumer (captured side).
-- applied_snapshot_rows  snapshot rows the sink wrote to the destination
--                        (applied side).
--
-- NULL = not reported (a batch pipeline, or a row written before this change,
-- whose inserts may still include snapshot reads).

ALTER TABLE pipeline_run_table_stats
    ADD COLUMN IF NOT EXISTS snapshot_rows BIGINT NULL;

ALTER TABLE pipeline_run_table_stats
    ADD COLUMN IF NOT EXISTS applied_snapshot_rows BIGINT NULL;
