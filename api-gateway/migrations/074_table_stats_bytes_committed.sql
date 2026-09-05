-- 074_table_stats_bytes_committed.sql
-- Destination-committed byte high-water mark per (pipeline, execution, table) — the
-- DISPLAY meter for the per-pipeline breakdown on /usage. Populated by the
-- kafka-mcp-sink as a cumulative total and GREATEST-upsert'd by the projector,
-- exactly like the existing row counters on this table.
--
-- Billing truth is the append-only data_transfer_usage_events ledger (073); this
-- column is the convenient per-pipeline view (and is subject to the retention sweep,
-- unlike the ledger).
--
-- The migration runner wraps each file in a transaction — no BEGIN/COMMIT here.

ALTER TABLE pipeline_run_table_stats
    ADD COLUMN IF NOT EXISTS bytes_committed BIGINT NULL;
