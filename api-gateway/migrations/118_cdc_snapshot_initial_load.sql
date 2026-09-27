-- Migration 118: cdc_snapshot_requests also records a pipeline's initial (full) load.
--
-- The pipeline page shows a load status the way AWS DMS does ("Full load in
-- progress · 3 / 5 tables", "Load completed, replication ongoing"). Nothing
-- recorded the initial load: a CDC pipeline's snapshot is run by Debezium on its
-- own, not by a request, so the table only ever held Re-snapshot / Edit tables /
-- auto-pickup requests.
--
-- source = 'initial' rows are written by the orchestrator only:
--   * the snapshot dispatcher, from the snapshot rows the CDC stats consumer
--     sees that no request claims (cdcsnapshot/dispatcher.go recordInitial).
--     It is recorded from OBSERVED rows, never when the executor starts: Debezium
--     skips the snapshot when the connector already has offsets, and a row
--     written at start would then say "Full load in progress" forever;
--   * the hybrid executor, around its batch load (executor/hybrid_cdc.go).
-- The API never creates one (cdcsnapshot.NormalizeSource maps any caller value
-- onto resnapshot / table_edit / auto_pickup), and the dispatcher never sends a
-- signal for one: its status only moves from observed rows.
--
-- The partial unique index keeps at most one open initial load per pipeline, so
-- two orchestrator replicas that see the same rows cannot both record it (the
-- insert is ON CONFLICT DO NOTHING). No row can match it before this migration,
-- because the CHECK did not allow 'initial'.
--
-- Re-running this file is a no-op: the CHECK is dropped before it is re-added and
-- the index is IF NOT EXISTS.

ALTER TABLE cdc_snapshot_requests
    DROP CONSTRAINT IF EXISTS cdc_snapshot_requests_source_check;

ALTER TABLE cdc_snapshot_requests
    ADD CONSTRAINT cdc_snapshot_requests_source_check
        CHECK (source IN ('resnapshot', 'table_edit', 'auto_pickup', 'initial'));

CREATE UNIQUE INDEX IF NOT EXISTS uq_cdc_snapshot_requests_open_initial
    ON cdc_snapshot_requests (pipeline_id)
    WHERE source = 'initial' AND status IN ('sent', 'started');
