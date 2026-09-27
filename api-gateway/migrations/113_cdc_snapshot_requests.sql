-- 113_cdc_snapshot_requests.sql
--
-- Track every CDC snapshot request (Re-snapshot, Edit tables with "load existing
-- rows", the auto-pickup watcher) from the moment it is asked for until Debezium
-- has finished reading the tables.
--
-- Why: the request used to be fire-and-forget. The execute-snapshot signal was
-- produced the instant the API was called, so:
--   * after Edit tables it could reach the OLD connector task, which does not
--     capture the new table yet, ignores it and marks the signal done — the
--     existing rows were never loaded while the UI said they were;
--   * a blocking snapshot's signal is acknowledged within seconds, so a restart
--     mid-snapshot lost it with no error;
--   * the UI had nothing to show between "requested" and rows arriving (55-60 s).
--
-- cdc_snapshot_requests: one row per request.
--   status  queued      waiting for the connector to run with the tables
--           sent        signal produced, no snapshot row seen yet
--           started     snapshot rows for the tables are arriving
--           completed   Debezium marked the snapshot finished (or, for an
--                       incremental snapshot, rows stopped arriving)
--           unconfirmed sent (up to the retry cap) but no snapshot row arrived
--           failed      could not be sent (connector missing or failed)
--   tables            the Debezium data-collections the signal names.
--   completed_tables  tables whose last snapshot row has been seen.
--   not_before        earliest send time (a table edit waits for the restart).
--   attempts          how many times the signal was produced.
--   sent_at           the FIRST send (snapshot rows are credited from here);
--   last_sent_at      the latest one (the retry clock runs from here).
--   cleans_folder     layout-v2 object storage: the send also asks the sink to
--                     empty each table's folder (cdc_object_reload_requests).
--   last_error        why it is unconfirmed / failed (connector state only,
--                     never row data).
--
-- cdc_object_reload_requests: one row per (pipeline, data topic) whose
-- object-storage folder must be emptied by the next snapshot of that table
-- (layout v2). The orchestrator writes it when it sends a snapshot signal; the
-- sink consumes it on the FIRST snapshot batch of that topic whose event time
-- is not older than requested_at, starting a new generation (folder clean) in
-- the same transaction. Keyed by the Kafka data topic, which both sides derive
-- from the connector's topic prefix, so neither side mirrors the other's
-- table-key encoding. The sink — not the orchestrator — bumps the generation:
-- a generation bumped ahead of a snapshot that never comes would let any sink
-- restart empty the folder with nothing to reload it.

CREATE TABLE IF NOT EXISTS cdc_snapshot_requests (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id      UUID        NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    connector_name   TEXT        NOT NULL,
    mode             TEXT        NOT NULL
        CONSTRAINT cdc_snapshot_requests_mode_check
        CHECK (mode IN ('incremental', 'blocking')),
    tables           JSONB       NOT NULL DEFAULT '[]'::jsonb,
    source           TEXT        NOT NULL DEFAULT 'resnapshot'
        CONSTRAINT cdc_snapshot_requests_source_check
        CHECK (source IN ('resnapshot', 'table_edit', 'auto_pickup')),
    status           TEXT        NOT NULL DEFAULT 'queued'
        CONSTRAINT cdc_snapshot_requests_status_check
        CHECK (status IN ('queued', 'sent', 'started', 'completed', 'unconfirmed', 'failed')),
    attempts         INTEGER     NOT NULL DEFAULT 0,
    completed_tables JSONB       NOT NULL DEFAULT '[]'::jsonb,
    last_error       TEXT        NULL,
    not_before       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    requested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cleans_folder    BOOLEAN     NOT NULL DEFAULT FALSE,
    sent_at          TIMESTAMPTZ NULL,
    last_sent_at     TIMESTAMPTZ NULL,
    started_at       TIMESTAMPTZ NULL,
    last_progress_at TIMESTAMPTZ NULL,
    completed_at     TIMESTAMPTZ NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cdc_snapshot_requests_pipeline_requested
    ON cdc_snapshot_requests (pipeline_id, requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_cdc_snapshot_requests_open
    ON cdc_snapshot_requests (status, requested_at)
    WHERE status IN ('queued', 'sent', 'started');

CREATE TABLE IF NOT EXISTS cdc_object_reload_requests (
    pipeline_id  UUID        NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    topic        TEXT        NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    request_id   UUID        NULL,
    PRIMARY KEY (pipeline_id, topic)
);
