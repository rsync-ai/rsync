-- The CDC sink's live drain reading, per pipeline, kept where a tenant-scoped read
-- can reach it.
--
-- WHY THIS TABLE EXISTS
--
-- The pipeline page's "Waiting in Kafka" tile is genuine sink consumer-group lag,
-- but it reaches the UI as a side effect. emitCDCStatusMetrics
-- (backend-orchestrator/cmd/orchestrator/main.go) folds the lag into a
-- DATA_PLANE_METRICS domain event, the gateway projects it into
-- pipeline_run_events, and GetPipelineMonitoringOverview reads the newest such row
-- inside a 24h window. But emitCDCStatusMetrics is only reachable from
-- GET /api/v1/cdc/pipelines/:id/status, whose only caller is a one-shot fetch on
-- page load. So `lag_measured_at` means "whenever somebody last opened this page",
-- and after a day of no visits the tile reads "No reading".
--
-- Meanwhile CDCSentinel.checkSinkConsumerLag already reads the authoritative
-- broker-side drain every tick -- lag per topic, the committed position, and
-- whether the sink has stopped moving -- and then throws all three numbers away
-- once it has decided whether to raise cdc-sink-lag-<id>. This table is where that
-- reading lands, so the page can show a measurement that is at most one tick old
-- whether or not anyone was watching.
--
-- WHY NOT sentinel_component_health
--
-- That table has no workspace_id, and its component ids name Kafka topics and
-- containers belonging to every workspace, which is why
-- GET /api/v1/monitoring/sentinel/health is admin-only. Keying on pipeline_id
-- instead makes an ordinary workspace-scoped read possible: infrastructure
-- monitoring stays on admin/health, and the pipeline page gets the pipeline's own
-- numbers.
--
-- WHY committed IS STORED AT ALL
--
-- Lag alone cannot separate a sink working through a first load from a sink that
-- has died: both show a large backlog. The committed offset moves only when the
-- sink commits, so `committed_moving` is the second fact that makes "stalled"
-- sayable -- and, just as importantly, makes a lag of 0 distinguishable from a
-- producer that is dead (KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED: with Debezium
-- gone, every topic drains to lag 0 and every signal reads healthy).
-- The absolute value of `committed` means nothing; only its change between
-- readings does.

CREATE TABLE IF NOT EXISTS pipeline_sink_lag (
    -- One row per pipeline, rewritten in place each tick. History belongs in
    -- monitoring events; this table answers "right now".
    pipeline_id      UUID PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,

    -- The group the reading was taken from, resolved through
    -- handlers.ResolveSinkConsumerGroup (manifest first). Stored so the UI can
    -- name it and so a reading taken from the wrong group is diagnosable rather
    -- than merely wrong.
    consumer_group   TEXT   NOT NULL,

    -- Sum of lag over the pipeline's own topics. computeConsumerGroupLag skips
    -- partitions the group never committed, so this is scoped to this pipeline
    -- and not the cluster-wide phantom that once read 14.8M.
    total_lag        BIGINT NOT NULL,

    -- NOTE: the per-topic breakdown lives in pipeline_consumer_lag (migration
    -- 117), one row per (pipeline, group, topic). It was briefly carried here as
    -- a JSONB column; two places holding the same per-topic lag is the drift
    -- class this repo keeps paying for, and nothing ever read the column.

    -- Sum of the group's committed offsets. Meaningful only as a delta.
    committed        BIGINT NOT NULL,

    -- Did `committed` move since the previous reading? False on the first
    -- reading for a pipeline and after a group reset (committed went backwards),
    -- because neither says the sink is stuck -- it only starts the clock.
    committed_moving BOOLEAN NOT NULL DEFAULT FALSE,

    -- The sentinel's two-signal verdict (backlog AND no movement for
    -- CDC_SINK_DRAIN_STALL_AFTER, default 5 min) -- the same value that decides
    -- whether cdc-sink-lag-<id> is raised, so the tile and the alert can never
    -- disagree.
    stalled          BOOLEAN NOT NULL DEFAULT FALSE,
    stalled_seconds  BIGINT  NOT NULL DEFAULT 0,

    measured_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- A reading is only worth showing while it is fresh; the UI marks a stale one
-- rather than trusting it, and a sweep may want the oldest first.
CREATE INDEX IF NOT EXISTS idx_pipeline_sink_lag_measured
    ON pipeline_sink_lag(measured_at DESC);

COMMENT ON TABLE pipeline_sink_lag IS
    'Latest CDC sink consumer-group drain reading per pipeline, written by the orchestrator Sentinel each tick and read workspace-scoped by GET /api/v1/pipelines/:id/runtime.';
