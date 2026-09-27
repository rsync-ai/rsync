-- Per-consumer, per-topic lag for one pipeline: the census behind the Consumers
-- card on the pipeline page.
--
-- WHY A SECOND TABLE RATHER THAN MORE COLUMNS ON pipeline_sink_lag
--
-- 116 answers ONE question -- "is this pipeline's data arriving, and if not, is
-- the sink stuck?" -- and it answers it once per pipeline, because that is the
-- grain the health tiles and the cdc-sink-lag-<id> alarm work at. This table
-- answers a different question at a different grain: "which consumer is reading
-- which topic, and how far behind is it?" One row per (pipeline, group, topic).
--
-- The two are written from the same tick and the same readings, so they cannot
-- disagree about a number they share; 116 deliberately stopped carrying a
-- per-topic JSONB blob once this table existed, because two places holding the
-- same per-topic lag is exactly the drift class this repo keeps paying for.
--
-- WHY THE PIPELINE'S OWN CONSUMERS, AND NOT THE ADMIN VIEW
--
-- sentinel_component_health also has a consumer_lag column, and Admin -> Health
-- renders it. That table is keyed by TOPIC, has no workspace column, and its
-- kafka_consumer rows come from a hardcoded list of the nine
-- agent.control.commands.* control-plane topics the orchestrator itself consumes
-- (sentinel/health_monitor.go orchestratorConsumedTopics). It contains no
-- pipeline data topic and no sink group, and it is admin-only by design.
--
-- So the split is not a duplication: Admin -> Health is the INFRASTRUCTURE view
-- (the platform's own workers), and this is a PIPELINE's view (the consumers
-- moving that customer's rows). Keying on pipeline_id is what lets the read be
-- workspace-scoped instead of admin-gated.
--
-- SCOPE: SINK GROUPS
--
-- The rows written today are the pipeline's kafka-mcp-sink consumer groups --
-- the ones that actually move rows to the destination -- taken from the
-- pipeline_dependencies manifest (kind='kafka_sink_worker'), which is
-- authoritative because the sink itself wrote it. A hybrid pipeline has more
-- than one (a -batch worker and a streaming one), and until now only ONE was
-- ever measured: ResolveSinkConsumerGroup takes the newest row with LIMIT 1, so
-- a hybrid pipeline's second sink could be wedged with nothing reporting it.
--
-- The internal per-pipeline groups (cdc-table-stats-<uuid>,
-- cdc-schema-changes-<uuid>) are deliberately NOT written: they are rsync's own
-- bookkeeping consumers, and a user cannot act on their lag. The schema does not
-- exclude them -- `role` exists so they can be added behind a toggle later
-- without a migration.

CREATE TABLE IF NOT EXISTS pipeline_consumer_lag (
    pipeline_id      UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,

    -- The group id as it exists ON THE BROKER, namespace prefix included
    -- (kafkaclient.Group, default "rsync."). Stored as measured, never derived
    -- for display: a reading taken from a group that does not exist is then
    -- diagnosable rather than merely absent.
    consumer_group   TEXT NOT NULL,

    -- The topic this row's lag is for.
    topic            TEXT NOT NULL,

    -- What this consumer is for, derived from the group's suffix:
    -- 'sink' (CDC streaming), 'sink_batch' (hybrid backfill),
    -- 'sink_stream' (cdc_mode streaming_only / never). Future: 'stats', 'schema'.
    role             TEXT NOT NULL,

    -- Messages this group has not yet read from this topic. Scoped by
    -- computeConsumerGroupLag to partitions the group has actually committed, so
    -- a foreign topic's backlog can never be counted here.
    lag              BIGINT NOT NULL,

    -- The group's committed position on this topic. Absolute value is
    -- meaningless; only its movement between readings says the sink is working.
    committed        BIGINT NOT NULL,

    -- From DescribeConsumerGroups: 'Stable', 'PreparingRebalance',
    -- 'CompletingRebalance', 'Empty', 'Dead'. NULL when the describe call
    -- failed -- which must render as unknown, never as Stable. 'Empty' is the
    -- one that matters most next to a lag of 0: it means NOBODY IS CONSUMING,
    -- which lag alone cannot say.
    group_state      TEXT,
    members          INT,

    measured_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (pipeline_id, consumer_group, topic)
);

-- The read is always "this pipeline's consumers", newest first.
CREATE INDEX IF NOT EXISTS idx_pipeline_consumer_lag_pipeline
    ON pipeline_consumer_lag(pipeline_id, measured_at DESC);

COMMENT ON TABLE pipeline_consumer_lag IS
    'Per-consumer-group, per-topic lag for one pipeline, written by the orchestrator Sentinel each tick and read workspace-scoped by GET /api/v1/pipelines/:id/consumers. Infrastructure consumers live in sentinel_component_health (admin-only); this is the pipeline''s own.';
