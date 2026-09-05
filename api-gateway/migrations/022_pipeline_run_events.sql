-- Migration 022: Pipeline run events + metrics
--
-- Adds append-only event history for replayable monitoring UX.
-- Events are redacted before persisting (server-side).
--
BEGIN;

-- Discrete events (high value, low-ish volume)
CREATE TABLE IF NOT EXISTS pipeline_run_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    execution_id UUID NULL,

    -- Event envelope
    event_id TEXT NOT NULL,
    seq BIGINT NULL,
    event_type TEXT NOT NULL,
    stage_id TEXT NULL,
    stage_group TEXT NULL,
    severity TEXT NULL, -- info|warn|error
    trace_id TEXT NULL,

    occurred_at TIMESTAMPTZ NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Full event payload (redacted)
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    redacted BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Prevent duplicate inserts across restarts/replays (best-effort stable event_id).
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_run_events_unique_event
ON pipeline_run_events(pipeline_id, event_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_events_pipeline_received
ON pipeline_run_events(pipeline_id, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_events_exec_seq
ON pipeline_run_events(execution_id, seq DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_events_type
ON pipeline_run_events(event_type);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_events_trace
ON pipeline_run_events(trace_id);

-- Time-series-ish rollups (written every 5–10s; not every heartbeat)
CREATE TABLE IF NOT EXISTS pipeline_run_metrics (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    execution_id UUID NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_metrics_pipeline_ts
ON pipeline_run_metrics(pipeline_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_metrics_exec_ts
ON pipeline_run_metrics(execution_id, ts DESC);

COMMIT;


