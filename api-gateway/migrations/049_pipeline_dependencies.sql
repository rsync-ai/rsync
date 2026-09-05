-- Pipeline runtime dependencies + their observed liveness.
--
-- Why: Today a pipeline can be marked "completed/streaming" while its underlying
-- dependencies (destination MCP container, Debezium task, kafka-mcp-sink worker)
-- are silently dead. We have no manifest of what each pipeline needs, and no
-- observed-state table to verify. This migration adds both, so the canonical
-- /api/pipelines/{id}/runtime endpoint can downgrade `streaming → degraded → stalled`
-- when a required dependency stops responding.

-- Manifest: declared at planner/executor time. One row per dependency the pipeline
-- needs to keep running. Static for the lifetime of an execution; rewritten if the
-- plan is regenerated.
CREATE TABLE IF NOT EXISTS pipeline_dependencies (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    execution_id    UUID,                              -- NULL = applies to all executions
    kind            VARCHAR(64) NOT NULL,              -- mcp_source | mcp_dest | debezium_task | kafka_sink_worker | ...
    identifier      VARCHAR(255) NOT NULL,             -- e.g. "postgresql@v1.0.14", "rsync-ai.mysql.e2e_db.big_table", "pipeline-{id}-sink"
    required_phases TEXT[] NOT NULL DEFAULT '{}',      -- which runtime phases need this alive: e.g. {streaming} or {syncing,streaming}
    metadata        JSONB DEFAULT '{}'::jsonb,         -- transport hints (host, port, container_name), connector config refs
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, execution_id, kind, identifier)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_dependencies_pipeline ON pipeline_dependencies(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_dependencies_kind     ON pipeline_dependencies(kind);

-- Observed state: written by the orchestrator's liveness probe (~10s tick).
-- One row per dependency; updated in place rather than appending history (history
-- belongs in monitoring events).
CREATE TABLE IF NOT EXISTS pipeline_dependency_health (
    dependency_id   UUID PRIMARY KEY REFERENCES pipeline_dependencies(id) ON DELETE CASCADE,
    status          VARCHAR(32) NOT NULL DEFAULT 'unknown',  -- healthy | degraded | unhealthy | unknown
    last_checked_at TIMESTAMPTZ,
    last_healthy_at TIMESTAMPTZ,
    last_error      TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    details         JSONB DEFAULT '{}'::jsonb,         -- probe-specific diagnostics (HTTP status, lag, task state, ...)
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_dependency_health_status ON pipeline_dependency_health(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_dependency_health_updated ON pipeline_dependency_health(updated_at);
