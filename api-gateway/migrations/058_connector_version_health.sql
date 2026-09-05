-- Connector Health Watchdog (Pillar 5).
--
-- Pattern stolen from Fivetran's internal connector-ops dashboards. The
-- watchdog computes per-(connector_type, version) success rates over a
-- rolling 24-hour window and alerts ops when a version's success rate
-- drops more than 20% below the previous minor version — the classic
-- signal that a freshly-shipped template fix regressed.
--
-- Without this, regressions only surface when a customer files a support
-- ticket. With it, ops are paged within an hour of the regression.
--
-- This table is the materialized rollup, refreshed hourly by a cron in
-- the orchestrator. It is NOT the source of truth — that's `executions`
-- joined to `connections` to get the connector_type/version. The rollup
-- exists so the admin dashboard + alert query don't scan executions every
-- minute.

CREATE TABLE IF NOT EXISTS connector_version_health (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connector_type  TEXT NOT NULL,   -- e.g. "stripe-rest", "postgresql"
    version         TEXT NOT NULL,   -- e.g. "1.0.4", "v1.0.4"
    -- Rolling 24h counters as of computed_at.
    success_count_24h INTEGER NOT NULL DEFAULT 0,
    failure_count_24h INTEGER NOT NULL DEFAULT 0,
    -- Success rate ∈ [0.0, 1.0]. Stored separately so dashboards can sort
    -- without computing on every read.
    success_rate    DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    -- Previous minor version's success rate at the same computed_at —
    -- captured here for the alert query so we don't need a self-join.
    prev_version            TEXT,
    prev_success_rate       DOUBLE PRECISION,
    -- Regression flag. Computed by the watchdog cron — true when this
    -- version's success rate is >20pp below prev_version's AND we have at
    -- least 10 executions (avoid false positives on tiny samples).
    regressed       BOOLEAN NOT NULL DEFAULT FALSE,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Each (type, version, computed_at) row is unique — the cron INSERT
-- pattern uses UPSERT on (type, version) to keep latest only.
CREATE UNIQUE INDEX IF NOT EXISTS idx_connector_version_health_type_version
    ON connector_version_health(connector_type, version);

CREATE INDEX IF NOT EXISTS idx_connector_version_health_regressed
    ON connector_version_health(computed_at DESC)
    WHERE regressed = TRUE;

COMMENT ON TABLE connector_version_health IS
'Hourly rollup of per-(connector_type, version) success rate over rolling 24h. Drives the Pillar 5 anomaly-detection watchdog: alerts ops when a version regresses vs. its predecessor. Refreshed by orchestrator cron; admin dashboard reads from here.';
