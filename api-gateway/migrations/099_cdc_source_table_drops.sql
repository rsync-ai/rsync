-- 099: record which selected CDC source tables were dropped at the origin
-- (KI-CDC-DROPPED-SOURCE-TABLE-REPORTS-HEALTHY)
--
-- A CDC stream whose selected source table is dropped keeps reporting every
-- dependency Healthy. That is not a probe bug: `probeOne`'s `debezium_task` case
-- (backend-orchestrator/internal/workers/dependency_probe.go) asks Kafka Connect
-- whether the connector and its tasks are RUNNING, and after a source DROP they
-- genuinely are — there is simply nothing left to capture. The missing input was
-- never a liveness check; it was table awareness.
--
-- The signal already exists. Debezium publishes source DDL to the bare
-- topic.prefix topic, the cdcstats agent already consumes it, and
-- cdcstats/schema_changes.go already classifies `tableChanges[].type = "DROP"`.
-- What was missing is a durable place to keep that fact between the moment the
-- DDL arrives (a Kafka consumer, on its own goroutine) and the moment the probe
-- forms a verdict (a 15s sweep in a different package). This table is that place.
--
-- Why not reuse schema_change_approvals (migration 051), which the same DDL
-- consumer already writes to via the healer:
--
--   * an operator clicking Acknowledge there would silently un-degrade a stream
--     that is still capturing nothing — acknowledging a drop is not the same
--     statement as "the table is back";
--   * nothing in that table ever clears when the table is recreated, so the
--     recovery edge has no representation at all.
--
-- Shape notes:
--
--   * PRIMARY KEY (pipeline_id, table_name) — one open fact per table per
--     pipeline. A re-drop after a recreate reuses the row (the writer's
--     ON CONFLICT ... DO UPDATE resets dropped_at and clears restored_at) rather
--     than accumulating history; this table is current state, and the audit trail
--     of drops lives in schema_change_approvals where it already did.
--   * restored_at IS NULL means "open" — the table is gone as far as CDC knows.
--     A CREATE for the same table stamps restored_at, and the probe returns the
--     dependency to healthy on its next tick.
--   * table_name holds the name as the pipeline's own config->'selected_tables'
--     spells it, not as Debezium reported it. The writer matches the two
--     case- and form-insensitively and then stores the selection's spelling, so
--     the DROP and the CREATE key the same row even when one record arrives
--     qualified (`pipeline_test.cdc_drift`) and the selection is bare
--     (`cdc_drift`) — an asymmetry there would pin a stream degraded forever.
--   * ON DELETE CASCADE keeps this row out of the way of pipeline deletion, like
--     the other 21 pipeline children.
--
-- The partial index is what keeps the probe's added per-tick query cheap: it is
-- read once per `debezium_task` dependency on every 15s sweep, always with
-- restored_at IS NULL, and the open set is empty on a healthy deployment.

CREATE TABLE IF NOT EXISTS cdc_source_table_drops (
    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    table_name  TEXT NOT NULL,
    dropped_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    restored_at TIMESTAMPTZ,
    PRIMARY KEY (pipeline_id, table_name)
);

CREATE INDEX IF NOT EXISTS idx_cdc_source_table_drops_open
    ON cdc_source_table_drops (pipeline_id)
    WHERE restored_at IS NULL;
