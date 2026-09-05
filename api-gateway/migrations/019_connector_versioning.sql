-- Migration 019: Connector Versioning + Pipeline Connector Snapshots
--
-- This codebase uses a simple filename-based migration runner (schema_migrations.version = filename),
-- so we ship a single .sql migration file (no separate up/down files).
--
-- Key features:
-- - connections.connector_version: pin connections to "latest" or a specific version
-- - pipelines.source_connector_snapshot / destination_connector_snapshot: immutable JSONB snapshot written at pipeline start
-- - indexes on snapshot->>'type' for query performance
--
-- Author: System
-- Date: 2025-12-28

BEGIN;

-- 1) Connections: add connector_version (default "latest")
ALTER TABLE connections
ADD COLUMN IF NOT EXISTS connector_version VARCHAR(50) NOT NULL DEFAULT 'latest';

COMMENT ON COLUMN connections.connector_version IS
'Connector version pinned by this connection (e.g., "latest", "v1.0.0").';

-- Backfill any existing NULL/empty values to "latest"
UPDATE connections
SET connector_version = 'latest'
WHERE connector_version IS NULL OR connector_version = '';

-- Index for efficient routing/lookup
CREATE INDEX IF NOT EXISTS idx_connections_connector_type_version
ON connections(connector_type, connector_version);

-- 2) Pipelines: add connector snapshot JSONB columns
ALTER TABLE pipelines
ADD COLUMN IF NOT EXISTS source_connector_snapshot JSONB;

ALTER TABLE pipelines
ADD COLUMN IF NOT EXISTS destination_connector_snapshot JSONB;

COMMENT ON COLUMN pipelines.source_connector_snapshot IS
'Snapshot of source connector at pipeline start: {type, version, path, image, hash, resolved_at}.';

COMMENT ON COLUMN pipelines.destination_connector_snapshot IS
'Snapshot of destination connector at pipeline start: {type, version, path, image, hash, resolved_at}.';

-- Index on snapshot type for query performance (requested)
CREATE INDEX IF NOT EXISTS idx_pipelines_source_snapshot_type
ON pipelines ((source_connector_snapshot->>'type'));

CREATE INDEX IF NOT EXISTS idx_pipelines_destination_snapshot_type
ON pipelines ((destination_connector_snapshot->>'type'));

COMMIT;

