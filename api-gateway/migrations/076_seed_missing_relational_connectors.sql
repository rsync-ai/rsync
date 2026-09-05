-- 076_seed_missing_relational_connectors.sql
--
-- RENUMBERED 072 → 076 to resolve a migration-number collision: this file (merged
-- via #478) shared "072" with 072_plan_gb_allowances.sql (merged earlier via #477).
-- The migrator keys schema_migrations on the FULL filename, so both applied fine and
-- environments already at 072 will simply re-apply this one under its new name — safe
-- because it is fully idempotent (ON CONFLICT DO UPDATE / DO NOTHING below).
--
-- KI-NLCHAT-CONNECTOR-VS-CONNECTION: the chat NL handler decides "is this a
-- supported connector?" via isKnownConnector, which runs
--   SELECT COUNT(*) FROM connector_catalog WHERE name = $1 AND status = 'active'
-- The hardcoded connector maps in chat_nl_pipeline.go are only a fallback used
-- when the DB is unavailable — never on prod. But connector_catalog was seeded
-- only through migration 052 (mysql/postgresql via 008; snowflake/bigquery/... via
-- 040; mongodb via 050; shopify via 052). oracle, sqlserver and databricks are
-- REAL, deployed, hand-curated connectors (dirs under
-- shared/mcp-connectors/public/database/, running as mcp-*:v1.0.0 on prod) that
-- were never added to connector_catalog. So a chat request like
--   "sync data from oracle to snowflake"
-- classified oracle as UNKNOWN → the connector_missing / "generate a connector"
-- path fired, offering to GENERATE an already-existing hand-curated connector
-- instead of prompting the user to create an oracle CONNECTION.
--
-- This migration backfills the missing rows so isKnownConnector returns true for
-- them and the chat flow routes to the correct "create/select a connection" path.
-- It is data-only (no DDL) and idempotent (ON CONFLICT). Executed inside the
-- migrator's own transaction — do NOT add a literal "BEGIN;".

INSERT INTO connector_catalog (name, display_name, description, category, source, latest_stable_version, supported_operations, auth_type, status)
VALUES
    ('oracle',     'Oracle',            'Connect to Oracle Database for batch export/import and Debezium LogMiner CDC', 'relational_db', 'builtin', '1.0.0', '["export", "import", "discover_schema", "test_connection"]', 'password', 'active'),
    ('sqlserver',  'SQL Server',        'Connect to Microsoft SQL Server for batch export/import and CDC',              'relational_db', 'builtin', '1.0.0', '["export", "import", "discover_schema", "test_connection"]', 'password', 'active'),
    ('databricks', 'Databricks',        'Connect to Databricks (Delta) for batch export/import and CDC-as-destination', 'warehouse',     'builtin', '1.0.0', '["export", "import", "discover_schema", "test_connection"]', 'password', 'active')
ON CONFLICT (name) DO UPDATE SET
    display_name          = EXCLUDED.display_name,
    description           = EXCLUDED.description,
    category              = EXCLUDED.category,
    latest_stable_version = EXCLUDED.latest_stable_version,
    supported_operations  = EXCLUDED.supported_operations,
    auth_type             = EXCLUDED.auth_type,
    status                = EXCLUDED.status;

-- Companion connector_versions rows (mirrors migration 052).
INSERT INTO connector_versions (connector_id, version, version_major, version_minor, version_patch, docker_image, status)
SELECT
    id,
    '1.0.0',
    1,
    0,
    0,
    'mcp-' || name || ':v1.0.0',
    'stable'
FROM connector_catalog
WHERE name IN ('oracle', 'sqlserver', 'databricks')
ON CONFLICT (connector_id, version) DO NOTHING;
