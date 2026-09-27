-- 120_seed_sample_data_connector.sql
--
-- The zero-credential first-run demo (sample-data → postgresql) hands off to the
-- chat, and the chat NL handler decides "is this a supported connector?" with
-- isKnownConnector:
--   SELECT COUNT(*) FROM connector_catalog WHERE name = $1 AND status = 'active'
-- sample-data is a real, deployed connector (shared/mcp-connectors/public/
-- sample-data, running as mcp-sample-data:v1.0.0) that was never added to
-- connector_catalog. So "sync sample-data to postgresql" found no catalog row,
-- every deterministic fast path rejected the pair, and the request fell through to
-- the LLM — which a zero-credential install does not have, so the demo ended on
-- "no language model is configured" instead of a confirmation card.
--
-- Same shape as 076: backfill the row so isKnownConnector returns true and the
-- deterministic parser reaches the confirmation without the LLM. Data-only (no
-- DDL) and idempotent (ON CONFLICT). Executed inside the migrator's own
-- transaction — do NOT add a literal "BEGIN;".

INSERT INTO connector_catalog (name, display_name, description, category, source, latest_stable_version, supported_operations, auth_type, status)
VALUES
    ('sample-data', 'Sample Data (Demo)', 'Zero-setup demo source with bundled customers, orders and products datasets', 'sample', 'builtin', '1.0.0', '["export", "discover_schema", "test_connection"]', 'none', 'active')
ON CONFLICT (name) DO UPDATE SET
    display_name          = EXCLUDED.display_name,
    description           = EXCLUDED.description,
    category              = EXCLUDED.category,
    latest_stable_version = EXCLUDED.latest_stable_version,
    supported_operations  = EXCLUDED.supported_operations,
    auth_type             = EXCLUDED.auth_type,
    status                = EXCLUDED.status;

-- Companion connector_versions row (mirrors 052/076); the image name matches
-- docker-compose.mcp.yml's sample-data-v1-0-0-mcp service.
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
WHERE name = 'sample-data'
ON CONFLICT (connector_id, version) DO NOTHING;
