-- Migration 040: Seed additional builtin connectors into connector_catalog
--
-- Root cause: chat validation uses connector_catalog; many connectors exist in the repo
-- but were not seeded in migration 008, causing destValid/sourceValid to fail.
--
-- This migration is idempotent (ON CONFLICT DO NOTHING).

BEGIN;

-- Seed additional builtin connectors that ship with this repo.
INSERT INTO connector_catalog (
    name,
    display_name,
    description,
    category,
    source,
    latest_stable_version,
    supported_operations,
    auth_type,
    status
)
VALUES
    ('redshift', 'Amazon Redshift', 'Connect to Amazon Redshift for data export and import', 'warehouse', 'builtin', '1.0.0', '["export", "import", "discover_schema", "test_connection"]', 'password', 'active'),
    ('snowflake', 'Snowflake', 'Connect to Snowflake for data export and import', 'warehouse', 'builtin', '1.0.0', '["export", "import", "discover_schema", "test_connection"]', 'password', 'active'),
    ('bigquery', 'BigQuery', 'Connect to Google BigQuery for data export and import', 'warehouse', 'builtin', '1.0.0', '["export", "import", "discover_schema", "test_connection"]', 'oauth', 'active'),
    ('elasticsearch', 'Elasticsearch', 'Connect to Elasticsearch for indexing/search workloads', 'search', 'builtin', '1.0.0', '["export", "import", "test_connection"]', 'api_key', 'active'),
    ('redis', 'Redis', 'Connect to Redis for caching and key-value workloads', 'database', 'builtin', '1.0.0', '["export", "import", "test_connection"]', 'password', 'active'),
    ('google-cloud-storage', 'Google Cloud Storage', 'Connect to Google Cloud Storage buckets for file storage operations', 'storage', 'builtin', '1.0.0', '["export", "import", "list_buckets", "test_connection"]', 'oauth', 'active'),
    ('azure-blob-storage', 'Azure Blob Storage', 'Connect to Azure Blob Storage containers for file storage operations', 'storage', 'builtin', '1.0.0', '["export", "import", "list_containers", "test_connection"]', 'oauth', 'active')
ON CONFLICT (name) DO NOTHING;

-- Insert initial versions for these connectors (same pattern as migration 008).
INSERT INTO connector_versions (connector_id, version, version_major, version_minor, version_patch, docker_image, status)
SELECT
    id,
    '1.0.0',
    1,
    0,
    0,
    'rsync-ai/' || name || '-mcp:1.0.0',
    'stable'
FROM connector_catalog
WHERE source = 'builtin'
  AND name IN (
      'redshift',
      'snowflake',
      'bigquery',
      'elasticsearch',
      'redis',
      'google-cloud-storage',
      'azure-blob-storage'
  )
ON CONFLICT (connector_id, version) DO NOTHING;

COMMIT;

