-- Migration 050: API polling pipeline type + new connector catalog entries
--
-- Adds:
-- 1. api_polling as a valid sync_mode (extends the check constraint on pipelines
--    and connections so api_polling pipelines can be created via chat/API).
-- 2. pipeline_api_cursors table — stores the last-seen cursor per (pipeline, table)
--    so incremental API polling picks up where the previous run left off.
-- 3. Catalog entries for the five new MCP connectors: hubspot, salesforce, github,
--    mongodb, slack.

-- ──────────────────────────────────────────────────────────────────────────────
-- 1. Extend sync_mode to accept "api_polling"
-- ──────────────────────────────────────────────────────────────────────────────

-- pipelines table constraint
ALTER TABLE pipelines DROP CONSTRAINT IF EXISTS pipelines_sync_mode_check;
ALTER TABLE pipelines ADD CONSTRAINT pipelines_sync_mode_check
    CHECK (sync_mode IS NULL OR sync_mode IN ('batch', 'cdc', 'api_polling'));

-- connections table constraint (api_polling not meaningful here, but keep consistent)
ALTER TABLE connections DROP CONSTRAINT IF EXISTS connections_sync_mode_check;
ALTER TABLE connections ADD CONSTRAINT connections_sync_mode_check
    CHECK (sync_mode IS NULL OR sync_mode IN ('batch', 'cdc', 'api_polling'));

-- ──────────────────────────────────────────────────────────────────────────────
-- 2. Cursor / watermark store for API polling pipelines
-- ──────────────────────────────────────────────────────────────────────────────
-- Each row tracks the opaque cursor (a timestamp, ISO string, or numeric ID)
-- returned by the source connector after a successful export.  The orchestrator
-- reads this before each polling run and writes it back after the run succeeds.

CREATE TABLE IF NOT EXISTS pipeline_api_cursors (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    -- "table" in the broad sense: API resource name (e.g. "contacts", "deals")
    resource_name   VARCHAR(255) NOT NULL,
    -- opaque cursor string returned by the connector (timestamp, next_page token, id, …)
    cursor_value    TEXT,
    -- ISO-8601 timestamp of the last successful export that produced this cursor
    last_synced_at  TIMESTAMPTZ,
    -- number of records transferred in the most recent run
    last_row_count  BIGINT DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, resource_name)
);

CREATE INDEX IF NOT EXISTS idx_api_cursors_pipeline ON pipeline_api_cursors(pipeline_id);

COMMENT ON TABLE pipeline_api_cursors IS
    'Watermark store for api_polling pipelines. One row per (pipeline, resource). '
    'The orchestrator reads cursor_value before each run and updates it on success.';

COMMENT ON COLUMN pipeline_api_cursors.cursor_value IS
    'Opaque cursor returned by the source connector: ISO-8601 timestamp, '
    'numeric record ID, or pagination token depending on the connector.';

-- ──────────────────────────────────────────────────────────────────────────────
-- 3. Polling schedule hints on pipelines (nullable, only used for api_polling)
-- ──────────────────────────────────────────────────────────────────────────────
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS polling_interval_seconds INTEGER;
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS polling_cursor_field      VARCHAR(255);
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS polling_initial_lookback  INTERVAL;

COMMENT ON COLUMN pipelines.polling_interval_seconds IS
    'For api_polling pipelines: how often to run (seconds). NULL = use schedule.';
COMMENT ON COLUMN pipelines.polling_cursor_field IS
    'Field used for incremental filtering, e.g. "updated_at", "LastModifiedDate".';
COMMENT ON COLUMN pipelines.polling_initial_lookback IS
    'On first run (no cursor), fetch records this far back. E.g. ''7 days''.';

-- ──────────────────────────────────────────────────────────────────────────────
-- 4. New connector catalog entries
-- ──────────────────────────────────────────────────────────────────────────────

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
    (
        'hubspot',
        'HubSpot',
        'Connect to HubSpot CRM for contacts, companies, deals, tickets, and pipeline data',
        'crm',
        'builtin',
        '1.0.0',
        '["export", "discover_schema", "test_connection", "list_contacts", "list_companies", "list_deals", "list_tickets"]',
        'bearer',
        'active'
    ),
    (
        'salesforce',
        'Salesforce',
        'Connect to Salesforce CRM for Account, Contact, Lead, Opportunity, and Case objects via REST API',
        'crm',
        'builtin',
        '1.0.0',
        '["export", "import", "discover_schema", "test_connection", "run_soql", "list_accounts", "list_contacts", "list_leads", "list_opportunities"]',
        'oauth',
        'active'
    ),
    (
        'github',
        'GitHub',
        'Connect to GitHub for repositories, issues, pull requests, commits, releases, and workflow runs',
        'developer',
        'builtin',
        '1.0.0',
        '["export", "discover_schema", "test_connection", "list_repos", "list_issues", "list_pull_requests", "list_commits", "list_releases"]',
        'bearer',
        'active'
    ),
    (
        'mongodb',
        'MongoDB',
        'Connect to MongoDB for flexible document storage with source, destination, and CDC change-stream support',
        'nosql',
        'builtin',
        '1.0.0',
        '["export", "import", "discover_schema", "test_connection", "aggregate", "list_collections", "upsert_data"]',
        'basic',
        'active'
    ),
    (
        'slack',
        'Slack',
        'Connect to Slack for channels, messages, users, and files; also supports posting messages as destination',
        'messaging',
        'builtin',
        '1.0.0',
        '["export", "import", "discover_schema", "test_connection", "list_channels", "list_users", "list_messages", "post_message"]',
        'bearer',
        'active'
    )
ON CONFLICT (name) DO UPDATE SET
    display_name          = EXCLUDED.display_name,
    description           = EXCLUDED.description,
    category              = EXCLUDED.category,
    latest_stable_version = EXCLUDED.latest_stable_version,
    supported_operations  = EXCLUDED.supported_operations,
    auth_type             = EXCLUDED.auth_type,
    status                = EXCLUDED.status;

-- Version rows
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
  AND name IN ('hubspot', 'salesforce', 'github', 'mongodb', 'slack')
ON CONFLICT (connector_id, version) DO NOTHING;
