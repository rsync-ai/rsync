-- Seed the shopify-admin-graphql connector into connector_catalog.
--
-- The MCP connector exists on disk under shared/mcp-connectors/public/shopify-admin-graphql,
-- but the catalog table (which chat / discovery / capability checks query) was never seeded.
-- That made the chat NL pipeline reply "I don't have a connector for shopify" even though
-- pipelines built against this connector run successfully.

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
VALUES (
    'shopify-admin-graphql',
    'Shopify',
    'Connect to Shopify Admin via the GraphQL API for products, orders, customers, draft orders, inventory, and fulfillment data',
    'api_saas',
    'builtin',
    '1.0.0',
    '["export", "discover_schema", "test_connection", "list_products", "list_orders", "list_customers", "list_draft_orders"]',
    'api_key',
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
WHERE name = 'shopify-admin-graphql'
ON CONFLICT (connector_id, version) DO NOTHING;
