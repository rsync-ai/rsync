-- Migration: 004_genericity_enhancements
-- Purpose: Add connector registry and execution plan persistence for 100% generic agent system
-- Date: December 8, 2025

-- =============================================================================
-- 1. CONNECTOR REGISTRY TABLE
-- =============================================================================
-- Stores connector metadata and capabilities dynamically.
-- Agents query this instead of hardcoding connector types.

CREATE TABLE IF NOT EXISTS connector_registry (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    connector_type character varying(100) NOT NULL UNIQUE,
    connector_category character varying(50) NOT NULL,
    display_name character varying(255) NOT NULL,
    description text,
    supports_source boolean DEFAULT true,
    supports_destination boolean DEFAULT false,
    default_source_operation character varying(100),
    default_destination_operation character varying(100),
    capabilities jsonb NOT NULL DEFAULT '{}',
    -- capabilities structure:
    -- {
    --   "operations": [{"name": "export", "type": "source", "description": "..."}],
    --   "terminology": {"entity": "table", "record": "row", "field": "column"},
    --   "features": {"batch_export": true, "incremental_sync": true}
    -- }
    config_schema jsonb,
    -- config_schema: JSON Schema for configuration validation
    -- {"required": ["host", "user", "password"], "properties": {...}}
    is_builtin boolean DEFAULT false,
    -- Built-in connectors (mysql, postgresql, aws-s3) vs JIT-generated
    version character varying(50) DEFAULT '1.0.0',
    status character varying(50) DEFAULT 'active' CHECK (status IN ('active', 'deprecated', 'disabled')),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_connector_registry_category ON connector_registry(connector_category);
CREATE INDEX IF NOT EXISTS idx_connector_registry_status ON connector_registry(status);
CREATE INDEX IF NOT EXISTS idx_connector_registry_capabilities ON connector_registry USING GIN(capabilities);

-- Trigger for updated_at
DROP TRIGGER IF EXISTS update_connector_registry_updated_at ON connector_registry;
CREATE TRIGGER update_connector_registry_updated_at 
    BEFORE UPDATE ON connector_registry 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- 2. SEED BUILT-IN CONNECTORS
-- =============================================================================
-- Pre-populate with known connector types.
-- Agents can query this registry instead of hardcoding.

INSERT INTO connector_registry (connector_type, connector_category, display_name, description, supports_source, supports_destination, default_source_operation, default_destination_operation, capabilities, is_builtin)
VALUES 
-- Relational Databases
('mysql', 'relational_db', 'MySQL', 'MySQL relational database', true, true, 'export', 'import', 
 '{"operations": [{"name": "export", "type": "source"}, {"name": "import", "type": "destination"}, {"name": "query", "type": "source"}], "terminology": {"entity": "table", "record": "row", "field": "column"}, "features": {"batch_export": true, "incremental_sync": true}}'::jsonb, 
 true),

('postgresql', 'relational_db', 'PostgreSQL', 'PostgreSQL relational database', true, true, 'export', 'import',
 '{"operations": [{"name": "export", "type": "source"}, {"name": "import", "type": "destination"}, {"name": "query", "type": "source"}], "terminology": {"entity": "table", "record": "row", "field": "column"}, "features": {"batch_export": true, "incremental_sync": true}}'::jsonb,
 true),

('mariadb', 'relational_db', 'MariaDB', 'MariaDB relational database', true, true, 'export', 'import',
 '{"operations": [{"name": "export", "type": "source"}, {"name": "import", "type": "destination"}], "terminology": {"entity": "table", "record": "row", "field": "column"}}'::jsonb,
 true),

('oracle', 'relational_db', 'Oracle', 'Oracle Database', true, true, 'export', 'import',
 '{"operations": [{"name": "export", "type": "source"}, {"name": "import", "type": "destination"}], "terminology": {"entity": "table", "record": "row", "field": "column"}}'::jsonb,
 true),

('sqlserver', 'relational_db', 'SQL Server', 'Microsoft SQL Server', true, true, 'export', 'import',
 '{"operations": [{"name": "export", "type": "source"}, {"name": "import", "type": "destination"}], "terminology": {"entity": "table", "record": "row", "field": "column"}}'::jsonb,
 true),

('sqlite', 'relational_db', 'SQLite', 'SQLite database', true, true, 'export', 'import',
 '{"operations": [{"name": "export", "type": "source"}, {"name": "import", "type": "destination"}], "terminology": {"entity": "table", "record": "row", "field": "column"}}'::jsonb,
 true),

-- Document Databases
('mongodb', 'document_db', 'MongoDB', 'MongoDB document database', true, true, 'export', 'import',
 '{"operations": [{"name": "export", "type": "source"}, {"name": "import", "type": "destination"}], "terminology": {"entity": "collection", "record": "document", "field": "field"}, "features": {"schema_inference": true}}'::jsonb,
 true),

('elasticsearch', 'document_db', 'Elasticsearch', 'Elasticsearch search engine', true, true, 'export', 'import',
 '{"operations": [{"name": "export", "type": "source"}, {"name": "import", "type": "destination"}], "terminology": {"entity": "index", "record": "document", "field": "field"}}'::jsonb,
 true),

-- Cloud Storage
('aws-s3', 'cloud_storage', 'AWS S3', 'Amazon S3 storage', true, true, 'read', 'import_data',
 '{"operations": [{"name": "read", "type": "source"}, {"name": "import_data", "type": "destination"}, {"name": "list", "type": "source"}], "terminology": {"entity": "bucket", "record": "file", "field": "key"}, "features": {"formats": ["json", "csv", "parquet"]}}'::jsonb,
 true),

('gcs', 'cloud_storage', 'Google Cloud Storage', 'Google Cloud Storage', true, true, 'read', 'import_data',
 '{"operations": [{"name": "read", "type": "source"}, {"name": "import_data", "type": "destination"}], "terminology": {"entity": "bucket", "record": "file", "field": "key"}}'::jsonb,
 true),

('azure_blob', 'cloud_storage', 'Azure Blob', 'Azure Blob Storage', true, true, 'read', 'import_data',
 '{"operations": [{"name": "read", "type": "source"}, {"name": "import_data", "type": "destination"}], "terminology": {"entity": "container", "record": "blob", "field": "key"}}'::jsonb,
 true),

-- Data Warehouses
('snowflake', 'data_warehouse', 'Snowflake', 'Snowflake data warehouse', true, true, 'query', 'load',
 '{"operations": [{"name": "query", "type": "source"}, {"name": "load", "type": "destination"}], "terminology": {"entity": "table", "record": "row", "field": "column"}, "features": {"batch_load": true}}'::jsonb,
 true),

('bigquery', 'data_warehouse', 'BigQuery', 'Google BigQuery', true, true, 'query', 'load',
 '{"operations": [{"name": "query", "type": "source"}, {"name": "load", "type": "destination"}], "terminology": {"entity": "table", "record": "row", "field": "column"}}'::jsonb,
 true),

('redshift', 'data_warehouse', 'Redshift', 'Amazon Redshift', true, true, 'query', 'load',
 '{"operations": [{"name": "query", "type": "source"}, {"name": "load", "type": "destination"}], "terminology": {"entity": "table", "record": "row", "field": "column"}}'::jsonb,
 true),

-- Streaming
('kafka', 'streaming', 'Kafka', 'Apache Kafka', true, true, 'consume', 'produce',
 '{"operations": [{"name": "consume", "type": "source"}, {"name": "produce", "type": "destination"}], "terminology": {"entity": "topic", "record": "message", "field": "field"}}'::jsonb,
 true)

ON CONFLICT (connector_type) DO UPDATE SET
    capabilities = EXCLUDED.capabilities,
    updated_at = now();

-- =============================================================================
-- 3. EXECUTION PLAN PERSISTENCE
-- =============================================================================
-- Store execution plans in pipelines table so they survive restarts.

ALTER TABLE pipelines 
ADD COLUMN IF NOT EXISTS execution_plan jsonb;

-- Add index for plan queries
CREATE INDEX IF NOT EXISTS idx_pipelines_execution_plan ON pipelines USING GIN(execution_plan);

-- =============================================================================
-- 4. CATEGORY OPERATIONS LOOKUP TABLE (Optional but useful)
-- =============================================================================
-- Stores category-level defaults. Agents can query this for operation mapping.

CREATE TABLE IF NOT EXISTS connector_categories (
    category character varying(50) PRIMARY KEY,
    display_name character varying(100) NOT NULL,
    source_operations jsonb NOT NULL DEFAULT '[]',
    destination_operations jsonb NOT NULL DEFAULT '[]',
    default_source_op character varying(100),
    default_dest_op character varying(100),
    terminology jsonb NOT NULL DEFAULT '{}'
);

INSERT INTO connector_categories (category, display_name, source_operations, destination_operations, default_source_op, default_dest_op, terminology)
VALUES
('relational_db', 'Relational Database', 
 '["export", "query", "discover_schema"]'::jsonb, 
 '["import", "execute"]'::jsonb, 
 'export', 'import',
 '{"entity": "table", "entityPlural": "tables", "record": "row", "recordPlural": "rows", "field": "column", "fieldPlural": "columns"}'::jsonb),

('document_db', 'Document Database',
 '["export", "query", "discover_schema"]'::jsonb,
 '["import", "upsert"]'::jsonb,
 'export', 'import',
 '{"entity": "collection", "entityPlural": "collections", "record": "document", "recordPlural": "documents", "field": "field", "fieldPlural": "fields"}'::jsonb),

('cloud_storage', 'Cloud Storage',
 '["read", "list", "discover_schema"]'::jsonb,
 '["import_data", "write"]'::jsonb,
 'read', 'import_data',
 '{"entity": "bucket", "entityPlural": "buckets", "record": "file", "recordPlural": "files", "field": "key", "fieldPlural": "keys"}'::jsonb),

('data_warehouse', 'Data Warehouse',
 '["query", "export", "discover_schema"]'::jsonb,
 '["load", "merge"]'::jsonb,
 'query', 'load',
 '{"entity": "table", "entityPlural": "tables", "record": "row", "recordPlural": "rows", "field": "column", "fieldPlural": "columns"}'::jsonb),

('streaming', 'Streaming Platform',
 '["consume", "subscribe"]'::jsonb,
 '["produce", "publish"]'::jsonb,
 'consume', 'produce',
 '{"entity": "topic", "entityPlural": "topics", "record": "message", "recordPlural": "messages", "field": "field", "fieldPlural": "fields"}'::jsonb),

('api_saas', 'API / SaaS',
 '["fetch", "list", "discover_schema"]'::jsonb,
 '["push", "create", "update"]'::jsonb,
 'fetch', 'push',
 '{"entity": "endpoint", "entityPlural": "endpoints", "record": "record", "recordPlural": "records", "field": "field", "fieldPlural": "fields"}'::jsonb)

ON CONFLICT (category) DO UPDATE SET
    source_operations = EXCLUDED.source_operations,
    destination_operations = EXCLUDED.destination_operations,
    default_source_op = EXCLUDED.default_source_op,
    default_dest_op = EXCLUDED.default_dest_op,
    terminology = EXCLUDED.terminology;

-- =============================================================================
-- 5. COMMENTS FOR DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE connector_registry IS 'Generic connector capability registry - agents query this instead of hardcoding connector types';
COMMENT ON TABLE connector_categories IS 'Category-level operation defaults - used when specific connector capabilities are unknown';
COMMENT ON COLUMN pipelines.execution_plan IS 'Persisted execution plan from Planner agent - survives restarts';
COMMENT ON COLUMN connector_registry.capabilities IS 'JSONB containing operations, terminology, and features for this connector';

