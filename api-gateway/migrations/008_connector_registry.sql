-- =============================================================================
-- Migration 008: Connector Registry System
-- =============================================================================
-- This migration creates the schema for the JIT Connector Discovery & Generation
-- system, including global connector catalog, version management, user-scoped
-- registry, running instances tracking, and generation job monitoring.
-- =============================================================================

-- =============================================================================
-- GLOBAL CONNECTOR CATALOG
-- =============================================================================
-- Available to ALL users. Source of truth for connector definitions.

CREATE TABLE connector_catalog (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identity
    name VARCHAR(100) UNIQUE NOT NULL,          -- "mysql", "notion", "stripe"
    display_name VARCHAR(200) NOT NULL,         -- "MySQL Database"
    description TEXT,
    category VARCHAR(50) NOT NULL,              -- "relational_db", "api_saas", "cloud_storage"
    
    -- Source & Generation
    source VARCHAR(50) DEFAULT 'builtin',       -- "builtin", "generated", "community"
    generated_by_user_id UUID REFERENCES users(id),
    generation_job_id UUID,                     -- Link to generation job
    context7_library_id VARCHAR(500),           -- "/mongodb/docs"
    
    -- Versioning
    latest_stable_version VARCHAR(20),          -- "2.1.0"
    latest_beta_version VARCHAR(20),            -- "2.2.0-beta"
    
    -- Capabilities (cached from spec)
    supported_operations JSONB DEFAULT '[]',    -- ["export", "import", "discover_schema"]
    config_schema JSONB,                        -- JSON Schema for validation
    auth_type VARCHAR(50),                      -- "api_key", "oauth2", "basic"
    
    -- Status & Visibility
    status VARCHAR(20) DEFAULT 'active',        -- "active", "deprecated", "disabled"
    is_public BOOLEAN DEFAULT TRUE,             -- Available to all users
    
    -- Metadata
    logo_url VARCHAR(500),
    documentation_url VARCHAR(500),
    repository_url VARCHAR(500),
    
    -- Stats
    total_users INTEGER DEFAULT 0,
    total_connections INTEGER DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- CONNECTOR VERSIONS
-- =============================================================================
-- Each release of a connector. Supports multiple versions running simultaneously.

CREATE TABLE connector_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connector_id UUID NOT NULL REFERENCES connector_catalog(id) ON DELETE CASCADE,
    
    -- Version Info
    version VARCHAR(20) NOT NULL,               -- "2.1.0", "1.0.0-beta.1"
    version_major INTEGER NOT NULL,             -- 2
    version_minor INTEGER NOT NULL,             -- 1
    version_patch INTEGER NOT NULL,             -- 0
    version_prerelease VARCHAR(50),             -- "beta.1" or NULL
    
    -- Docker Image (Immutable)
    docker_image VARCHAR(500) NOT NULL,         -- "registry.rsync.ai/mcp/mysql:2.1.0"
    docker_digest VARCHAR(100),                 -- "sha256:abc123..."
    image_size_mb INTEGER,
    
    -- Build Info
    git_commit_sha VARCHAR(40),                 -- Full SHA for traceability
    build_timestamp TIMESTAMP,
    
    -- Compatibility
    min_orchestrator_version VARCHAR(20),       -- "1.5.0"
    max_orchestrator_version VARCHAR(20),       -- NULL = no upper limit
    
    -- Changelog
    changelog TEXT,
    breaking_changes TEXT[],
    new_features TEXT[],
    bug_fixes TEXT[],
    
    -- Status
    status VARCHAR(20) DEFAULT 'stable',        -- "stable", "beta", "deprecated", "yanked"
    deprecation_message TEXT,
    successor_version_id UUID,                  -- Points to replacement version
    
    -- Usage Stats
    downloads INTEGER DEFAULT 0,
    active_instances INTEGER DEFAULT 0,
    
    -- Timestamps
    released_at TIMESTAMP DEFAULT NOW(),
    released_by_user_id UUID REFERENCES users(id),
    deprecated_at TIMESTAMP,
    yanked_at TIMESTAMP,
    
    UNIQUE(connector_id, version)
);

-- =============================================================================
-- USER CONNECTOR REGISTRY
-- =============================================================================
-- Per-user connector access and preferences.

CREATE TABLE user_connectors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    connector_id UUID NOT NULL REFERENCES connector_catalog(id) ON DELETE CASCADE,
    
    -- Version Preferences
    preferred_version VARCHAR(20),              -- NULL = use latest stable
    auto_update BOOLEAN DEFAULT TRUE,           -- Auto-update to latest stable
    allow_beta BOOLEAN DEFAULT FALSE,           -- Include beta versions in auto-update
    
    -- Usage Tracking
    first_used_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP,
    usage_count INTEGER DEFAULT 0,
    connections_count INTEGER DEFAULT 0,
    
    -- User Preferences
    is_favorite BOOLEAN DEFAULT FALSE,
    is_hidden BOOLEAN DEFAULT FALSE,            -- Hide from connector browser
    custom_alias VARCHAR(100),                  -- User-defined name
    notes TEXT,
    
    -- Timestamps
    added_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(user_id, connector_id)
);

-- =============================================================================
-- CONNECTOR INSTANCES (Running Containers)
-- =============================================================================
-- Active Docker containers for MCP connectors.

CREATE TABLE connector_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connector_id UUID NOT NULL REFERENCES connector_catalog(id),
    version_id UUID NOT NULL REFERENCES connector_versions(id),
    
    -- Container Info
    container_id VARCHAR(100) NOT NULL UNIQUE,  -- Docker container ID
    container_name VARCHAR(200) NOT NULL,       -- "mysql-mcp-abc123"
    
    -- Network
    host VARCHAR(100) NOT NULL,                 -- Container hostname
    port INTEGER NOT NULL DEFAULT 8080,         -- MCP server port
    internal_ip VARCHAR(45),                    -- Docker network IP
    
    -- Status
    status VARCHAR(20) DEFAULT 'starting',      -- "starting", "running", "stopping", "stopped", "failed"
    health_status VARCHAR(20) DEFAULT 'unknown', -- "healthy", "unhealthy", "unknown"
    last_health_check TIMESTAMP,
    consecutive_failures INTEGER DEFAULT 0,
    
    -- Resources
    cpu_limit FLOAT,                            -- CPU quota (0.5 = 50%)
    memory_limit_mb INTEGER,                    -- Memory limit in MB
    cpu_usage FLOAT,                            -- Current CPU usage
    memory_usage_mb INTEGER,                    -- Current memory usage
    
    -- Lifecycle
    started_at TIMESTAMP DEFAULT NOW(),
    stopped_at TIMESTAMP,
    restart_count INTEGER DEFAULT 0,
    last_restart_reason TEXT,
    
    -- Usage
    active_connections INTEGER DEFAULT 0,       -- Current active connections
    total_requests INTEGER DEFAULT 0,           -- Total requests served
    avg_response_time_ms FLOAT,                 -- Average response time
    
    -- Error Tracking
    last_error TEXT,
    last_error_at TIMESTAMP
);

-- =============================================================================
-- CONNECTOR GENERATION JOBS
-- =============================================================================
-- Track JIT connector generation progress.

CREATE TABLE connector_generation_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Request
    requested_by_user_id UUID NOT NULL REFERENCES users(id),
    connector_name VARCHAR(100) NOT NULL,
    description TEXT,
    
    -- Context7 Discovery
    context7_query VARCHAR(500),                -- Original search query
    context7_library_id VARCHAR(500),           -- Resolved library ID
    context7_library_name VARCHAR(200),         -- Display name from Context7
    documentation_fetched BOOLEAN DEFAULT FALSE,
    documentation_length INTEGER,
    documentation_topics TEXT[],                -- Topics fetched
    
    -- Pipeline Stages
    status VARCHAR(50) DEFAULT 'pending',
    current_stage VARCHAR(50),
    stages_completed TEXT[] DEFAULT '{}',
    progress_percent INTEGER DEFAULT 0,
    
    -- Stage Details (JSONB for flexibility)
    stage_results JSONB DEFAULT '{}',
    /*
    {
      "doc_research": {"duration_ms": 1200, "docs_length": 15000},
      "research": {"duration_ms": 3500, "endpoints_found": 12},
      "architect": {"duration_ms": 2800, "operations": ["export", "import"]},
      "qa": {"duration_ms": 5000, "tests_passed": 8, "tests_failed": 0},
      "build": {"duration_ms": 45000, "image_size_mb": 125}
    }
    */
    
    -- Result
    connector_id UUID REFERENCES connector_catalog(id),
    version_id UUID REFERENCES connector_versions(id),
    docker_image VARCHAR(500),
    output_path VARCHAR(500),                   -- Path to generated files
    
    -- Error Handling
    error_message TEXT,
    error_stage VARCHAR(50),
    error_details JSONB,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    
    -- Timing
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    duration_ms INTEGER,
    
    -- Metadata
    request_metadata JSONB                      -- Original request params
);

-- =============================================================================
-- INDEXES
-- =============================================================================

-- Catalog lookups
CREATE INDEX idx_catalog_name ON connector_catalog(name);
CREATE INDEX idx_catalog_category ON connector_catalog(category);
CREATE INDEX idx_catalog_status ON connector_catalog(status);
CREATE INDEX idx_catalog_source ON connector_catalog(source);
CREATE INDEX idx_catalog_context7 ON connector_catalog(context7_library_id);

-- Version lookups
CREATE INDEX idx_versions_connector ON connector_versions(connector_id);
CREATE INDEX idx_versions_status ON connector_versions(status);
CREATE INDEX idx_versions_semver ON connector_versions(version_major, version_minor, version_patch);

-- User connector lookups
CREATE INDEX idx_user_connectors_user ON user_connectors(user_id);
CREATE INDEX idx_user_connectors_connector ON user_connectors(connector_id);
CREATE INDEX idx_user_connectors_usage ON user_connectors(last_used_at DESC);

-- Instance lookups
CREATE INDEX idx_instances_connector ON connector_instances(connector_id);
CREATE INDEX idx_instances_status ON connector_instances(status);
CREATE INDEX idx_instances_health ON connector_instances(health_status);
CREATE INDEX idx_instances_container ON connector_instances(container_id);

-- Job lookups
CREATE INDEX idx_jobs_user ON connector_generation_jobs(requested_by_user_id);
CREATE INDEX idx_jobs_status ON connector_generation_jobs(status);
CREATE INDEX idx_jobs_connector_name ON connector_generation_jobs(connector_name);

-- =============================================================================
-- VIEWS
-- =============================================================================

-- User's connector view with version info
CREATE VIEW user_connector_details AS
SELECT 
    uc.user_id,
    cc.id AS connector_id,
    cc.name,
    cc.display_name,
    cc.category,
    cc.source,
    COALESCE(uc.preferred_version, cc.latest_stable_version) AS version,
    uc.auto_update,
    uc.is_favorite,
    uc.usage_count,
    uc.last_used_at,
    cc.status AS connector_status,
    cc.logo_url
FROM user_connectors uc
JOIN connector_catalog cc ON cc.id = uc.connector_id
WHERE uc.is_hidden = FALSE;

-- Available connectors (for catalog browsing)
CREATE VIEW available_connectors AS
SELECT 
    cc.*,
    cv.version AS latest_version,
    cv.docker_image AS latest_image,
    cv.released_at AS latest_released_at,
    COUNT(DISTINCT uc.user_id) AS users_count
FROM connector_catalog cc
LEFT JOIN connector_versions cv ON cv.connector_id = cc.id 
    AND cv.version = cc.latest_stable_version
LEFT JOIN user_connectors uc ON uc.connector_id = cc.id
WHERE cc.status = 'active' AND cc.is_public = TRUE
GROUP BY cc.id, cv.version, cv.docker_image, cv.released_at;

-- =============================================================================
-- SEED DATA: Builtin Connectors
-- =============================================================================

INSERT INTO connector_catalog (name, display_name, description, category, source, latest_stable_version, supported_operations, auth_type, status)
VALUES 
    ('mysql', 'MySQL Database', 'Connect to MySQL databases for data export and import', 'database', 'builtin', '1.0.0', '["export", "import", "discover_schema", "test_connection"]', 'password', 'active'),
    ('postgresql', 'PostgreSQL Database', 'Connect to PostgreSQL databases for data operations', 'database', 'builtin', '1.0.0', '["export", "import", "discover_schema", "test_connection"]', 'password', 'active'),
    ('aws-s3', 'AWS S3', 'Connect to AWS S3 buckets for file storage operations', 'storage', 'builtin', '1.0.0', '["export", "import", "list_buckets", "test_connection"]', 'api_key', 'active'),
    ('kafka', 'Apache Kafka', 'Connect to Kafka clusters for streaming data', 'streaming', 'builtin', '1.0.0', '["export", "import", "list_topics", "test_connection"]', 'sasl', 'active'),
    ('minio', 'MinIO', 'Connect to MinIO object storage', 'storage', 'builtin', '1.0.0', '["export", "import", "list_buckets", "test_connection"]', 'api_key', 'active');

-- Insert initial versions for builtin connectors
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
WHERE source = 'builtin';

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_connector_catalog_updated_at
    BEFORE UPDATE ON connector_catalog
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_connectors_updated_at
    BEFORE UPDATE ON user_connectors
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Update connector stats when user_connectors changes
CREATE OR REPLACE FUNCTION update_connector_user_stats()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE connector_catalog 
        SET total_users = total_users + 1 
        WHERE id = NEW.connector_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE connector_catalog 
        SET total_users = total_users - 1 
        WHERE id = OLD.connector_id;
    END IF;
    RETURN NULL;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_connector_stats_on_user_change
    AFTER INSERT OR DELETE ON user_connectors
    FOR EACH ROW
    EXECUTE FUNCTION update_connector_user_stats();
