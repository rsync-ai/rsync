-- Agentic Enhancements
-- Adds Tool Registry and Pipeline Versioning for robust agent operations

-- Tools Registry
-- Enables agents to search for available tools by capability, not just file scanning.
CREATE TABLE IF NOT EXISTS tools (
    name VARCHAR(255) PRIMARY KEY,
    description TEXT,
    capabilities JSONB,       -- e.g. ["read", "write", "sql", "api"]
    schema JSONB,             -- The full JSON schema of the tool
    source_type VARCHAR(50) NOT NULL CHECK (source_type IN ('built-in', 'jit-generated')), 
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tools_capabilities ON tools USING gin(capabilities);

-- Pipeline Versions
-- Enables "Time Travel" for pipelines. If an agent breaks a pipeline, we can revert.
CREATE TABLE IF NOT EXISTS pipeline_versions (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    pipeline_id UUID REFERENCES pipelines(id) ON DELETE CASCADE,
    version INT NOT NULL,
    config JSONB NOT NULL,
    natural_language_request TEXT, -- The request that triggered this version
    change_reason TEXT,            -- e.g. "Agent Planner: Optimized step 2"
    created_by UUID REFERENCES users(id), -- Or Agent ID if we had one
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(pipeline_id, version)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_versions_pipeline_id ON pipeline_versions(pipeline_id);
