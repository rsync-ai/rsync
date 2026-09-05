-- Add CDC resource lifecycle management tables
-- These tables track CDC resources (replication slots, publications, server IDs) and checkpoints

-- Table: cdc_resources
-- Tracks CDC resources created/used by pipelines
-- Supports both shared (publication) and per-pipeline (replication slot) resources
CREATE TABLE IF NOT EXISTS cdc_resources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Ownership (NULL pipeline_id means shared resource scoped to connection)
    pipeline_id UUID REFERENCES pipelines(id) ON DELETE CASCADE,
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    source_table VARCHAR(255), -- NULL for database-level resources
    
    -- Resource identification
    resource_type VARCHAR(50) NOT NULL, -- 'publication', 'replication_slot', 'server_id', 'change_stream', etc.
    resource_name VARCHAR(255) NOT NULL, -- Actual name/ID of the resource
    
    -- State tracking
    status VARCHAR(50) NOT NULL DEFAULT 'active', -- 'active', 'orphaned', 'deleted', 'failed', 'inactive'
    
    -- Database-specific metadata
    database_type VARCHAR(50) NOT NULL, -- 'postgresql', 'mysql', 'mongodb', etc.
    metadata JSONB, -- Store additional resource details (e.g., reference_count for publications, LSN for slots)
    
    -- Lifecycle timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    last_verified_at TIMESTAMPTZ,
    
    -- Constraints
    UNIQUE(resource_type, resource_name, connection_id)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_cdc_resources_status ON cdc_resources(status, created_at);
CREATE INDEX IF NOT EXISTS idx_cdc_resources_pipeline ON cdc_resources(pipeline_id) WHERE pipeline_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cdc_resources_connection ON cdc_resources(connection_id);
CREATE INDEX IF NOT EXISTS idx_cdc_resources_type ON cdc_resources(resource_type);

-- Comments for documentation
COMMENT ON TABLE cdc_resources IS 'Tracks CDC resources (replication slots, publications, server IDs) for lifecycle management';
COMMENT ON COLUMN cdc_resources.pipeline_id IS 'NULL for shared resources (e.g., publications), otherwise specific pipeline';
COMMENT ON COLUMN cdc_resources.metadata IS 'JSONB storing resource-specific details like reference_count, LSN, etc.';


-- Table: pipeline_checkpoints
-- Stores per-table "last processed position" for CDC pipelines
-- Used for correctness/monitoring and to power UI decisions (new table backfill vs resume)
CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Ownership
    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    source_table VARCHAR(255) NOT NULL, -- Table name
    
    -- Link to CDC resource (optional)
    cdc_resource_id UUID REFERENCES cdc_resources(id) ON DELETE SET NULL,
    
    -- Position tracking (database-specific)
    position JSONB NOT NULL, -- Postgres: {lsn, tx_id}, MySQL: {gtid, binlog_file, binlog_pos}, Mongo: {resume_token}
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(pipeline_id, source_table)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_checkpoints_pipeline ON pipeline_checkpoints(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_checkpoints_connection ON pipeline_checkpoints(connection_id);
CREATE INDEX IF NOT EXISTS idx_checkpoints_resource ON pipeline_checkpoints(cdc_resource_id) WHERE cdc_resource_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_checkpoints_updated ON pipeline_checkpoints(updated_at);

-- Comments
COMMENT ON TABLE pipeline_checkpoints IS 'Per-table checkpoints for CDC pipelines, tracks last processed position';
COMMENT ON COLUMN pipeline_checkpoints.position IS 'JSONB storing database-specific position (LSN, GTID, resume token)';


-- Trigger to mark CDC resources as orphaned when pipeline is soft-deleted
-- Note: This assumes pipelines has a deleted_at column. If using hard deletes, cascade will handle cleanup.
CREATE OR REPLACE FUNCTION mark_cdc_resources_orphaned()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.deleted_at IS NOT NULL AND OLD.deleted_at IS NULL THEN
        UPDATE cdc_resources
        SET status = 'orphaned', deleted_at = NOW()
        WHERE pipeline_id = NEW.id AND status = 'active';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Only create trigger if pipelines table has deleted_at column
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pipelines' AND column_name = 'deleted_at'
    ) THEN
        DROP TRIGGER IF EXISTS trigger_mark_cdc_resources_orphaned ON pipelines;
        CREATE TRIGGER trigger_mark_cdc_resources_orphaned
        AFTER UPDATE OF deleted_at ON pipelines
        FOR EACH ROW
        EXECUTE FUNCTION mark_cdc_resources_orphaned();
    END IF;
END $$;

