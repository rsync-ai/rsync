-- Add sync_mode, cdc_mode, and sync_mode_source columns to pipelines table
-- This allows pipelines to override connection-level sync settings

-- sync_mode: "batch" or "cdc" - nullable allows falling back to connection default
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS sync_mode character varying(20);

-- cdc_mode: "initial" (full snapshot first) or "streaming_only" (no snapshot)
-- Only applicable when sync_mode = 'cdc', nullable to fall back to connection default
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS cdc_mode character varying(30);

-- sync_mode_source: tracks why the pipeline has its current sync settings
-- Values: 'connection_default' | 'pipeline_override' | 'user_manual'
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS sync_mode_source character varying(30);

-- Add check constraint for sync_mode
ALTER TABLE pipelines DROP CONSTRAINT IF EXISTS pipelines_sync_mode_check;
ALTER TABLE pipelines ADD CONSTRAINT pipelines_sync_mode_check 
    CHECK (sync_mode IS NULL OR sync_mode IN ('batch', 'cdc'));

-- Add check constraint for cdc_mode
ALTER TABLE pipelines DROP CONSTRAINT IF EXISTS pipelines_cdc_mode_check;
ALTER TABLE pipelines ADD CONSTRAINT pipelines_cdc_mode_check 
    CHECK (cdc_mode IS NULL OR cdc_mode IN ('initial', 'streaming_only'));

-- Add check constraint for sync_mode_source
ALTER TABLE pipelines DROP CONSTRAINT IF EXISTS pipelines_sync_mode_source_check;
ALTER TABLE pipelines ADD CONSTRAINT pipelines_sync_mode_source_check 
    CHECK (sync_mode_source IS NULL OR sync_mode_source IN ('connection_default', 'pipeline_override', 'user_manual'));

-- Create indexes for filtering and analytics
CREATE INDEX IF NOT EXISTS idx_pipelines_sync_mode ON pipelines(sync_mode) WHERE sync_mode IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pipelines_sync_mode_source ON pipelines(sync_mode_source) WHERE sync_mode_source IS NOT NULL;

-- Add comment for documentation
COMMENT ON COLUMN pipelines.sync_mode IS 'Optional override for connection-level sync_mode. NULL means use connection default.';
COMMENT ON COLUMN pipelines.cdc_mode IS 'Optional override for CDC start mode. NULL means use connection default.';
COMMENT ON COLUMN pipelines.sync_mode_source IS 'Tracks the origin of sync_mode setting: connection_default, pipeline_override, or user_manual';

