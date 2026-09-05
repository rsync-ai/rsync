-- Add dataset namespace column to pipelines table for stable cloud storage paths
-- The dataset is a human-readable, slug-ified namespace used in storage paths:
-- {prefix}/{dataset}/{db_or_schema}/{table}/dt=YYYY-MM-DD/...

-- Add dataset column (nullable initially for existing pipelines)
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS dataset character varying(63);

-- Add run_mode column for reload vs resume semantics
-- Values: 'resume' (default, continue from checkpoints) or 'reload' (rebuild from scratch)
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS default_run_mode character varying(20) DEFAULT 'resume';

-- Add check constraint for dataset format (lowercase alphanumeric with hyphens)
ALTER TABLE pipelines DROP CONSTRAINT IF EXISTS pipelines_dataset_format_check;
ALTER TABLE pipelines ADD CONSTRAINT pipelines_dataset_format_check
    CHECK (dataset IS NULL OR dataset ~ '^[a-z0-9][a-z0-9-]*[a-z0-9]$' OR dataset ~ '^[a-z0-9]$');

-- Add check constraint for default_run_mode
ALTER TABLE pipelines DROP CONSTRAINT IF EXISTS pipelines_default_run_mode_check;
ALTER TABLE pipelines ADD CONSTRAINT pipelines_default_run_mode_check
    CHECK (default_run_mode IS NULL OR default_run_mode IN ('resume', 'reload'));

-- Create unique index for (destination_connection_id, dataset) to prevent collisions
-- Only applies to cloud storage destinations where dataset uniqueness matters
-- This is a partial unique index that allows NULL datasets and only enforces
-- uniqueness when both destination_connection_id and dataset are set
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipelines_dest_dataset_unique
    ON pipelines (destination_connection_id, dataset)
    WHERE destination_connection_id IS NOT NULL AND dataset IS NOT NULL;

-- Index for querying pipelines by dataset
CREATE INDEX IF NOT EXISTS idx_pipelines_dataset ON pipelines (dataset) WHERE dataset IS NOT NULL;

-- Add comments for documentation
COMMENT ON COLUMN pipelines.dataset IS 'Stable namespace for cloud storage paths. Auto-derived from pipeline name if not specified. Must be unique per destination connection.';
COMMENT ON COLUMN pipelines.default_run_mode IS 'Default execution mode: resume (continue from checkpoints) or reload (rebuild from scratch).';
