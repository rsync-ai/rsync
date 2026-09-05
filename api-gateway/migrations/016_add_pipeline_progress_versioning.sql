-- Migration: Add versioning and optimistic locking to pipeline_progress
-- Purpose: Enable safe concurrent updates from Event Projector and StateUpdateActivity

-- Add version column for optimistic locking
ALTER TABLE pipeline_progress
  ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1;

-- Create index for version-based queries
CREATE INDEX IF NOT EXISTS idx_pipeline_progress_version ON pipeline_progress(pipeline_id, version);

-- Add comment for documentation
COMMENT ON COLUMN pipeline_progress.version IS 'Optimistic locking version. Incremented on each update. Used to prevent race conditions between Projector (best-effort) and StateUpdateActivity (authoritative).';
