-- Add lifecycle tracking to pipeline_drafts
ALTER TABLE pipeline_drafts ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE pipeline_drafts ADD COLUMN IF NOT EXISTS state VARCHAR(50) DEFAULT 'ready';

-- Create index for cleanup queries
CREATE INDEX IF NOT EXISTS idx_pipeline_drafts_last_activity ON pipeline_drafts(last_activity_at) WHERE status = 'active';

-- Add comments
COMMENT ON COLUMN pipeline_drafts.last_activity_at IS 'Last time user interacted with draft (for abandonment detection)';
COMMENT ON COLUMN pipeline_drafts.state IS 'Draft state: creating, ready, validating, promoting, deployed, abandoned, failed';

-- Set last_activity_at to updated_at for existing drafts
UPDATE pipeline_drafts SET last_activity_at = updated_at WHERE last_activity_at IS NULL;
