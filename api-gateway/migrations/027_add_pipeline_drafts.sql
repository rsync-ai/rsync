-- Migration 027: Add pipeline_drafts table for wizard-first interactive builder

-- Create pipeline_drafts table
CREATE TABLE IF NOT EXISTS pipeline_drafts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    draft JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'promoted', 'archived')),
    last_opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '30 days'),
    promoted_pipeline_id UUID REFERENCES pipelines(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_pipeline_drafts_user_status_updated 
    ON pipeline_drafts(user_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_drafts_expires 
    ON pipeline_drafts(expires_at) 
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_pipeline_drafts_promoted 
    ON pipeline_drafts(promoted_pipeline_id) 
    WHERE promoted_pipeline_id IS NOT NULL;

-- Add comments for documentation
COMMENT ON TABLE pipeline_drafts IS 'Stores in-progress pipeline drafts for the wizard-first builder';
COMMENT ON COLUMN pipeline_drafts.draft IS 'JSONB state containing wizard progress: source, tables, transforms, destination';
COMMENT ON COLUMN pipeline_drafts.status IS 'active: in progress, promoted: converted to real pipeline, archived: user deleted';
COMMENT ON COLUMN pipeline_drafts.last_opened_at IS 'Updated when user opens/resumes this draft';
COMMENT ON COLUMN pipeline_drafts.expires_at IS 'Drafts are auto-cleaned after 30 days of inactivity';
COMMENT ON COLUMN pipeline_drafts.promoted_pipeline_id IS 'The real pipeline ID created from this draft';

-- Function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_pipeline_drafts_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to call the update function
-- Make idempotent: migration table may be missing even if trigger exists.
DROP TRIGGER IF EXISTS trigger_pipeline_drafts_updated_at ON pipeline_drafts;
CREATE TRIGGER trigger_pipeline_drafts_updated_at
BEFORE UPDATE ON pipeline_drafts
FOR EACH ROW
EXECUTE FUNCTION update_pipeline_drafts_updated_at();

