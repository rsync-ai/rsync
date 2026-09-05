-- Migration 032: Reintroduce pipeline_drafts for right-panel persistence
-- Previous draft flow was removed in migration 030; this is a redesigned schema
-- for the new right-side panel UX with optimistic concurrency control.

CREATE TABLE IF NOT EXISTS pipeline_drafts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  
  -- Draft lifecycle
  state VARCHAR(50) NOT NULL DEFAULT 'intent', 
  -- Values: intent | connections | tables | config | review
  
  version INTEGER NOT NULL DEFAULT 1,
  -- Optimistic concurrency control: increment on every update
  
  status VARCHAR(20) NOT NULL DEFAULT 'active' 
    CHECK (status IN ('active', 'promoted', 'archived')),
  -- active: in progress (editable)
  -- promoted: pipeline created (terminal, read-only)
  -- archived: user discarded OR TTL expired (terminal)
  
  template_id VARCHAR(100),
  -- Stable slugs: postgres-s3 | pipedrive-google-sheets | mysql-s3 | NULL (custom)
  
  -- Draft content (JSONB for flexibility)
  chat_history JSONB NOT NULL DEFAULT '[]'::jsonb,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  
  -- Timestamps
  last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_pipeline_drafts_user_active_updated
  ON pipeline_drafts(user_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_drafts_expires_active
  ON pipeline_drafts(expires_at)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_pipeline_drafts_last_activity_active
  ON pipeline_drafts(last_activity_at)
  WHERE status = 'active';

-- Auto-update updated_at trigger
DROP TRIGGER IF EXISTS update_pipeline_drafts_updated_at ON pipeline_drafts;
CREATE TRIGGER update_pipeline_drafts_updated_at
  BEFORE UPDATE ON pipeline_drafts
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();

-- Comments for documentation
COMMENT ON TABLE pipeline_drafts IS 'Stores in-progress pipeline drafts for right-panel agentic chat flow';
COMMENT ON COLUMN pipeline_drafts.state IS 'Current wizard state: intent, connections, tables, config, review';
COMMENT ON COLUMN pipeline_drafts.version IS 'Optimistic concurrency control: increment on every update';
COMMENT ON COLUMN pipeline_drafts.status IS 'active: editable, promoted: pipeline created (terminal), archived: discarded (terminal)';
COMMENT ON COLUMN pipeline_drafts.template_id IS 'Template used (postgres-s3, pipedrive-google-sheets, mysql-s3) or NULL for custom';
COMMENT ON COLUMN pipeline_drafts.expires_at IS 'TTL for active drafts (configurable, default 24h)';
COMMENT ON COLUMN pipeline_drafts.last_activity_at IS 'Last user interaction (for cleanup/abandonment tracking)';
