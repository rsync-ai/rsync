-- Migration: Pipeline Progress Stage Contract
-- Description: Add stage contract fields needed for UX hardening (timeline, heartbeats, retries)

ALTER TABLE pipeline_progress
  ADD COLUMN IF NOT EXISTS schema_version INT NOT NULL DEFAULT 1;

ALTER TABLE pipeline_progress
  ADD COLUMN IF NOT EXISTS stage_group VARCHAR(50),
  ADD COLUMN IF NOT EXISTS stage_state VARCHAR(20),
  ADD COLUMN IF NOT EXISTS stage_started_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS stage_last_heartbeat_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS stage_duration_ms BIGINT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS stage_attempt INT DEFAULT 1,
  ADD COLUMN IF NOT EXISTS stage_max_attempts INT DEFAULT 1,
  ADD COLUMN IF NOT EXISTS stage_summary TEXT;

CREATE INDEX IF NOT EXISTS idx_pipeline_progress_stage_state ON pipeline_progress(stage_state);
CREATE INDEX IF NOT EXISTS idx_pipeline_progress_stage_group ON pipeline_progress(stage_group);
CREATE INDEX IF NOT EXISTS idx_pipeline_progress_stage_heartbeat ON pipeline_progress(stage_last_heartbeat_at DESC);

