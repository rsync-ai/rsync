-- Migration: Pipeline Schedules (Temporal Schedules Integration)
-- Description: Create pipeline_schedules table for durable scheduled pipeline executions
-- Integrates with Temporal Schedules API for reliable recurring runs

CREATE TABLE IF NOT EXISTS pipeline_schedules (
    schedule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    
    -- Schedule definition
    schedule_type TEXT NOT NULL CHECK (schedule_type IN ('cron', 'interval')),
    schedule_spec JSONB NOT NULL,
    -- cron example: { "cron": "0 * * * *", "timezone": "UTC" }
    -- interval example: { "every_seconds": 3600, "timezone": "UTC" }
    
    -- Temporal integration
    temporal_schedule_id TEXT UNIQUE NOT NULL,
    
    -- State
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'deleted')),
    
    -- Metadata
    created_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    paused_at TIMESTAMPTZ,
    paused_reason TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_pipeline_schedules_pipeline ON pipeline_schedules(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_schedules_status ON pipeline_schedules(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_schedules_temporal ON pipeline_schedules(temporal_schedule_id);

-- Uniqueness constraint: exactly one schedule per pipeline (V1)
-- This prevents schedule conflicts and UX confusion
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_schedules_unique_pipeline 
ON pipeline_schedules(pipeline_id) 
WHERE status != 'deleted';

-- Add trigger for updated_at
DROP TRIGGER IF EXISTS update_pipeline_schedules_updated_at ON pipeline_schedules;
CREATE TRIGGER update_pipeline_schedules_updated_at
BEFORE UPDATE ON pipeline_schedules
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Comments for documentation
COMMENT ON TABLE pipeline_schedules IS 'Durable pipeline schedules managed via Temporal Schedules API';
COMMENT ON COLUMN pipeline_schedules.schedule_type IS 'Type of schedule: cron (cron expression) or interval (fixed duration)';
COMMENT ON COLUMN pipeline_schedules.schedule_spec IS 'JSON specification for the schedule (format depends on schedule_type)';
COMMENT ON COLUMN pipeline_schedules.temporal_schedule_id IS 'Temporal Schedule ID for pause/resume/delete operations';
COMMENT ON COLUMN pipeline_schedules.status IS 'Schedule state: active (running), paused (stopped), deleted (soft-deleted)';

