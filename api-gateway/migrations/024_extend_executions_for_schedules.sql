-- Migration: Extend executions table for scheduled runs
-- Description: Add trigger source tracking to unify manual and scheduled execution history
-- Preserves existing queries (all columns are additive with safe defaults)

-- Add trigger source (defaults to 'manual' for existing rows)
ALTER TABLE executions ADD COLUMN IF NOT EXISTS trigger_source TEXT NOT NULL DEFAULT 'manual' 
CHECK (trigger_source IN ('manual', 'scheduled'));

-- Add schedule reference (NULL for manual triggers)
ALTER TABLE executions ADD COLUMN IF NOT EXISTS schedule_id UUID NULL 
REFERENCES pipeline_schedules(schedule_id) ON DELETE SET NULL;

-- Add scheduled time (when the schedule said it should run, NULL for manual triggers)
ALTER TABLE executions ADD COLUMN IF NOT EXISTS scheduled_time TIMESTAMPTZ NULL;

-- Indexes for schedule queries
CREATE INDEX IF NOT EXISTS idx_executions_trigger_source ON executions(trigger_source);
CREATE INDEX IF NOT EXISTS idx_executions_schedule_runs 
ON executions(schedule_id, start_time DESC) 
WHERE schedule_id IS NOT NULL;

-- Comments for documentation
COMMENT ON COLUMN executions.trigger_source IS 'How this execution was triggered: manual (user clicked run) or scheduled (Temporal schedule)';
COMMENT ON COLUMN executions.schedule_id IS 'Reference to the schedule that triggered this execution (NULL for manual runs)';
COMMENT ON COLUMN executions.scheduled_time IS 'When the schedule intended this execution to run (may differ from start_time due to lag)';

