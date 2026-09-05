-- Migration 031: Optimize pipelines list query (no N+1)
-- Add indexes to support the enhanced ListPipelines endpoint

-- Pipelines: support filtering by user + created_at with DESC ordering
CREATE INDEX IF NOT EXISTS idx_pipelines_created_by_created_at 
  ON pipelines(created_by, created_at DESC);

-- Executions: support DISTINCT ON by pipeline_id + start_time DESC for latest execution
CREATE INDEX IF NOT EXISTS idx_executions_pipeline_start_time 
  ON executions(pipeline_id, start_time DESC);

-- Pipeline schedules: support DISTINCT ON by pipeline_id + created_at DESC
CREATE INDEX IF NOT EXISTS idx_pipeline_schedules_pipeline_created_at 
  ON pipeline_schedules(pipeline_id, created_at DESC) 
  WHERE status != 'deleted';

-- NOTE: Postgres requires predicate functions in partial indexes to be IMMUTABLE.
-- `NOW()` is STABLE, so this partial index fails on some Postgres versions/configs.
-- The existing idx_executions_pipeline_start_time covers the access pattern well enough.
DROP INDEX IF EXISTS idx_executions_pipeline_recent;
