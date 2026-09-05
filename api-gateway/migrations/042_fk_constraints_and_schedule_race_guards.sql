-- Phase 2: Database integrity + idempotency guards
--
-- Adds missing FK constraints and cleans up legacy/orphan fields so constraints can validate.
-- This migration is designed to be safe to run on existing dev DBs.

BEGIN;

-- ---------------------------------------------------------------------------
-- pipeline_run_events.execution_id: clean up orphan execution references
-- ---------------------------------------------------------------------------
UPDATE pipeline_run_events e
SET execution_id = NULL
WHERE e.execution_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM executions x WHERE x.id = e.execution_id);

-- Add FK for execution_id (optional column, preserve events when executions are pruned)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_pipeline_run_events_execution_id'
  ) THEN
    ALTER TABLE pipeline_run_events
      ADD CONSTRAINT fk_pipeline_run_events_execution_id
      FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE SET NULL;
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- pipeline_run_metrics.execution_id: add FK (optional)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_pipeline_run_metrics_execution_id'
  ) THEN
    ALTER TABLE pipeline_run_metrics
      ADD CONSTRAINT fk_pipeline_run_metrics_execution_id
      FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE SET NULL;
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- pipeline_schedules.created_by: add FK to users
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_pipeline_schedules_created_by'
  ) THEN
    ALTER TABLE pipeline_schedules
      ADD CONSTRAINT fk_pipeline_schedules_created_by
      FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT;
  END IF;
END $$;

COMMIT;

