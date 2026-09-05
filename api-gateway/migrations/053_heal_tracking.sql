-- Migration 053: Heal tracking on executions
--
-- Adds a nullable timestamp the HealWorker writes after it has processed
-- an execution. Prevents the worker from re-diagnosing the same execution
-- on every poll cycle.
BEGIN;

ALTER TABLE executions
  ADD COLUMN IF NOT EXISTS heal_attempted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_executions_heal_attempted
  ON executions (heal_attempted_at)
  WHERE heal_attempted_at IS NULL;

COMMIT;
