-- Migration 041: Improve admin overview recent-failure query performance
-- Adds a composite index to accelerate:
--   WHERE status IN (...) AND start_time > NOW() - interval ...
--   ORDER BY start_time DESC

CREATE INDEX IF NOT EXISTS idx_executions_status_start_time
ON executions(status, start_time DESC);

