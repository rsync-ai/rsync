-- Migration 020: Pipeline completion fields
--
-- Adds completion metadata to the pipelines table so the top-level pipelines list
-- reflects end-of-workflow outcomes (completed/failed) instead of staying "running".
--
-- Note: This codebase uses a forward-only migration runner (applies all *.sql once).

BEGIN;

ALTER TABLE pipelines
ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ NULL,
ADD COLUMN IF NOT EXISTS error_message TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_pipelines_status_completed
ON pipelines(status, completed_at);

COMMIT;


