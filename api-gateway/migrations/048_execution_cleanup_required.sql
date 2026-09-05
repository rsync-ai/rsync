-- Add cleanup_required flag to executions table.
-- Set by the Temporal workflow compensation step when an executor fails after partially writing
-- data to the destination, so operators / background jobs can identify and clean up partial data.
ALTER TABLE executions
    ADD COLUMN IF NOT EXISTS cleanup_required boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_executions_cleanup_required
    ON executions (cleanup_required)
    WHERE cleanup_required = true;
