-- Migration: Add idempotency_log table for exactly-once processing
-- Phase 2.2: Idempotency Keys
-- Date: 2025-12-21

-- Idempotency log table to track completed operations and prevent duplicate processing
CREATE TABLE IF NOT EXISTS idempotency_log (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    pipeline_id UUID NOT NULL,
    execution_id UUID NOT NULL,
    step_id VARCHAR(100) NOT NULL,
    chunk_id VARCHAR(100) DEFAULT '',
    operation_type VARCHAR(50) NOT NULL, -- e.g., 'discovery', 'executor_chunk', 'validation'
    status VARCHAR(20) NOT NULL DEFAULT 'completed', -- 'completed', 'failed', 'in_progress'
    result_data JSONB, -- Store operation result for idempotent replay
    error_message TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    expires_at TIMESTAMP NOT NULL DEFAULT NOW() + INTERVAL '7 days', -- Auto-cleanup after 7 days
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_idempotency_pipeline_execution ON idempotency_log(pipeline_id, execution_id);
CREATE INDEX IF NOT EXISTS idx_idempotency_status ON idempotency_log(status);
CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_log(expires_at);
CREATE INDEX IF NOT EXISTS idx_idempotency_operation_type ON idempotency_log(operation_type);

-- Composite index for common query pattern
CREATE INDEX IF NOT EXISTS idx_idempotency_lookup ON idempotency_log(pipeline_id, execution_id, step_id, chunk_id);

-- Foreign key constraint
ALTER TABLE idempotency_log
    ADD CONSTRAINT fk_idempotency_pipeline
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE;

-- Function to automatically clean up expired idempotency records
CREATE OR REPLACE FUNCTION cleanup_expired_idempotency_records()
RETURNS void AS $$
BEGIN
    DELETE FROM idempotency_log WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;

-- Comment for documentation
COMMENT ON TABLE idempotency_log IS 'Tracks completed operations to ensure exactly-once processing semantics (Phase 2.2)';
COMMENT ON COLUMN idempotency_log.idempotency_key IS 'Deterministic key: {pipeline_id}-{execution_id}-{step_id}-{chunk_id}';
COMMENT ON COLUMN idempotency_log.result_data IS 'Cached result for idempotent replay if operation is retried';
COMMENT ON COLUMN idempotency_log.expires_at IS 'Automatic cleanup timestamp (default 7 days)';
