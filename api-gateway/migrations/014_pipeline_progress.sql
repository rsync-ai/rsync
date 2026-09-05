-- Migration: Pipeline Progress Tracking
-- Description: Create pipeline_progress table for authoritative state API (source of truth for UI)
-- Separate from pipeline_states which tracks session/workflow state

CREATE TABLE IF NOT EXISTS pipeline_progress (
    pipeline_id UUID PRIMARY KEY,
    execution_id UUID,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    current_stage VARCHAR(50),
    progress_percent INT DEFAULT 0 CHECK (progress_percent >= 0 AND progress_percent <= 100),
    progress_current_step INT DEFAULT 0,
    progress_total_steps INT DEFAULT 7,
    blocking_reason_type VARCHAR(50),
    blocking_reason_description TEXT,
    blocking_reason_estimated_seconds INT,
    message TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pipeline_progress_updated ON pipeline_progress(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_progress_status ON pipeline_progress(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_progress_execution ON pipeline_progress(execution_id);

-- Add trigger for updated_at
CREATE TRIGGER update_pipeline_progress_updated_at
BEFORE UPDATE ON pipeline_progress
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Comments for documentation
COMMENT ON TABLE pipeline_progress IS 'Authoritative source of truth for pipeline execution progress (for UI reconciliation)';
COMMENT ON COLUMN pipeline_progress.status IS 'processing, waiting_for_user, completed, failed';
COMMENT ON COLUMN pipeline_progress.blocking_reason_type IS 'connector_generation, large_table_scan, external_api_latency, user_input_required';
COMMENT ON COLUMN pipeline_progress.progress_percent IS 'Actual progress percentage (0-100), calculated by backend';
COMMENT ON COLUMN pipeline_progress.metadata IS 'Stage-specific data: tables_found, rows_to_transfer, rows_transferred, action_required';
