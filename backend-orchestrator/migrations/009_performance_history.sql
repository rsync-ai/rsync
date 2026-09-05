-- PHASE 3: Learning System - Performance History Table
-- This table stores execution metrics for the learning optimizer

CREATE TABLE IF NOT EXISTS pipeline_performance_history (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    source_type VARCHAR(100) NOT NULL,
    destination_type VARCHAR(100) NOT NULL,
    rows_processed INTEGER NOT NULL,
    batch_size INTEGER NOT NULL,
    duration_seconds FLOAT NOT NULL,
    throughput FLOAT NOT NULL, -- rows per second
    success BOOLEAN NOT NULL DEFAULT true,
    executed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    INDEX idx_performance_source_dest (source_type, destination_type),
    INDEX idx_performance_pipeline (pipeline_id),
    INDEX idx_performance_executed (executed_at DESC),
    INDEX idx_performance_success (success)
);

-- Add comments for documentation
COMMENT ON TABLE pipeline_performance_history IS 'Stores execution performance metrics for learning-based optimization';
COMMENT ON COLUMN pipeline_performance_history.throughput IS 'Rows processed per second';
COMMENT ON COLUMN pipeline_performance_history.duration_seconds IS 'Total execution time in seconds';
COMMENT ON COLUMN pipeline_performance_history.batch_size IS 'Batch size used for this execution';
