-- Migration: Add pipeline_batch_acks table for durable ACK ledger
-- This table stores acknowledgments from the sink worker to enable:
-- 1. Idempotent writes (skip already-acked batches on retry)
-- 2. Reconciliation (compare outbox vs acks to find missing batches)
-- 3. Destination-truth accounting (TABLE_STATS based on actual writes)

CREATE TABLE IF NOT EXISTS pipeline_batch_acks (
    id SERIAL PRIMARY KEY,
    
    -- Idempotency key (unique constraint)
    pipeline_id VARCHAR(255) NOT NULL,
    execution_id VARCHAR(255) NOT NULL,
    table_name VARCHAR(512) NOT NULL,
    batch_offset BIGINT NOT NULL,
    
    -- ACK details
    rows_written BIGINT NOT NULL DEFAULT 0,
    rows_read BIGINT NOT NULL DEFAULT 0,
    dest_key VARCHAR(1024),  -- S3 key or DB table for audit
    storage_type VARCHAR(50) NOT NULL DEFAULT 'inline',  -- inline, minio, cdc
    
    -- Kafka position (for debugging/reconciliation)
    kafka_topic VARCHAR(512),
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    
    -- CDC-specific fields
    cdc_op VARCHAR(10),  -- c, u, d, r (create, update, delete, read)
    cdc_tx_id VARCHAR(255),
    cdc_lsn BIGINT,
    cdc_source_ts BIGINT,
    
    -- Timestamps
    acked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint for idempotency
    CONSTRAINT unique_batch_ack UNIQUE (pipeline_id, execution_id, table_name, batch_offset)
);

-- Index for fast lookups by pipeline/execution
CREATE INDEX IF NOT EXISTS idx_batch_acks_pipeline_exec 
ON pipeline_batch_acks(pipeline_id, execution_id);

-- Index for reconciliation queries (finding acks by table)
CREATE INDEX IF NOT EXISTS idx_batch_acks_table 
ON pipeline_batch_acks(pipeline_id, table_name);

-- Index for CDC reconciliation (by LSN/TxID)
CREATE INDEX IF NOT EXISTS idx_batch_acks_cdc 
ON pipeline_batch_acks(pipeline_id, cdc_tx_id, cdc_lsn) 
WHERE cdc_op IS NOT NULL;

-- Partial index for recent acks (most queries are for recent data)
CREATE INDEX IF NOT EXISTS idx_batch_acks_recent 
ON pipeline_batch_acks(pipeline_id, acked_at DESC);

-- Comment for documentation
COMMENT ON TABLE pipeline_batch_acks IS 'Durable ACK ledger for sink worker to track successful writes to destination';
