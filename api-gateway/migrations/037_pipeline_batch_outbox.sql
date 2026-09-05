-- Migration: Add pipeline_batch_outbox table for producer-side tracking
-- This table tracks all batches produced to Kafka to enable:
-- 1. Reconciliation (compare outbox vs acks to find missing batches)
-- 2. ACK-driven checkpoints (only advance when ack exists)
-- 3. Recovery from producer failures (re-produce from outbox)

CREATE TABLE IF NOT EXISTS pipeline_batch_outbox (
    id SERIAL PRIMARY KEY,
    
    -- Batch identity (unique constraint)
    pipeline_id VARCHAR(255) NOT NULL,
    execution_id VARCHAR(255) NOT NULL,
    table_name VARCHAR(512) NOT NULL,
    batch_offset BIGINT NOT NULL,
    
    -- Batch details
    row_count BIGINT NOT NULL DEFAULT 0,
    byte_size BIGINT NOT NULL DEFAULT 0,
    storage_type VARCHAR(50) NOT NULL DEFAULT 'inline',  -- inline, minio
    storage_reference VARCHAR(1024),  -- MinIO URL if claim-check, or null for inline
    
    -- Status tracking
    status VARCHAR(50) NOT NULL DEFAULT 'pending',  -- pending, produced, acked, failed
    produce_attempts INTEGER NOT NULL DEFAULT 0,
    last_produce_error TEXT,
    
    -- Kafka position (set after successful produce)
    kafka_topic VARCHAR(512),
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    produced_at TIMESTAMP WITH TIME ZONE,
    acked_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint for idempotency
    CONSTRAINT unique_batch_outbox UNIQUE (pipeline_id, execution_id, table_name, batch_offset)
);

-- Index for status-based queries (find pending/failed batches)
CREATE INDEX IF NOT EXISTS idx_batch_outbox_status 
ON pipeline_batch_outbox(pipeline_id, status);

-- Index for reconciliation (find unacked batches)
CREATE INDEX IF NOT EXISTS idx_batch_outbox_reconcile 
ON pipeline_batch_outbox(pipeline_id, execution_id, status) 
WHERE status IN ('pending', 'produced');

-- Index for table-level queries
CREATE INDEX IF NOT EXISTS idx_batch_outbox_table 
ON pipeline_batch_outbox(pipeline_id, table_name);

-- Partial index for recent entries
CREATE INDEX IF NOT EXISTS idx_batch_outbox_recent 
ON pipeline_batch_outbox(pipeline_id, created_at DESC);

-- Comment for documentation
COMMENT ON TABLE pipeline_batch_outbox IS 'Producer outbox for tracking batches sent to Kafka for reconciliation and recovery';
