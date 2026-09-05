-- Migration 046: Add pii_scan_jobs table for async scan tracking
-- Tracks async PII scan requests triggered via Kafka

CREATE TABLE IF NOT EXISTS pii_scan_jobs (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    tables      TEXT[],                     -- subset of tables to scan (empty = all)
    include_ml  BOOLEAN NOT NULL DEFAULT false,
    status      VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    error       TEXT,
    result      JSONB,                      -- raw scan output from llm-service
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pii_scan_jobs_connection ON pii_scan_jobs(connection_id);
CREATE INDEX IF NOT EXISTS idx_pii_scan_jobs_status    ON pii_scan_jobs(status);
