-- Ensure executions table exists (required for execution history + pipeline runs)
-- NOTE: Some environments were missing this table even though code writes to it.

CREATE TABLE IF NOT EXISTS executions (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    pipeline_id uuid REFERENCES pipelines(id) ON DELETE CASCADE,
    status character varying(50) DEFAULT 'pending'::character varying NOT NULL,
    start_time timestamp with time zone DEFAULT now(),
    end_time timestamp with time zone,
    error_message text,
    metrics jsonb
);

CREATE INDEX IF NOT EXISTS idx_executions_pipeline_id ON executions(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_executions_status ON executions(status);
CREATE INDEX IF NOT EXISTS idx_executions_start_time ON executions(start_time);


