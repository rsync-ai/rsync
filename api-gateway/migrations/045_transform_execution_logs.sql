-- Per-transform execution stats for batch + CDC paths.
-- Aggregated per (pipeline_id, execution_id, table_name, transform_order).

CREATE TABLE IF NOT EXISTS transform_execution_logs (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    pipeline_id uuid NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    execution_id uuid NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    table_name text NOT NULL,
    transform_id text,
    transform_order integer NOT NULL,
    transform_type text NOT NULL,
    status text NOT NULL DEFAULT 'success',
    error_message text,
    input_rows bigint NOT NULL DEFAULT 0,
    output_rows bigint NOT NULL DEFAULT 0,
    duration_ms bigint NOT NULL DEFAULT 0,
    config_snapshot jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

-- One row per transform per table execution.
CREATE UNIQUE INDEX IF NOT EXISTS idx_transform_execution_logs_unique
    ON transform_execution_logs(pipeline_id, execution_id, table_name, transform_order);

CREATE INDEX IF NOT EXISTS idx_transform_execution_logs_lookup
    ON transform_execution_logs(pipeline_id, execution_id, table_name);

CREATE INDEX IF NOT EXISTS idx_transform_execution_logs_execution
    ON transform_execution_logs(execution_id);

