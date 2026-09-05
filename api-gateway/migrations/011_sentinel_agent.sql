-- Migration: Sentinel Agent Tables
-- Description: Add tables for Sentinel agent monitoring, audit logging, and metrics

-- Sentinel Audit Logs: Track all Sentinel actions
CREATE TABLE IF NOT EXISTS sentinel_audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    timestamp TIMESTAMP NOT NULL,
    level VARCHAR(20) NOT NULL CHECK (level IN ('info', 'warn', 'error')),
    component VARCHAR(100) NOT NULL DEFAULT 'sentinel',
    action VARCHAR(100) NOT NULL,
    target VARCHAR(255) NOT NULL, -- Component ID being acted upon
    issue_type VARCHAR(100), -- Type of issue detected
    issue_id VARCHAR(255), -- Unique issue identifier
    severity VARCHAR(20) CHECK (severity IN ('info', 'warning', 'critical')),
    details JSONB DEFAULT '{}',
    success BOOLEAN NOT NULL DEFAULT TRUE,
    error_message TEXT,
    trace_id VARCHAR(64), -- OpenTelemetry trace ID
    span_id VARCHAR(32), -- OpenTelemetry span ID
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for audit logs
CREATE INDEX IF NOT EXISTS idx_sentinel_audit_logs_timestamp ON sentinel_audit_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sentinel_audit_logs_target ON sentinel_audit_logs(target);
CREATE INDEX IF NOT EXISTS idx_sentinel_audit_logs_action ON sentinel_audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_sentinel_audit_logs_issue_id ON sentinel_audit_logs(issue_id);
CREATE INDEX IF NOT EXISTS idx_sentinel_audit_logs_trace_id ON sentinel_audit_logs(trace_id);

-- Sentinel Healing Results: Track healing action outcomes
CREATE TABLE IF NOT EXISTS sentinel_healing_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    issue_id VARCHAR(255) NOT NULL,
    action VARCHAR(100) NOT NULL,
    component_id VARCHAR(255) NOT NULL,
    component_type VARCHAR(50) NOT NULL CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure')),
    success BOOLEAN NOT NULL,
    error_message TEXT,
    duration_ms BIGINT NOT NULL,
    details JSONB DEFAULT '{}',
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for healing results
CREATE INDEX IF NOT EXISTS idx_sentinel_healing_results_issue_id ON sentinel_healing_results(issue_id);
CREATE INDEX IF NOT EXISTS idx_sentinel_healing_results_component_id ON sentinel_healing_results(component_id);
CREATE INDEX IF NOT EXISTS idx_sentinel_healing_results_timestamp ON sentinel_healing_results(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sentinel_healing_results_success ON sentinel_healing_results(success);

-- Sentinel Metrics: Store time-series health metrics
CREATE TABLE IF NOT EXISTS sentinel_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    component_id VARCHAR(255) NOT NULL,
    component_type VARCHAR(50) NOT NULL CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure')),
    metric_name VARCHAR(100) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for metrics (optimized for time-series queries)
CREATE INDEX IF NOT EXISTS idx_sentinel_metrics_component_metric_time ON sentinel_metrics(component_id, metric_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sentinel_metrics_timestamp ON sentinel_metrics(timestamp DESC);

-- Sentinel Configuration: Configurable thresholds and rules
CREATE TABLE IF NOT EXISTS sentinel_config (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    config_key VARCHAR(100) NOT NULL UNIQUE,
    config_value JSONB NOT NULL,
    description TEXT,
    updated_by UUID REFERENCES users(id) ON DELETE SET NULL,
    updated_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert default configurations
INSERT INTO sentinel_config (config_key, config_value, description) VALUES
('heartbeat_timeout', '"30s"', 'Timeout before considering a component dead (no heartbeat)'),
('heartbeat_check_interval', '"10s"', 'How often to check for missing heartbeats'),
('consumer_lag_warning_threshold', '5000', 'Consumer lag warning threshold (number of messages)'),
('consumer_lag_critical_threshold', '10000', 'Consumer lag critical threshold (number of messages)'),
('dlq_warning_threshold', '50', 'DLQ message count warning threshold'),
('dlq_critical_threshold', '100', 'DLQ message count critical threshold'),
('max_restart_attempts', '3', 'Maximum restart attempts before circuit breaking'),
('restart_backoff_base', '"10s"', 'Base backoff duration for restart attempts'),
('restart_backoff_max', '"5m"', 'Maximum backoff duration for restart attempts'),
('circuit_breaker_threshold', '5', 'Number of failures before tripping circuit breaker'),
('circuit_breaker_cooldown', '"10m"', 'How long circuit breaker stays tripped'),
('issue_cooldown_period', '"5m"', 'Minimum time between reporting the same issue'),
('anomaly_stddev_threshold', '3.0', 'Standard deviations for anomaly detection'),
('anomaly_window_size', '100', 'Number of data points to keep for anomaly detection'),
('enable_signoz_export', 'true', 'Enable metrics export to SigNoz'),
('signoz_endpoint', '"http://otel-collector:4318"', 'SigNoz OTLP endpoint'),
('metric_export_interval', '"30s"', 'Interval for exporting metrics to SigNoz')
ON CONFLICT (config_key) DO NOTHING;

-- Component Health Status: Current health state of all components
CREATE TABLE IF NOT EXISTS sentinel_component_health (
    component_id VARCHAR(255) PRIMARY KEY,
    component_type VARCHAR(50) NOT NULL CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure')),
    status VARCHAR(20) NOT NULL CHECK (status IN ('healthy', 'degraded', 'unhealthy', 'dead', 'unknown')),
    last_heartbeat TIMESTAMP NOT NULL,
    messages_processed BIGINT DEFAULT 0,
    error_count BIGINT DEFAULT 0,
    consumer_lag BIGINT DEFAULT 0,
    last_error TEXT,
    metadata JSONB DEFAULT '{}',
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for component health
CREATE INDEX IF NOT EXISTS idx_sentinel_component_health_status ON sentinel_component_health(status);
CREATE INDEX IF NOT EXISTS idx_sentinel_component_health_type ON sentinel_component_health(component_type);
CREATE INDEX IF NOT EXISTS idx_sentinel_component_health_updated ON sentinel_component_health(updated_at DESC);

-- Active Issues: Currently active issues in the system
CREATE TABLE IF NOT EXISTS sentinel_active_issues (
    id VARCHAR(255) PRIMARY KEY, -- Deterministic ID: component_id:issue_type
    type VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    component_id VARCHAR(255) NOT NULL,
    component_type VARCHAR(50) NOT NULL CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure')),
    description TEXT NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP,
    occurrence_count INTEGER DEFAULT 1,
    last_occurrence TIMESTAMP NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for active issues
CREATE INDEX IF NOT EXISTS idx_sentinel_active_issues_component ON sentinel_active_issues(component_id);
CREATE INDEX IF NOT EXISTS idx_sentinel_active_issues_type ON sentinel_active_issues(type);
CREATE INDEX IF NOT EXISTS idx_sentinel_active_issues_severity ON sentinel_active_issues(severity);
CREATE INDEX IF NOT EXISTS idx_sentinel_active_issues_resolved ON sentinel_active_issues(resolved_at) WHERE resolved_at IS NULL;

-- Sentinel Statistics: Aggregated statistics for monitoring
CREATE TABLE IF NOT EXISTS sentinel_statistics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    period_start TIMESTAMP NOT NULL,
    period_end TIMESTAMP NOT NULL,
    issues_detected INTEGER DEFAULT 0,
    issues_resolved INTEGER DEFAULT 0,
    healing_actions INTEGER DEFAULT 0,
    healing_successes INTEGER DEFAULT 0,
    healing_failures INTEGER DEFAULT 0,
    components_monitored INTEGER DEFAULT 0,
    active_issues INTEGER DEFAULT 0,
    circuit_breakers_tripped INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(period_start, period_end)
);

-- Index for statistics
CREATE INDEX IF NOT EXISTS idx_sentinel_statistics_period ON sentinel_statistics(period_start DESC);

-- Add trigger to update updated_at columns
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'sentinel_config') THEN
        DROP TRIGGER IF EXISTS update_sentinel_config_updated_at ON sentinel_config;
        CREATE TRIGGER update_sentinel_config_updated_at
        BEFORE UPDATE ON sentinel_config
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'sentinel_component_health') THEN
        DROP TRIGGER IF EXISTS update_sentinel_component_health_updated_at ON sentinel_component_health;
        CREATE TRIGGER update_sentinel_component_health_updated_at
        BEFORE UPDATE ON sentinel_component_health
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- Note: GRANT permissions skipped (rsync_user role not configured)
-- In production, apply appropriate grants based on your security model

-- Add comments for documentation
COMMENT ON TABLE sentinel_audit_logs IS 'Audit log of all Sentinel agent actions for observability';
COMMENT ON TABLE sentinel_healing_results IS 'Results of healing actions executed by Sentinel';
COMMENT ON TABLE sentinel_metrics IS 'Time-series metrics collected by Sentinel for predictive monitoring';
COMMENT ON TABLE sentinel_config IS 'Configurable thresholds and rules for Sentinel agent';
COMMENT ON TABLE sentinel_component_health IS 'Current health status of all monitored components';
COMMENT ON TABLE sentinel_active_issues IS 'Currently active issues detected by Sentinel';
COMMENT ON TABLE sentinel_statistics IS 'Aggregated statistics for Sentinel monitoring dashboard';

