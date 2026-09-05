-- Migration 021: Ensure audit_logs exists for tracking user actions/system changes
-- Safe across environments: uses uuid-ossp's uuid_generate_v4() and IF NOT EXISTS.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS audit_logs (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id uuid REFERENCES users(id),
    action character varying(100) NOT NULL,
    resource_type character varying(50) NOT NULL,
    resource_id uuid,
    details jsonb,
    ip_address character varying(50),
    created_at timestamp with time zone DEFAULT now()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource ON audit_logs(resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at DESC);

-- Composite index for user activity queries
CREATE INDEX IF NOT EXISTS idx_audit_logs_user_activity ON audit_logs(user_id, created_at DESC);

-- Comment
COMMENT ON TABLE audit_logs IS 'Audit log for tracking user actions and system changes';
COMMENT ON COLUMN audit_logs.action IS 'Action performed (e.g., upgrade_connector_version, update_connection, delete_pipeline)';
COMMENT ON COLUMN audit_logs.resource_type IS 'Type of resource affected (e.g., connection, pipeline, user)';
COMMENT ON COLUMN audit_logs.resource_id IS 'ID of the affected resource';
COMMENT ON COLUMN audit_logs.details IS 'Additional context/details for the action (JSONB payload)';

