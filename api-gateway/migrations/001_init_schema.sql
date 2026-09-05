-- Initial Schema Migration
-- Extracted from system state on Dec 6, 2025

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Functions
CREATE OR REPLACE FUNCTION update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- Users Table
CREATE TABLE IF NOT EXISTS users (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    email character varying(255) NOT NULL UNIQUE,
    password_hash character varying(255) NOT NULL,
    role character varying(50) DEFAULT 'viewer'::character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

-- Connections Table
CREATE TABLE IF NOT EXISTS connections (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name character varying(255) NOT NULL,
    type character varying(50) NOT NULL CHECK (type IN ('source', 'destination')),
    connector_type character varying(100) NOT NULL,
    config text NOT NULL, -- Encrypted JSON
    status character varying(50) DEFAULT 'active'::character varying NOT NULL CHECK (status IN ('active', 'inactive', 'error')),
    last_tested_at timestamp with time zone,
    last_test_status character varying(50) CHECK (last_test_status IN ('success', 'failed')),
    last_test_error text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    UNIQUE (user_id, name)
);

DROP TRIGGER IF EXISTS update_connections_updated_at ON connections;
CREATE TRIGGER update_connections_updated_at BEFORE UPDATE ON connections FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Pipelines Table
CREATE TABLE IF NOT EXISTS pipelines (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    name character varying(255) NOT NULL,
    description text,
    natural_language_request text NOT NULL,
    status character varying(50) DEFAULT 'draft'::character varying NOT NULL,
    schedule character varying(100),
    config jsonb,
    created_by uuid REFERENCES users(id),
    source_connection_id uuid REFERENCES connections(id) ON DELETE SET NULL,
    destination_connection_id uuid REFERENCES connections(id) ON DELETE SET NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

-- Executions Table
CREATE TABLE IF NOT EXISTS executions (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    pipeline_id uuid REFERENCES pipelines(id),
    status character varying(50) DEFAULT 'pending'::character varying NOT NULL,
    start_time timestamp with time zone DEFAULT now(),
    end_time timestamp with time zone,
    error_message text,
    metrics jsonb
);

-- Decisions (HITL) Table
CREATE TABLE IF NOT EXISTS decisions (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    trace_id uuid NOT NULL,
    agent_name character varying(50) NOT NULL,
    title character varying(255) NOT NULL,
    description text NOT NULL,
    status character varying(50) DEFAULT 'pending'::character varying NOT NULL,
    impact_analysis jsonb NOT NULL,
    proposed_actions jsonb NOT NULL,
    decision_reason text,
    decided_by uuid REFERENCES users(id),
    decided_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now()
);

-- Agent Thinking Logs (Observability)
CREATE TABLE IF NOT EXISTS agent_thinking_logs (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    trace_id uuid NOT NULL,
    agent_name character varying(50) NOT NULL,
    step character varying(100) NOT NULL,
    content text NOT NULL,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now()
);

-- Audit Logs
CREATE TABLE IF NOT EXISTS audit_logs (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id uuid REFERENCES users(id),
    action character varying(100) NOT NULL,
    resource_type character varying(50) NOT NULL,
    resource_id uuid,
    details jsonb,
    ip_address character varying(50),
    created_at timestamp with time zone DEFAULT now()
);

-- Connection Access Logs
CREATE TABLE IF NOT EXISTS connection_access_logs (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    connection_id uuid NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    accessed_by character varying(255) NOT NULL,
    access_type character varying(50) NOT NULL,
    success boolean NOT NULL,
    error_message text,
    created_at timestamp with time zone DEFAULT now()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_connections_user_id ON connections(user_id);
CREATE INDEX IF NOT EXISTS idx_connections_type ON connections(type);
CREATE INDEX IF NOT EXISTS idx_connections_connector_type ON connections(connector_type);
CREATE INDEX IF NOT EXISTS idx_connections_status ON connections(status);

CREATE INDEX IF NOT EXISTS idx_pipelines_source_connection ON pipelines(source_connection_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_connection ON pipelines(destination_connection_id);

CREATE INDEX IF NOT EXISTS idx_executions_pipeline_id ON executions(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_decisions_status ON decisions(status);

CREATE INDEX IF NOT EXISTS idx_thinking_trace_id ON agent_thinking_logs(trace_id);

CREATE INDEX IF NOT EXISTS idx_connection_access_logs_connection_id ON connection_access_logs(connection_id);
CREATE INDEX IF NOT EXISTS idx_connection_access_logs_created_at ON connection_access_logs(created_at);

