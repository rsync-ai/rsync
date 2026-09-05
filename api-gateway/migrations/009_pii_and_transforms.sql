-- Migration: PII Detection and Transformation Tables
-- Description: Add tables for PII scanning, custom hash functions, policies, and audit logging

-- PII scan results
CREATE TABLE IF NOT EXISTS pii_scan_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID REFERENCES pipelines(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    pii_type VARCHAR(50) NOT NULL,
    confidence DECIMAL(5,4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    detection_method VARCHAR(50) NOT NULL,
    suggested_masking VARCHAR(50),
    approved_action VARCHAR(50),
    approved_by UUID REFERENCES users(id),
    approved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(pipeline_id, table_name, column_name)
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_pii_scan_results_pipeline ON pii_scan_results(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pii_scan_results_pii_type ON pii_scan_results(pii_type);

-- Custom hash functions for organizations
CREATE TABLE IF NOT EXISTS custom_hash_functions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id UUID, -- Can reference organizations table if exists
    name VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL CHECK (type IN ('python', 'api', 'lookup_table', 'builtin')),
    description TEXT,
    code TEXT, -- For Python type
    endpoint VARCHAR(500), -- For API type
    config JSONB DEFAULT '{}', -- Configuration parameters
    reversible BOOLEAN DEFAULT FALSE,
    enabled BOOLEAN DEFAULT TRUE,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create unique index for custom_hash_functions
CREATE UNIQUE INDEX IF NOT EXISTS idx_custom_hash_functions_org_name 
ON custom_hash_functions ((COALESCE(org_id::text, '00000000-0000-0000-0000-000000000000')), name);

-- Lookup table for pseudonymization
CREATE TABLE IF NOT EXISTS pii_pseudonyms (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id UUID,
    pii_type VARCHAR(50) NOT NULL,
    original_hash VARCHAR(64) NOT NULL, -- SHA256 of original value
    pseudonym VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create unique index for pii_pseudonyms
CREATE UNIQUE INDEX IF NOT EXISTS idx_pii_pseudonyms_unique 
ON pii_pseudonyms ((COALESCE(org_id::text, '00000000-0000-0000-0000-000000000000')), pii_type, original_hash);

CREATE INDEX IF NOT EXISTS idx_pii_pseudonyms_lookup ON pii_pseudonyms(org_id, pii_type, original_hash);

-- Transform definitions for pipelines
CREATE TABLE IF NOT EXISTS transform_definitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID REFERENCES pipelines(id) ON DELETE CASCADE,
    transform_type VARCHAR(50) NOT NULL CHECK (transform_type IN ('producer', 'consumer')),
    transform_order INT NOT NULL DEFAULT 0,
    transform_config JSONB NOT NULL,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transform_definitions_pipeline ON transform_definitions(pipeline_id);

-- PII policies (global and per-organization)
CREATE TABLE IF NOT EXISTS pii_policies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id UUID, -- NULL for global policies
    policy_type VARCHAR(50) NOT NULL CHECK (policy_type IN ('always_mask', 'require_approval', 'audit_required')),
    pii_type VARCHAR(50) NOT NULL, -- '*' for all types
    action VARCHAR(50) NOT NULL,
    hash_function VARCHAR(100),
    condition VARCHAR(255), -- e.g., 'raw_transfer', 'destination_external'
    priority INT DEFAULT 0,
    enabled BOOLEAN DEFAULT TRUE,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pii_policies_org ON pii_policies(org_id);
CREATE INDEX IF NOT EXISTS idx_pii_policies_type ON pii_policies(pii_type);

-- PII approval requests
CREATE TABLE IF NOT EXISTS pii_approval_requests (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID REFERENCES pipelines(id) ON DELETE CASCADE,
    requested_by UUID REFERENCES users(id) NOT NULL,
    columns_requested JSONB NOT NULL,
    justification TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'denied', 'partial', 'expired')),
    decided_by UUID REFERENCES users(id),
    decided_at TIMESTAMP,
    conditions JSONB,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pii_approval_requests_pipeline ON pii_approval_requests(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pii_approval_requests_status ON pii_approval_requests(status);

-- PII approval audit log (detailed decision history)
CREATE TABLE IF NOT EXISTS pii_approval_audit (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    approval_request_id UUID REFERENCES pii_approval_requests(id) ON DELETE CASCADE,
    pipeline_id UUID REFERENCES pipelines(id) ON DELETE SET NULL,
    action VARCHAR(50) NOT NULL,
    actor_id UUID REFERENCES users(id),
    actor_role VARCHAR(50),
    previous_status VARCHAR(20),
    new_status VARCHAR(20),
    column_decisions JSONB,
    reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pii_approval_audit_request ON pii_approval_audit(approval_request_id);

-- PII access audit (every time PII is accessed/transferred)
CREATE TABLE IF NOT EXISTS pii_access_audit (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID REFERENCES pipelines(id) ON DELETE SET NULL,
    execution_id UUID REFERENCES executions(id) ON DELETE SET NULL,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    pii_type VARCHAR(50) NOT NULL,
    action_taken VARCHAR(50) NOT NULL,
    hash_function VARCHAR(100),
    row_count BIGINT,
    source_connection_id UUID,
    destination_connection_id UUID,
    user_id UUID REFERENCES users(id),
    client_ip VARCHAR(45),
    accessed_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pii_access_audit_pipeline ON pii_access_audit(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pii_access_audit_execution ON pii_access_audit(execution_id);
CREATE INDEX IF NOT EXISTS idx_pii_access_audit_time ON pii_access_audit(accessed_at);

-- Insert default built-in hash functions
INSERT INTO custom_hash_functions (id, name, type, description, reversible, enabled)
VALUES 
    (uuid_generate_v4(), 'sha256', 'builtin', 'SHA-256 cryptographic hash (recommended)', false, true),
    (uuid_generate_v4(), 'sha512', 'builtin', 'SHA-512 cryptographic hash', false, true),
    (uuid_generate_v4(), 'md5', 'builtin', 'MD5 hash (legacy, not recommended for security)', false, true),
    (uuid_generate_v4(), 'blake2', 'builtin', 'BLAKE2 hash (fast and secure)', false, true),
    (uuid_generate_v4(), 'hmac_sha256', 'builtin', 'HMAC-SHA256 keyed hash', false, true)
ON CONFLICT DO NOTHING;

-- Insert default global PII policies
INSERT INTO pii_policies (id, policy_type, pii_type, action, hash_function, priority, enabled)
VALUES 
    -- Always mask high-risk PII
    (uuid_generate_v4(), 'always_mask', 'ssn', 'hash', 'sha256', 100, true),
    (uuid_generate_v4(), 'always_mask', 'credit_card', 'hash', 'sha256', 100, true),
    (uuid_generate_v4(), 'always_mask', 'password', 'remove', NULL, 100, true),
    (uuid_generate_v4(), 'always_mask', 'bank_account', 'hash', 'sha256', 90, true),
    
    -- Require approval for raw transfers
    (uuid_generate_v4(), 'require_approval', 'email', 'raw', NULL, 50, true),
    (uuid_generate_v4(), 'require_approval', 'phone', 'raw', NULL, 50, true),
    (uuid_generate_v4(), 'require_approval', 'name', 'raw', NULL, 50, true),
    
    -- Audit all PII access
    (uuid_generate_v4(), 'audit_required', '*', 'log', NULL, 0, true)
ON CONFLICT DO NOTHING;

-- Add trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_pii_scan_results_updated_at') THEN
        CREATE TRIGGER update_pii_scan_results_updated_at
            BEFORE UPDATE ON pii_scan_results
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_custom_hash_functions_updated_at') THEN
        CREATE TRIGGER update_custom_hash_functions_updated_at
            BEFORE UPDATE ON custom_hash_functions
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_transform_definitions_updated_at') THEN
        CREATE TRIGGER update_transform_definitions_updated_at
            BEFORE UPDATE ON transform_definitions
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_pii_policies_updated_at') THEN
        CREATE TRIGGER update_pii_policies_updated_at
            BEFORE UPDATE ON pii_policies
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END
$$;

