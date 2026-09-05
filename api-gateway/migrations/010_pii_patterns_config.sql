-- Migration: Configurable PII Detection Patterns
-- Description: Make PII detection patterns configurable via database instead of hardcoded

-- Table for configurable PII detection patterns
CREATE TABLE IF NOT EXISTS pii_detection_patterns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pii_type VARCHAR(50) NOT NULL UNIQUE,
    pattern TEXT NOT NULL, -- Regex pattern for value detection
    column_hints JSONB DEFAULT '[]', -- Array of column name hints
    masking_strategy VARCHAR(50) NOT NULL DEFAULT 'hash', -- hash, partial_mask, redact, remove
    high_risk BOOLEAN DEFAULT FALSE,
    description TEXT,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pii_detection_patterns_enabled ON pii_detection_patterns(enabled);
CREATE INDEX IF NOT EXISTS idx_pii_detection_patterns_type ON pii_detection_patterns(pii_type);

-- Insert default PII patterns
INSERT INTO pii_detection_patterns (pii_type, pattern, column_hints, masking_strategy, high_risk, description) VALUES
('email', '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', 
 '["email", "email_address", "user_email", "contact_email", "mail"]', 
 'hash', FALSE, 'Email addresses'),

('ssn', '^\d{3}-\d{2}-\d{4}$|^\d{9}$', 
 '["ssn", "social_security", "social_security_number", "tax_id"]', 
 'hash', TRUE, 'Social Security Numbers'),

('phone', '^(\+\d{1,3})?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$', 
 '["phone", "mobile", "telephone", "phone_number", "cell", "contact_number"]', 
 'partial_mask', FALSE, 'Phone numbers'),

('credit_card', '^\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}$', 
 '["card_number", "cc_number", "credit_card", "pan", "card"]', 
 'hash', TRUE, 'Credit card numbers'),

('ip_address', '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', 
 '["ip", "ip_address", "client_ip", "remote_ip", "user_ip"]', 
 'partial_mask', FALSE, 'IP addresses'),

('date_of_birth', '^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$', 
 '["dob", "date_of_birth", "birth_date", "birthdate"]', 
 'redact', FALSE, 'Date of birth'),

('name', '^[A-Z][a-z]+ [A-Z][a-z]+$', 
 '["first_name", "last_name", "full_name", "name", "customer_name", "user_name"]', 
 'hash', FALSE, 'Personal names'),

('address', '.+', 
 '["address", "street", "street_address", "home_address", "mailing_address"]', 
 'redact', FALSE, 'Physical addresses'),

('postal_code', '^\d{5}(-\d{4})?$|^[A-Z]\d[A-Z]\s?\d[A-Z]\d$', 
 '["zip", "zipcode", "postal_code", "zip_code"]', 
 'partial_mask', FALSE, 'Postal/ZIP codes'),

('password', '.+', 
 '["password", "passwd", "pwd", "secret", "hash", "credential"]', 
 'remove', TRUE, 'Passwords and secrets'),

('api_key', '^[A-Za-z0-9_\-]{20,}$', 
 '["api_key", "apikey", "token", "access_token", "secret_key"]', 
 'remove', TRUE, 'API keys and tokens'),

('bank_account', '^\d{8,17}$', 
 '["account_number", "bank_account", "account_no", "iban"]', 
 'hash', TRUE, 'Bank account numbers'),

('driver_license', '^[A-Z0-9]{5,20}$', 
 '["license", "driver_license", "drivers_license", "dl_number"]', 
 'hash', TRUE, 'Driver license numbers'),

('passport', '^[A-Z0-9]{6,9}$', 
 '["passport", "passport_number", "passport_no"]', 
 'hash', TRUE, 'Passport numbers'),

('medical_record', '^\d{6,10}$', 
 '["mrn", "medical_record", "patient_id", "medical_record_number"]', 
 'hash', TRUE, 'Medical record numbers');

-- Table for custom PII detection rules (organization-specific)
CREATE TABLE IF NOT EXISTS pii_detection_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id UUID, -- NULL for global rules
    name VARCHAR(100) NOT NULL,
    description TEXT,
    pii_type VARCHAR(50) NOT NULL,
    detection_method VARCHAR(50) NOT NULL CHECK (detection_method IN ('pattern', 'column_hint', 'ml', 'custom_function')),
    pattern TEXT, -- For pattern method
    column_hints JSONB, -- For column_hint method
    ml_model_name VARCHAR(100), -- For ml method
    custom_function_code TEXT, -- For custom_function method (Python/JS)
    confidence_threshold FLOAT DEFAULT 0.7 CHECK (confidence_threshold >= 0 AND confidence_threshold <= 1),
    masking_strategy VARCHAR(50) NOT NULL DEFAULT 'hash',
    priority INT DEFAULT 100, -- Higher priority rules are evaluated first
    enabled BOOLEAN DEFAULT TRUE,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create unique index with COALESCE (can't use COALESCE in table constraint)
CREATE UNIQUE INDEX IF NOT EXISTS idx_pii_detection_rules_org_name 
ON pii_detection_rules(COALESCE(org_id::text, '00000000-0000-0000-0000-000000000000'), name);

CREATE INDEX IF NOT EXISTS idx_pii_detection_rules_org ON pii_detection_rules(org_id);
CREATE INDEX IF NOT EXISTS idx_pii_detection_rules_enabled ON pii_detection_rules(enabled);
CREATE INDEX IF NOT EXISTS idx_pii_detection_rules_priority ON pii_detection_rules(priority DESC);

-- Table for transformation type registry (make transformation types configurable)
CREATE TABLE IF NOT EXISTS transformation_types (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type_name VARCHAR(50) NOT NULL UNIQUE,
    category VARCHAR(50) NOT NULL CHECK (category IN ('filter', 'transform', 'aggregate', 'join', 'pii', 'custom')),
    stage VARCHAR(50) NOT NULL CHECK (stage IN ('producer', 'consumer', 'either')),
    complexity VARCHAR(20) NOT NULL CHECK (complexity IN ('light', 'medium', 'heavy')),
    description TEXT,
    config_schema JSONB, -- JSON Schema for validation
    requires_setup BOOLEAN DEFAULT FALSE,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transformation_types_enabled ON transformation_types(enabled);
CREATE INDEX IF NOT EXISTS idx_transformation_types_category ON transformation_types(category);

-- Insert default transformation types
INSERT INTO transformation_types (type_name, category, stage, complexity, description, requires_setup) VALUES
('filter', 'filter', 'producer', 'light', 'Filter rows based on conditions', FALSE),
('column_select', 'filter', 'producer', 'light', 'Select specific columns', FALSE),
('column_rename', 'transform', 'producer', 'light', 'Rename columns', FALSE),
('column_drop', 'filter', 'producer', 'light', 'Drop specific columns', FALSE),
('pii_mask', 'pii', 'producer', 'light', 'Mask PII data', FALSE),
('pii_hash', 'pii', 'producer', 'light', 'Hash PII data', FALSE),
('pii_redact', 'pii', 'producer', 'light', 'Redact PII data', FALSE),
('pii_remove', 'pii', 'producer', 'light', 'Remove PII columns entirely', FALSE),
('aggregate', 'aggregate', 'consumer', 'heavy', 'Aggregate data (SUM, COUNT, AVG)', TRUE),
('group_by', 'aggregate', 'consumer', 'heavy', 'Group data by columns', TRUE),
('join', 'join', 'consumer', 'heavy', 'Join with another dataset', TRUE),
('window', 'aggregate', 'consumer', 'heavy', 'Window functions (ROW_NUMBER, RANK)', TRUE),
('pivot', 'transform', 'consumer', 'heavy', 'Pivot rows to columns', TRUE),
('unpivot', 'transform', 'consumer', 'heavy', 'Unpivot columns to rows', TRUE),
('deduplicate', 'filter', 'either', 'medium', 'Remove duplicate rows', FALSE),
('sort', 'transform', 'either', 'medium', 'Sort rows', FALSE),
('limit', 'filter', 'either', 'light', 'Limit number of rows', FALSE),
('cast', 'transform', 'either', 'light', 'Cast column types', FALSE),
('custom_python', 'custom', 'consumer', 'heavy', 'Custom Python transformation', TRUE),
('custom_sql', 'custom', 'consumer', 'heavy', 'Custom SQL transformation', TRUE);

-- Apply trigger for updated_at
CREATE TRIGGER update_pii_detection_patterns_updated_at
BEFORE UPDATE ON pii_detection_patterns
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_pii_detection_rules_updated_at
BEFORE UPDATE ON pii_detection_rules
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_transformation_types_updated_at
BEFORE UPDATE ON transformation_types
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE pii_detection_patterns IS 'Configurable PII detection patterns (global, database-driven)';
COMMENT ON TABLE pii_detection_rules IS 'Custom PII detection rules (organization-specific or global)';
COMMENT ON TABLE transformation_types IS 'Registry of available transformation types';
COMMENT ON COLUMN pii_detection_patterns.pattern IS 'Regular expression for value-based detection';
COMMENT ON COLUMN pii_detection_patterns.column_hints IS 'Array of column name patterns that indicate this PII type';
COMMENT ON COLUMN pii_detection_rules.detection_method IS 'Method used for detection: pattern, column_hint, ml, or custom_function';
COMMENT ON COLUMN transformation_types.stage IS 'Where transformation should run: producer (lightweight), consumer (heavy), or either';

