-- Migration 020: Workspace Connection References
--
-- Add support for workspace-level connection references (alias, environment, tags)
-- to enable natural language resolution of "production", "staging", etc.
--
-- This is backward-compatible: all new columns are nullable.
--
-- Author: System
-- Date: 2025-12-29

-- Enable pg_trgm extension for fuzzy text search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Add workspace reference columns to connections
ALTER TABLE connections
ADD COLUMN IF NOT EXISTS alias VARCHAR(255),
ADD COLUMN IF NOT EXISTS environment VARCHAR(50),
ADD COLUMN IF NOT EXISTS tags TEXT[];

COMMENT ON COLUMN connections.alias IS
'Alternative name for this connection (e.g., "prod-db", "staging-mysql")';

COMMENT ON COLUMN connections.environment IS
'Environment label for this connection (e.g., "production", "staging", "development")';

COMMENT ON COLUMN connections.tags IS
'Array of tags for flexible categorization (e.g., ["analytics", "customer-data"])';

-- Indexes for fast workspace resolution lookups

-- Exact alias lookup
CREATE INDEX IF NOT EXISTS idx_connections_alias
ON connections(user_id, alias)
WHERE alias IS NOT NULL;

-- Exact environment lookup
CREATE INDEX IF NOT EXISTS idx_connections_environment
ON connections(user_id, environment)
WHERE environment IS NOT NULL;

-- Tag array lookup (GIN index for array containment)
CREATE INDEX IF NOT EXISTS idx_connections_tags
ON connections USING GIN(tags)
WHERE tags IS NOT NULL;

-- Trigram indexes for fuzzy substring search on name and description
CREATE INDEX IF NOT EXISTS idx_connections_name_trgm
ON connections USING gin(name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_connections_description_trgm
ON connections USING gin(description gin_trgm_ops)
WHERE description IS NOT NULL;

