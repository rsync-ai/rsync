-- Migration: 013_cleanup_unused_tables.sql
-- Description: Remove unused tables, columns, and clean up database schema
-- Date: 2024-12-20
-- Reason: System refactoring - removing CDC, redundant tables, and legacy code

-- ============================================================================
-- 1. Drop Unused Tables
-- ============================================================================

-- Executions table (replaced by Temporal workflow history)
DROP TABLE IF EXISTS executions CASCADE;
DROP TABLE IF EXISTS execution_logs CASCADE;

-- CDC tables (explicitly skipped per user requirements)
DROP TABLE IF EXISTS cdc_connectors CASCADE;
DROP TABLE IF EXISTS cdc_snapshots CASCADE;
DROP TABLE IF EXISTS cdc_offsets CASCADE;

-- Connector registry (redundant with connector_catalog)
DROP TABLE IF EXISTS connector_registry CASCADE;

-- User connectors (redundant - merged into connector_catalog)
DROP TABLE IF EXISTS user_connectors CASCADE;

-- PII tables not yet implemented
DROP TABLE IF EXISTS custom_hash_functions CASCADE;
DROP TABLE IF EXISTS pii_scan_results CASCADE;
DROP TABLE IF EXISTS pii_access_audit CASCADE;
DROP TABLE IF EXISTS pii_approval_audit CASCADE;

-- Sentinel tables (simplified monitoring)
DROP TABLE IF EXISTS sentinel_config CASCADE;
DROP TABLE IF EXISTS sentinel_alerts CASCADE;
DROP TABLE IF EXISTS sentinel_healths CASCADE;

-- Task management (legacy - replaced by Temporal)
DROP TABLE IF EXISTS task_queue CASCADE;
DROP TABLE IF EXISTS task_results CASCADE;

-- Old agent heartbeat table (not using registry pattern)
DROP TABLE IF EXISTS agent_heartbeats CASCADE;

-- ============================================================================
-- 2. Remove Legacy Columns from Active Tables
-- ============================================================================

-- Remove legacy source/destination columns from pipelines
-- (Now using source_connection_id and destination_connection_id)
ALTER TABLE pipelines DROP COLUMN IF EXISTS source;
ALTER TABLE pipelines DROP COLUMN IF EXISTS destination;

-- Remove old status fields if they exist
ALTER TABLE pipelines DROP COLUMN IF EXISTS old_status;
ALTER TABLE pipelines DROP COLUMN IF EXISTS legacy_config;

-- ============================================================================
-- 3. Clean Up Indexes on Dropped Tables
-- ============================================================================

-- Drop any remaining indexes related to deleted tables
DROP INDEX IF EXISTS idx_executions_pipeline_id;
DROP INDEX IF EXISTS idx_executions_status;
DROP INDEX IF EXISTS idx_cdc_connectors_name;
DROP INDEX IF EXISTS idx_connector_registry_type;
DROP INDEX IF EXISTS idx_user_connectors_user_id;
DROP INDEX IF EXISTS idx_sentinel_alerts_severity;
DROP INDEX IF EXISTS idx_task_queue_status;

-- ============================================================================
-- 4. Add Comments to Active Tables for Documentation
-- ============================================================================

COMMENT ON TABLE pipelines IS 'Main table for data pipelines managed by Temporal workflows';
COMMENT ON TABLE pipeline_states IS 'Stores resumable pipeline states for HITL workflows';
COMMENT ON TABLE connections IS 'Source and destination connections';
COMMENT ON TABLE connector_catalog IS 'Available MCP connectors';
COMMENT ON TABLE connector_instances IS 'Configured connector instances';
COMMENT ON TABLE connector_generation_jobs IS 'JIT connector generation job tracking';
COMMENT ON TABLE decisions IS 'Human-in-the-loop decision tracking';
COMMENT ON TABLE pii_policies IS 'PII detection and handling policies';
COMMENT ON TABLE pii_approval_requests IS 'Pending PII-related approvals';
COMMENT ON TABLE audit_logs IS 'System audit trail';
COMMENT ON TABLE oauth_tokens IS 'OAuth authentication tokens';
COMMENT ON TABLE sessions IS 'User sessions';
COMMENT ON TABLE users IS 'System users';

-- ============================================================================
-- 5. Verify Active Tables
-- ============================================================================

-- Active tables after cleanup:
-- ✅ users
-- ✅ pipelines
-- ✅ pipeline_states
-- ✅ connections
-- ✅ connector_catalog
-- ✅ connector_versions
-- ✅ connector_instances
-- ✅ connector_generation_jobs
-- ✅ decisions
-- ✅ pii_policies
-- ✅ pii_approval_requests
-- ✅ audit_logs
-- ✅ oauth_tokens
-- ✅ oauth_states
-- ✅ sessions

-- ============================================================================
-- Cleanup Complete
-- ============================================================================

