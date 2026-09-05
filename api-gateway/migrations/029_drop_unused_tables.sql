-- Migration: Drop unused tables (confirmed 0 SQL references)
-- Created: 2026-01-10
-- Proof: See docs/cleanup/SHADOW_INVENTORY.md

-- Drop unused tables (no code references found)
DROP TABLE IF EXISTS decisions CASCADE;
DROP TABLE IF EXISTS agent_thinking_logs CASCADE;
DROP TABLE IF EXISTS tools CASCADE;
DROP TABLE IF EXISTS pipeline_versions CASCADE;
DROP TABLE IF EXISTS test_users CASCADE;

-- Note: connection_access_logs is KEPT (actively used for security auditing)
-- Note: All other tables are KEPT (active in draft-first flow)
