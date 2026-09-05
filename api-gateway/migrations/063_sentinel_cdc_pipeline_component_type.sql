-- 063_sentinel_cdc_pipeline_component_type.sql
-- Allow 'cdc_pipeline' as a sentinel component_type.
--
-- Why: The CDC sentinel (backend-orchestrator .../sentinel/cdc_sentinel.go emitSourceLagAlert)
-- persists CDC source/binlog lag alerts into sentinel_active_issues with
-- component_type = 'cdc_pipeline', and the frontend CDC lag panel
-- (frontend/src/components/pipeline/CDCLagAlertsPanel.tsx) filters issues by
-- ?component_type=cdc_pipeline. But migration 011 only permitted
-- ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure'), so every insert
-- failed with:
--   pq: new row for relation "sentinel_active_issues" violates check constraint
--   "sentinel_active_issues_component_type_check"
-- and CDC lag issues were silently dropped (panel always empty).
--
-- Fix: widen the component_type CHECK on all sentinel tables that carry it to also
-- accept 'cdc_pipeline'. Pure widening — backward compatible, no data migration.

ALTER TABLE sentinel_active_issues DROP CONSTRAINT IF EXISTS sentinel_active_issues_component_type_check;
ALTER TABLE sentinel_active_issues ADD CONSTRAINT sentinel_active_issues_component_type_check
    CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure', 'cdc_pipeline'));

ALTER TABLE sentinel_healing_results DROP CONSTRAINT IF EXISTS sentinel_healing_results_component_type_check;
ALTER TABLE sentinel_healing_results ADD CONSTRAINT sentinel_healing_results_component_type_check
    CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure', 'cdc_pipeline'));

ALTER TABLE sentinel_metrics DROP CONSTRAINT IF EXISTS sentinel_metrics_component_type_check;
ALTER TABLE sentinel_metrics ADD CONSTRAINT sentinel_metrics_component_type_check
    CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure', 'cdc_pipeline'));

ALTER TABLE sentinel_component_health DROP CONSTRAINT IF EXISTS sentinel_component_health_component_type_check;
ALTER TABLE sentinel_component_health ADD CONSTRAINT sentinel_component_health_component_type_check
    CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure', 'cdc_pipeline'));
