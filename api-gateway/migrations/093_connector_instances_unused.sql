-- Migration 093: label connector_instances as what it actually is — unused.
--
-- The table was created by 008_connector_registry.sql with four indexes and a
-- plausible shape (container_id, container_name, host, port, status,
-- health_status, consecutive_failures). 013_cleanup_unused_tables.sql then
-- listed it among the ACTIVE tables and commented it "Configured connector
-- instances". Both of those are claims; neither was ever true. Nothing in this
-- repository has ever inserted a row into it — not the orchestrator, not the
-- api-gateway, not the connector-deployer, not the compose generator.
--
-- That mattered because the sentinel's MCP connector health check read it:
--
--   SELECT container_name, port, status FROM connector_instances
--    WHERE status = 'running'
--
-- so the check iterated zero rows on every 30s tick, forever. Prod ran 25 MCP
-- connector containers and sentinel_component_health held zero mcp_connector
-- rows. Nothing errored, nothing logged; a monitor that checks nothing looks
-- exactly like a monitor that finds everything healthy.
--
-- Note the DEFAULT on port: 8080. MCP connectors listen on 8000. Even a
-- populated table would have sent every probe to the wrong port.
--
-- The check now enumerates the connector tree (the same source
-- scripts/mcp_generate_compose.py reads to decide which containers exist), so
-- this table has no readers left either. It is not dropped here: dropping is
-- irreversible, it holds no data worth protecting, and the failure being fixed
-- was caused by a WRONG claim about the table rather than by the table itself.
-- Relabelling it removes the trap for the next reader without a destructive
-- change; BACKLOG.md carries the drop.

BEGIN;

COMMENT ON TABLE connector_instances IS
    'UNUSED as of migration 093 (2026-08-18). No writer has ever existed in this '
    'repository, and its one reader -- the sentinel MCP connector health check -- '
    'was repointed at the on-disk connector tree because reading this table meant '
    'checking nothing. Do NOT read this table to answer "which connectors are '
    'running"; enumerate shared/mcp-connectors (latest.json -> '
    'versions/<current_version>/) the way scripts/mcp_generate_compose.py does, '
    'or query Docker for containers labelled rsync-ai.mcp=true. Superseded '
    'comment: "Configured connector instances" (migration 013).';

COMMIT;
