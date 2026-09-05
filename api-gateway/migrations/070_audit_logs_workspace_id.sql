-- 070_audit_logs_workspace_id.sql
--
-- Add workspace scoping to the audit trail. With single-company multi-user
-- workspaces (047/069) live, an audited action happens INSIDE a workspace, but
-- audit_logs (021) never recorded which one — so per-workspace audit views and
-- attribution were impossible. logAudit() now stamps the caller's ACTIVE
-- workspace.
--
-- Nullable on purpose:
--   * auth-only routes (login / register / verify-email / logout) run before any
--     workspace context exists, so their audit rows legitimately have no
--     workspace_id;
--   * historical rows written before this migration predate the column.
-- A future migration may SET NOT NULL once every writer populates it and a
-- backfill has run (tracked in BACKLOG alongside the oauth_apps workspace_id
-- NOT-NULL work).
--
-- ON DELETE SET NULL (not CASCADE): the audit record must SURVIVE a workspace
-- deletion — losing the workspace linkage is acceptable, losing the audit row is
-- not.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + CREATE INDEX IF NOT EXISTS, so a re-run
-- is a no-op. The runner auto-wraps this file in one transaction; do NOT add a
-- literal "BEGIN;" (it would trip the self-managed-txn detection in migrate.go).

ALTER TABLE audit_logs
    ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_audit_logs_workspace
    ON audit_logs(workspace_id);

CREATE INDEX IF NOT EXISTS idx_audit_logs_workspace_created
    ON audit_logs(workspace_id, created_at DESC);
