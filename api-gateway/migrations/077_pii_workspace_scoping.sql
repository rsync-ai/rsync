-- 077_pii_workspace_scoping.sql
--
-- SECURITY (cross-tenant IDOR): the PII feature tables were never workspace-scoped.
-- migration 009 created pii_policies / custom_hash_functions with only a nullable
-- `org_id` (never written — the API left it NULL) and the pii_scan_* / approval
-- tables carried only a pipeline_id / connection_id FK. With no per-query tenancy
-- filter and no Postgres RLS, an authenticated member of workspace A could read,
-- modify, and delete workspace B's PII policies, scan results, jobs, and approvals
-- (proven live in a 2-tenant pentest against the #601 code). The handler fix in
-- pii.go must bind `workspace_id`, so every one of these tables needs the column.
--
-- Scoping model (mirrors connections/pipelines from 069 and audit_logs from 070):
--   * pii_scan_results / pii_approval_requests  -> workspace of their pipeline
--   * pii_scan_jobs                             -> workspace of their connection
--   * pii_policies / custom_hash_functions      -> workspace of the CREATING user
--       (created_by). Rows with created_by IS NULL are the SEEDED built-in defaults
--       (009: the always_mask SSN/credit_card/password policies + sha256/…/hmac
--       hash functions) — they stay workspace_id NULL = GLOBAL, shared read-only
--       defaults that every workspace sees but no tenant can mutate. Only
--       user-created rows are attributed to a workspace, so no tenant's private
--       policy/hash leaks to another after backfill.
--
-- Nullable on purpose: the seeded globals legitimately keep NULL, and any row we
-- cannot attribute (creator has no personal workspace, or an orphan scan result
-- whose pipeline was TRUNCATEd by 069) stays NULL — invisible to a `workspace_id =
-- $ws` read, which fails closed. A future migration may SET NOT NULL once every
-- writer populates it (tracked in BACKLOG alongside the oauth_apps NOT-NULL work).
--
-- ON DELETE CASCADE: a workspace-scoped policy/scan/approval belongs to that
-- workspace and should die with it (the parent pipeline/connection FKs already
-- CASCADE); the NULL globals have no workspace to cascade from.
--
-- IMPORTANT — every ALTER/UPDATE/CREATE INDEX is guarded by `to_regclass(...) IS
-- NOT NULL`, INCLUDING the index creation. `013_cleanup_unused_tables.sql` DROPPED
-- `custom_hash_functions` + `pii_scan_results` (never recreated), so on any current
-- schema those two tables are ABSENT. A bare `CREATE INDEX IF NOT EXISTS ... ON
-- custom_hash_functions` would ERROR ("relation does not exist") — IF NOT EXISTS
-- guards the INDEX, not the TABLE — and, because the runner wraps this whole file
-- in one transaction, that error would roll back the entire migration (columns and
-- all). Keeping the index inside each table's guard makes the migration robust
-- whether or not the table exists.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + CREATE INDEX IF NOT EXISTS, all guarded by
-- table existence, so a re-run is a no-op. The runner auto-wraps this file in one
-- transaction; do NOT add a literal "BEGIN;" (it trips migrate.go's
-- self-managed-txn detection).

DO $$
BEGIN
    -- pii_policies: workspace of the creating user; seeded globals (created_by NULL) stay NULL.
    IF to_regclass('public.pii_policies') IS NOT NULL THEN
        ALTER TABLE pii_policies
            ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE;
        UPDATE pii_policies p
        SET workspace_id = (
            SELECT w.id FROM workspaces w
            WHERE w.owner_id = p.created_by AND w.is_personal
            ORDER BY w.created_at ASC LIMIT 1
        )
        WHERE p.workspace_id IS NULL AND p.created_by IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_pii_policies_workspace ON pii_policies(workspace_id);
    END IF;

    -- custom_hash_functions: same model; seeded built-ins (created_by NULL, type='builtin') stay global.
    -- (Dropped by 013 on current schemas → this block is a no-op there.)
    IF to_regclass('public.custom_hash_functions') IS NOT NULL THEN
        ALTER TABLE custom_hash_functions
            ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE;
        UPDATE custom_hash_functions f
        SET workspace_id = (
            SELECT w.id FROM workspaces w
            WHERE w.owner_id = f.created_by AND w.is_personal
            ORDER BY w.created_at ASC LIMIT 1
        )
        WHERE f.workspace_id IS NULL AND f.created_by IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_custom_hash_functions_workspace ON custom_hash_functions(workspace_id);
    END IF;

    -- pii_scan_results: workspace of the scanned pipeline (pipeline_id nullable → orphan stays NULL).
    -- (Dropped by 013 on current schemas → no-op there.)
    IF to_regclass('public.pii_scan_results') IS NOT NULL THEN
        ALTER TABLE pii_scan_results
            ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE;
        UPDATE pii_scan_results r
        SET workspace_id = p.workspace_id
        FROM pipelines p
        WHERE r.pipeline_id = p.id AND r.workspace_id IS NULL;
        CREATE INDEX IF NOT EXISTS idx_pii_scan_results_workspace ON pii_scan_results(workspace_id);
    END IF;

    -- pii_scan_jobs: workspace of the target connection (connection_id NOT NULL).
    IF to_regclass('public.pii_scan_jobs') IS NOT NULL THEN
        ALTER TABLE pii_scan_jobs
            ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE;
        UPDATE pii_scan_jobs j
        SET workspace_id = c.workspace_id
        FROM connections c
        WHERE j.connection_id = c.id AND j.workspace_id IS NULL;
        CREATE INDEX IF NOT EXISTS idx_pii_scan_jobs_workspace ON pii_scan_jobs(workspace_id);
    END IF;

    -- pii_approval_requests: workspace of the pipeline the approval is for.
    IF to_regclass('public.pii_approval_requests') IS NOT NULL THEN
        ALTER TABLE pii_approval_requests
            ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE;
        UPDATE pii_approval_requests a
        SET workspace_id = p.workspace_id
        FROM pipelines p
        WHERE a.pipeline_id = p.id AND a.workspace_id IS NULL;
        CREATE INDEX IF NOT EXISTS idx_pii_approval_requests_workspace ON pii_approval_requests(workspace_id);
    END IF;
END $$;
