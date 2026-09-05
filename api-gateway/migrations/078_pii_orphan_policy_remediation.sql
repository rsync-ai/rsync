-- 078_pii_orphan_policy_remediation.sql
--
-- SECURITY (read-side residual of the cross-tenant PII IDOR closed by 077 / #605):
-- GetPolicies / GetHashFunctions treated EVERY workspace_id-NULL row as a shared
-- global default via `WHERE workspace_id = $active OR workspace_id IS NULL`. But
-- 077's backfill only sets workspace_id when created_by resolves to the creator's
-- PERSONAL workspace, so a user-created policy whose creator has no personal
-- workspace (a legacy pre-069 account, or a genuine orphan) stays workspace_id NULL
-- WITH created_by NOT NULL — and those rows leaked cross-tenant READ to EVERY
-- workspace (PII *configuration* metadata only: pii_type -> action / priority /
-- condition / hash_function; never PII row values; mutations were already 404).
--
-- The handler fix (pii.go: `OR (workspace_id IS NULL AND created_by IS NULL)`)
-- closes the leak by treating ONLY seeded built-ins (created_by IS NULL, 009-seeded)
-- as global. This migration is the data-layer companion, so no orphan row is left in
-- a silently-invisible / ambiguously-global state. Two idempotent steps, each
-- guarded by to_regclass (013_cleanup_unused_tables.sql DROPPED custom_hash_functions
-- on current schemas -> that block is a no-op there):
--
--   1. ATTRIBUTE (recover what is safe): re-run 077's rule — attribute an orphan to
--      its creator's personal workspace IF one now exists. This is identical
--      semantics to 077 (no NEW attribution rule, so no new misattribution risk); it
--      only catches personal workspaces created AFTER 077 ran.
--
--   2. QUARANTINE (fail explicit, not silent): any row STILL orphaned after step 1
--      (workspace_id NULL AND created_by NOT NULL — the creator has no personal
--      workspace, so we cannot safely attribute it to a tenant) is disabled
--      (enabled = false). It stays workspace_id NULL / created_by NOT NULL so the
--      hardened handler keeps it invisible, and enabled = false makes it inert to any
--      current/future reader that keys off `workspace_id IS NULL`. This turns
--      "silently invisible" into an explicit, auditable disabled state:
--          SELECT * FROM pii_policies
--          WHERE workspace_id IS NULL AND created_by IS NOT NULL AND enabled = false;
--
-- Deliberately NOT done: (a) guess a workspace for a multi-membership creator — that
-- would re-home the policy to a WRONG tenant; (b) NULL-out created_by — that would
-- EXPOSE the row as a seeded global, i.e. re-open the very leak we are closing.
-- Genuine recovery of a quarantined policy (re-creating it in its owning workspace)
-- is a human decision, tracked in BACKLOG.md.
--
-- Idempotent: step 1's WHERE re-filters on workspace_id IS NULL; step 2's `enabled =
-- true` guard makes a re-run a no-op. The runner auto-wraps this file in one
-- transaction; do NOT add a literal "BEGIN;" (it trips migrate.go's
-- self-managed-txn detection).

DO $$
BEGIN
    IF to_regclass('public.pii_policies') IS NOT NULL THEN
        -- 1. Attribute recoverable orphans to the creator's personal workspace (077's rule).
        UPDATE pii_policies p
        SET workspace_id = (
            SELECT w.id FROM workspaces w
            WHERE w.owner_id = p.created_by AND w.is_personal
            ORDER BY w.created_at ASC LIMIT 1
        )
        WHERE p.workspace_id IS NULL
          AND p.created_by IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM workspaces w
              WHERE w.owner_id = p.created_by AND w.is_personal
          );

        -- 2. Quarantine the genuine orphans that remain (no personal workspace): make
        --    them explicit + inert. They stay invisible via the hardened handler.
        UPDATE pii_policies
        SET enabled = false
        WHERE workspace_id IS NULL
          AND created_by IS NOT NULL
          AND enabled = true;
    END IF;

    -- custom_hash_functions: same model (dropped by 013 on current schemas -> no-op).
    IF to_regclass('public.custom_hash_functions') IS NOT NULL THEN
        UPDATE custom_hash_functions f
        SET workspace_id = (
            SELECT w.id FROM workspaces w
            WHERE w.owner_id = f.created_by AND w.is_personal
            ORDER BY w.created_at ASC LIMIT 1
        )
        WHERE f.workspace_id IS NULL
          AND f.created_by IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM workspaces w
              WHERE w.owner_id = f.created_by AND w.is_personal
          );

        UPDATE custom_hash_functions
        SET enabled = false
        WHERE workspace_id IS NULL
          AND created_by IS NOT NULL
          AND enabled = true;
    END IF;
END $$;
