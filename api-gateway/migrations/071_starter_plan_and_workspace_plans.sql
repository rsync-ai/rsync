-- 071_starter_plan_and_workspace_plans.sql
--
-- P0 "truthful billing". Three data changes so the app's entitlements match what
-- rsync.ai/pricing sells:
--
--   1. Add the marketed `starter` tier (5 pipelines). Migration 060 seeded only
--      pro / free / trial, so a workspace could never actually be on Starter.
--   2. Align the trial window with the pricing page ("Free Trial ... 1 month").
--      060 seeded 14 days; the page is the source of truth → 30 days. The trial
--      pipeline cap (1) already matches, so it is left alone.
--   3. Move entitlements onto the WORKSPACE (the billable tenant). resolvePlanQuota
--      now keys on workspaces.plan (with a one-release COALESCE fallback to the
--      owner's users.plan); backfill workspaces.plan from each workspace owner so
--      no workspace's effective entitlement changes at deploy time.
--
-- workspaces already has plan / plan_expires_at / pipeline_limit_override
-- (migrations 047 + 069), so this migration only backfills data — no DDL on
-- workspaces.
--
-- This file is executed inside the migrator's own transaction; do NOT add a
-- literal "BEGIN;" (it would trip the self-managed-txn detection in migrate.go).

-- ---------------------------------------------------------------------------
-- 1. Starter tier: $99 / 5 pipelines, no expiry, no downgrade (a paid plan).
--    Upsert so re-running the migration keeps the row in sync with these caps.
-- ---------------------------------------------------------------------------
INSERT INTO plans (name, display_name, pipeline_limit, can_run, duration_days, downgrades_to)
VALUES ('starter', 'Starter', 5, true, NULL, NULL)
ON CONFLICT (name) DO UPDATE
    SET display_name   = EXCLUDED.display_name,
        pipeline_limit = EXCLUDED.pipeline_limit,
        can_run        = EXCLUDED.can_run,
        duration_days  = EXCLUDED.duration_days,
        downgrades_to  = EXCLUDED.downgrades_to,
        updated_at     = now();

-- ---------------------------------------------------------------------------
-- 2. Trial window → 1 month, to match the pricing page. Catalogue alignment
--    only: resolvePlanQuota reads duration_days from here.
--
--    NOTE (flagged, intentionally NOT changed here): new signups currently land
--    directly on 'free' (2 pipelines / 30 days) in auth.go, NOT on 'trial'
--    (1 pipeline). Which tier a new signup lands on is a product decision, so it
--    is deferred out of this P0 data migration. See BACKLOG (trial reconcile).
-- ---------------------------------------------------------------------------
UPDATE plans SET duration_days = 30, updated_at = now() WHERE name = 'trial';

-- ---------------------------------------------------------------------------
-- 3. Backfill each workspace's plan from its OWNER's user plan. Keeps every
--    workspace's effective entitlement identical to today at deploy time; the
--    resolver's COALESCE fallback covers any row this misses for one release.
--    workspaces.plan is a plain VARCHAR (047) with no FK to plans, so unknown
--    owner-plan values (e.g. a legacy 'enterprise') are copied verbatim and the
--    resolver treats an unknown name as unlimited (fail-open) — safe.
-- ---------------------------------------------------------------------------
UPDATE workspaces w
   SET plan                    = u.plan,
       plan_expires_at         = u.plan_expires_at,
       pipeline_limit_override = u.pipeline_limit_override
  FROM users u
 WHERE u.id = w.owner_id;
