-- 060_user_plans.sql
-- Per-user subscription plans + pipeline-count entitlements.
--
-- Why: new signups get a time-boxed free trial; the product needs to cap how
-- many data pipelines a user can create/run by plan, and let an admin retune
-- those caps later without a code change. Following the standard SaaS
-- entitlements pattern, limits live as DATA in a `plans` table (editable by a
-- future admin panel), and users reference a plan plus an optional per-user
-- override.
--
-- Lifecycle (all numbers configurable in the plans table):
--   signup -> trial (14 days, 1 pipeline)
--          -> auto-downgrade to free (30 days, 2 pipelines)
--          -> blocked (must upgrade to the paid `pro` plan)
--
-- Resolution precedence (computed in resolvePlanQuota, Go side):
--   users.pipeline_limit_override  >  plans.pipeline_limit
-- Trial/free expiry is computed on read (no background sweep) and the downgrade
-- is persisted lazily on the next request.

-- ---------------------------------------------------------------------------
-- plans: the configurable catalogue. -1 pipeline_limit means unlimited.
-- duration_days NULL means the plan never expires (e.g. pro).
-- downgrades_to is the plan a user falls to when this one expires; NULL means
-- the user is blocked from creating/running until they upgrade.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plans (
    name           TEXT PRIMARY KEY,
    display_name   TEXT        NOT NULL,
    pipeline_limit INTEGER     NOT NULL,             -- -1 = unlimited
    can_run        BOOLEAN     NOT NULL DEFAULT true,
    duration_days  INTEGER,                          -- NULL = never expires
    downgrades_to  TEXT        REFERENCES plans(name),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO plans (name, display_name, pipeline_limit, can_run, duration_days, downgrades_to) VALUES
    ('pro',   'Pro',         -1, true, NULL, NULL),   -- inserted first: FK target for 'free'
    ('free',  'Free',         2, true, 30,   NULL),
    ('trial', 'Free Trial',   1, true, 14,   'free')
ON CONFLICT (name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- users: plan assignment + optional per-user override (configurable per user).
-- ---------------------------------------------------------------------------
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS plan TEXT NOT NULL DEFAULT 'trial' REFERENCES plans(name);
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS plan_expires_at TIMESTAMPTZ;
-- NULL ⇒ use the plan's pipeline_limit. Set per-user to grant a custom cap.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS pipeline_limit_override INTEGER;

-- ---------------------------------------------------------------------------
-- Grandfather every user that already exists at migration time onto the
-- unlimited `pro` plan with no expiry. Without this, the DEFAULT 'trial' above
-- would retroactively cap long-standing prod users at 1 pipeline and lock out
-- anyone created more than 14 days ago. New signups created AFTER this
-- migration get 'trial' via the application code, not this backfill.
-- ---------------------------------------------------------------------------
UPDATE users
   SET plan = 'pro',
       plan_expires_at = NULL,
       pipeline_limit_override = NULL
 WHERE plan = 'trial' AND plan_expires_at IS NULL;
