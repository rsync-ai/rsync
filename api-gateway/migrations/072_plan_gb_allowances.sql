-- 072_plan_gb_allowances.sql
-- Per-tier included data-transfer allowance (GB) + overage price, for the GB meter
-- (Ship 2). Mirrors 057's money model but in GB/bytes. Marketed numbers from
-- rsync.ai/pricing (Free 10 / Starter 100 / Pro 500 GB, $3/GB overage).
--
--   included_gb          — monthly included data transfer in GB. NULL / -1 = unlimited.
--   overage_price_cents  — $/GB (in cents) charged beyond the allowance. 0 = hard cap;
--                          >0 is reserved for the Stripe overage path (Phase 3).
--
-- Note: the migration runner wraps each file in a transaction — no BEGIN/COMMIT here.

ALTER TABLE plans
    ADD COLUMN IF NOT EXISTS included_gb INTEGER;                             -- NULL / -1 = unlimited
ALTER TABLE plans
    ADD COLUMN IF NOT EXISTS overage_price_cents INTEGER NOT NULL DEFAULT 0;  -- $/GB in cents

-- Seed the marketed allowances (idempotent: only set when still unset).
UPDATE plans SET included_gb = 10  WHERE name = 'trial'   AND included_gb IS NULL;
UPDATE plans SET included_gb = 10  WHERE name = 'free'    AND included_gb IS NULL;
UPDATE plans SET included_gb = 100 WHERE name = 'starter' AND included_gb IS NULL;
UPDATE plans SET included_gb = 500 WHERE name = 'pro'     AND included_gb IS NULL;

UPDATE plans SET overage_price_cents = 300 WHERE overage_price_cents = 0;     -- $3/GB
