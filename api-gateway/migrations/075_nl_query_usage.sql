-- 075_nl_query_usage.sql
-- Per-workspace NL→SQL query metering — the "queries / month" dimension the pricing
-- page sells (Free 1,000 / Starter 3,000 / Pro 10,000; direct SQL is Unlimited and is
-- NOT metered). Mirrors 072 (plan allowance) + 073 (counter + ledger + charge fn).
--
--   plans.included_queries              — monthly NL→SQL allowance. NULL / -1 = unlimited.
--   workspaces.monthly_query_count/_period — materialised current-month counter.
--   nl_query_usage_events               — append-only ledger (audit + recompute).
--   charge_workspace_queries()          — accrue-only (+1), rolls the month.
--
-- IMPORTANT: nl_query_usage_events is billing truth — it is DELIBERATELY EXCLUDED
-- from the pipeline-run retention sweep (internal/retention/pipeline_run_retention.go).
-- Do not add it there.
--
-- The migration runner wraps each file in a transaction — no BEGIN/COMMIT here.

ALTER TABLE plans
    ADD COLUMN IF NOT EXISTS included_queries INTEGER;   -- NULL / -1 = unlimited

UPDATE plans SET included_queries = 1000  WHERE name = 'trial'   AND included_queries IS NULL;
UPDATE plans SET included_queries = 1000  WHERE name = 'free'    AND included_queries IS NULL;
UPDATE plans SET included_queries = 3000  WHERE name = 'starter' AND included_queries IS NULL;
UPDATE plans SET included_queries = 10000 WHERE name = 'pro'     AND included_queries IS NULL;

ALTER TABLE workspaces
    ADD COLUMN IF NOT EXISTS monthly_query_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE workspaces
    ADD COLUMN IF NOT EXISTS monthly_query_period TEXT NOT NULL DEFAULT to_char(now(), 'YYYY-MM');

CREATE TABLE IF NOT EXISTS nl_query_usage_events (
    id            BIGSERIAL   PRIMARY KEY,
    workspace_id  UUID        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    user_id       UUID,
    connection_id UUID,
    usage_month   TEXT        NOT NULL DEFAULT to_char(now(), 'YYYY-MM'),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nlq_ws_month
    ON nl_query_usage_events (workspace_id, usage_month);

-- Accrue-only: rolls the month under a row lock if needed, then +1. Never refuses
-- (enforcement is a separate pre-generate gate, Phase 2). Mirrors charge_workspace_bytes.
CREATE OR REPLACE FUNCTION charge_workspace_queries(p_workspace_id UUID)
RETURNS void AS $$
DECLARE
    v_period TEXT := to_char(now(), 'YYYY-MM');
BEGIN
    UPDATE workspaces
       SET monthly_query_count  = 0,
           monthly_query_period = v_period
     WHERE id = p_workspace_id
       AND monthly_query_period <> v_period;

    UPDATE workspaces
       SET monthly_query_count = monthly_query_count + 1
     WHERE id = p_workspace_id;
END;
$$ LANGUAGE plpgsql;
