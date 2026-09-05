-- 057_llm_usage_quotas.sql
-- Per-user LLM cost tracking + spend quotas.
--
-- Why: Without enforcement a single bug or compromised account can burn
-- through Azure OpenAI / OpenAI credits in hours. This adds:
--   1. Append-only llm_usage_events log (audit trail + recompute source)
--   2. users.cost_quota_usd_cents (per-month cap, default $50 = 5000 cents)
--   3. Materialised users.monthly_cost_usd_cents (rolling current calendar month)
--   4. Helper SQL function for atomic spend-and-check.
--
-- All cost values stored in INTEGER cents (USD) to avoid float drift.

CREATE TABLE IF NOT EXISTS llm_usage_events (
    id           BIGSERIAL PRIMARY KEY,
    user_id      UUID         REFERENCES users(id) ON DELETE SET NULL,
    pipeline_id  UUID,
    prompt_name  TEXT         NOT NULL,          -- e.g. 'planner/dynamic_planning'
    provider     TEXT         NOT NULL,          -- 'openai' | 'azure-openai' | 'groq' | 'ollama'
    model        TEXT         NOT NULL,          -- e.g. 'gpt-4o-mini'
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens      INTEGER NOT NULL DEFAULT 0,
    cost_usd_cents    INTEGER NOT NULL DEFAULT 0,  -- pre-computed at write time
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_usage_events_user_month
    ON llm_usage_events (user_id, created_at);

CREATE INDEX IF NOT EXISTS idx_llm_usage_events_pipeline
    ON llm_usage_events (pipeline_id) WHERE pipeline_id IS NOT NULL;

-- Per-user monthly quota in cents. Default $50/month — easy to bump.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS cost_quota_usd_cents INTEGER NOT NULL DEFAULT 5000;

-- Materialised running total for the CURRENT calendar month. Resets to 0
-- automatically via the helper below when month rolls over.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS monthly_cost_usd_cents INTEGER NOT NULL DEFAULT 0;
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS monthly_cost_period TEXT NOT NULL DEFAULT to_char(now(),'YYYY-MM');

-- check_and_charge_user atomically: rolls the month if needed, then checks
-- that quota would not be exceeded, then increments. Returns TRUE on OK,
-- FALSE if the charge was refused.
CREATE OR REPLACE FUNCTION check_and_charge_user(p_user_id UUID, p_cents INTEGER)
RETURNS BOOLEAN AS $$
DECLARE
    v_period TEXT := to_char(now(), 'YYYY-MM');
    v_quota  INTEGER;
    v_cur    INTEGER;
    v_curp   TEXT;
BEGIN
    -- Roll the period if a new month started
    UPDATE users
       SET monthly_cost_usd_cents = 0,
           monthly_cost_period    = v_period
     WHERE id = p_user_id
       AND monthly_cost_period <> v_period;

    -- Read current state
    SELECT cost_quota_usd_cents, monthly_cost_usd_cents
      INTO v_quota, v_cur
      FROM users WHERE id = p_user_id;

    IF v_quota IS NULL THEN
        RETURN FALSE;  -- unknown user
    END IF;

    IF v_cur + p_cents > v_quota THEN
        RETURN FALSE;  -- over quota
    END IF;

    UPDATE users
       SET monthly_cost_usd_cents = monthly_cost_usd_cents + p_cents
     WHERE id = p_user_id;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
