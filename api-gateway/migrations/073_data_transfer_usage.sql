-- 073_data_transfer_usage.sql
-- Per-workspace data-transfer (GB) metering — the BILLING METER OF RECORD for the
-- GB dimension the pricing page sells. Mirrors 057's llm_usage_events shape but
-- keyed on the WORKSPACE (the billable tenant) and measured in BYTES.
--
--   data_transfer_usage_events  — append-only ledger (audit trail + recompute source).
--                                 One row per positive committed byte delta, stamped
--                                 with usage_month for monthly rollup.
--   workspaces.monthly_transfer_bytes / _period — materialised current-month counter
--                                 for O(1) reads on the usage surface (and, Phase 2,
--                                 the enforcement gate).
--   charge_workspace_bytes()    — accrue-only helper (rolls the month, increments).
--
-- IMPORTANT: this ledger is billing truth — it is DELIBERATELY EXCLUDED from the
-- pipeline-run retention sweep (internal/retention/pipeline_run_retention.go). Do
-- not add it there.
--
-- The migration runner wraps each file in a transaction — no BEGIN/COMMIT here.

CREATE TABLE IF NOT EXISTS data_transfer_usage_events (
    id             BIGSERIAL    PRIMARY KEY,
    workspace_id   UUID         NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    pipeline_id    UUID,
    execution_id   UUID,
    qualified_name TEXT,
    mode           TEXT         NOT NULL,                                   -- 'batch' | 'cdc'
    bytes          BIGINT       NOT NULL,                                   -- positive committed delta (destination-truth)
    usage_month    TEXT         NOT NULL DEFAULT to_char(now(), 'YYYY-MM'),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dtu_ws_month
    ON data_transfer_usage_events (workspace_id, usage_month);

-- Materialised running total for the CURRENT calendar month (bytes). Reset to 0 by
-- charge_workspace_bytes() when the month rolls over.
ALTER TABLE workspaces
    ADD COLUMN IF NOT EXISTS monthly_transfer_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE workspaces
    ADD COLUMN IF NOT EXISTS monthly_transfer_period TEXT NOT NULL DEFAULT to_char(now(), 'YYYY-MM');

-- Accrue-only: rolls the month under a row lock if needed, then increments the
-- counter. Unlike check_and_charge_user this NEVER refuses — GB is metered
-- continuously (CDC streams) and enforcement is a separate pre-run gate (Phase 2),
-- matching the LLM-spend soft-accrue pattern.
CREATE OR REPLACE FUNCTION charge_workspace_bytes(p_workspace_id UUID, p_bytes BIGINT)
RETURNS void AS $$
DECLARE
    v_period TEXT := to_char(now(), 'YYYY-MM');
BEGIN
    IF p_bytes <= 0 THEN
        RETURN;
    END IF;

    -- Roll the period if a new month started (the UPDATE takes the row lock).
    UPDATE workspaces
       SET monthly_transfer_bytes  = 0,
           monthly_transfer_period = v_period
     WHERE id = p_workspace_id
       AND monthly_transfer_period <> v_period;

    UPDATE workspaces
       SET monthly_transfer_bytes = monthly_transfer_bytes + p_bytes
     WHERE id = p_workspace_id;
END;
$$ LANGUAGE plpgsql;
