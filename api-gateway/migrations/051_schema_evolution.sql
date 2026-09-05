-- Schema evolution tables: schema_change_approvals and healing_history
-- Used by healer agent to store pending schema migrations requiring user approval.

CREATE TABLE IF NOT EXISTS schema_change_approvals (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    change_type     TEXT NOT NULL,          -- add_column, drop_column, modify_column, create_table, drop_table
    table_name      TEXT NOT NULL,
    ddl             TEXT NOT NULL,
    reasoning       TEXT,
    risks           TEXT,
    user_message    TEXT,
    suggested_ddl   TEXT,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending, approved, rejected, applied, failed
    reviewed_by     TEXT,
    reviewed_at     TIMESTAMPTZ,
    applied_at      TIMESTAMPTZ,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, ddl)
);

CREATE INDEX IF NOT EXISTS idx_sca_pipeline_status ON schema_change_approvals(pipeline_id, status);

CREATE TABLE IF NOT EXISTS healing_history (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id   UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    change_type   TEXT NOT NULL,
    table_name    TEXT,
    status        TEXT NOT NULL,   -- applied, skipped, pending_approval, failed
    reason        TEXT,
    ddl_applied   TEXT,
    error_message TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_healing_pipeline ON healing_history(pipeline_id);
