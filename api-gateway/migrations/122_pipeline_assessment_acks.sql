-- 122_pipeline_assessment_acks.sql
-- Remembered acknowledgements for the pre-migration assessment gate (U-19).
--
-- RunPipeline stops a run on any assessment WARNING until the operator
-- acknowledges it. The acknowledgement used to be forgotten once the run
-- started, so every later Start / Reload stopped at the gate again for the
-- same warnings. One row per pipeline + warning identity: warning_key is a
-- sha256 of the finding's code, table, object and message
-- (api-gateway assessment_acks.go assessmentWarningKey), so a warning that
-- changes, or a new one, is asked again. Errors are never stored or waived.

CREATE TABLE IF NOT EXISTS pipeline_assessment_acks (
    pipeline_id      UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    warning_key      TEXT NOT NULL,
    -- For people reading the table; the gate matches on warning_key only.
    code             TEXT NOT NULL DEFAULT '',
    table_name       TEXT NOT NULL DEFAULT '',
    acknowledged_by  UUID REFERENCES users(id) ON DELETE SET NULL,
    acknowledged_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, warning_key)
);
