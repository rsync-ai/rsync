-- 081_heal_attempts_and_batch_component_type.sql
--
-- Two changes, both prerequisites for closing the self-healing loop.
--
-- ── Part A: heal_attempts — the outcome ledger ────────────────────────────────
--
-- Until now the healer was an OPEN loop. backend-orchestrator/internal/agents/heal/
-- worker.go ran Diagnose -> Heal -> writeDecisionEvent -> markHealed, and markHealed
-- was a bare `UPDATE executions SET heal_attempted_at = NOW()` executed
-- "regardless of outcome so we don't loop". Nothing ever asked whether the fix
-- worked. The only autonomous remediation (BackoffRetryExecutor) treated
-- `resp.StatusCode < 300` from the api-gateway run endpoint as success, so a 202
-- was recorded as a heal whether or not the resulting run completed or landed a
-- single row.
--
-- Two capabilities need durable per-attempt state, and neither can be reconstructed
-- from pipeline_run_events (which is an append-only display timeline with no
-- verdict column and no stable failure key):
--
--   1. VERIFY — after an action fires, look at what happened next and record a
--      verdict (healed / failed_again / inconclusive / superseded).
--   2. RECALL — when the same failure signature recurs, prefer the action that has
--      actually worked before instead of re-deriving it from a substring match.
--
-- failure_signature is a stable hash-free key built by the orchestrator
-- (heal/signature.go) over (category, action-independent status, source type,
-- destination type, normalized error text). It is TEXT, not an FK: signatures
-- outlive the pipelines that produced them, which is the entire point of recall.
--
-- ── Part B: 'batch_pipeline' component_type ───────────────────────────────────
--
-- Exactly the same defect migration 063 fixed for CDC, one connector-mode over.
-- Migration 011 constrained sentinel component_type to
-- ('agent','mcp_connector','kafka_consumer','infrastructure'); 063 added
-- 'cdc_pipeline'. There is still no value a BATCH pipeline finding could use, so
-- the batch sentinel (backend-orchestrator/internal/agents/sentinel/
-- batch_sentinel.go) could not persist a single row -- every insert would fail
-- with:
--   pq: new row for relation "sentinel_active_issues" violates check constraint
--   "sentinel_active_issues_component_type_check"
-- and the finding would be silently dropped, exactly as CDC lag issues were before
-- 063. Pure widening -- backward compatible, no data migration.

-- ── Part A ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS heal_attempts (
    id BIGSERIAL PRIMARY KEY,

    pipeline_id  UUID NOT NULL REFERENCES pipelines(id)  ON DELETE CASCADE,
    -- The FAILED execution that triggered this heal. Nullable because a heal can
    -- be driven by a sentinel finding that has no execution row (a stalled batch
    -- run detected before it terminates).
    execution_id UUID NULL REFERENCES executions(id) ON DELETE SET NULL,

    -- nth attempt against this failure_signature within the recall window. Lets
    -- the healer escalate on repetition instead of retrying the same losing move.
    attempt_no INT NOT NULL DEFAULT 1,

    -- Stable recall key. See heal/signature.go.
    failure_signature TEXT NOT NULL,

    -- diagnose.Category / diagnose.Action / confidence at decision time. Stored as
    -- TEXT rather than an enum: both are closed Go enums that gain members
    -- (ActionReSnapshot was added after the first four), and a CHECK here would
    -- turn every new Go constant into a migration.
    category   TEXT NOT NULL,
    action     TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- heal.Outcome as returned by Healer.Heal (auto_executed, hitl_requested,
    -- escalated, action_failed, no_action_defined).
    outcome TEXT NOT NULL,

    -- The run this heal produced, once one exists. Populated by the verify loop,
    -- not at decision time -- BackoffRetryExecutor POSTs to api-gateway and the new
    -- execution row is minted asynchronously by the workflow.
    successor_execution_id UUID NULL REFERENCES executions(id) ON DELETE SET NULL,

    -- NULL until the verify loop reaches a conclusion.
    --   healed        -- a later run for this pipeline reached a terminal success
    --   failed_again  -- a later run reached a terminal failure
    --   inconclusive  -- settle window expired with no successor run
    --   superseded    -- a newer attempt for the same signature took over
    verdict     TEXT NULL CHECK (verdict IN ('healed','failed_again','inconclusive','superseded')),
    verified_at TIMESTAMPTZ NULL,

    -- Free-form detail for the timeline (rationale, error text, HITL prompt).
    details JSONB NOT NULL DEFAULT '{}',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The verify loop's hot path: unverified attempts, oldest first.
CREATE INDEX IF NOT EXISTS idx_heal_attempts_unverified
    ON heal_attempts(created_at)
    WHERE verdict IS NULL;

-- Recall: "what has worked for this signature before?"
CREATE INDEX IF NOT EXISTS idx_heal_attempts_signature
    ON heal_attempts(failure_signature, created_at DESC);

-- Per-pipeline history for the UI timeline and the attempt_no counter.
CREATE INDEX IF NOT EXISTS idx_heal_attempts_pipeline
    ON heal_attempts(pipeline_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_heal_attempts_execution
    ON heal_attempts(execution_id)
    WHERE execution_id IS NOT NULL;

COMMENT ON TABLE  heal_attempts IS
    'Outcome ledger for the self-healing loop: one row per heal decision, with the verdict of whether it actually worked. Read by the verify loop and by action recall.';
COMMENT ON COLUMN heal_attempts.failure_signature IS
    'Stable key over (category, executor status, source type, destination type, normalized error). Built by heal/signature.go; deliberately not an FK so it outlives the pipeline.';
COMMENT ON COLUMN heal_attempts.verdict IS
    'NULL until verified. healed | failed_again | inconclusive | superseded.';

-- ── Part B ───────────────────────────────────────────────────────────────────

ALTER TABLE sentinel_active_issues DROP CONSTRAINT IF EXISTS sentinel_active_issues_component_type_check;
ALTER TABLE sentinel_active_issues ADD CONSTRAINT sentinel_active_issues_component_type_check
    CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure', 'cdc_pipeline', 'batch_pipeline'));

ALTER TABLE sentinel_healing_results DROP CONSTRAINT IF EXISTS sentinel_healing_results_component_type_check;
ALTER TABLE sentinel_healing_results ADD CONSTRAINT sentinel_healing_results_component_type_check
    CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure', 'cdc_pipeline', 'batch_pipeline'));

ALTER TABLE sentinel_metrics DROP CONSTRAINT IF EXISTS sentinel_metrics_component_type_check;
ALTER TABLE sentinel_metrics ADD CONSTRAINT sentinel_metrics_component_type_check
    CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure', 'cdc_pipeline', 'batch_pipeline'));

ALTER TABLE sentinel_component_health DROP CONSTRAINT IF EXISTS sentinel_component_health_component_type_check;
ALTER TABLE sentinel_component_health ADD CONSTRAINT sentinel_component_health_component_type_check
    CHECK (component_type IN ('agent', 'mcp_connector', 'kafka_consumer', 'infrastructure', 'cdc_pipeline', 'batch_pipeline'));
