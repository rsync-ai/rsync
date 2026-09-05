-- Pipeline notifications (G1 / F-Obs-1 — notifier consumer).
--
-- The healer + sentinel + executor agents publish notifications to
-- the Kafka topic `rsync.notifications` (and related `rsync.healer.*`
-- topics) when a pipeline needs human attention (run failure, schema
-- change escalation, dependency recovery exhausted, etc.). Pre-fix
-- those topics had NO consumer — notifications were emitted to the
-- void and customers never knew their syncs broke.
--
-- This table is the durable record consumed by:
--   1. The notifier service (api-gateway/internal/notifier) which
--      delivers each row to email / Slack / webhook per the
--      owning user's settings + global env (NOTIFIER_SLACK_WEBHOOK_URL,
--      SMTP_HOST).
--   2. The frontend pipeline-detail page (future) which renders
--      unread notifications as an inbox.

CREATE TABLE IF NOT EXISTS pipeline_notifications (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Event-shape fields from the Kafka payload (schema_change_notification,
    -- pipeline_failed, healer_retry_cap_exceeded, dependency_recovery_exhausted, ...)
    type            TEXT NOT NULL,
    severity        TEXT NOT NULL DEFAULT 'info', -- info | warning | critical
    title           TEXT NOT NULL,
    message         TEXT NOT NULL,
    action_url      TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Delivery tracking
    delivery_status TEXT NOT NULL DEFAULT 'pending', -- pending | delivered | failed | suppressed
    delivered_at    TIMESTAMPTZ,
    delivery_error  TEXT,
    read_at         TIMESTAMPTZ,
    -- Dedup: same (pipeline_id, type, action_url) within 1h is dropped
    -- by the consumer's pre-insert idempotency check.
    dedup_key       TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_notifications_user_unread
    ON pipeline_notifications(user_id, created_at DESC)
    WHERE read_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_pipeline_notifications_pipeline
    ON pipeline_notifications(pipeline_id, created_at DESC);

-- Dedup index — used by the consumer to skip duplicate emissions
-- within the dedup window. Not UNIQUE because we still want
-- repeat events after the window elapses.
CREATE INDEX IF NOT EXISTS idx_pipeline_notifications_dedup
    ON pipeline_notifications(dedup_key, created_at DESC);

COMMENT ON TABLE pipeline_notifications IS
'Durable record of healer/sentinel/executor events that need user attention. Consumed from Kafka by api-gateway/internal/notifier and surfaced via the UI + email/Slack.';
