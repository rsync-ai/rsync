-- Admin-configured notification channels + per-user email category preferences.
--
-- Until now the notifier (api-gateway/internal/notifier) read its Slack webhook
-- and SMTP relay from environment variables only, so turning alerts on meant
-- editing the compose env and restarting api-gateway, and every pipeline owner
-- received every alert with no way to opt out of any of them. The personal
-- "Pipeline Executions" / "System Updates" switches on /settings were React
-- state with nothing behind them.
--
-- notification_channel_settings is ONE row (id = 1) holding the instance-wide
-- channels an admin sets up in the UI:
--   * Slack: one incoming-webhook URL for the whole instance. The URL itself is
--     the credential (anyone holding it can post to the channel), so it is
--     stored encrypted with shared/go/crypto (enc.v1:<key_id>:...) and never
--     returned by the API — only whether one is configured.
--   * Email: an SMTP relay. Alerts go to the pipeline owner's address. The
--     password is encrypted the same way and likewise never returned.
-- Both encrypted columns are covered by POST /admin/encryption/rotate.
--
-- Precedence: when this row EXISTS it is the whole truth and the SMTP_* /
-- NOTIFIER_SLACK_WEBHOOK_URL env vars are ignored, including when the row has
-- a channel disabled. When it does not exist the notifier falls back to the env
-- vars exactly as before, so an existing env-configured deploy keeps alerting
-- across this migration with no action.
--
-- Categories are stored as MUTED lists, not enabled lists. Every category is on
-- unless someone switched it off, so a category added in a later release is
-- delivered by default instead of being silently dropped for everyone who had
-- ever saved their preferences. Category ids are validated in Go
-- (notifier.Categories); an unknown id left in an array is ignored.
--
-- user_notification_preferences holds a user's EMAIL choices only. Slack is an
-- instance channel the admin controls. A missing row means the defaults: email
-- on, nothing muted.
--
-- pipeline_notifications.delivery_status gains the value 'skipped': a channel
-- was configured but every configured channel was muted for this alert's
-- category. It is kept distinct from 'suppressed' (no channel configured at
-- all) and 'failed' (a send was attempted and errored), because "you asked not
-- to be told" and "we could not tell you" must never look the same in the row.
-- The column is TEXT with no CHECK, so no constraint changes.

CREATE TABLE IF NOT EXISTS notification_channel_settings (
    id                       SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),

    slack_enabled            BOOLEAN NOT NULL DEFAULT FALSE,
    slack_webhook_encrypted  TEXT    NOT NULL DEFAULT '',
    slack_muted_categories   TEXT[]  NOT NULL DEFAULT '{}',

    email_enabled            BOOLEAN NOT NULL DEFAULT FALSE,
    smtp_host                TEXT    NOT NULL DEFAULT '',
    smtp_port                INTEGER NOT NULL DEFAULT 587 CHECK (smtp_port BETWEEN 1 AND 65535),
    smtp_username            TEXT    NOT NULL DEFAULT '',
    smtp_password_encrypted  TEXT    NOT NULL DEFAULT '',
    smtp_from                TEXT    NOT NULL DEFAULT '',
    -- starttls: STARTTLS is REQUIRED (the send fails if the server does not offer it)
    -- tls:      implicit TLS from the first byte (usually port 465)
    -- none:     plaintext; for a relay on a trusted network only
    smtp_tls_mode            TEXT    NOT NULL DEFAULT 'starttls'
                             CHECK (smtp_tls_mode IN ('starttls', 'tls', 'none')),

    updated_by               UUID REFERENCES users(id) ON DELETE SET NULL,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_notification_preferences (
    user_id                  UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    email_enabled            BOOLEAN NOT NULL DEFAULT TRUE,
    email_muted_categories   TEXT[]  NOT NULL DEFAULT '{}',
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE notification_channel_settings IS
'Single-row (id=1) instance-wide Slack + SMTP notification channels, set by an admin. Secrets are shared/go/crypto ciphertext. When present, overrides the NOTIFIER_SLACK_WEBHOOK_URL / SMTP_* env vars.';

COMMENT ON TABLE user_notification_preferences IS
'Per-user email alert preferences (master switch + muted categories). Missing row = email on, nothing muted.';

COMMENT ON COLUMN pipeline_notifications.delivery_status IS
'pending | delivered | failed | suppressed (no channel configured) | skipped (channels configured, but every one is muted for this category)';
