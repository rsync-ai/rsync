-- 058_email_verification.sql
-- Email verification via Resend API.
--
-- New users must verify their email before accessing any rsync-ai features.
-- This adds three columns to users + a cleanup index.
--
-- Columns:
--   email_verified          BOOLEAN  -- false until user clicks verification link
--   email_verify_token      TEXT     -- crypto-random 64-hex-char token (NULL once verified)
--   email_verify_expires_at TIMESTAMPTZ -- token TTL (24h from registration)
--
-- Notes:
--   * Existing users are marked verified = true so we don't lock out anyone already active.
--   * The first admin (self-bootstrap) is also auto-verified.
--   * If RESEND_API_KEY is unset (self-hosted / air-gapped), the API layer skips
--     email sending and auto-verifies users — the middleware checks the env var.

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS email_verified BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS email_verify_token TEXT DEFAULT NULL;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS email_verify_expires_at TIMESTAMPTZ DEFAULT NULL;

-- Mark all existing users as verified (we can't retroactively ask them to verify).
UPDATE users SET email_verified = true WHERE email_verified = false;

-- Index for fast token lookup on the verify-email endpoint.
CREATE INDEX IF NOT EXISTS idx_users_email_verify_token
    ON users (email_verify_token)
    WHERE email_verify_token IS NOT NULL;
