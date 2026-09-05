-- Per-user "bring your own" (BYO) OAuth application credentials.
--
-- The boot-time provider map (NewOAuthHandler) only activates providers whose
-- operator env vars (<PROVIDER>_CLIENT_ID/_SECRET) are set. That works for the
-- curated few but leaves every spec-generated OAuth2 connector permanently
-- `provider_disabled` — there was no path for a user to supply their own app
-- credentials. This table is that path: one OAuth app per (user_id, provider),
-- combined at request time with the provider's endpoints from providers.json
-- (see internal/handlers/oauth_byo.go — resolveProvider / SaveOAuthApp).
--
-- Self-hosted rsync-ai cannot ship a shared OAuth app for arbitrary vendors
-- (the vendor pre-registers a redirect_uri the operator's host can't satisfy,
-- and many vendors only issue per-tenant credentials), so BYO is the only
-- mechanism that unlocks generated connectors. Operator-env stays the default
-- for curated providers (it takes precedence in resolveProvider).
--
-- client_secret is stored ENCRYPTED at rest (crypto.EncryptString, AES-256-GCM,
-- "enc.v1:" envelope) — never plaintext. Same crypto path as oauth_tokens.
CREATE TABLE IF NOT EXISTS oauth_apps (
    user_id       uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider      VARCHAR(50) NOT NULL,
    client_id     TEXT NOT NULL,
    client_secret TEXT NOT NULL,
    scopes        TEXT,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, provider)
);

CREATE INDEX IF NOT EXISTS idx_oauth_apps_user_id ON oauth_apps(user_id);
