-- Add user_id ownership to oauth_tokens (T1-1 cross-tenant fix).
--
-- Pre-fix the oauth_tokens table had NO user_id column. ListTokens /
-- RefreshToken / RevokeToken queried by token id only, so any
-- authenticated tenant could list every other tenant's OAuth
-- credentials, force a refresh, or delete them — full cross-tenant
-- credential takeover. See security audit T1-1.
--
-- BACKFILL STRATEGY:
--
-- There is NO in-band backfill source in this repo's schema:
--
--   * `oauth_states.user_id` exists (migration 007) but the state row
--     is deleted in /oauth/callback once the token is minted, so it
--     can't populate user_id for already-issued tokens.
--
--   * `connections.oauth_token_id` does NOT exist as a column —
--     OAuth-token linkage is stored inside the connection's
--     encrypted config (see internal/handlers/oauth.go line 77).
--     We deliberately don't decrypt every connection here to
--     enumerate token→user mappings; that's a manual operation an
--     admin can run if there's enough orphan data to be worth
--     recovering.
--
-- Pre-existing rows therefore get `user_id = NULL`. Handler code post-
-- migration treats `user_id IS NULL` as "orphan / pending cleanup"
-- and refuses to list / refresh / revoke them from any user context.
-- An admin can either:
--   (a) `DELETE FROM oauth_tokens WHERE user_id IS NULL;` and have
--       affected users re-authorize, or
--   (b) Manually UPDATE the rows after decrypting each connection's
--       config to find its linked token.
--
-- For a brand-new install this is a no-op (zero pre-existing tokens).

-- Add the column. NULLABLE because we can't backfill in-band; new
-- inserts post-migration require it via handler-side validation
-- (storeToken refuses empty userID).
ALTER TABLE oauth_tokens
    ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE CASCADE;

-- Per-user lookup index. Critical because every read path post-fix
-- includes "AND user_id = $X" and the existing primary key on `id`
-- alone is fine for point lookups but not for ListTokens.
CREATE INDEX IF NOT EXISTS idx_oauth_tokens_user_id
    ON oauth_tokens(user_id);

-- Observability: count how many rows ended up orphaned so the operator
-- knows whether manual cleanup is needed. RAISE NOTICE so it shows in
-- migration logs without breaking the run.
DO $$
DECLARE
    orphan_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO orphan_count FROM oauth_tokens WHERE user_id IS NULL;
    IF orphan_count > 0 THEN
        RAISE NOTICE '⚠️  oauth_tokens: % rows have NULL user_id after backfill (no connection references them). They cannot be listed/refreshed/revoked by any user. Inspect: SELECT id, provider, account_name FROM oauth_tokens WHERE user_id IS NULL;', orphan_count;
    ELSE
        RAISE NOTICE '✅ oauth_tokens: backfill complete, no orphan rows';
    END IF;
END $$;

COMMENT ON COLUMN oauth_tokens.user_id IS 'Owning user. Every per-id read MUST filter on user_id to prevent cross-tenant credential takeover (T1-1). Rows with NULL user_id are system-owned / orphaned and inaccessible from user-scoped handlers.';
