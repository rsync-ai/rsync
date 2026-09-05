-- Migration 094: the sessions table stored working credentials, not references to them
--
-- sessions.token held the bearer token verbatim -- the exact string the client
-- sends back in `Authorization:` or the auth_token cookie -- and every lookup was
-- an equality test against that plaintext (auth_middleware.go, admin_middleware.go,
-- main.go, five handlers in auth.go, and the orchestrator's own middleware).
--
-- That makes a row in this table a login, not a pointer to one. Anything that can
-- read a single row can be the user until it expires: a backup, a managed-service
-- snapshot, a read replica, a support query pasted into a ticket, a read-only SQL
-- injection anywhere in the app, or -- the reason this is being fixed now -- a
-- database dump carried from one cloud to another. None of those need a password,
-- a key, or a decrypt; they need a SELECT.
--
-- Note what does NOT save you here. users.password_hash is bcrypt, connection
-- configs are encrypted at rest, oauth_tokens and oauth_apps.client_secret are
-- encrypted. Every other credential in this schema is protected. The session token
-- -- which grants the same access those credentials lead to, without any of them --
-- was the one stored in the clear.
--
-- FIX: store sha256(token) and look up by sha256(token). See
-- shared/go/crypto/session_token.go for why SHA-256 rather than bcrypt (the token
-- is 122 bits of crypto/rand, so it is not guessable at any work factor, and a
-- slow KDF on every authenticated request is a self-inflicted DoS).
--
-- LIVE SESSIONS SURVIVE. The existing plaintext is hashed IN PLACE below, so the
-- stored value becomes the hash of the token the client is still holding, and the
-- next request matches. Nobody is signed out. The plaintext is destroyed in the
-- same statement -- this is a rewrite, not a copy, so there is no column left
-- behind holding the old values.
--
-- DEPLOY BOTH SERVICES TOGETHER. api-gateway and backend-orchestrator each compare
-- this column independently. Once this migration runs, a binary that still compares
-- plaintext matches nothing and rejects every request with 401. The failure is loud
-- and reverses the instant the lagging service is deployed, but it is real: ship
-- api-gateway and backend-orchestrator in the same rollout.

BEGIN;

-- Idempotent by shape, not just by the runner's filename ledger. applyMigration
-- records the version in a SEPARATE transaction from the DDL (migrate.go:96-106),
-- so a crash in the gap re-runs this file. A UUID token is 36 characters and can
-- never match a 64-hex pattern, so already-hashed rows are skipped and a second
-- run cannot double-hash -- which would silently sign out every user, with the
-- original token unrecoverable.
UPDATE sessions
   SET token = encode(sha256(token::bytea), 'hex')
 WHERE token !~ '^[0-9a-f]{64}$';

-- The guard that keeps this fixed. A future code path that forgets to hash before
-- INSERT now fails at the database instead of quietly storing a live credential --
-- the difference between a loud error at write time and a leak nobody notices.
-- Sessions are the one table where a silent regression is indistinguishable from
-- correct behaviour: auth still works either way.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'sessions_token_is_sha256'
    ) THEN
        ALTER TABLE sessions
            ADD CONSTRAINT sessions_token_is_sha256
            CHECK (token ~ '^[0-9a-f]{64}$');
    END IF;
END
$$;

COMMENT ON COLUMN sessions.token IS
    'sha256(bearer token), lowercase hex. NEVER the token itself -- see migration 094 '
    'and shared/go/crypto/HashSessionToken. Hash on write and on lookup; the plaintext '
    'exists only in the Set-Cookie header and the inbound Authorization header.';

COMMIT;
