-- Migration 092: "Last Login" was a session census wearing a login label
--
-- admin_users.go computed the column, in both the list and the single-user read, as
--
--     (SELECT MAX(s.created_at) FROM sessions s WHERE s.user_id = u.id)
--
-- and Logout DELETEs the session row (auth.go:701). So the aggregate ranged over
-- sessions that still exist, not over logins that happened, and the value moved
-- BACKWARDS when a user signed out -- to their previous surviving session, or to
-- nothing at all when that was their only one. The ordering it produced was the
-- inverse of the one an admin reads it for: a user who logs in daily and signs out
-- looked less recently active than one who logged in once months ago and never
-- signed out. Three other call sites bulk-delete sessions (auth.go:915 "sign out
-- other devices", admin_users.go:331 deactivate, :376 delete), each of which
-- silently rewrote history the same way.
--
-- WHY A COLUMN AND NOT A BETTER QUERY. There is nothing left to query. The login
-- event is not recorded anywhere durable -- audit_log gets a 'login_success' entry,
-- but that table is a retention-bounded audit stream, not a user-attribute store,
-- and reading a per-user scalar out of it would be the same derivation mistake one
-- table over. A login is a fact about the user; it belongs on the user.
--
-- THE BACKFILL IS A LOWER BOUND, DELIBERATELY. It seeds each user with the very
-- number this migration exists to replace -- but frozen. That is the point: from
-- here the value only ever moves forward, and a lower bound is strictly better than
-- the alternative. NULL renders as "-" in the admin UI, which reads as "never
-- logged in" -- actively false for every existing user, and a worse lie than a
-- slightly stale timestamp. Users with no session rows at all stay NULL, which is
-- the honest answer for them.
--
-- No index. The column is projected, never filtered or sorted on (both reads
-- ORDER BY u.created_at DESC). An index with no reader is a cost with no payoff
-- and a puzzle for whoever finds it next.
BEGIN;

ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at timestamp with time zone;

COMMENT ON COLUMN users.last_login_at IS
    'Stamped by handlers.stampLastLogin on every path that mints a session (Login, '
    'Register). Monotonic by construction -- unlike the MAX(sessions.created_at) '
    'aggregate it replaced, it is unaffected by logout, session expiry, or an admin '
    'purging a user''s sessions. NULL = no login has been recorded since migration 092 '
    'and the user had no surviving session to backfill from.';

-- Seed from surviving sessions once. WHERE last_login_at IS NULL keeps this a no-op
-- on re-run and, more importantly, keeps it from ever clobbering a real stamp should
-- the migration be replayed after the gateway has been serving logins.
UPDATE users u
   SET last_login_at = (SELECT MAX(s.created_at) FROM sessions s WHERE s.user_id = u.id)
 WHERE u.last_login_at IS NULL;

COMMIT;
