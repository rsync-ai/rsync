-- 097: retention policy for saved_query_versions
--
-- saved_query_versions (084) is append-only. Nothing has ever deleted from it, and
-- the read path (ListSavedQueryVersions) stops at LIMIT 100 — so every version past
-- the hundredth is stored forever and shown to nobody. That is the whole of
-- DX-VersionRetention: not a correctness bug, a table that only grows behind a
-- history UI that quietly stops describing it.
--
-- The policy has two axes because one is not enough:
--
--   retention_days — versions older than this are eligible for deletion. NULL means
--                    keep forever, which is exactly today's behaviour, so applying
--                    this migration changes nothing until a workspace admin opts in.
--                    An age-only policy is wrong on its own: a query edited twice a
--                    year would lose its whole history to a 90-day rule.
--
--   retention_min  — the newest N versions are never deleted, whatever their age.
--                    This is the floor that makes the age axis safe. The DEFAULT of
--                    20 is inert while retention_days is NULL; it starts mattering
--                    only the moment someone sets an age.
--
-- Both are per workspace, not global: what a shared analytics workspace considers
-- disposable history is not what a regulated one does.
--
-- The CHECK on retention_min is deliberately a hard floor in the schema and not only
-- in the handler. A retention policy is a data-destroying setting, and the failure
-- mode worth designing against is an admin (or a careless PATCH) setting it to 0 and
-- discovering afterwards that "restore the previous version" has no previous version
-- left to restore. The handler enforces the same floor with a readable message; this
-- is what holds when the next writer of that handler forgets.
--
-- Safe to apply online: two ADD COLUMNs on a table with a handful of rows. Neither
-- rewrites the table — the nullable column has no default at all, and the NOT NULL
-- one takes a constant default, which PostgreSQL 11+ records in the catalogue
-- instead of backfilling every row.
--
-- "Does not rewrite the table" is not the same as "does not lock it", and on THIS
-- table the difference matters more than it would almost anywhere else. Every
-- ALTER TABLE below takes ACCESS EXCLUSIVE on workspaces, and workspaces is read by
-- resolveActiveWorkspace and requireWorkspaceRole on effectively every authenticated
-- request. The lock is held for microseconds — but a lock request that has to WAIT
-- queues every later reader behind it, so a single long-running transaction touching
-- workspaces at the wrong moment turns a catalogue update into a stall across the
-- whole API. lock_timeout bounds that: if the lock is not free almost immediately the
-- migration fails instead of forming a convoy.
--
-- SET LOCAL is scoped to the runner's surrounding transaction and reverts on commit,
-- so it cannot leak a 3-second lock_timeout into the pooled connection's next user.
--
-- Failing is not free — see the recovery note in 098's header, which applies to this
-- file too and for the same reason: a migration that returns an error aborts the
-- whole run and leaves /ready red. It is still the better of the two outcomes. A
-- failed migration is loud, one restart away from retrying, and does not stall live
-- traffic; a lock convoy on workspaces is an outage.
--
-- No transaction control in this file. The migration runner wraps each file in its
-- own transaction and detects self-management by scanning the raw text — including
-- inside comments — so naming the statement here, even to say we are not using it,
-- would silently unwrap the file. See 084's header for the incident that taught us.

SET LOCAL lock_timeout = '3s';

ALTER TABLE workspaces
    ADD COLUMN IF NOT EXISTS saved_query_version_retention_days INT,
    ADD COLUMN IF NOT EXISTS saved_query_version_retention_min  INT NOT NULL DEFAULT 20;

-- Range guards. DROP-then-ADD rather than a DO block, because ADD CONSTRAINT has no
-- IF NOT EXISTS in PostgreSQL 16 and this file has to survive re-application. The
-- runner's transaction makes the drop/add pair atomic, so there is no window in
-- which the column sits unguarded.
ALTER TABLE workspaces
    DROP CONSTRAINT IF EXISTS saved_query_version_retention_days_range;
ALTER TABLE workspaces
    ADD CONSTRAINT saved_query_version_retention_days_range
    CHECK (saved_query_version_retention_days IS NULL
           OR (saved_query_version_retention_days >= 1
               AND saved_query_version_retention_days <= 3650));

ALTER TABLE workspaces
    DROP CONSTRAINT IF EXISTS saved_query_version_retention_min_range;
ALTER TABLE workspaces
    ADD CONSTRAINT saved_query_version_retention_min_range
    CHECK (saved_query_version_retention_min >= 5
           AND saved_query_version_retention_min <= 1000);

COMMENT ON COLUMN workspaces.saved_query_version_retention_days IS
    'Saved-query edit history: delete versions older than this many days. NULL = keep forever (default).';
COMMENT ON COLUMN workspaces.saved_query_version_retention_min IS
    'Saved-query edit history: never delete the newest N versions of a query, whatever their age. Hard floor 5.';

-- Supports the prune predicate (saved_query_id = $1 AND created_at < $2). The
-- existing UNIQUE (saved_query_id, version) already orders by version for the
-- keep-newest-N bound; this covers the age bound so a prune on a query with a long
-- history does not degrade into a scan of that query's versions.
CREATE INDEX IF NOT EXISTS idx_saved_query_versions_created
    ON saved_query_versions (saved_query_id, created_at);
