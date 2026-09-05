-- 084_saved_queries.sql
--
-- Server-side saved queries for the Data Explorer.
--
-- Before this, "history" was localStorage only (rsync_explorer_history_v1, capped
-- at 20 entries in explorer/page.tsx) — per-browser, unshareable, unversioned, and
-- gone with a cache clear. This table makes a query a first-class workspace
-- resource so it can be shared with teammates, versioned, and (084+) scheduled.
--
-- Scoping model (mirrors connections/pipelines from 069):
--   workspace_id NOT NULL — there is no such thing as a global saved query, so
--   unlike 077's PII policies there is no NULL/global tier here. This column is
--   what requireResourceRole joins against (`r.workspace_id = $activeWS`), so it
--   MUST stay NOT NULL for the IDOR gate to be meaningful.
--
-- ON DELETE CASCADE on connection_id: a saved query is written against one
-- connection's schema. When that connection is removed the SQL is no longer
-- runnable and keeping it would strand a schedule pointing at a dead target.
--
-- statement_class is a SNAPSHOT for display only (list badges, filtering). It is
-- deliberately NOT the authorization input: sql_text is mutable, so any execution
-- path re-runs validators.ClassifyStatement against the CURRENT sql_text and gates
-- on that. A stale or forged class column must never be able to widen access.
--
-- The runner (internal/db/migrate.go) auto-wraps this file in one transaction. Do NOT
-- start a transaction explicitly here: migrate.go decides a file is self-managed by
-- searching the raw text for a transaction-start keyword, and it does not skip comments
-- while looking. This very block used to name that keyword in order to warn about it,
-- which turned the warning into the bug — 084 was being applied with no wrapper at all.
-- Idempotent: CREATE TABLE/INDEX IF NOT EXISTS throughout, so a re-run is a no-op.

CREATE TABLE IF NOT EXISTS saved_queries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Tenancy. Both CASCADE: the query cannot outlive its workspace or its target.
    workspace_id  UUID NOT NULL REFERENCES workspaces(id)  ON DELETE CASCADE,
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,

    -- Content
    name        TEXT NOT NULL,
    description TEXT,
    sql_text    TEXT NOT NULL,
    nl_prompt   TEXT,          -- the natural-language ask this SQL came from, if any

    -- Advisory snapshot — see header. Never an authorization input.
    statement_class TEXT NOT NULL DEFAULT 'unknown',

    -- 'workspace' = every member can see it; 'private' = creator only.
    visibility TEXT NOT NULL DEFAULT 'workspace'
        CHECK (visibility IN ('private', 'workspace')),

    -- Attribution. No FK to users: mirrors pipeline_schedules.created_by (023) and
    -- keeps a saved query readable after the author's account row is removed.
    created_by UUID NOT NULL,
    updated_by UUID,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_at TIMESTAMPTZ
);

-- Append-only edit history. Row N holds the content as it was BEFORE the edit that
-- produced N+1, so "restore" is a plain read of the target version's sql_text.
CREATE TABLE IF NOT EXISTS saved_query_versions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    saved_query_id UUID NOT NULL REFERENCES saved_queries(id) ON DELETE CASCADE,
    version         INT  NOT NULL,
    name            TEXT NOT NULL,
    sql_text        TEXT NOT NULL,
    statement_class TEXT NOT NULL DEFAULT 'unknown',
    changed_by      UUID NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (saved_query_id, version)
);

-- Listing is always "this workspace, newest first" — the index matches that shape.
CREATE INDEX IF NOT EXISTS idx_saved_queries_workspace
    ON saved_queries(workspace_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_saved_queries_connection
    ON saved_queries(connection_id);
CREATE INDEX IF NOT EXISTS idx_saved_queries_created_by
    ON saved_queries(created_by);
CREATE INDEX IF NOT EXISTS idx_saved_query_versions_query
    ON saved_query_versions(saved_query_id, version DESC);

-- Names are a human handle, so collisions must be case-insensitive: "Daily MRR"
-- and "daily mrr" in one workspace would be indistinguishable in the picker.
-- Scoped per workspace, not global — two tenants may both have "Daily MRR".
CREATE UNIQUE INDEX IF NOT EXISTS idx_saved_queries_unique_name
    ON saved_queries(workspace_id, lower(name));

DROP TRIGGER IF EXISTS update_saved_queries_updated_at ON saved_queries;
CREATE TRIGGER update_saved_queries_updated_at
BEFORE UPDATE ON saved_queries
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE  saved_queries IS 'Workspace-scoped saved Explorer queries; replaces per-browser localStorage history';
COMMENT ON COLUMN saved_queries.statement_class IS 'Advisory snapshot for UI badges only — execution re-classifies current sql_text and gates on that';
COMMENT ON COLUMN saved_queries.visibility IS 'workspace = all members may read; private = created_by only';
COMMENT ON COLUMN saved_queries.nl_prompt IS 'Natural-language prompt this SQL was generated from, when it came from the NL path';
COMMENT ON TABLE  saved_query_versions IS 'Append-only edit history for saved_queries; one row per pre-edit state';
