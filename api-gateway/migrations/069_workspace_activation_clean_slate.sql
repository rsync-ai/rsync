-- 069_workspace_activation_clean_slate.sql
-- Activate the dormant 047 workspace scaffold for single-company multi-user
-- collaboration (owner/admin/member/viewer share connections + pipelines).
--
-- CLEAN-SLATE strategy (authorized 2026-06-27: prod is live but has NO active
-- users and NO real data pipelines):
--   * pipelines are TRUNCATEd (not migrated) -> every pipeline-referencing child
--     row is cleared in the same operation.
--   * connections are PRESERVED and backfilled to each owner's personal
--     workspace, so nobody has to re-enter credentials.
--   * workspace_id becomes NOT NULL on both tables IMMEDIATELY (no later 07x):
--     pipelines is empty and connections is fully backfilled, so the constraint
--     holds at once. Any future INSERT that omits workspace_id now fails fast —
--     this is intentional and is the hard pre-req the R2 code (P1 INSERTs) meets.
--
-- Row-count assessment (clean slate): pipelines ~0 after TRUNCATE; connections
-- small. The runner's single auto-wrapped txn is safe; no batching and no
-- CREATE INDEX CONCURRENTLY needed (workspace_invites is new+empty; the audit
-- ALTERs are metadata-only nullable adds).
--
-- Idempotent: every step is guarded by IF NOT EXISTS / WHERE ... IS NULL /
-- NOT EXISTS, so a re-run is a no-op. The runner auto-wraps this file in one
-- transaction; do NOT add a literal "BEGIN;" (it would trip the self-managed-txn
-- detection in migrate.go).

-- 1. Workspace columns needed for personal-workspace targeting + quota Phase-1.
ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS is_personal             BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS plan_expires_at         TIMESTAMPTZ;
ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS pipeline_limit_override INT;

-- 2. Ensure every user has exactly ONE personal workspace with an owner
--    membership. Post-047 signups got none; 047-era users have one but it is not
--    yet flagged is_personal. Adopt an eligible existing single-member owned
--    workspace before creating a fresh one, so we never duplicate.
DO $$
DECLARE
    rec     RECORD;
    ws_id   UUID;
    ws_slug TEXT;
BEGIN
    FOR rec IN SELECT id, email FROM users LOOP
        -- already has a personal workspace -> nothing to do (idempotent re-run)
        IF EXISTS (SELECT 1 FROM workspaces WHERE owner_id = rec.id AND is_personal) THEN
            CONTINUE;
        END IF;

        -- prefer adopting an existing single-member workspace this user owns
        -- (this is the workspace 047 created for pre-047 users)
        SELECT w.id INTO ws_id
        FROM workspaces w
        WHERE w.owner_id = rec.id
          AND (SELECT count(*) FROM workspace_members m WHERE m.workspace_id = w.id) = 1
        ORDER BY w.created_at ASC
        LIMIT 1;

        IF ws_id IS NOT NULL THEN
            UPDATE workspaces SET is_personal = true WHERE id = ws_id;
        ELSE
            -- create a fresh personal workspace (mirror 047 slug derivation)
            ws_slug := LOWER(REGEXP_REPLACE(SPLIT_PART(rec.email, '@', 1), '[^a-z0-9]', '-', 'g'));
            WHILE EXISTS (SELECT 1 FROM workspaces WHERE slug = ws_slug) LOOP
                ws_slug := ws_slug || '-' || SUBSTRING(gen_random_uuid()::text, 1, 4);
            END LOOP;
            INSERT INTO workspaces (name, slug, owner_id, plan, is_personal)
            VALUES (SPLIT_PART(rec.email, '@', 1) || '''s workspace', ws_slug, rec.id, 'free', true)
            RETURNING id INTO ws_id;
        END IF;

        -- guarantee owner membership on the personal workspace
        INSERT INTO workspace_members (workspace_id, user_id, role)
        VALUES (ws_id, rec.id, 'owner')
        ON CONFLICT (workspace_id, user_id) DO NOTHING;
    END LOOP;
END $$;

-- 3. Backfill connections to each owner's personal workspace (PRESERVE creds).
UPDATE connections c
SET workspace_id = (
    SELECT w.id FROM workspaces w
    WHERE w.owner_id = c.user_id AND w.is_personal
    ORDER BY w.created_at ASC
    LIMIT 1
)
WHERE c.workspace_id IS NULL;

-- 4. CLEAN-SLATE: empty pipelines and ALL pipeline-referencing child rows.
--    TRUNCATE ... CASCADE clears children regardless of their ON DELETE action.
--    A plain DELETE FROM pipelines would be BLOCKED by executions' NO-ACTION FK
--    (001_init_schema.sql:66) and would orphan ON DELETE SET NULL children.
TRUNCATE TABLE pipelines CASCADE;

-- 5. Going-forward uniqueness: a connection name is unique within a workspace
--    (previously unique per user_id). Partial index tolerates legacy NULLs that
--    no longer exist after step 3 but keeps the predicate explicit.
CREATE UNIQUE INDEX IF NOT EXISTS uq_connections_ws_name
    ON connections (workspace_id, lower(name))
    WHERE workspace_id IS NOT NULL;

-- 6. workspace_invites — team membership invitations (distinct from the
--    instance-level 044 invitations table). Role is never 'owner'.
CREATE TABLE IF NOT EXISTS workspace_invites (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    email        VARCHAR(255) NOT NULL,
    role         VARCHAR(50)  NOT NULL DEFAULT 'member'
                 CHECK (role IN ('admin','member','viewer')),
    token        VARCHAR(64)  NOT NULL UNIQUE,
    invited_by   UUID NOT NULL REFERENCES users(id),
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','accepted','revoked')),
    expires_at   TIMESTAMPTZ  NOT NULL,
    accepted_at  TIMESTAMPTZ,
    accepted_by  UUID REFERENCES users(id),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_workspace_invites_pending
    ON workspace_invites (workspace_id, lower(email)) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_workspace_invites_token ON workspace_invites (token);
CREATE INDEX IF NOT EXISTS idx_workspace_invites_email ON workspace_invites (lower(email));

-- 7. Additive workspace_id audit/oauth columns (nullable; historical rows stay
--    NULL). Guarded by table existence so the migration is robust across
--    environments where an optional table may be absent.
DO $$
BEGIN
    IF to_regclass('public.audit_logs') IS NOT NULL THEN
        ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS workspace_id UUID;
    END IF;
    IF to_regclass('public.connection_access_logs') IS NOT NULL THEN
        ALTER TABLE connection_access_logs ADD COLUMN IF NOT EXISTS workspace_id UUID;
    END IF;
    IF to_regclass('public.oauth_tokens') IS NOT NULL THEN
        ALTER TABLE oauth_tokens ADD COLUMN IF NOT EXISTS workspace_id UUID;
        -- backfill token workspace from the token owner's personal workspace
        UPDATE oauth_tokens t
        SET workspace_id = (
            SELECT w.id FROM workspaces w
            WHERE w.owner_id = t.user_id AND w.is_personal
            ORDER BY w.created_at ASC LIMIT 1
        )
        WHERE t.workspace_id IS NULL;
    END IF;
    IF to_regclass('public.oauth_states') IS NOT NULL THEN
        ALTER TABLE oauth_states ADD COLUMN IF NOT EXISTS workspace_id UUID;
    END IF;
END $$;

-- 8. Enforce NOT NULL immediately. pipelines is empty (step 4) so it holds
--    trivially; connections is fully backfilled (step 3). SET NOT NULL is a
--    no-op on a column that is already NOT NULL, so this is idempotent.
ALTER TABLE connections ALTER COLUMN workspace_id SET NOT NULL;
ALTER TABLE pipelines   ALTER COLUMN workspace_id SET NOT NULL;
