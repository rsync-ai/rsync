-- Workspaces (multi-tenant isolation unit)
-- Each user gets a personal workspace on signup; teams share one workspace.

CREATE TABLE IF NOT EXISTS workspaces (
    id          UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    slug        VARCHAR(100) NOT NULL UNIQUE,   -- URL-safe identifier, e.g. "acme-corp"
    owner_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    plan        VARCHAR(50) DEFAULT 'free',      -- free | pro | enterprise
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workspaces_owner ON workspaces(owner_id);
CREATE INDEX IF NOT EXISTS idx_workspaces_slug  ON workspaces(slug);

-- Workspace members (owner always present; others are invitees)
CREATE TABLE IF NOT EXISTS workspace_members (
    id           UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    user_id      UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role         VARCHAR(50) DEFAULT 'member',  -- owner | admin | member | viewer
    invited_by   UUID REFERENCES users(id),
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (workspace_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_members_user      ON workspace_members(user_id);
CREATE INDEX IF NOT EXISTS idx_workspace_members_workspace ON workspace_members(workspace_id);

-- Add workspace_id to connections (nullable; filled by migration below)
ALTER TABLE connections
    ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_connections_workspace ON connections(workspace_id);

-- Add workspace_id to pipelines (nullable; filled by migration below)
ALTER TABLE pipelines
    ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_pipelines_workspace ON pipelines(workspace_id);

-- -----------------------------------------------------------------------
-- Data migration: create a personal workspace for every existing user
-- and assign their existing connections + pipelines to it.
-- -----------------------------------------------------------------------
DO $$
DECLARE
    rec RECORD;
    ws_id UUID;
    ws_slug TEXT;
BEGIN
    FOR rec IN SELECT id, email FROM users LOOP
        -- Derive a URL-safe slug from the email local-part
        ws_slug := LOWER(REGEXP_REPLACE(SPLIT_PART(rec.email, '@', 1), '[^a-z0-9]', '-', 'g'));
        -- Ensure uniqueness with a short suffix if needed
        WHILE EXISTS (SELECT 1 FROM workspaces WHERE slug = ws_slug) LOOP
            ws_slug := ws_slug || '-' || SUBSTRING(gen_random_uuid()::text, 1, 4);
        END LOOP;

        INSERT INTO workspaces (name, slug, owner_id, plan)
        VALUES (SPLIT_PART(rec.email, '@', 1) || '''s workspace', ws_slug, rec.id, 'free')
        ON CONFLICT DO NOTHING
        RETURNING id INTO ws_id;

        -- If the workspace already existed (conflict), look it up
        IF ws_id IS NULL THEN
            SELECT id INTO ws_id FROM workspaces WHERE owner_id = rec.id LIMIT 1;
        END IF;

        IF ws_id IS NULL THEN CONTINUE; END IF;

        -- Add owner as workspace member
        INSERT INTO workspace_members (workspace_id, user_id, role)
        VALUES (ws_id, rec.id, 'owner')
        ON CONFLICT DO NOTHING;

        -- Assign existing connections
        UPDATE connections SET workspace_id = ws_id
        WHERE user_id = rec.id AND workspace_id IS NULL;

        -- Assign existing pipelines
        UPDATE pipelines SET workspace_id = ws_id
        WHERE created_by = rec.id AND workspace_id IS NULL;
    END LOOP;
END $$;

COMMENT ON TABLE workspaces        IS 'Tenant isolation unit; each user has a personal workspace, teams share one.';
COMMENT ON TABLE workspace_members IS 'Maps users to workspaces with a role; owner always present.';
