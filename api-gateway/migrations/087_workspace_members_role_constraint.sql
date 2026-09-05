-- workspace_members.role: enforce the four-role vocabulary in the database.
--
-- Why this exists. 047 declared the column as `role VARCHAR(50) DEFAULT 'member'`
-- and named the four roles in a COMMENT only — nullable, unconstrained. Its
-- sibling workspace_invites.role has carried CHECK (role IN (...)) since 069, so
-- the invite path was already protected while the membership path — the one that
-- actually decides authorization — was not.
--
-- What goes wrong without it. security.WorkspaceRole.Meets fails closed: a role
-- outside the vocabulary has level 0 and satisfies NO gate. That is the safe
-- failure direction (never an escalation), but it is a silent one — an 'Owner'
-- typed with a capital O, a NULL from a hand-written INSERT, or a value from a
-- future code path all produce a membership row that the UI lists with a role
-- badge while every request under it is refused. The user appears to be a member
-- and cannot do anything, with nothing in the logs naming the cause. Constraining
-- the column turns that into a write-time error at the one place it can be fixed.
--
-- Idempotent and non-transactional by design: the runner wraps each file in a
-- single transaction, so this file must not start one itself.

-- 1. Normalize before constraining, so the ALTERs below cannot fail on existing
--    data. Case/whitespace variants map onto their canonical role; anything still
--    unrecognized (including NULL) becomes 'viewer' — the LEAST privileged role,
--    so a repair can only ever reduce access, never grant it. A row like this is
--    already dead weight in practice (it meets no gate today), and 'viewer' keeps
--    that user visible to their admins instead of silently vanishing.
UPDATE workspace_members
SET role = lower(btrim(role))
WHERE role IS NOT NULL
  AND role <> lower(btrim(role));

UPDATE workspace_members
SET role = 'viewer'
WHERE role IS NULL
   OR role NOT IN ('owner', 'admin', 'member', 'viewer');

-- 2. Constrain. Each step is guarded so a re-run (or a database where an earlier
--    attempt already applied part of this) is a no-op rather than an error.
ALTER TABLE workspace_members ALTER COLUMN role SET DEFAULT 'member';
ALTER TABLE workspace_members ALTER COLUMN role SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'workspace_members_role_check'
          AND conrelid = 'public.workspace_members'::regclass
    ) THEN
        ALTER TABLE workspace_members
            ADD CONSTRAINT workspace_members_role_check
            CHECK (role IN ('owner', 'admin', 'member', 'viewer'));
    END IF;
END $$;

COMMENT ON COLUMN workspace_members.role IS
    'owner | admin | member | viewer — enforced by workspace_members_role_check; mirrors security.WorkspaceRole in the api-gateway.';
