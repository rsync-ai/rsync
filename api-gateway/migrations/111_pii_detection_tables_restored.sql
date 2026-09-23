-- 111_pii_detection_tables_restored.sql
--
-- The PII detection and masking tier has had nowhere to store anything since
-- December 2024, and the UI has been reporting that as "0 detected columns".
--
-- `009_pii_and_transforms.sql` is the only migration that ever created the PII
-- tables. Four of them were dropped nineteen migrations later by
-- `013_cleanup_unused_tables.sql:26-29`, under the comment "PII tables not yet
-- implemented", and nothing has recreated them since:
--
--     custom_hash_functions, pii_scan_results, pii_access_audit, pii_approval_audit
--
-- The handlers that read the first two were written (or rewritten) afterwards and
-- are live on every route table today: `GetScanResults` / `GetScanResultsByPipeline`
-- (pii.go:156, :213) and `GetHashFunctions` / `CreateHashFunction` (pii.go:1021, :1074).
-- Against a current schema each of them fails on a missing relation and returns
-- HTTP 500. The PII page rendered zeros rather than surfacing the error, because
-- every one of its four fetches was written `if (res.ok)` with no else — so the
-- feature read as "empty" rather than "broken" for the whole period. That half is
-- fixed alongside this migration (pii/page.tsx `load`), so a future failure here
-- is named on the page instead of being rounded down to zero.
-- Migrations 077 and 078 both already encode the absence: every one of their
-- ALTER/UPDATE/CREATE INDEX statements for these two tables is wrapped in a
-- `to_regclass(...) IS NOT NULL` guard precisely because 013 had removed them.
--
-- WHY ONLY TWO OF THE FOUR COME BACK
--
-- Detection and masking are the public product and stay that way permanently;
-- the *governance* layer on top of them — policy enforcement, the approval
-- workflow, and the immutable access audit — is the paid layer, and its schema
-- belongs to the private service that will own it, not to this migration set.
-- See docs/internal/public-vs-cloud-feature-split.md §4.2 Layer 4 and §7.2.
-- So `pii_scan_results` and `custom_hash_functions` are restored here, and
-- `pii_approval_audit` / `pii_access_audit` are deliberately left dropped.
--
-- There is a second, independent reason `pii_access_audit` cannot simply come
-- back as it was: its `execution_id` referenced `executions(id)`, and 013 dropped
-- `executions` in the same block (replaced by Temporal workflow history). Its 009
-- definition no longer applies to any schema this product runs on.
--
-- One consequence to be aware of rather than surprised by: `pii.go:786` writes a
-- row to `pii_approval_audit` when an approval is decided, and logs-and-ignores
-- the failure. That write continues to fail until the private governance service
-- creates the table it owns. It was already failing before this migration; this
-- migration neither fixes nor worsens it, and it is tracked with the rest of
-- wave 3 in BACKLOG.md.
--
-- WHY workspace_id IS BUILT IN RATHER THAN ADDED LATER
--
-- `077_pii_workspace_scoping.sql` is the cross-tenant IDOR fix: every PII table
-- needs `workspace_id` because the handlers bind it in every query. On a current
-- schema 077's blocks for these two tables were no-ops (the tables were absent),
-- and 077 has already run, so it will not revisit them. A table recreated without
-- the column would therefore reintroduce the 500s that 077 exists to have made
-- impossible. The column is declared here with exactly 077's shape — nullable,
-- `REFERENCES workspaces(id) ON DELETE CASCADE`, with the same index name — so a
-- schema that arrives here by either route is identical.
--
-- Nullable is deliberate and carries meaning, unchanged from 077/078: a row is a
-- shared, read-only seeded default if and only if `workspace_id IS NULL AND
-- created_by IS NULL`. `GetHashFunctions` (pii.go:1022) reads exactly that pair, so
-- an unattributed row (workspace NULL, creator set) stays invisible and fails
-- closed instead of leaking to every tenant.
--
-- TWO DELIBERATE DIFFERENCES FROM THE 009 DEFINITIONS
--
--   1. `custom_hash_functions.org_id` is not restored. 077's header records that
--      the API never wrote it, and the unique index 009 built over it
--      (COALESCE(org_id, zero-uuid), name) would now be actively wrong: with
--      org_id permanently NULL, two different workspaces could not both name a
--      hash function "my_hash". The unique index is rebuilt over `workspace_id`,
--      which is the tenancy unit the handlers actually use.
--
--   2. `pii_scan_results.connection_id` is added. A scan is triggered against a
--      *connection* (`TriggerScan`, pii.go:284 → `pii_scan_jobs.connection_id`),
--      not a pipeline, so every row the scan path writes would otherwise carry a
--      NULL pipeline_id — and 009's `UNIQUE(pipeline_id, table_name, column_name)`
--      does not constrain those at all, because NULLs are distinct in a Postgres
--      unique index. Re-scanning a connection would silently accumulate a fresh
--      duplicate set of every finding on every run. The partial unique index
--      below is what makes the scan path's rows keyed on something real.
--
-- Idempotent: IF NOT EXISTS throughout, plus ADD COLUMN IF NOT EXISTS for the
-- case of a schema restored from a backup predating 013, where the tables exist
-- but may be missing the newer columns. Re-running is a no-op.
--
-- The runner wraps this file in one transaction; do NOT add a literal
-- transaction-start statement (db/migrate.go:169 treats its presence anywhere in
-- the file, comments included, as "this file manages its own transaction").

-- ============================================================================
-- 1. pii_scan_results — what a PII scan found, per column
-- ============================================================================

CREATE TABLE IF NOT EXISTS pii_scan_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    -- Scans are connection-scoped today; pipeline_id is kept for the 009-era
    -- pipeline-scoped reader (GetScanResultsByPipeline) and stays NULL otherwise.
    pipeline_id UUID REFERENCES pipelines(id) ON DELETE CASCADE,
    connection_id UUID REFERENCES connections(id) ON DELETE CASCADE,
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    pii_type VARCHAR(50) NOT NULL,
    confidence DECIMAL(5,4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    detection_method VARCHAR(50) NOT NULL,
    suggested_masking VARCHAR(50),
    -- The approval columns are the seam the private governance layer writes
    -- through. Nothing in the public tree sets them.
    approved_action VARCHAR(50),
    approved_by UUID REFERENCES users(id),
    approved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(pipeline_id, table_name, column_name)
);

-- For a schema restored from a pre-013 backup, where the table exists but
-- predates both 077's column and this migration's.
ALTER TABLE pii_scan_results
    ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE;
ALTER TABLE pii_scan_results
    ADD COLUMN IF NOT EXISTS connection_id UUID REFERENCES connections(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_pii_scan_results_pipeline ON pii_scan_results(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pii_scan_results_pii_type ON pii_scan_results(pii_type);
-- Same name 077 would have created, so both routes to this schema agree.
CREATE INDEX IF NOT EXISTS idx_pii_scan_results_workspace ON pii_scan_results(workspace_id);
CREATE INDEX IF NOT EXISTS idx_pii_scan_results_connection ON pii_scan_results(connection_id);

-- The real key for everything the scan path writes. Partial, so it constrains
-- connection-scoped rows without touching the 009-era pipeline-scoped shape.
-- A connection belongs to exactly one workspace, so connection_id already
-- carries the tenancy — adding workspace_id here would only weaken it.
CREATE UNIQUE INDEX IF NOT EXISTS uq_pii_scan_results_connection_column
    ON pii_scan_results(connection_id, table_name, column_name)
    WHERE connection_id IS NOT NULL;

-- ============================================================================
-- 2. custom_hash_functions — the masking functions a column can be hashed with
-- ============================================================================

CREATE TABLE IF NOT EXISTS custom_hash_functions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL CHECK (type IN ('python', 'api', 'lookup_table', 'builtin')),
    description TEXT,
    code TEXT,                      -- python type
    endpoint VARCHAR(500),          -- api type
    config JSONB DEFAULT '{}',
    reversible BOOLEAN DEFAULT FALSE,
    enabled BOOLEAN DEFAULT TRUE,
    -- NULL together with workspace_id marks a seeded global (see header).
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE custom_hash_functions
    ADD COLUMN IF NOT EXISTS workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_custom_hash_functions_workspace ON custom_hash_functions(workspace_id);

-- Unique per tenant, not globally: the zero UUID stands in for "global" so the
-- seeded built-ins cannot be shadowed by a duplicate global, while two
-- workspaces may each define their own function of the same name.
CREATE UNIQUE INDEX IF NOT EXISTS uq_custom_hash_functions_workspace_name
    ON custom_hash_functions ((COALESCE(workspace_id, '00000000-0000-0000-0000-000000000000'::uuid)), name);

-- The 009 seeds, which 013 destroyed along with the table. workspace_id and
-- created_by both NULL, which is what makes them the shared read-only globals
-- GetHashFunctions returns to every workspace.
INSERT INTO custom_hash_functions (id, name, type, description, reversible, enabled)
VALUES
    (uuid_generate_v4(), 'sha256', 'builtin', 'SHA-256 cryptographic hash (recommended)', false, true),
    (uuid_generate_v4(), 'sha512', 'builtin', 'SHA-512 cryptographic hash', false, true),
    (uuid_generate_v4(), 'md5', 'builtin', 'MD5 hash (legacy, not recommended for security)', false, true),
    (uuid_generate_v4(), 'blake2', 'builtin', 'BLAKE2 hash (fast and secure)', false, true),
    (uuid_generate_v4(), 'hmac_sha256', 'builtin', 'HMAC-SHA256 keyed hash', false, true)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- 3. updated_at triggers, matching the ones 009 installed
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_pii_scan_results_updated_at') THEN
        CREATE TRIGGER update_pii_scan_results_updated_at
            BEFORE UPDATE ON pii_scan_results
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_custom_hash_functions_updated_at') THEN
        CREATE TRIGGER update_custom_hash_functions_updated_at
            BEFORE UPDATE ON custom_hash_functions
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;
