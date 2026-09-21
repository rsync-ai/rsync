-- 110_destination_namespace_tombstones.sql
-- Who owned a destination namespace, after the pipeline that owned it is gone.
--
-- Deleting a pipeline never drops anything on the destination — that data is the
-- customer's, and no delete path in this repo issues a DROP SCHEMA or DROP TABLE
-- against a destination. What the delete DID do is silently un-own it. First-run
-- namespace resolution answers "does somebody else already write this table
-- here?" from the CONTROL PLANE (destination_mapping.go namespaceTableOwner —
-- a query over `pipelines`), so the moment the owning row is deleted the answer
-- flips to "nobody". A new pipeline pointed at the same destination connection
-- and namespace then locks the dead pipeline's schema instead of relocating to
-- rsync_<namespace>, adopts its tables, and a later run_mode=reload — the one
-- destructive destination path — drops them with the customer's data in them.
--
-- The tombstone keeps that answer alive. It is deliberately NOT a foreign key to
-- pipelines: the whole point is that it outlives the row. It is written inside
-- the delete transaction, so the pipeline row and its tombstone can never
-- disagree.
--
-- A stale tombstone is harmless by construction. Relocation requires BOTH an
-- owner AND tables that actually exist on the destination (namespaceProbe.
-- isCollision), so once a user drops the dead pipeline's tables themselves, the
-- next pipeline locks the namespace normally. That is why there is no expiry:
-- the row stops mattering exactly when the data it protects stops existing.

CREATE TABLE IF NOT EXISTS destination_namespace_tombstones (
    id                        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id              UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    -- The deleted pipeline. NOT a foreign key, on purpose — see above.
    pipeline_id               UUID NOT NULL,
    -- Name at delete time, so the relocation notice can say whose data is there
    -- instead of quoting a UUID at the user. May be empty.
    pipeline_name             TEXT NOT NULL DEFAULT '',
    -- The connection is what makes two namespaces the same namespace. If the
    -- destination connection itself is deleted there is nothing left to collide
    -- with, so the tombstone goes with it.
    destination_connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    namespace                 TEXT NOT NULL,
    -- The pipeline's selected_tables verbatim, as a JSON array of strings —
    -- the same shape namespaceTableOwner reads from pipelines.config, so the
    -- live and tombstoned lookups run the identical table-matching code.
    tables                    JSONB NOT NULL DEFAULT '[]'::jsonb,
    deleted_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Re-deleting a pipeline id that was already tombstoned (a restore, or a delete
-- retried after a partial failure) must update the row, not accumulate rows.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dest_ns_tombstone_pipeline_namespace
    ON destination_namespace_tombstones (pipeline_id, destination_connection_id, namespace);

-- The ownership lookup's access path: one namespace on one destination
-- connection within one workspace.
CREATE INDEX IF NOT EXISTS idx_dest_ns_tombstone_lookup
    ON destination_namespace_tombstones (workspace_id, destination_connection_id, namespace);
