-- 064_cdc_resources_pipeline_setnull.sql
-- Make cdc_resources rows SURVIVE a pipeline hard-delete so the physical
-- PostgreSQL replication slot (which lives on the remote source DB, NOT in this
-- control-plane table) can still be dropped.
--
-- Bug this fixes: 026 declared
--     pipeline_id UUID REFERENCES pipelines(id) ON DELETE CASCADE
-- and api-gateway DeletePipeline HARD-deletes the pipelines row. The cascade
-- instantly removed the cdc_resources rows, so the (fire-and-forget) CDC cleanup
-- then read GetCDCResources(pipeline_id) → EMPTY → dropped nothing → the actual
-- replication slot leaked on the source server, eventually saturating Azure's
-- max_replication_slots and bricking all PG-source CDC. Cascade cleans the
-- bookkeeping row; it can never drop a remote slot.
--
-- Fix: ON DELETE SET NULL. When a pipeline is deleted the cdc_resources rows
-- remain (now with pipeline_id = NULL), still keyed by connection_id +
-- resource_name, so:
--   * the synchronous pre-delete cleanup (api-gateway) can drop the slot while
--     pipeline_id is still set, and
--   * the CDC reconciler safety-net can reap any slot whose pipeline_id became
--     NULL (orphaned) if cleanup did not run (orchestrator down, network flake).
--
-- The publication branch (also per-pipeline) benefits identically.
-- The companion refcount table (026) keeps its own CASCADE — it is bookkeeping
-- with no remote side effect.

ALTER TABLE cdc_resources DROP CONSTRAINT IF EXISTS cdc_resources_pipeline_id_fkey;

ALTER TABLE cdc_resources
    ADD CONSTRAINT cdc_resources_pipeline_id_fkey
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE SET NULL;

COMMENT ON COLUMN cdc_resources.pipeline_id IS 'Owning pipeline. ON DELETE SET NULL (NOT cascade): a deleted pipeline leaves the row so the remote replication slot/publication can still be dropped by cleanup or the reconciler. NULL also means a shared/orphaned resource.';
