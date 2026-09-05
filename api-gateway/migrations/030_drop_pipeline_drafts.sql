-- Migration 030: Remove pipeline_drafts (draft flow removed)

-- Draft pipeline creation flow has been removed from the product.
-- Clean up the DB objects to avoid unused tables and reduce schema surface area.

DROP TRIGGER IF EXISTS trigger_pipeline_drafts_updated_at ON pipeline_drafts;
DROP FUNCTION IF EXISTS update_pipeline_drafts_updated_at();
DROP TABLE IF EXISTS pipeline_drafts CASCADE;

