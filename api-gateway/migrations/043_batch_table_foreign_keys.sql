-- Phase 2: Batch table FK constraints
--
-- The batch ACK/outbox tables were originally created with VARCHAR ids.
-- Core tables (`pipelines`, `executions`) use UUID primary keys.
-- This migration:
-- 1) Deletes rows with non-UUID ids (derived/rebuildable data)
-- 2) Converts id columns to UUID
-- 3) Adds FK constraints for integrity

BEGIN;

-- ---------------------------------------------------------------------------
-- Helper: UUID regex (accepts canonical UUID strings)
-- ---------------------------------------------------------------------------
-- NOTE: We use regex prefilter to avoid casting errors during ALTER TYPE.

-- ---------------------------------------------------------------------------
-- pipeline_batch_acks: cleanup + type conversion
-- ---------------------------------------------------------------------------
DELETE FROM pipeline_batch_acks
WHERE pipeline_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
   OR execution_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'pipeline_batch_acks'
      AND column_name = 'pipeline_id'
      AND data_type = 'character varying'
  ) THEN
    ALTER TABLE pipeline_batch_acks
      ALTER COLUMN pipeline_id TYPE uuid USING pipeline_id::uuid;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'pipeline_batch_acks'
      AND column_name = 'execution_id'
      AND data_type = 'character varying'
  ) THEN
    ALTER TABLE pipeline_batch_acks
      ALTER COLUMN execution_id TYPE uuid USING execution_id::uuid;
  END IF;
END $$;

-- Remove orphan rows (best-effort; derived data)
DELETE FROM pipeline_batch_acks a
WHERE NOT EXISTS (SELECT 1 FROM pipelines p WHERE p.id = a.pipeline_id)
   OR NOT EXISTS (SELECT 1 FROM executions e WHERE e.id = a.execution_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fk_batch_acks_pipeline'
  ) THEN
    ALTER TABLE pipeline_batch_acks
      ADD CONSTRAINT fk_batch_acks_pipeline
      FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fk_batch_acks_execution'
  ) THEN
    ALTER TABLE pipeline_batch_acks
      ADD CONSTRAINT fk_batch_acks_execution
      FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE CASCADE;
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- pipeline_batch_outbox: cleanup + type conversion
-- ---------------------------------------------------------------------------
DELETE FROM pipeline_batch_outbox
WHERE pipeline_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
   OR execution_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'pipeline_batch_outbox'
      AND column_name = 'pipeline_id'
      AND data_type = 'character varying'
  ) THEN
    ALTER TABLE pipeline_batch_outbox
      ALTER COLUMN pipeline_id TYPE uuid USING pipeline_id::uuid;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'pipeline_batch_outbox'
      AND column_name = 'execution_id'
      AND data_type = 'character varying'
  ) THEN
    ALTER TABLE pipeline_batch_outbox
      ALTER COLUMN execution_id TYPE uuid USING execution_id::uuid;
  END IF;
END $$;

-- Remove orphan rows (best-effort; derived data)
DELETE FROM pipeline_batch_outbox o
WHERE NOT EXISTS (SELECT 1 FROM pipelines p WHERE p.id = o.pipeline_id)
   OR NOT EXISTS (SELECT 1 FROM executions e WHERE e.id = o.execution_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fk_batch_outbox_pipeline'
  ) THEN
    ALTER TABLE pipeline_batch_outbox
      ADD CONSTRAINT fk_batch_outbox_pipeline
      FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fk_batch_outbox_execution'
  ) THEN
    ALTER TABLE pipeline_batch_outbox
      ADD CONSTRAINT fk_batch_outbox_execution
      FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE CASCADE;
  END IF;
END $$;

COMMIT;

