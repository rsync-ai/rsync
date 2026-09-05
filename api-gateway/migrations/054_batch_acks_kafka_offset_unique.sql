-- Migration: switch pipeline_batch_acks dedup key from batch_offset to kafka_offset.
--
-- Why: the orchestrator's batch_offset is the source-side pagination offset.
-- With keyset/cursor paging it stays at 0 for every batch (executor.go skips
-- the `offset += sourceRowCount` branch when cursor != nil). Every batch's
-- `(pipeline_id, execution_id, table_name, batch_offset)` collides, so
-- ON CONFLICT DO NOTHING accepted only the first batch and silently
-- discarded the rest. kafka_offset is monotonic per partition and never
-- collides, making it the correct idempotency key for sink writes.
--
-- The Redis dedup gate is being removed in the same change. The Postgres
-- ack ledger is now the sole source of truth for "have we already written
-- this Kafka offset to the destination?"

BEGIN;

-- Drop the broken unique constraint (kept in 036).
ALTER TABLE pipeline_batch_acks
    DROP CONSTRAINT IF EXISTS unique_batch_ack;

-- The new key includes the Kafka coordinates which are guaranteed unique
-- per partition+offset. (kafka_topic + kafka_partition + kafka_offset).
-- Including pipeline_id/execution_id/table_name keeps the ledger query
-- pattern unchanged and prevents accidental cross-pipeline collisions
-- on a topic name reuse.
ALTER TABLE pipeline_batch_acks
    ADD CONSTRAINT unique_batch_ack_kafka
    UNIQUE (pipeline_id, execution_id, table_name, kafka_topic, kafka_partition, kafka_offset);

-- Fast existence check used by the sink before each write.
CREATE INDEX IF NOT EXISTS idx_batch_acks_kafka_lookup
    ON pipeline_batch_acks(pipeline_id, execution_id, kafka_topic, kafka_partition, kafka_offset);

COMMIT;
