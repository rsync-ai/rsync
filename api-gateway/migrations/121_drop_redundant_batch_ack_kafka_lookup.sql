-- 121: drop idx_batch_acks_kafka_lookup, the redundant twin of unique_batch_ack_kafka
--
-- 054 created both in the same transaction:
--   unique_batch_ack_kafka       (pipeline_id, execution_id, table_name, kafka_topic, kafka_partition, kafka_offset)
--   idx_batch_acks_kafka_lookup  (pipeline_id, execution_id,             kafka_topic, kafka_partition, kafka_offset)
-- The lookup index is the constraint minus table_name, and its header calls it the "fast
-- existence check used by the sink". That check (kafka-sink-worker/main.go:4605) pins all
-- six columns, so the constraint answers it exactly, and it does: 1.96M scans on the
-- constraint against 17 on the lookup index, measured on prod 2026-09-26.
--
-- 098 kept this index on 2026-08-19 on the strength of 1,095 scans. Those scans came from the
-- (pipeline_id, execution_id) reads -- silent_drop_check.go:224, workers/executor.go:710,
-- cdc_counter_ledger.go:57 -- which the planner could serve from any index with that prefix.
-- idx_batch_acks_pipeline_exec (036) is exactly (pipeline_id, execution_id), so those reads
-- keep an index built for them. No statement in the tree filters on kafka_topic /
-- kafka_partition / kafka_offset without table_name.
--
-- What it costs to keep: this table churns (one insert and one delete per batch) and a btree
-- never returns emptied pages to the OS, so every index on it bloats. On 2026-09-26 this one
-- held 281 MB of the table's 657 MB of indexes, over 16 MB of rows. Dropping it removes that
-- space and one index write from every sink insert. It is a plain index, so no guarantee goes
-- with it: the ON CONFLICT target is unique_batch_ack_kafka, which stays.
--
-- DROP INDEX CONCURRENTLY is unavailable: the runner wraps every file without a literal
-- BEGIN; in a transaction, and CONCURRENTLY cannot run inside one. Plain DROP INDEX takes
-- ACCESS EXCLUSIVE for as long as the drop takes, which is a catalog update plus an unlink,
-- not a scan. lock_timeout bounds the wait for the lock, so a long sink transaction makes
-- this fail fast, and a restart of api-gateway re-runs it, rather than queueing every later
-- sink write behind it. Same reasoning, same timeout, as 098; its header covers the /ready
-- consequence of a failure.
--
-- Executed inside the migrator's own transaction -- do NOT add a literal "BEGIN;".

SET LOCAL lock_timeout = '3s';

DROP INDEX IF EXISTS idx_batch_acks_kafka_lookup;
