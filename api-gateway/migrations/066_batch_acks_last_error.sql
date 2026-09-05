-- Migration: Add last_error column to pipeline_batch_acks for NEGATIVE acks.
--
-- Why: when the kafka-mcp-sink exhausts its retries and DLQs a batch, it used
-- to commit the Kafka offset WITHOUT writing any ack row. The executor's
-- landed-row reconciliation then saw (landed=0, ackRows=0) — the AMBIGUOUS
-- "absence of evidence" branch — and reported `unverified_completion` with an
-- EMPTY error, surfacing the literal "Data transfer failed: " in /state. The
-- real failure (e.g. `invalid input syntax for type bigint: "gid://..."`) was
-- lost.
--
-- The fix makes the sink write a NEGATIVE ack on DLQ (rows_written=0 +
-- last_error). The executor then hits the existing, correct
-- (landed=0, ackRows>0) hard-fail path and can surface the sink's real reason.
--
-- Positive (successful-write) acks leave last_error NULL. Additive + nullable,
-- so it is safe to apply online with zero downtime.

ALTER TABLE pipeline_batch_acks ADD COLUMN IF NOT EXISTS last_error TEXT;

COMMENT ON COLUMN pipeline_batch_acks.last_error IS
    'Sink error string recorded on a NEGATIVE ack (rows_written=0) when a batch is DLQ-failed after exhausting retries. NULL on successful writes.';
