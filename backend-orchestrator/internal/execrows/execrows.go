// Package execrows holds the one predicate that separates a real pipeline run
// from the synthetic CDC audit anchor, for every query that reads `executions`
// as if each row were a run.
//
// # What the anchor is
//
// CDC has no per-run execution. The kafka-mcp-sink's streaming audit writers
// (the transform-log persister and the CDC-ack persister) need SOMETHING in
// `executions` to point their foreign keys at — transform_execution_logs.
// execution_id (migration 045) and pipeline_batch_acks.fk_batch_acks_execution
// (migration 043) are both hard FKs — so the sink pre-creates a placeholder row
// keyed `id = pipeline_id` (ensureExecutionRowForCDCAudit). It is written once
// per CDC pipeline with status 'running' and no end_time, and it is never
// closed, because there is no run for it to be the end of.
//
// The key is a deliberate, enforced convention, not a coincidence:
// parseCDCMessage forces executionID = pipelineID for every CDC message, and
// real executions are minted with uuid.New(), so `id = pipeline_id` cannot
// collide with a real run.
//
// # Why this predicate exists
//
// api-gateway already excludes the anchor from every user-visible executions
// read, so it never inflates "Total runs", the Running card, or run history
// (internal/handlers/pipelines.go, admin.go — eight sites). The orchestrator's
// sweeps never learned the same thing, and each of them reads an `executions`
// row as a claim about a run:
//
//   - heal's zombie sweep reaps anything 'running' with a null end_time past
//     ZombieTimeout. The anchor matches by construction, so four hours into a
//     perfectly healthy stream it was stamped status='failed' with
//     "zombie: execution timed out with no end_time (healer cleanup)" —
//     permanently, since ensureExecutionRowForCDCAudit is ON CONFLICT DO
//     NOTHING and never reopens it.
//   - heal's candidate sweep then admitted that fabricated failure, and the
//     rule-based diagnoser matched the zombie marker it had just written
//     itself (pkg/diagnose: ActionBackoffRetry, confidence 0.75) — an
//     operator-facing "Suggested: backoff_retry … Approve?" against a stream
//     with nothing wrong with it.
//   - healthwatch's 24h rollup counts completed/failed executions per
//     connector version, so the same reaped anchor scored as a failure for a
//     connector that had not failed.
//   - heal's verifier looks for a NEW execution to grade a heal against; the
//     anchor is older than every heal and is not a run to grade.
//
// The CDC carve-outs already in those files do not cover this. They are keyed
// on pipelines.sync_mode and they guard the PIPELINES row (sweepZombiesQuery's
// pipelines_closed CTE, batch_sentinel's stalledBatchRunsQuery); nothing
// guarded the executions row itself.
//
// # Using it
//
// Every new query that treats an `executions` row as a run must AND in
// NotSynthetic (or IsSynthetic, to select only anchors). Do not hand-write the
// comparison — execrows_test.go in the heal and healthwatch packages scans the
// query text for this exact fragment, and a hand-written copy is how the next
// reader ends up with a fifth near-copy that drifts.
//
// The fragment is written against the alias `e`, which every affected query
// already uses for `executions`. A query that aliases it differently must
// alias it `e` rather than editing the fragment.
package execrows

// NotSynthetic excludes the synthetic CDC audit anchor. AND this into any query
// whose rows are meant to be runs. See the package doc for what the anchor is
// and what each sweep did to it without this.
const NotSynthetic = `e.id <> e.pipeline_id`

// IsSynthetic selects only the anchors — the complement of NotSynthetic, for
// the rare query that wants them (a repair or an audit of the anchors
// themselves) rather than the runs.
const IsSynthetic = `e.id = e.pipeline_id`
