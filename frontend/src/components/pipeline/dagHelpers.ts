import type { ExecutionPlanStage } from "./DAGVisualization"

// Deleted from this file: `parseRecordCount`, `applyMagnitude`, `formatCount`
// and `extractPiiInfo`. Do not re-add a reader for any of them.
//
// `parseRecordCount` scraped a row count out of `result_summary` prose. Nothing
// writes a count there: the only three values that field ever holds are the
// literals "Plan created", "Plan validated" and "Pipeline executed"
// (`nl_pipeline_v2_workflow.go:1168`, `:1237`, `:1995`), none of which contain a
// digit, so the helper returned null on every stage of every pipeline. The
// TypeScript comment on `ExecutionPlanStage.result_summary` claimed
// `"12,450 rows transferred"`; the Go struct that writes the field documents it
// as `"2-step plan created"` (`types/execution_plan.go:55`). The writer's
// comment was the true one, and the reader was built against the other.
//
// Worse than dead: the scraper was a latent wrong-number generator. Its fallback
// matched any `<digits><k|m|b>` anywhere in the string, so the first backend
// change to write real prose there — "read 2 batches", "5 b-tree scans" — would
// have rendered a confident, wrong "records loaded" figure to the user rather
// than nothing at all. A count must come from a numeric field. There is none on
// this contract: `ExecutionStage` (`types/execution_plan.go:19-57`) has no
// record-count field, and adding one is a cross-service change, filed in
// BACKLOG.md rather than approximated here.
//
// `extractPiiInfo` read five metadata keys — `pii_fields`, `masked_fields`,
// `redacted_fields`, `pii_count`, `pii_fields_masked`. A census across every
// `.go`/`.py`/`.sql`/`.json`/`.yml` file in the repo finds zero writers of any
// of them; its own comment hedged with "shapes the orchestrator/transform *may*
// emit". The masking it purported to report is real — the `mask_pii` transform,
// dispatched at `shared/go/transforms/engine.go:116` into the `applyMask`
// method at `:168` (the package-level `applyMask` at `:913` is a same-named
// single-value helper, not the transform) — but it is already surfaced
// truthfully, per pipeline, on the Transforms tab
// (`PipelineTransformsTab.tsx`), which reads the configured transform list from
// an endpoint instead of a metadata key nobody sets.
//
// `formatCount` had no remaining caller once those two went.

export function isThinkingKind(kind?: string): boolean {
  const k = (kind ?? "").toLowerCase()
  return k === "llm" || k === "planner" || k === "ai" || k === "agent"
}

/**
 * The one place a stage duration is read. Everything on this screen formats
 * milliseconds (`formatDuration(ms)`), but the field only carried them for
 * stages the frontend synthesised itself — the temporal-adapter wrote whole
 * seconds into the same key, so a 42 s stage read "42ms" in the DAG and "0s" in
 * the insights bar. The backend now sends `actual_duration_ms`; this helper
 * keeps already-persisted plans (seconds-only) readable at the same time.
 *
 * Returns `null`, never 0, when the stage was never timed: callers decide
 * whether to render a row on that distinction, and "measured zero" is a
 * different fact from "no measurement".
 */
export function stageDurationMs(stage: {
  actual_duration_ms?: number
  actual_duration?: number
}): number | null {
  if (typeof stage.actual_duration_ms === "number" && Number.isFinite(stage.actual_duration_ms)) {
    return stage.actual_duration_ms
  }
  // Legacy seconds. Only reachable for plan JSON written before the ms field.
  if (typeof stage.actual_duration === "number" && Number.isFinite(stage.actual_duration)) {
    return stage.actual_duration * 1000
  }
  return null
}

// Heuristic: anomaly if duration is > 3x the median across same-kind stages.
// Returns the multiplier (e.g. 4.2) if anomalous, null otherwise.
export function detectDurationAnomaly(
  stage: ExecutionPlanStage,
  allStages: ExecutionPlanStage[],
): number | null {
  const ms = stageDurationMs(stage)
  if (ms === null || ms <= 0) return null
  if (stage.status !== "complete" && stage.status !== "failed") return null

  const peers = allStages.flatMap((s) => {
    if (s.id === stage.id || s.node_kind !== stage.node_kind) return []
    const peerMs = stageDurationMs(s)
    return peerMs !== null && peerMs > 0 ? [peerMs] : []
  })
  if (peers.length < 1) return null

  const sorted = peers.sort((a, b) => a - b)
  const median = sorted[Math.floor(sorted.length / 2)]
  if (!median) return null

  const ratio = ms / median
  return ratio >= 3 ? ratio : null
}

// Extract PII info from stage metadata. Returns count of masked/redacted fields.
export function extractPiiInfo(stage: ExecutionPlanStage): {
  count: number
  fields?: string[]
} | null {
  const meta = stage.metadata ?? {}
  // Common shapes the orchestrator/transform may emit
  const piiFields = (meta.pii_fields ?? meta.masked_fields ?? meta.redacted_fields) as
    | string[]
    | undefined
  const piiCount = (meta.pii_count ?? meta.pii_fields_masked) as number | undefined

  if (Array.isArray(piiFields) && piiFields.length > 0) {
    return { count: piiFields.length, fields: piiFields }
  }
  if (typeof piiCount === "number" && piiCount > 0) {
    return { count: piiCount }
  }
  return null
}
