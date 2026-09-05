/**
 * Turning a row of `pipeline_run_events` into something an operator can read.
 *
 * The Live events feed used to render `ev.event_type` and `ev.stage_id`
 * verbatim, which meant the most-watched panel on the pipeline page said
 * `STAGE_PROGRESS · executor` sixty times in a row. Everything here exists to
 * answer three questions from data the row already carries:
 *
 *   - is this row worth a line at all?   → isNoiseEvent
 *   - what happened?                     → eventLabel
 *   - what happened *to what*?           → eventDetail
 *
 * Pure functions, no React: the filtering rule in particular is the part most
 * able to do harm, and it should be assertable without a DOM.
 */

import { stageLabel } from "@/lib/pipeline/stageDefinitions"

export interface DisplayEvent {
  event_type: string
  stage_id?: string
  severity?: string
  payload?: Record<string, unknown> | null
}

function asObject(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null
}

function asText(v: unknown): string {
  return typeof v === "string" && v.trim() !== "" ? v.trim() : ""
}

function asFiniteNumber(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v)
    if (Number.isFinite(n)) return n
  }
  return null
}

/** Severities that mean "a human should look at this". */
function isAttentionWorthy(severity: string | undefined): boolean {
  const s = (severity || "").toLowerCase()
  return s === "error" || s === "critical" || s === "fatal" || s === "warn" || s === "warning"
}

/**
 * Event types that repeat on a timer and carry no state change of their own.
 *
 * DATA_PLANE_METRICS is here because the Throughput card sits directly above
 * this feed and shows the very numbers it carries — as a feed row it is a
 * duplicate that arrives every few seconds.
 */
const ROUTINE_EVENT_TYPES = new Set(["DATA_PLANE_METRICS"])

/**
 * True when a row is a routine tick rather than something that happened.
 *
 * `metadata.heartbeat` is not a heuristic: `ProgressEmitter.StartStageHeartbeat`
 * sets it explicitly so the UI can compress these rows
 * (`backend-orchestrator/internal/workers/progress_events.go:98-102`, whose
 * comment reads "noise-controlled in UI"). This is the reader that flag was
 * always waiting for.
 *
 * Severity wins over every other rule. A filter that can swallow an alarm is
 * worse than the noise it removes, so a warn/error row is never routine however
 * it is flagged — including a heartbeat that arrived carrying an error.
 */
export function isNoiseEvent(ev: DisplayEvent): boolean {
  if (isAttentionWorthy(ev.severity)) return false
  if (ROUTINE_EVENT_TYPES.has(ev.event_type)) return true
  const meta = asObject(asObject(ev.payload)?.["metadata"])
  return meta?.["heartbeat"] === true
}

const EVENT_LABELS: Record<string, string> = {
  PIPELINE_STARTED: "Pipeline started",
  PIPELINE_WAITING: "Waiting for you",
  PIPELINE_COMPLETED: "Pipeline completed",
  PIPELINE_FAILED: "Pipeline failed",
  STAGE_STARTED: "Stage started",
  STAGE_PROGRESS: "Stage in progress",
  STAGE_COMPLETED: "Stage completed",
  STAGE_FAILED: "Stage failed",
  DATA_PLANE_METRICS: "Throughput update",
  SENTINEL_ALERT: "Monitoring alert",
  healer_decision: "Self-healing decision",
  healer_verified: "Self-healing outcome",
  healer_backoff_retry: "Self-healing: retrying after a transient failure",
  healer_retry_cap_reached: "Self-healing: retry limit reached",
  healer_refresh_auth: "Self-healing: refreshing credentials",
  healer_regen_connector: "Self-healing: regenerating connector",
  healer_cleanup_cdc_resources: "Self-healing: cleaning up CDC resources",
  healer_cleanup_cdc_skipped: "Self-healing: CDC cleanup skipped",
  healer_cleanup_cdc_failed: "Self-healing: CDC cleanup failed",
  healer_repair_ownership_row: "Self-healing: repaired pipeline ownership",
  healer_repair_ownership_skipped: "Self-healing: ownership repair skipped",
  healer_repair_ownership_failed: "Self-healing: ownership repair failed",
}

/** `SOME_TOKEN` / `some-token` → `Some token`. */
function humanize(token: string): string {
  const words = token.split(/[_\-.]+/).filter(Boolean)
  if (words.length === 0) return token
  const lower = words.map((w) => (w === w.toUpperCase() ? w.toLowerCase() : w)).join(" ")
  return lower.charAt(0).toUpperCase() + lower.slice(1)
}

/**
 * What to call this event in front of a user.
 *
 * Unknown types degrade to readable prose rather than to the token, because new
 * event types get added by services that never look at this file — an unmapped
 * one should read as slightly generic, not as a database dump.
 */
export function eventLabel(ev: DisplayEvent): string {
  const known = EVENT_LABELS[ev.event_type]
  if (known) return known
  if (ev.event_type.startsWith("healer_")) {
    return `Self-healing: ${humanize(ev.event_type.slice("healer_".length)).toLowerCase()}`
  }
  return humanize(ev.event_type)
}

/** The stage's product-wide name, or "" when the row is not stage-scoped. */
export function eventStageLabel(ev: DisplayEvent): string {
  const id = asText(ev.stage_id)
  return id ? stageLabel(id) : ""
}

/**
 * The one line of detail under the label.
 *
 * Ordered by how directly it answers "what is happening to my data". An explicit
 * message from the producer always wins — a service that took the trouble to
 * write a sentence knows more about the event than this file does. Everything
 * below it is derived from payload the row already carried and the card used to
 * throw away.
 */
export function eventDetail(ev: DisplayEvent): string {
  const p = asObject(ev.payload) || {}
  const meta = asObject(p["metadata"]) || {}

  // 1. What the producer said.
  const explicit = asText(p["message"]) || asText(p["summary"]) || asText(p["description"])
  if (explicit) return explicit

  // 2. Why the run is parked. On a waiting pipeline this is the single most
  //    useful string in the whole stream.
  const blocking = asObject(p["blocking_reason"])
  if (blocking) {
    const desc = asText(blocking["description"])
    if (desc) return desc
    const type = asText(blocking["type"])
    if (type) return `Waiting: ${humanize(type).toLowerCase()}`
  }

  // 3. The healer's evidence. Without it the card shows a verdict and no reason.
  const rationale = asText(p["rationale"]) || asText(p["error_message"]) || asText(p["error"])
  if (rationale) return rationale

  // 4. Which table this is about.
  const table =
    asText(meta["qualified_name"]) ||
    asText(p["qualified_name"]) ||
    asText(meta["table_name"]) ||
    asText(p["table_name"]) ||
    asText(meta["table"]) ||
    asText(p["table"])
  if (table) return table

  // 5. Where in the stage we are.
  const progress = asObject(p["progress"]) || {}
  const step = asFiniteNumber(progress["current_step"] ?? meta["current_step"])
  const total = asFiniteNumber(progress["total_steps"] ?? meta["total_steps"])
  if (step !== null && total !== null && total > 0) return `Step ${step} of ${total}`
  const percent = asFiniteNumber(progress["percent"])
  if (percent !== null) return `${Math.round(percent)}% complete`

  // 6. Rows moved — last, because the Throughput card says it better.
  const metrics = asObject(meta["metrics"]) || asObject(p["metrics"]) || {}
  const read = asFiniteNumber(metrics["records_read"] ?? metrics["rows_read"] ?? meta["records_read"])
  const written = asFiniteNumber(
    metrics["records_written"] ?? metrics["rows_written"] ?? meta["records_written"],
  )
  if (read !== null || written !== null) {
    const parts: string[] = []
    if (read !== null) parts.push(`${read.toLocaleString()} rows read`)
    if (written !== null) parts.push(`${written.toLocaleString()} rows written`)
    return parts.join(" · ")
  }

  return ""
}
