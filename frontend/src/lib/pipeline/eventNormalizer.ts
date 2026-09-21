/**
 * EventNormalizer: Maps raw pipeline_run_events into stable UI primitives
 * for ReasoningTimeline, ToolCallLedger, and other agent-native views.
 */

import type { DomainEventData } from "@/lib/api/types"
import { stageTiming } from "@/lib/duration"

// ============================================================================
// 1) Run-history event normalization (pipeline_run_events → UI timeline)
// ============================================================================

export type PipelineRunEvent = {
  pipeline_id: string
  execution_id?: string
  event_id: string
  seq?: number
  event_type: string
  stage_id?: string
  stage_group?: string
  severity?: string
  trace_id?: string
  occurred_at?: string
  received_at: string
  payload: Record<string, any>
}

export type EventSeverity = "info" | "warn" | "error" | "unknown"

export type NormalizedRunEvent = {
  id: string
  seq?: number
  type: string
  timestamp: string
  severity: EventSeverity
  stage?: string
  stageGroup?: string
  traceId?: string
  title: string
  description?: string
  metadata?: Record<string, any>
}

export type StageGroup = {
  id: string
  name: string
  events: NormalizedRunEvent[]
  startedAt?: string
  completedAt?: string
  // "unknown": nothing loaded says where the stage is. A group only exists
  // because it has events, so it is never "pending" — that label was the
  // fallback for a group whose STAGE_STARTED sat on a page not yet loaded.
  status: "unknown" | "running" | "completed" | "failed"
  /** Working time, summed across attempts — not first-start-to-last-end. */
  duration?: number
  /**
   * How many times the stage started. Above 1 the lane says so, because
   * `duration` deliberately leaves out the idle gap between attempts and a
   * reader comparing it against the wall clock would otherwise find it short.
   */
  attempts?: number
}

export type DecisionCard = {
  id: string
  timestamp: string
  summary: string
  rationale?: string
  confidence?: number
  alternatives?: Array<{ option: string; rejected_reason: string }>
  dataSources?: Array<{ type: string; ref: string }>
  reversibility?: { reversible: boolean; risk: string }
}

export type ToolCall = {
  id: string
  toolName: string
  startedAt?: string
  completedAt?: string
  duration?: number
  status: "started" | "completed" | "failed" | "cancelled"
  attempt?: number
  maxAttempts?: number
  retryReason?: string
  inputs?: Record<string, any>
  outputs?: Record<string, any>
  error?: string
}

export type SelfHealEvent = {
  id: string
  timestamp: string
  trigger: string
  action: string
  outcome: "success" | "failed"
  reversibility?: string
}

// ---------------------------------------------------------------------------
// Self-healing (healer_*) events
// ---------------------------------------------------------------------------
//
// Every event the heal agent writes is `healer_`-prefixed and lower_snake_case
// (backend-orchestrator/internal/agents/heal — worker.go, executors.go,
// auto_executors.go). Two shapes matter:
//
//   healer_decision  — one per swept failure, ALWAYS written, including for
//                      escalate/no-op outcomes where no executor ran. Carries
//                      the diagnosis, the evidence it was drawn from, and the
//                      attempt_id.
//   healer_verified  — written minutes later by the verify loop with the
//                      outcome of the attempt. Joins back on attempt_id.
//
// Everything else is an action marker written by an individual executor.
//
// Two properties of these rows drive the UI that consumes them:
//   1. They have NO stage_id and NO seq. Any grouping keyed on stage silently
//      drops them, and `ORDER BY seq DESC NULLS LAST` sorts them to the very
//      end of an unfiltered page — which is why the panel queries them by
//      event_type instead of filtering a general fetch client-side.
//   2. They never reach the WebSocket. The healer writes straight to Postgres
//      with no Kafka producer, so a healer view must poll; the socket will
//      never wake it.

/**
 * The explicit list for the API's `?event_types=` allowlist filter. The prefix
 * test below is what classifies a row once it has arrived — keep this list in
 * sync when an executor adds an event, or its rows simply won't be fetched.
 */
export const HEALER_EVENT_TYPES = [
  "healer_decision",
  "healer_verified",
  "healer_backoff_retry",
  "healer_refresh_auth",
  "healer_regen_connector",
  "healer_request_config",
  "healer_retry_cap_reached",
  "healer_cleanup_cdc_resources",
  "healer_cleanup_cdc_failed",
  "healer_cleanup_cdc_skipped",
  "healer_repair_ownership_row",
  "healer_repair_ownership_failed",
  "healer_repair_ownership_skipped",
  "healer_zombie_swept",
] as const

export function isHealerEvent(eventType: string): boolean {
  return String(eventType || "").toLowerCase().startsWith("healer_")
}

/** What the healer decided, and — once the verify loop grades it — whether it worked. */
export type HealerActivity = {
  id: string
  timestamp: string
  executionId?: string
  /** "decision" | "verdict" | "action" */
  kind: "decision" | "verdict" | "action"
  eventType: string
  severity: EventSeverity

  // decision
  category?: string
  action?: string
  confidence?: number
  rationale?: string
  outcome?: string
  actionExecuted?: boolean
  hitlPrompt?: string
  /** Normalised failure identity — safe to display, groups repeat occurrences. */
  failureSignature?: string
  /** The raw failure the healer was reacting to (truncated server-side). */
  errorMessage?: string
  executorStatus?: string
  /** Why confidence differs from the rule's own — set when the ledger downgraded it. */
  memoryNote?: string
  /** Joins a decision to the verdict written for it later. 0/undefined when unrecorded. */
  attemptId?: number

  // verdict
  verdict?: string
  attemptNo?: number
  successorExecutionId?: string

  // action markers
  description?: string
}

function num(v: unknown): number | undefined {
  const n = typeof v === "number" ? v : Number(v)
  return Number.isFinite(n) ? n : undefined
}

function str(v: unknown): string | undefined {
  const s = typeof v === "string" ? v.trim() : ""
  return s === "" ? undefined : s
}

/**
 * Whether a healer row represents something that actually worked.
 *
 * Deliberately strict. `hitl_requested` is the healer asking a human, and
 * `inconclusive` is the verifier declining to claim credit — reporting either as
 * a success is how a self-healing UI ends up quietly overstating itself, which
 * is worse than showing nothing. Only an executed action or a "healed" verdict
 * counts.
 */
export function healSucceeded(a: HealerActivity): boolean {
  if (a.kind === "verdict") return a.verdict === "healed"
  if (a.kind === "decision") return a.outcome === "auto_executed" && a.actionExecuted === true
  // Action markers are written at the point of doing the thing; the ones that
  // record a non-event say so in their type.
  return !/_(failed|skipped)$/.test(a.eventType)
}

/**
 * Extract every self-healing event, newest first.
 *
 * Deliberately tolerant: an event whose payload is missing the fields this
 * release writes still yields a row with its type and timestamp. The healer has
 * been writing decision events for longer than it has been recording the
 * evidence behind them, so a run history contains both shapes and the older one
 * must not render as a blank.
 */
export function extractHealerActivity(events: PipelineRunEvent[]): HealerActivity[] {
  const out: HealerActivity[] = []

  for (const e of events) {
    if (!isHealerEvent(e.event_type)) continue
    const p = e.payload || {}
    const base = {
      id: e.event_id,
      timestamp: e.occurred_at || e.received_at,
      executionId: e.execution_id,
      eventType: e.event_type,
      severity: normalizeSeverity(e.severity),
    }

    if (e.event_type === "healer_decision") {
      out.push({
        ...base,
        kind: "decision",
        category: str(p.category),
        action: str(p.suggested_action),
        confidence: num(p.confidence),
        rationale: str(p.rationale),
        outcome: str(p.outcome),
        actionExecuted: p.action_executed === true,
        hitlPrompt: str(p.hitl_prompt),
        failureSignature: str(p.failure_signature),
        // `error` is the EXECUTOR's failure and is only set when an action threw;
        // `error_message` is the failure being diagnosed. Prefer the latter.
        errorMessage: str(p.error_message) || str(p.error),
        executorStatus: str(p.executor_status),
        memoryNote: str(p.memory_note),
        attemptId: num(p.attempt_id),
      })
    } else if (e.event_type === "healer_verified") {
      out.push({
        ...base,
        kind: "verdict",
        verdict: str(p.verdict),
        action: str(p.action),
        outcome: str(p.outcome),
        attemptId: num(p.attempt_id),
        attemptNo: num(p.attempt_no),
        failureSignature: str(p.failure_signature),
        successorExecutionId: str(p.successor_execution_id),
      })
    } else {
      out.push({
        ...base,
        kind: "action",
        action: str(p.healer_action) || e.event_type.replace(/^healer_/, ""),
        description: str(p.description),
      })
    }
  }

  return out.sort(
    (a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
  )
}

// ---------------------------------------------------------------------------
// Stage lifecycle de-duplication (Monitoring -> Trace)
// ---------------------------------------------------------------------------
//
// Every stage lifecycle event has two producers:
//   - the Temporal workflow (`emitStageEvent`, nl_pipeline_v2_workflow.go), stage
//     ids `capability_resolver` / `connection_validation`, trace_id = execution id;
//   - the orchestrator worker that runs the stage (`ProgressEmitter.EmitProgress`,
//     workers/progress_events.go), stage ids `resolver` / `connection_validator`,
//     trace_id = the worker's OTel trace.
// Both land in pipeline_run_events with different event_ids (the projector hashes
// the raw bytes when no event_id is sent), so the Trace tab showed every step
// twice under two trace ids. Both writers are load-bearing (the workflow's copy
// carries the execution plan, the worker's copy carries the human summary), so
// the timeline collapses the pair instead of either producer going quiet.

const STAGE_LIFECYCLE_TYPES = new Set(["STAGE_STARTED", "STAGE_COMPLETED", "STAGE_FAILED"])

/**
 * The transitions a stage group's status and duration are read from. The feed is
 * paged newest-first, so a long-running stage's STAGE_STARTED/STAGE_COMPLETED is
 * usually on a page nobody has loaded; the panel fetches these types on their
 * own (`event_types=`) and hands them to `groupByStage`, so the badge says the
 * same thing before and after "Load More".
 */
export const STATUS_EVENT_TYPES = [
  "STAGE_STARTED",
  "STAGE_COMPLETED",
  "STAGE_FAILED",
  "PIPELINE_COMPLETED",
  "PIPELINE_FAILED",
  "PIPELINE_WAITING",
] as const
const STATUS_EVENT_TYPE_SET = new Set<string>(STATUS_EVENT_TYPES)

// Two copies of one transition are emitted within moments of each other; a real
// retry of the same stage is separated by a different transition (FAILED ->
// STARTED), so it is never collapsed by the "same type as the last kept" test.
const LIFECYCLE_DUPLICATE_WINDOW_MS = 120_000

const CANONICAL_STAGE: Record<string, string> = {
  resolver: "capability_resolver",
  connection_validator: "connection_validation",
}

// Mirrors stageGroupForStage (temporal-adapter) / stageGroupFor (orchestrator)
// for the stages both know. Used when a row carries a stage_id but no
// stage_group — the workflow's PIPELINE_WAITING event (table selection) did, so
// it formed its own "Executor" group with no lifecycle events and sat at
// "Pending" after the tables were chosen.
const STAGE_GROUP_FOR_STAGE: Record<string, string> = {
  intent: "understanding",
  capability_resolver: "connecting",
  resolver: "connecting",
  connection_validator: "connecting",
  connection_validation: "connecting",
  connector_check: "connecting",
  connector_generation: "connecting",
  discovery: "discovering",
  planner: "planning",
  infra_preflight: "infra_preflight",
  validator: "validating",
  policy_check: "validating",
  cost_estimation: "validating",
  schema_validation: "validating",
  executor: "executing",
}

export function canonicalStageId(stage?: string): string {
  const s = String(stage || "").trim().toLowerCase()
  return CANONICAL_STAGE[s] || s
}

// Stages that are their own group whatever group the row carries. Both
// backends filed infra_preflight under their default "planning" group until
// 2026-09-19, so stored rows showed the preflight as a second "Planning".
const OWN_GROUP_STAGES = new Set(["infra_preflight"])

// Group keys whose prettified id is jargon. "ungrouped" is the bucket for rows
// that name no stage (PIPELINE_CREATED and friends); the Activity tab showed it
// to users as "Ungrouped".
const STAGE_GROUP_LABEL: Record<string, string> = {
  ungrouped: "Other events",
}

export function stageGroupKey(event: Pick<PipelineRunEvent, "stage_group" | "stage_id">): string {
  const s = String(event.stage_id || "").trim().toLowerCase()
  if (OWN_GROUP_STAGES.has(s)) return s
  if (event.stage_group) return event.stage_group
  return STAGE_GROUP_FOR_STAGE[s] || event.stage_id || "ungrouped"
}

/**
 * The fields duplicate-collapsing reads. Declared apart from `PipelineRunEvent`
 * because the Overview panel carries its own, looser row type for the same API
 * rows; a dedupe that only the feed's type could call is a dedupe half the
 * panels cannot use, which is how the Overview came to time stages from raw
 * events while the feed timed them from collapsed ones.
 */
export type StageLifecycleEvent = {
  event_type: string
  stage_id?: string
  execution_id?: string
  seq?: number
  occurred_at?: string
  received_at?: string
  payload?: Record<string, any>
}

function eventTime(e: StageLifecycleEvent): number {
  const t = new Date(e.occurred_at || e.received_at || "").getTime()
  return Number.isFinite(t) ? t : 0
}

function eventTimestamp(e?: PipelineRunEvent): string | undefined {
  return e ? e.occurred_at || e.received_at : undefined
}

/** The earlier (`Math.min`) or later (`Math.max`) of two timestamps; unreadable ones don't count. */
function pickTime(
  a: string | undefined,
  b: string | undefined,
  pick: (x: number, y: number) => number,
): string | undefined {
  const ta = a ? new Date(a).getTime() : NaN
  const tb = b ? new Date(b).getTime() : NaN
  if (!Number.isFinite(ta)) return Number.isFinite(tb) ? b : a ?? b
  if (!Number.isFinite(tb)) return a
  return pick(ta, tb) === ta ? a : b
}

/** Chronological order with deterministic tie-breaks (seq, then received_at). */
export function compareRunEventsAsc(a: StageLifecycleEvent, b: StageLifecycleEvent): number {
  const dt = eventTime(a) - eventTime(b)
  if (dt !== 0) return dt
  const sa = typeof a.seq === "number" ? a.seq : Number.MAX_SAFE_INTEGER
  const sb = typeof b.seq === "number" ? b.seq : Number.MAX_SAFE_INTEGER
  if (sa !== sb) return sa - sb
  const ra = new Date(a.received_at || "").getTime() || 0
  const rb = new Date(b.received_at || "").getTime() || 0
  return ra - rb
}

function hasHumanTitle(e: StageLifecycleEvent): boolean {
  return Boolean(e.payload?.summary || e.payload?.message || e.payload?.stage_summary)
}

/**
 * Collapse the two producers' copies of one stage transition into one row,
 * returning events in chronological order. Non-lifecycle events pass through.
 */
export function dedupeStageLifecycleEvents<T extends StageLifecycleEvent>(events: T[]): T[] {
  const sorted = [...events].sort(compareRunEventsAsc)
  const out: T[] = []
  const lastKept = new Map<string, { index: number; type: string; at: number }>()
  let retimed = false
  for (const e of sorted) {
    const type = String(e.event_type || "").toUpperCase()
    if (!STAGE_LIFECYCLE_TYPES.has(type) || !e.stage_id) {
      out.push(e)
      continue
    }
    const key = `${e.execution_id || ""}|${canonicalStageId(e.stage_id)}`
    const prev = lastKept.get(key)
    const at = eventTime(e)
    if (prev && prev.type === type && Math.abs(at - prev.at) <= LIFECYCLE_DUPLICATE_WINDOW_MS) {
      const kept = out[prev.index]
      // Keep whichever copy carries the human-readable summary, timed from the
      // first report of a start and the last report of an end. The producers
      // report one transition seconds apart; taking the earlier end timed
      // Executing at 33.3s where the Overview (last completion) said 36.5s.
      const base = !hasHumanTitle(kept) && hasHumanTitle(e) ? e : kept
      const occurredAt =
        type === "STAGE_STARTED" ? kept.occurred_at || e.occurred_at : e.occurred_at || kept.occurred_at
      if (base !== kept || occurredAt !== kept.occurred_at) {
        out[prev.index] = { ...base, occurred_at: occurredAt }
        retimed = true
      }
      continue
    }
    lastKept.set(key, { index: out.length, type, at })
    out.push(e)
  }
  // A later end can move a row past the ones after it; restore the order.
  return retimed ? out.sort(compareRunEventsAsc) : out
}

function normalizeSeverity(raw?: string): EventSeverity {
  const s = String(raw || "").toLowerCase()
  if (s === "error" || s === "critical" || s === "fatal") return "error"
  if (s === "warn" || s === "warning") return "warn"
  if (s === "info") return "info"
  return "unknown"
}

/**
 * The row's severity, falling back to a sentinel alert's own level.
 *
 * SENTINEL_ALERT carries its level in `status` ("warning", "error", "critical"
 * — `cdc_wal_watchdog.go`, `batch_sentinel.go`) and no top-level `severity`, and
 * the projector fills the column only from `severity` (`event_projector.go`
 * storeRunEvent). The column is therefore empty on every alert row, which
 * rendered a WAL-retention alarm as a green check. Scoped to SENTINEL_ALERT:
 * on other rows `status` is a lifecycle word, not a level.
 */
function rowSeverity(event: PipelineRunEvent): EventSeverity {
  if (event.severity) return normalizeSeverity(event.severity)
  if (event.event_type === "SENTINEL_ALERT") return normalizeSeverity(event.payload?.status)
  return normalizeSeverity(undefined)
}

/**
 * Where a stage is, read from its latest transition, and how long it took.
 *
 * The latest transition decides, not "any failure anywhere": the feed mixes
 * every run's rows, and a stage that failed and was retried is not failed. Error
 * rows keep their own red mark; the badge only answers where the stage is. The
 * old rule also read error rows, which made the badge change with the pages
 * loaded, since an older page can hold an error the newest one does not.
 */
function stageState(
  transitions: PipelineRunEvent[], // chronological, one copy per transition
  latestEventAt: number,
): { status: StageGroup["status"]; duration?: number; attempts?: number } {
  const last = transitions[transitions.length - 1]
  if (!last) return { status: "unknown" }
  const lastAt = eventTime(last)

  // The duration is every attempt's own span added up, from `stageTiming` — the
  // same reduction the Overview's stage list runs. This walked back to the LAST
  // STAGE_STARTED and measured only from there, so a stage that ran twice
  // reported its second attempt and silently dropped the first: on the live demo
  // pipeline `infra_preflight` ran 15:58:29→15:58:46 and 16:13:40→16:13:55, and
  // read "15.0s" here against the Overview's "15m 26s".
  const timing = stageTiming(
    transitions.map((t) => ({ type: String(t.event_type || ""), at: eventTime(t) })),
    latestEventAt,
  )

  switch (String(last.event_type || "").toUpperCase()) {
    case "STAGE_FAILED":
    case "PIPELINE_FAILED":
      return { status: "failed", duration: timing.activeMs, attempts: timing.attempts }
    case "STAGE_COMPLETED":
    case "PIPELINE_COMPLETED":
      return { status: "completed", duration: timing.activeMs, attempts: timing.attempts }
    case "STAGE_STARTED":
      return { status: "running", duration: timing.activeMs, attempts: timing.attempts }
    default:
      // PIPELINE_WAITING. A wait with nothing after it anywhere in the run is
      // still open; once any later event exists the user answered it (e.g.
      // tables selected). When the stage ended after that is not recorded.
      return latestEventAt > lastAt
        ? { status: "completed", attempts: timing.attempts }
        : { status: "running", duration: timing.activeMs, attempts: timing.attempts }
  }
}

export class EventNormalizer {
  /**
   * Normalize a raw event into a consistent UI model
   */
  static normalize(event: PipelineRunEvent): NormalizedRunEvent {
    const title = this.extractTitle(event)
    const description = this.extractDescription(event)

    return {
      id: event.event_id,
      seq: event.seq,
      type: event.event_type,
      timestamp: event.occurred_at || event.received_at,
      severity: rowSeverity(event),
      stage: event.stage_id,
      stageGroup: event.stage_group,
      traceId: event.trace_id,
      title,
      description,
      metadata: event.payload,
    }
  }

  /**
   * Group events by stage for timeline view.
   *
   * `statusEvents` are lifecycle transitions fetched apart from the paged feed
   * (STATUS_EVENT_TYPES). They add no rows and no groups; they only decide each
   * group's status and duration, which is what keeps both from changing when
   * "Load More" pulls in an older page.
   */
  static groupByStage(events: PipelineRunEvent[], statusEvents: PipelineRunEvent[] = []): StageGroup[] {
    const groupMap = new Map<string, PipelineRunEvent[]>()
    const deduped = dedupeStageLifecycleEvents(events)

    // Each group's transitions: the loaded rows' own plus the fetched ones, once each.
    const seen = new Set<string>()
    const transitions: PipelineRunEvent[] = []
    for (const e of [...events, ...statusEvents]) {
      if (!STATUS_EVENT_TYPE_SET.has(String(e.event_type || "").toUpperCase())) continue
      if (e.event_id) {
        if (seen.has(e.event_id)) continue
        seen.add(e.event_id)
      }
      transitions.push(e)
    }
    const transitionsByGroup = new Map<string, PipelineRunEvent[]>()
    for (const e of dedupeStageLifecycleEvents(transitions)) {
      const key = stageGroupKey(e)
      if (!transitionsByGroup.has(key)) transitionsByGroup.set(key, [])
      transitionsByGroup.get(key)!.push(e)
    }

    // The newest page is always loaded, so this is the same on every page count.
    let latestEventAt = deduped.length ? eventTime(deduped[deduped.length - 1]) : 0
    for (const e of transitions) latestEventAt = Math.max(latestEventAt, eventTime(e))

    for (const event of deduped) {
      // Prefer stage_group because it's stable across event producers.
      // stage_id can be missing or vary (agent name vs plan stage id), which
      // fragments groups, so a bare stage_id is mapped to its group first.
      const key = stageGroupKey(event)
      if (!groupMap.has(key)) {
        groupMap.set(key, [])
      }
      groupMap.get(key)!.push(event)
    }

    const groups: StageGroup[] = []
    for (const [key, stageEvents] of groupMap.entries()) {
      // `deduped` is already chronological with tie-breaks; keep that order.
      const sortedByTime = stageEvents.map((e) => this.normalize(e))

      // The span is what orders the lanes, so — like the badge and the duration
      // — it must not depend on which pages are loaded. The newest page of a
      // streaming run holds rows from long after a stage began, so take the
      // group's transitions into account: they are fetched whole, apart from
      // the feed. Without them the loaded rows are all there is.
      const groupTransitions = transitionsByGroup.get(key) || []
      const firstTransition = groupTransitions[0]
      const lastTransition = groupTransitions[groupTransitions.length - 1]
      const startedAt = pickTime(sortedByTime[0]?.timestamp, eventTimestamp(firstTransition), Math.min)
      const completedAt = pickTime(
        sortedByTime[sortedByTime.length - 1]?.timestamp,
        eventTimestamp(lastTransition),
        Math.max,
      )
      const { status, duration: stageDuration, attempts } = stageState(groupTransitions, latestEventAt)

      // With no transition to measure from, the span of the loaded rows is the
      // only duration there is.
      const duration = status !== "unknown"
        ? stageDuration
        : startedAt && completedAt
          ? new Date(completedAt).getTime() - new Date(startedAt).getTime()
          : undefined

      groups.push({
        id: key,
        name: this.prettifyStageId(key),
        events: sortedByTime,
        startedAt,
        completedAt,
        status,
        duration,
        attempts,
      })
    }

    // A lane whose start is missing or unreadable sorts last rather than first:
    // `NaN - NaN` reads as a tie and would leave it wherever it happened to land.
    const laneStart = (g: StageGroup) => {
      const t = g.startedAt ? new Date(g.startedAt).getTime() : NaN
      return Number.isFinite(t) ? t : Number.POSITIVE_INFINITY
    }
    return groups.sort((a, b) => laneStart(a) - laneStart(b))
  }

  /**
   * Extract decision cards from events (best-effort)
   */
  static extractDecisions(events: PipelineRunEvent[]): DecisionCard[] {
  return events
      .filter((e) => e.event_type === "AGENT_DECISION" || e.payload?.decision || e.payload?.rationale)
      .map((e) => ({
        id: e.event_id,
        timestamp: e.occurred_at || e.received_at,
        summary: e.payload?.summary || e.payload?.decision || "Decision made",
        rationale: e.payload?.rationale,
        confidence: e.payload?.confidence,
        alternatives: e.payload?.alternatives_considered,
        dataSources: e.payload?.data_sources,
        reversibility: e.payload?.reversibility,
      }))
  }

  /**
   * Extract tool calls from events (best-effort)
   */
  static extractToolCalls(events: PipelineRunEvent[]): ToolCall[] {
    const calls: ToolCall[] = []
    const callMap = new Map<string, Partial<ToolCall>>()

    for (const event of events) {
      if (event.event_type === "TOOL_CALL_STARTED") {
        const callId = event.payload?.call_id || event.event_id
        callMap.set(callId, {
          id: callId,
          toolName: event.payload?.tool_name || "unknown",
          startedAt: event.occurred_at || event.received_at,
          status: "started",
          inputs: event.payload?.inputs,
        })
      } else if (event.event_type === "TOOL_CALL_COMPLETED") {
        const callId = event.payload?.call_id || event.event_id
        const existing = callMap.get(callId) || { id: callId }
        
        const status = event.payload?.success === false || event.payload?.error 
          ? "failed" 
          : "completed"
        
        callMap.set(callId, {
          ...existing,
          completedAt: event.occurred_at || event.received_at,
          status,
          outputs: event.payload?.outputs,
          error: event.payload?.error,
          retryReason: event.payload?.retry_reason,
          attempt: event.payload?.attempt,
          maxAttempts: event.payload?.max_attempts,
        })
      } else if (event.event_type === "TOOL_CALL_CANCELLED") {
        const callId = event.payload?.call_id || event.event_id
        const existing = callMap.get(callId) || { id: callId }
        callMap.set(callId, {
          ...existing,
          completedAt: event.occurred_at || event.received_at,
          status: "cancelled",
        })
      }
    }

    for (const [_, call] of callMap.entries()) {
      if (call.id) {
        const duration = call.startedAt && call.completedAt
          ? new Date(call.completedAt).getTime() - new Date(call.startedAt).getTime()
          : undefined
        
        calls.push({
          id: call.id,
          toolName: call.toolName || "unknown",
          startedAt: call.startedAt,
          completedAt: call.completedAt,
          duration,
          status: call.status || "started",
          attempt: call.attempt,
          maxAttempts: call.maxAttempts,
          retryReason: call.retryReason,
          inputs: call.inputs,
          outputs: call.outputs,
          error: call.error,
        })
      }
    }

    return calls.sort((a, b) => {
      const aTime = a.startedAt ? new Date(a.startedAt).getTime() : 0
      const bTime = b.startedAt ? new Date(b.startedAt).getTime() : 0
      return aTime - bTime
    })
  }

  /**
   * Extract self-healing events.
   *
   * This used to filter on `event_type === "SELF_HEALED"` or a `self_heal`
   * payload key. Nothing in the system has ever written either — the heal agent
   * emits lower_snake_case `healer_*` types (see HEALER_EVENT_TYPES) — so this
   * returned [] for every run ever recorded, and the "N self-healing action(s)
   * detected" card its only caller renders never appeared in production.
   *
   * The two legacy shapes are still accepted so nothing regresses if a producer
   * for them is ever added.
   *
   * This is the flat summary shape. For the decision → verdict detail an
   * operator needs, use extractHealerActivity.
   */
  static extractSelfHeals(events: PipelineRunEvent[]): SelfHealEvent[] {
    return extractHealerActivity(events)
      .filter((a) => a.kind !== "verdict" || a.verdict !== undefined)
      .map((a) => ({
        id: a.id,
        timestamp: a.timestamp,
        trigger: a.failureSignature || a.category || a.executorStatus || "unknown",
        action: a.action || a.description || "recovery attempted",
        outcome: (healSucceeded(a) ? "success" : "failed") as SelfHealEvent["outcome"],
      }))
      .concat(
        // Legacy shapes — kept so a future producer of either is picked up.
        events
          .filter((e) => e.event_type === "SELF_HEALED" || e.payload?.self_heal)
          .map((e) => ({
            id: e.event_id,
            timestamp: e.occurred_at || e.received_at,
            trigger: e.payload?.trigger || "unknown",
            action: e.payload?.action || e.payload?.self_heal || "recovery attempted",
            outcome: e.payload?.outcome === "failed" ? "failed" : "success",
            reversibility: e.payload?.reversibility,
          }))
      )
  }

  /**
   * Infer failures/retries from event sequences (best-effort)
   */
  static inferRetries(events: PipelineRunEvent[]): Array<{ stage: string; retries: number; lastError?: string }> {
    const retryMap = new Map<string, { count: number; lastError?: string }>()

    for (const event of events) {
      if (event.severity === "error" || event.payload?.error || event.payload?.retry) {
        const key = event.stage_id || "unknown"
        const existing = retryMap.get(key) || { count: 0 }
        retryMap.set(key, {
          count: existing.count + 1,
          lastError: event.payload?.error || event.payload?.message || existing.lastError,
        })
      }
    }

    return Array.from(retryMap.entries()).map(([stage, data]) => ({
      stage,
      retries: data.count,
      lastError: data.lastError,
    }))
  }

  // Helper functions
  private static extractTitle(event: PipelineRunEvent): string {
    return (
      event.payload?.summary ||
      event.payload?.message ||
      event.payload?.stage_summary ||
      event.payload?.action ||
      event.event_type ||
      "Event"
    )
  }

  private static extractDescription(event: PipelineRunEvent): string | undefined {
    return (
      event.payload?.description ||
      event.payload?.detail ||
      event.payload?.rationale ||
      undefined
    )
  }

  private static prettifyStageId(id: string): string {
    if (STAGE_GROUP_LABEL[id]) return STAGE_GROUP_LABEL[id]
    return id
      .replace(/_/g, " ")
      .replace(/\b\w/g, (char) => char.toUpperCase())
  }
}

// ============================================================================
// 2) Live UI pipeline-state normalization (WebSocket → reducer actions)
//
// IMPORTANT: This is used by `usePipelineState()` and MUST remain compatible
// with `pipelineStateReducer.ts`.
// ============================================================================

export type ExecutionState = "idle" | "running" | "waiting_for_user" | "completed" | "failed" | "cancelled"
export type StageStatus = "pending" | "running" | "retrying" | "waiting" | "completed" | "failed" | "cancelled"

export type NormalizedEvent =
  | { type: "PIPELINE_STARTED"; payload: { pipelineId: string; executionId: string; timestamp: number } }
  | { type: "PIPELINE_RESET"; payload: { timestamp: number } }
  | { type: "STAGE_UPDATE"; payload: { stage: string; status: StageStatus; progress?: number; message?: string; attempt?: number; timestamp: number } }
  | { type: "EXECUTION_STATE_CHANGE"; payload: { executionState: ExecutionState; timestamp: number } }
  | { type: "HITL_REQUIRED"; payload: { stage: string; blockingReason: any; timestamp: number } }
  | { type: "HITL_RESOLVED"; payload: { timestamp: number } }
  | { type: "ARTIFACTS_MERGED"; payload: { artifacts: Record<string, any>; timestamp: number } }
  | { type: "PIPELINE_COMPLETED"; payload: { summary: any; timestamp: number } }
  | { type: "PIPELINE_FAILED"; payload: { stage?: string; error: string; timestamp: number } }
  | { type: "METADATA_UPDATE"; payload: { metadata: Record<string, any>; timestamp: number } }

function toMillis(ts: any): number {
  if (typeof ts === "number") return ts
  if (typeof ts === "string") {
    const t = new Date(ts).getTime()
    if (Number.isFinite(t)) return t
  }
  return Date.now()
}

function mapStageStatus(eventType: string, state: string, status: string): StageStatus {
  const et = String(eventType || "").toUpperCase()
  const st = String(state || "").toLowerCase()

  if (et === "STAGE_STARTED") return "running"
  if (et === "STAGE_PROGRESS") return st === "retrying" ? "retrying" : "running"
  if (et === "STAGE_COMPLETED") return "completed"
  if (et === "STAGE_FAILED") return "failed"
  if (et === "STAGE_CANCELLED" || et === "STAGE_CANCELED") return "cancelled"

  // Fallback based on state/status strings
  if (st === "waiting") return "waiting"
  if (st === "running") return "running"
  if (st === "failed") return "failed"
  if (st === "cancelled" || st === "canceled" || st === "stopped") return "cancelled"
  if (st === "succeeded" || st === "success" || st === "completed") return "completed"

  const ps = String(status || "").toLowerCase()
  if (ps === "waiting_for_user") return "waiting"
  if (ps === "processing") return "running"
  if (ps === "failed") return "failed"
  if (ps === "completed") return "completed"
  if (ps === "cancelled" || ps === "canceled" || ps === "stopped") return "cancelled"

  return "pending"
}

function mapExecutionState(status: string, eventType: string): ExecutionState | null {
  const s = String(status || "").toLowerCase()
  const et = String(eventType || "").toUpperCase()

  if (et === "PIPELINE_COMPLETED") return "completed"
  if (et === "PIPELINE_FAILED") return "failed"
  if (et === "PIPELINE_WAITING") return "waiting_for_user"
  if (et === "PIPELINE_CANCELLED" || et === "PIPELINE_CANCELED" || et === "PIPELINE_STOPPED") return "cancelled"

  if (s === "completed") return "completed"
  if (s === "failed") return "failed"
  if (s === "waiting_for_user") return "waiting_for_user"
  if (s === "processing") return "running"
  if (s === "cancelled" || s === "canceled" || s === "stopped") return "cancelled"

  return null
}

function extractDomainData(rawEvent: unknown): DomainEventData | null {
  if (!rawEvent || typeof rawEvent !== "object") return null
  const ev = rawEvent as Record<string, unknown>
  // WebSocketEvent shape: { type, timestamp, data, trace_id? }
  if (ev["data"] && ev["type"] && typeof ev["data"] === "object") {
    return ev["data"] as DomainEventData
  }
  // Legacy agent messages may use `result`
  if (ev["result"] && typeof ev["result"] === "object") {
    return ev["result"] as DomainEventData
  }
  return ev as DomainEventData
}

/**
 * normalizeWebSocketEvent converts WS payloads into reducer actions.
 * It is intentionally tolerant: missing fields produce best-effort updates
 * rather than throwing (demo-safety).
 */
export function normalizeWebSocketEvent(rawEvent: unknown): NormalizedEvent[] {
  const data = extractDomainData(rawEvent)
  if (!data) return []

  const raw = rawEvent as Record<string, unknown> | null

  const eventType = String(data.event_type || data.eventType || data.type || "").toUpperCase()
  const status = String(data.status || "")
  const timestamp = toMillis(data.timestamp || raw?.["timestamp"] || Date.now())

  const pipelineId = String(data.pipeline_id || raw?.["pipeline_id"] || "")
  const executionId = String(data.execution_id || data.workflow_id || "")

  const stage = String(data.stage || data.progress?.stage || data.current_stage || "unknown")
  const stageState = String(data.state || "")

  const out: NormalizedEvent[] = []

  // Keep metadata hydrated (safe; reducer merges it)
  if (pipelineId || executionId) {
    out.push({
      type: "METADATA_UPDATE",
      payload: {
        metadata: {
          pipelineId: pipelineId || null,
          executionId: executionId || null,
        },
        timestamp,
      },
    })
  }

  const execState = mapExecutionState(status, eventType)
  if (execState) {
    out.push({ type: "EXECUTION_STATE_CHANGE", payload: { executionState: execState, timestamp } })
  }

  // HITL waiting
  if (eventType === "PIPELINE_WAITING") {
    out.push({
      type: "HITL_REQUIRED",
      payload: {
        stage,
        blockingReason: data.blocking_reason || data.blockingReason || { type: "user_input_required", description: data.message },
        timestamp,
      },
    })
    out.push({
      type: "STAGE_UPDATE",
      payload: {
        stage,
        status: "waiting",
        progress: data.progress?.percent,
        message: data.message,
        attempt: data.attempt,
        timestamp,
      },
    })
    return out
  }

  // Stage lifecycle
  if (eventType.startsWith("STAGE_")) {
    out.push({
      type: "STAGE_UPDATE",
      payload: {
        stage,
        status: mapStageStatus(eventType, stageState, status),
        progress: data.progress?.percent,
        message: data.message || data.summary,
        attempt: data.attempt,
        timestamp,
      },
    })
  }

  // Completion/failure
  if (eventType === "PIPELINE_COMPLETED") {
    out.push({ type: "PIPELINE_COMPLETED", payload: { summary: data, timestamp } })
  }
  if (eventType === "PIPELINE_FAILED") {
    out.push({ type: "PIPELINE_FAILED", payload: { stage, error: String(data.message || data.error || "Pipeline failed"), timestamp } })
  }

  return out
}

/**
 * EventBuffer: best-effort ordering for reducer actions.
 * The system is resilient even without perfect ordering (authoritative state polling exists).
 */
export class EventBuffer {
  private buffer: NormalizedEvent[] = []
  private lastTimestamp = 0

  add(events: NormalizedEvent[]): NormalizedEvent[] {
    if (!Array.isArray(events) || events.length === 0) return []
    this.buffer.push(...events)

    // Sort by payload timestamp (ascending)
    this.buffer.sort((a, b) => (a.payload.timestamp || 0) - (b.payload.timestamp || 0))

    const ready: NormalizedEvent[] = []
    const remaining: NormalizedEvent[] = []
    for (const ev of this.buffer) {
      const ts = ev.payload.timestamp || 0
      if (ts >= this.lastTimestamp) {
        ready.push(ev)
        this.lastTimestamp = ts
      } else {
        // Drop stale/out-of-order events (authoritative polling will correct state)
        remaining.push(ev)
      }
    }

    this.buffer = remaining
    return ready
  }

  clear() {
    this.buffer = []
    this.lastTimestamp = 0
  }
}
