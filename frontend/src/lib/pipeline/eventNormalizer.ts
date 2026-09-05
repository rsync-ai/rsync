/**
 * EventNormalizer: Maps raw pipeline_run_events into stable UI primitives
 * for ReasoningTimeline, ToolCallLedger, and other agent-native views.
 */

import type { DomainEventData } from "@/lib/api/types"

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
  status: "pending" | "running" | "completed" | "failed"
  duration?: number
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

function normalizeSeverity(raw?: string): EventSeverity {
  const s = String(raw || "").toLowerCase()
  if (s === "error") return "error"
  if (s === "warn" || s === "warning") return "warn"
  if (s === "info") return "info"
  return "unknown"
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
      severity: normalizeSeverity(event.severity),
      stage: event.stage_id,
      stageGroup: event.stage_group,
      traceId: event.trace_id,
      title,
      description,
      metadata: event.payload,
    }
  }

  /**
   * Group events by stage for timeline view
   */
  static groupByStage(events: PipelineRunEvent[]): StageGroup[] {
    const groupMap = new Map<string, PipelineRunEvent[]>()

    for (const event of events) {
      // Prefer stage_group because it's stable across event producers.
      // stage_id can be missing or vary (agent name vs plan stage id), which fragments groups.
      const key = event.stage_group || event.stage_id || "ungrouped"
      if (!groupMap.has(key)) {
        groupMap.set(key, [])
      }
      groupMap.get(key)!.push(event)
    }

    const groups: StageGroup[] = []
    for (const [key, stageEvents] of groupMap.entries()) {
      const normalized = stageEvents.map((e) => this.normalize(e))
      const sortedByTime = normalized.sort((a, b) => 
        new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
      )

      const startedAt = sortedByTime[0]?.timestamp
      const completedAt = sortedByTime[sortedByTime.length - 1]?.timestamp
      const hasError = sortedByTime.some((e) => e.severity === "error")
      const hasCompleted = stageEvents.some((e) => 
        e.event_type === "STAGE_COMPLETED" || e.event_type === "PIPELINE_COMPLETED"
      )

      let status: StageGroup["status"] = "pending"
      if (hasError) {
        status = "failed"
      } else if (hasCompleted) {
        status = "completed"
      } else if (stageEvents.some((e) => e.event_type === "STAGE_STARTED")) {
        status = "running"
      }

      const duration = startedAt && completedAt 
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
      })
    }

    return groups.sort((a, b) => {
      const aTime = a.startedAt ? new Date(a.startedAt).getTime() : 0
      const bTime = b.startedAt ? new Date(b.startedAt).getTime() : 0
      return aTime - bTime
    })
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
