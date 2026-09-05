"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import Link from "next/link"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import { AlertTriangle, ExternalLink, Loader2, RefreshCw, StopCircle, Activity } from "lucide-react"
import { toast } from "sonner"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch, authFetchOrThrow } from "@/lib/api/auth-fetch"
import { classifyError } from "@/lib/utils/error-handling"
import { usePipelineRuntime } from "@/lib/hooks/usePipelineRuntime"
import { StageTimeline } from "@/components/pipeline/StageTimeline"
import type { HITLState, StageExecution, StageStatus } from "@/lib/pipeline/stageDefinitions"
import { stageRegistry } from "@/lib/pipeline/stageDefinitions"
import {
  isTerminalPipelineStatus,
  normalizePipelineStatus,
  reconcilePipelineStatus,
} from "@/lib/pipeline/statusNormalization"
import { HITLPanel } from "@/components/pipeline/HITLPanel"
import { PipelineTableSelector, normalizeSuggestedTables } from "@/components/pipeline/PipelineTableSelector"
import { PipelineConnectionSelector } from "@/components/pipeline/PipelineConnectionSelector"
import { GenericHitlForm, UNSUPPORTED_HITL_TITLE, UNSUPPORTED_HITL_DETAIL } from "@/components/chat/GenericHitlForm"
import { resumePipelineConnectors, resumePipelineNodeInput, resumePipelineTables, type DestinationConfig } from "@/lib/api/pipelines"
import { getPipeline } from "@/lib/api/pipelines"
import type { PipelineStateResponse, BlockingReasonDetails } from "@/lib/api/types"

type HitlKind = "table_selection" | "connection_config" | "connector_generation" | "policy_approval" | "generic"

/**
 * Shown when a run is parked on a request this build has no UI for. The raw
 * blocking type is printed because it is the one fact this build genuinely has,
 * and it is what an operator needs to quote in a bug report.
 */
export function UnsupportedHitlNotice({ blockingType }: { blockingType?: string }) {
  const t = String(blockingType || "").trim()
  return (
    <div role="alert" className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-100">
      <div className="font-medium">{UNSUPPORTED_HITL_TITLE}</div>
      <div className="mt-1">{UNSUPPORTED_HITL_DETAIL}</div>
      {t ? <div className="mt-1 font-mono text-xs">{t}</div> : null}
    </div>
  )
}

type PipelineRunEvent = {
  pipeline_id: string
  execution_id?: string
  event_id: string
  seq?: number
  event_type: string
  stage_id?: string
  stage_group?: string
  severity?: string
  occurred_at?: string
  received_at?: string
  payload?: Record<string, unknown>
}

const AGENT_STAGE_ORDER = [
  "intent",
  "capability_resolver",
  "connector_check",
  "connector_generation",
  "connection_validation",
  "connection_validator",
  "planner",
  "validator",
  "infra_preflight",
  "executor",
] as const

const DAG_STAGE_GROUPS = new Set(["extracting", "transforming", "loading"])

function parseTs(s?: string): number | undefined {
  if (!s) return undefined
  const t = new Date(s).getTime()
  return Number.isFinite(t) ? t : undefined
}

function isProbablyDagNodeStageId(stageId?: string): boolean {
  const id = String(stageId || "")
  if (!id) return false
  return /^(source|dest|transform|notify)_[0-9]+$/.test(id)
}

function buildAgenticStagesFromEvents(
  rawEvents: PipelineRunEvent[],
  state: PipelineStateResponse | null
): StageExecution[] {
  if (!rawEvents || rawEvents.length === 0) return []

  const execId = (state?.execution_id || "").trim()

  // Filter to current execution where possible
  let events = rawEvents
  if (execId) {
    const filtered = rawEvents.filter((e) => String(e.execution_id || "").trim() === execId)
    if (filtered.length > 0) events = filtered
  }

  // Exclude DAG node-level events (we show those in Steps/DAG tab)
  events = events.filter((e) => {
    const sg = String(e.stage_group || "").toLowerCase()
    if (DAG_STAGE_GROUPS.has(sg)) return false
    if (isProbablyDagNodeStageId(e.stage_id)) return false
    return true
  })

  if (events.length === 0) return []

  // Normalize ordering (events API returns newest-first)
  const sorted = [...events].sort((a, b) => {
    const ta = parseTs(a.occurred_at || a.received_at) ?? 0
    const tb = parseTs(b.occurred_at || b.received_at) ?? 0
    if (ta !== tb) return ta - tb

    // If timestamps are identical (common for fast stages), enforce a stable lifecycle order
    // so STAGE_COMPLETED doesn't get reordered before STAGE_STARTED.
    const weight = (t: string) => {
      switch (t) {
        case "STAGE_STARTED":
          return 10
        case "STAGE_PROGRESS":
          return 20
        case "PIPELINE_WAITING":
          return 30
        case "STAGE_COMPLETED":
          return 40
        case "STAGE_FAILED":
          return 50
        case "PIPELINE_COMPLETED":
          return 60
        default:
          return 25
      }
    }
    return weight(String(a.event_type || "")) - weight(String(b.event_type || ""))
  })

  type Agg = {
    stageKey: string
    startedAt?: number
    completedAt?: number
    status: StageStatus
    progress?: number
    error?: string
    message?: string
    currentAttempt: number
    maxAttempts: number
    lastEventAt?: number
  }

  const byStage = new Map<string, Agg>()

  const ensure = (stageKey: string) => {
    if (!byStage.has(stageKey)) {
      byStage.set(stageKey, {
        stageKey,
        status: "pending",
        currentAttempt: 1,
        maxAttempts: 1,
      })
    }
    return byStage.get(stageKey)!
  }

  for (const e of sorted) {
    const stageKey = String(e.stage_id || "").trim()
    if (!stageKey) continue

    const agg = ensure(stageKey)
    const at = parseTs(e.occurred_at || e.received_at)
    agg.lastEventAt = at ?? agg.lastEventAt

    const p = (e.payload && typeof e.payload === "object" ? e.payload : {}) as Record<string, any>
    const attempt = Number(p.attempt ?? p.current_attempt ?? 1)
    const maxAttempts = Number(p.max_attempts ?? p.maxAttempts ?? 1)
    if (Number.isFinite(attempt) && attempt > agg.currentAttempt) agg.currentAttempt = attempt
    if (Number.isFinite(maxAttempts) && maxAttempts > agg.maxAttempts) agg.maxAttempts = maxAttempts

    if (e.event_type === "STAGE_STARTED") {
      // Don't let an out-of-order START overwrite a terminal state.
      if (agg.status !== "completed" && agg.status !== "failed") {
        agg.status = "running"
      }
      agg.startedAt = agg.startedAt ?? at
      agg.message = p.message || agg.message
    } else if (e.event_type === "STAGE_PROGRESS") {
      agg.status = "running"
      agg.startedAt = agg.startedAt ?? at
      const prog = p.progress?.percent
      if (typeof prog === "number" && Number.isFinite(prog)) agg.progress = prog
      agg.message = p.message || agg.message
    } else if (e.event_type === "STAGE_COMPLETED") {
      agg.status = "completed"
      agg.startedAt = agg.startedAt ?? at
      agg.completedAt = at ?? agg.completedAt
      agg.progress = 100
      agg.message = p.message || agg.message
    } else if (e.event_type === "STAGE_FAILED") {
      agg.status = "failed"
      agg.startedAt = agg.startedAt ?? at
      agg.completedAt = at ?? agg.completedAt
      agg.error = p.error || p.error_message || p.message || agg.error
    } else if (e.event_type === "PIPELINE_WAITING") {
      // Leave waiting inference to the final state overlay below.
      const baseMsg = String(p.message || agg.message || "").trim()
      const br = (p.blocking_reason && typeof p.blocking_reason === "object" ? p.blocking_reason : null) as BlockingReasonDetails | null
      const det = (br?.details && typeof br.details === "object" ? br.details : null) as BlockingReasonDetails | null
      const ve =
        det?.validation_errors && typeof det.validation_errors === "object"
          ? (det.validation_errors as Record<string, unknown>)
          : null
      if (ve && Object.keys(ve).length > 0) {
        const parts = Object.entries(ve)
          .map(([k, v]) => {
            const key = String(k || "").trim()
            const msg = String(v || "").trim()
            if (!key || !msg) return ""
            return `${key}: ${msg}`
          })
          .filter(Boolean)
        const suffix = parts.length > 0 ? parts.join(" | ") : ""
        agg.message = [baseMsg, suffix].filter(Boolean).join(" — ") || agg.message
      } else {
        agg.message = baseMsg || agg.message
      }
    }
  }

  // Ensure the full baseline stage list exists so the UI can render future stages as "pending".
  // Without this, early runs incorrectly display "Step N/N" (because only started stages exist),
  // which makes it look like the pipeline is almost finished.
  for (const k of AGENT_STAGE_ORDER) {
    ensure(k)
  }

  const pipelineStatus = normalizePipelineStatus(state?.status)
  const statusStageKey =
    String(state?.progress?.stage || "").trim() || String(state?.current_stage || "").trim() || ""

  const resetToPending = (agg: Agg) => {
    agg.status = "pending"
    agg.startedAt = undefined
    agg.completedAt = undefined
    agg.progress = undefined
    agg.error = undefined
    agg.message = undefined
  }

  const setCompleted = (agg: Agg) => {
    agg.status = "completed"
    agg.completedAt = agg.completedAt ?? agg.lastEventAt ?? Date.now()
    agg.progress = 100
  }

  const inferLinearStatuses = (activeKey: string, activeStatus: "waiting" | "failed") => {
    const idx = AGENT_STAGE_ORDER.indexOf(activeKey as (typeof AGENT_STAGE_ORDER)[number])
    if (idx < 0) return
    const err = String(state?.error_message || state?.message || "Failed").trim()

    for (let i = 0; i < AGENT_STAGE_ORDER.length; i++) {
      const key = AGENT_STAGE_ORDER[i]
      const agg = byStage.get(key)
      if (!agg) continue

      // Keep explicitly terminal states from events, except we force the activeKey below.
      const terminal = agg.status === "completed" || agg.status === "failed" || agg.status === "cancelled"
      if (i < idx) {
        if (!terminal) setCompleted(agg)
        continue
      }
      if (i === idx) {
        agg.status = activeStatus
        if (activeStatus === "failed") {
          agg.completedAt = agg.completedAt ?? agg.lastEventAt ?? Date.now()
          agg.error = agg.error || err
        }
        continue
      }
      if (!terminal) resetToPending(agg)
    }
  }

  // Overlay: if waiting_for_user, make earlier stages completed and later stages pending, with a single "waiting" stage.
  if (pipelineStatus === "waiting_for_user") {
    const waitingStage = statusStageKey || "executor"
    ensure(waitingStage)
    inferLinearStatuses(waitingStage, "waiting")
  }

  // Overlay: if the run completed, every stage it went through is done. Stage
  // completion is otherwise inferred purely from events, and the ones that
  // finish quickly (or whose STAGE_COMPLETED lands after the terminal event)
  // never get marked — so a successful run rendered its whole checklist
  // unchecked. This is the success counterpart of the cancelled/failed overlays
  // below; without it, completion was the one terminal state with no overlay.
  if (pipelineStatus === "completed") {
    for (const agg of byStage.values()) {
      // Leave a genuinely failed/cancelled stage alone — a run can complete with
      // a stage that was retried past a failure, and that history is worth keeping.
      if (agg.status === "failed" || agg.status === "cancelled") continue
      setCompleted(agg)
    }
  }

  // Overlay: if stopped/cancelled, ensure no stage stays active.
  if (pipelineStatus === "cancelled") {
    for (const agg of byStage.values()) {
      if (agg.status === "running" || agg.status === "pending" || agg.status === "waiting" || agg.status === "retrying") {
        agg.status = "cancelled"
        agg.completedAt = agg.completedAt ?? agg.lastEventAt ?? Date.now()
        agg.message = agg.message || "Stopped"
      }
    }
  }

  // Overlay: if failed, ensure we don't leave earlier/later stages "running".
  // Prefer failing exactly the backend-reported stage, and make later stages pending.
  if (pipelineStatus === "failed") {
    const failedStage = statusStageKey || AGENT_STAGE_ORDER.find((k) => byStage.get(k)?.status === "failed") || ""
    if (failedStage) ensure(String(failedStage))
    inferLinearStatuses(String(failedStage), "failed")
  }

  // Compose final ordered list:
  // - First: known agent stages (in fixed order if they exist)
  // - Then: any extra stages discovered from events, ordered by first-seen time
  const stageKeysInEvents = Array.from(byStage.keys())
  const extras = stageKeysInEvents
    .filter((k) => !(AGENT_STAGE_ORDER as unknown as string[]).includes(k))
    .sort((a, b) => (byStage.get(a)?.startedAt ?? 0) - (byStage.get(b)?.startedAt ?? 0))

  const ordered = [
    ...AGENT_STAGE_ORDER,
    ...extras,
  ]

  return ordered.map((stageKey) => {
    const agg = byStage.get(stageKey)!
    const base = stageRegistry.get(stageKey)
    const startedAt = agg.startedAt ?? parseTs(state?.created_at) ?? 0
    const attempt = {
      attemptNumber: agg.currentAttempt,
      status: agg.status,
      startedAt,
      completedAt: agg.completedAt,
      progress: agg.progress,
      error: agg.error,
      message: agg.message,
    }

    return {
      stage: stageKey,
      label: base.label,
      description: base.description,
      icon: base.icon,
      status: agg.status,
      currentAttempt: agg.currentAttempt,
      maxAttempts: agg.maxAttempts,
      attempts: [attempt],
      startedAt: agg.startedAt,
      completedAt: agg.completedAt,
      progress: agg.progress,
      estimatedDuration: base.estimatedDuration,
      metadata: base.metadata,
    } satisfies StageExecution
  })
}

function mapPlanStageStatus(status?: string): StageStatus {
  const s = String(status || "").toLowerCase()
  switch (s) {
    case "running":
      return "running"
    case "waiting":
      return "waiting"
    case "failed":
      return "failed"
    case "retrying":
      return "retrying"
    case "cancelled":
    case "canceled":
    case "stopped":
      return "cancelled"
    case "complete":
    case "completed":
      return "completed"
    case "skipped":
      // StageTimeline doesn't have a 'skipped' status; render as completed to keep flow readable.
      return "completed"
    case "pending":
    default:
      return "pending"
  }
}

function toTs(s?: string): number | undefined {
  if (!s) return undefined
  const d = new Date(s)
  const t = d.getTime()
  return Number.isFinite(t) ? t : undefined
}

/**
 * The stage the backend is currently reporting on.
 *
 * `pipeline_progress` holds one row per pipeline, and every `stage_*` column on
 * that row -- `stage_attempt`, `stage_max_attempts`, the message, the error --
 * describes its `current_stage`. Anything read off the top level of the live
 * state therefore belongs to this stage and to no other, which is why the
 * timeline builder needs to know which one it is.
 */
function resolveCurrentStageKey(state: PipelineStateResponse | null): string | undefined {
  const explicit = (state?.current_stage || "").trim()
  if (explicit) return explicit
  const pStage = (state?.progress?.stage || "").trim()
  if (pStage) return pStage
  const plan = state?.execution_plan?.stages || []
  const running = plan.find((s) => mapPlanStageStatus(s.status) === "running")
  return running?.id
}

/** A reported count is usable only when it is a finite positive integer. */
function toCount(n?: number): number | undefined {
  return typeof n === "number" && Number.isFinite(n) && n > 0 ? Math.floor(n) : undefined
}

/**
 * Build the stage timeline from the execution plan alone.
 *
 * Module-level and exported so the values it produces can be asserted directly.
 * Some of what it decides is invisible in the rendered timeline -- nothing reads
 * `attempts[].startedAt` today -- and a test that can only see the DOM passes
 * whether or not that field is honest, which is no guard at all.
 */
export function buildStagesFromExecutionPlan(state: PipelineStateResponse | null): StageExecution[] {
  const planStages = state?.execution_plan?.stages || []
  if (!Array.isArray(planStages) || planStages.length === 0) return []

  // The plan's own stage entries carry no attempt data at all -- not in
  // `PipelineStateResponse["execution_plan"]["stages"]`, and not in the
  // `ExecutionStage` struct that produces it. The real counters live at the top
  // level of the live state and belong to `current_stage` alone, so they are
  // attached to that one stage rather than copied across the whole timeline.
  const currentKey = resolveCurrentStageKey(state)
  const reportedAttempt = toCount(state?.attempt)
  const reportedMaxAttempts = toCount(state?.max_attempts)

  return planStages
    .filter((s) => (s?.id || "").trim() !== "")
    .map((s) => {
      const st = mapPlanStageStatus(s.status)
      const startedAt = toTs(s.started_at)
      const completedAt = toTs(s.completed_at)
      const progress = typeof s.progress === "number" ? s.progress : undefined
      const isCurrent = !!currentKey && s.id === currentKey
      const currentAttempt = isCurrent ? reportedAttempt : undefined
      const maxAttempts = isCurrent ? reportedMaxAttempts : undefined

      const attempt = {
        attemptNumber: currentAttempt ?? 1,
        status: st,
        // The plan reports when a stage started, or reports nothing. This used to
        // fall back to the pipeline's created_at (and then to 0), which dated every
        // not-yet-started stage to the moment the pipeline was created. An unknown
        // start time is left unknown; no Date.now() is involved either way.
        startedAt,
        completedAt,
        progress,
        // `error_message` / `message` describe current_stage, so only that stage
        // may show them. StageTimeline supplies its own generic fallback text.
        error: st === "failed" && isCurrent ? state?.error_message || state?.message : undefined,
        message: isCurrent ? state?.message : undefined,
      }

      return {
        stage: s.id!,
        label: s.display_name || s.id!,
        description: s.description || "",
        icon: s.icon || "⚙️",
        status: st,
        currentAttempt,
        maxAttempts,
        attempts: [attempt],
        startedAt,
        completedAt,
        progress,
      } satisfies StageExecution
    })
}


export function PipelineLiveStatePanel(props: { pipelineId: string }) {
  const { pipelineId } = props

  // Dependency-aware source of truth for stream liveness. /state can stay frozen at
  // "running" with a "streaming" message after the feed dies; /runtime reflects the
  // real dependency health (phase "failed" = deps down, "idle" = feed stale).
  const { runtime: pipelineRuntime } = usePipelineRuntime(pipelineId)

  const [state, setState] = useState<PipelineStateResponse | null>(null)
  const [pipelineInfo, setPipelineInfo] = useState<{
    sync_mode?: string
    cdc_mode?: string
    data_loading_strategy?: any
  } | null>(null)
  // Destination mapping (PR-C) shown inside the first-run table HITL. Pre-filled
  // from the pipeline's seeded destination_config; the user can edit before first write.
  const [destinationConfig, setDestinationConfig] = useState<DestinationConfig | null>(null)
  const [destinationType, setDestinationType] = useState<string | undefined>(undefined)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [events, setEvents] = useState<PipelineRunEvent[]>([])
  const [eventsError, setEventsError] = useState<string | null>(null)
  const [showTableModal, setShowTableModal] = useState(false)
  const [showConnectionsModal, setShowConnectionsModal] = useState(false)
  const [connectorSubmitting, setConnectorSubmitting] = useState(false)
  const [connectorError, setConnectorError] = useState<string | null>(null)
  const autoOpenedKeyRef = useRef<string>("")
  const [persistedSelectedTables, setPersistedSelectedTables] = useState<string[]>([])
  const [schemaIndexTables, setSchemaIndexTables] = useState<Array<{ name: string; schema?: string; row_count?: number; columns?: number }>>(
    []
  )
  const [schemaIndexLoading, setSchemaIndexLoading] = useState(false)
  const [schemaIndexError, setSchemaIndexError] = useState<string | null>(null)
  const schemaIndexFetchedKeyRef = useRef<string>("")

  const fetchState = useCallback(async () => {
    setError(null)
    try {
      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, {
        cache: "no-store",
      })
      if (!res.ok) {
        const text = await res.text().catch(() => "")
        throw new Error(text || `Failed to fetch pipeline state (${res.status})`)
      }
      const data = (await res.json()) as PipelineStateResponse
      setState(data)
    } catch (e: any) {
      setError(String(e?.message || e || "Failed to fetch pipeline state"))
    } finally {
      setLoading(false)
    }
  }, [pipelineId])

  useEffect(() => {
    fetchState()
  }, [fetchState])

  // Fetch persisted pipeline table selection (pipeline.selected_tables) so the selector can prefill.
  useEffect(() => {
    let cancelled = false
    const run = async () => {
      try {
        const p = await getPipeline(pipelineId)
        if (!cancelled) {
          setPipelineInfo({
            sync_mode: String(p.sync_mode || "").trim() || undefined,
            cdc_mode: String(p.cdc_mode || "").trim() || undefined,
            data_loading_strategy: p.data_loading_strategy,
          })
          // Destination mapping (PR-C): seed the namespace field from the pipeline's
          // destination_config (falls back to legacy destination_namespace).
          setDestinationConfig(
            p.destination_config ??
              (p.destination_namespace ? { namespace: p.destination_namespace } : null)
          )
          const destNode = Array.isArray(p.config?.nodes)
            ? p.config.nodes.find((n) => n.type === "destination")
            : undefined
          setDestinationType(p.destination_connection?.connector_type || destNode?.connector || undefined)
        }
        const selected: string[] =
          (Array.isArray(p.selected_tables) ? p.selected_tables : []) ||
          (Array.isArray(p.data_loading_strategy?.selected_tables)
            ? (p.data_loading_strategy.selected_tables as string[])
            : [])
        const normalized = (selected || []).map((t) => String(t || "").trim()).filter(Boolean)
        if (!cancelled) setPersistedSelectedTables(normalized)
      } catch {
        // best-effort; selector still works without prefills
      }
    }
    void run()
    return () => {
      cancelled = true
    }
  }, [pipelineId])

  const fetchEvents = useCallback(async () => {
    if (!pipelineId) return
    try {
      setEventsError(null)
      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/events?limit=200`, {
        cache: "no-store",
      })
      if (!res.ok) {
        const text = await res.text().catch(() => "")
        throw new Error(text || `Failed to fetch events (${res.status})`)
      }
      const data = (await res.json()) as { events?: PipelineRunEvent[] }
      const ev = Array.isArray(data?.events) ? data.events : []
      setEvents(ev)
    } catch (e: any) {
      setEventsError(String(e?.message || e || "Failed to fetch events"))
    }
  }, [pipelineId])

  useEffect(() => {
    void fetchEvents()
  }, [fetchEvents, state?.execution_id])

  // Realtime: subscribe to per-pipeline event stream; fall back to polling.
  useEffect(() => {
    if (!pipelineId) return
    if (typeof window === "undefined") return

    let ws: WebSocket | null = null
    const lastRefreshAtRef = { current: 0 }

    const toWsUrl = (httpUrl: string) => {
      if (httpUrl.startsWith("https://")) return httpUrl.replace("https://", "wss://")
      if (httpUrl.startsWith("http://")) return httpUrl.replace("http://", "ws://")
      return httpUrl
    }

    try {
      const url = `${toWsUrl(API_ENDPOINTS.API_GATEWAY_URL)}/api/v1/pipelines/${pipelineId}/events/stream`
      ws = new WebSocket(url)

      ws.onmessage = (event: MessageEvent) => {
        const now = Date.now()
        // Terminal/completion events always get an immediate refresh; bursty
        // progress events are throttled so we don't hammer the REST endpoints.
        let isTerminal = false
        try {
          const payload = JSON.parse(event.data as string) as { event_type?: string }
          const t = payload?.event_type ?? ""
          isTerminal = t === "STAGE_COMPLETED" || t === "PIPELINE_COMPLETED" ||
            t === "STAGE_FAILED" || t === "PIPELINE_FAILED"
        } catch { /* non-JSON frame */ }
        if (!isTerminal && now - lastRefreshAtRef.current < 750) return
        lastRefreshAtRef.current = now
        void fetchState()
        void fetchEvents()
      }
      ws.onerror = () => {
        // Ignore; polling will keep the UI updated.
      }
      ws.onclose = () => {}
    } catch {
      // Ignore; polling will keep the UI updated.
    }

    return () => {
      try {
        if (ws && ws.readyState === WebSocket.OPEN) ws.close()
      } catch {
        // ignore
      }
    }
  }, [pipelineId, fetchState])

  // Poll while processing / waiting. Status-aware interval: keep a fast cadence
  // while actively processing, back off for the slower waiting/pending states.
  useEffect(() => {
    const status = state?.status
    if (!status) return
    if (!["processing", "waiting_for_user", "pending"].includes(status)) return
    const pollIntervalMs = status === "processing" ? 2500 : 5000
    const t = setInterval(() => {
      void fetchState()
      void fetchEvents()
    }, pollIntervalMs)
    return () => clearInterval(t)
  }, [state?.status, fetchState, fetchEvents])

  const stagesFromExecutionPlan: StageExecution[] = useMemo(
    () => buildStagesFromExecutionPlan(state),
    [state]
  )

  const agenticStages: StageExecution[] = useMemo(() => {
    return buildAgenticStagesFromEvents(events, state)
  }, [events, state])

  const stages: StageExecution[] = useMemo(() => {
    // Prefer agentic stages from events so DAG plans don't replace the stage timeline.
    if (agenticStages.length > 0) return agenticStages
    return stagesFromExecutionPlan
  }, [agenticStages, stagesFromExecutionPlan])

  const pipelineTerminal = useMemo(() => {
    const normalized = normalizePipelineStatus(state?.status)
    return { status: normalized, isTerminal: isTerminalPipelineStatus(normalized) }
  }, [state?.status])

  // The /state verdict reconciled against what /runtime observed, shared with the
  // status pill, the CDC action cluster, the header menu and the monitoring panel.
  // `escalated` is what /state could not have known on its own — it is the only
  // case where this badge overrides the raw status word below.
  //
  // This used to compare the RAW status to "running", so a CDC run reporting
  // "processing" was never escalated here even though the pill beside it was.
  // Normalizing first closes that gap; batch is unaffected either way, since
  // computeRuntimePhase only ever returns "idle" for CDC and only returns "failed"
  // for a run whose status is already failed (so there is nothing to escalate).
  const reconciledStatus = useMemo(
    () => reconcilePipelineStatus(pipelineTerminal.status, pipelineRuntime?.phase),
    [pipelineTerminal.status, pipelineRuntime?.phase]
  )
  const statusEscalated = reconciledStatus !== pipelineTerminal.status

  // The backend uses a terminal pseudo-stage like "completed" as current_stage/progress.stage.
  // If that key isn't present in the stage list (which often only contains agent stages like "executor"),
  // the header can correctly show "Completed" while the timeline still looks like it's on the last agent stage.
  //
  // Fix: append a terminal marker stage to the timeline for terminal runs so the timeline matches the header.
  // IMPORTANT: we do NOT use this list for step counting (see derivedStepInfo) so "Step X/Y" stays stable.
  const timelineStages: StageExecution[] = useMemo(() => {
    if (!state) return stages
    if (!stages || stages.length === 0) return stages

    const normalizedStatus = normalizePipelineStatus(state.status)
    const isTerminalRun = isTerminalPipelineStatus(normalizedStatus)

    if (!isTerminalRun) return stages
    const terminalKey = `__terminal__${normalizedStatus || "completed"}`
    if (stages.some((s) => s.stage === terminalKey)) return stages

    const last = stages[stages.length - 1]
    const startedAt =
      last?.completedAt ??
      last?.startedAt ??
      toTs(state?.started_at) ??
      toTs(state?.created_at) ??
      0
    const completedAt = toTs(state?.updated_at) ?? Date.now()

    const terminalStatus: StageStatus =
      normalizedStatus === "failed" ? "failed" : normalizedStatus === "completed" ? "completed" : "cancelled"

    const terminalLabel =
      normalizedStatus === "failed"
        ? "Failed"
        : normalizedStatus === "completed"
          ? "Completed"
          : "Stopped"

    const terminalDescription =
      String(state.summary || state.message || "").trim() ||
      (terminalStatus === "failed"
        ? "Pipeline finished with errors"
        : terminalStatus === "completed"
          ? "Pipeline completed successfully"
          : "Pipeline was stopped before completion")

    const terminalIcon =
      terminalStatus === "failed" ? "❌" : terminalStatus === "completed" ? "✅" : "⏹️"

    const attempt = {
      attemptNumber: 1,
      status: terminalStatus,
      startedAt,
      completedAt,
      progress: terminalStatus === "completed" ? 100 : undefined,
      error: terminalStatus === "failed" ? state.error_message || state.message || "Pipeline failed" : undefined,
      message: terminalDescription,
    }

    const terminalStage: StageExecution = {
      stage: terminalKey,
      label: terminalLabel,
      description: terminalDescription,
      icon: terminalIcon,
      status: terminalStatus,
      // A synthesized end-marker is not a stage that can be attempted or retried,
      // so it reports no counters rather than a made-up 1 of 1.
      attempts: [attempt],
      startedAt,
      completedAt,
      progress: terminalStatus === "completed" ? 100 : undefined,
      metadata: { terminal_marker: true },
    }

    return [...stages, terminalStage]
  }, [stages, state])

  // Prefer backend-reported current_stage, else infer from plan. Shared with the
  // timeline builder so both agree on which stage the top-level fields describe.
  const currentStageKey = useMemo(() => resolveCurrentStageKey(state), [state])

  const derivedStepInfo = useMemo(() => {
    if (!stages || stages.length === 0) return null
    const key =
      (currentStageKey || "").trim() ||
      stages.find((s) => s.status === "running" || s.status === "retrying" || s.status === "waiting")?.stage ||
      ""
    const idx = key ? stages.findIndex((s) => s.stage === key) : -1
    const current_step = idx >= 0 ? idx + 1 : Math.max(1, stages.filter((s) => s.status === "completed").length)
    return { current_step, total_steps: stages.length, stage_key: key }
  }, [stages, currentStageKey])

  const currentStageLabel = useMemo(() => {
    const key = (derivedStepInfo?.stage_key || currentStageKey || state?.current_stage || state?.progress?.stage || "").trim()
    if (!key) return ""
    try {
      return stageRegistry.get(key).label
    } catch {
      // No registry entry — never show a raw key like "executor_step_2" or
      // "syncing" verbatim. Humanize: split on _/-/camelCase, title-case words.
      return key
        .replace(/[_-]+/g, " ")
        .replace(/([a-z])([A-Z])/g, "$1 $2")
        .replace(/\b\w/g, (c) => c.toUpperCase())
        .trim()
    }
  }, [derivedStepInfo?.stage_key, currentStageKey, state?.current_stage, state?.progress?.stage])

  const timelineCurrentStageKey = useMemo(() => {
    if (!pipelineTerminal.isTerminal) return currentStageKey
    const normalized = pipelineTerminal.status || "completed"
    return `__terminal__${normalized}`
  }, [pipelineTerminal.isTerminal, pipelineTerminal.status, currentStageKey])

  const blockingReasonFromEvents = useMemo((): BlockingReasonDetails | null => {
    if (!events || events.length === 0) return null
    let best: BlockingReasonDetails | null = null
    let bestTs = 0
    for (const e of events) {
      const p = e.payload
      const br = p?.["blocking_reason"]
      if (!br || typeof br !== "object") continue
      const t = new Date(String(e.occurred_at || e.received_at || "")).getTime()
      const ts = Number.isFinite(t) ? t : 0
      if (ts >= bestTs) {
        bestTs = ts
        best = br as BlockingReasonDetails
      }
    }
    return best
  }, [events])

  const blockingDetails = useMemo((): BlockingReasonDetails | null => {
    const d = state?.blocking_reason?.details
    if (d && typeof d === "object") return d
    const evd = blockingReasonFromEvents?.details
    if (evd && typeof evd === "object") return evd as BlockingReasonDetails
    return null
  }, [state?.blocking_reason?.details, blockingReasonFromEvents])

  const tableDiscoveryConnectionId = useMemo(() => {
    const id =
      String(blockingDetails?.source_connection_id || "").trim() ||
      String(state?.blocking_reason?.details?.source_connection_id || "").trim()
    return id
  }, [blockingDetails?.source_connection_id, state?.blocking_reason?.details])

  const availableTables = useMemo(() => {
    const fromState = Array.isArray(state?.blocking_reason?.available_tables) ? state?.blocking_reason?.available_tables : []
    const fromDetails = Array.isArray(blockingDetails?.available_tables) ? blockingDetails.available_tables : []
    const fromEvents = Array.isArray(blockingReasonFromEvents?.available_tables)
      ? blockingReasonFromEvents.available_tables
      : []

    // Prefer the richest source; fall back in order.
    const raw = (fromState.length > 0 ? fromState : fromDetails.length > 0 ? fromDetails : fromEvents) as Array<Record<string, unknown>>

    return (raw || [])
      .map((t) => ({
        name: String(t.name || t.table || t.table_name || ""),
        schema: t.schema ? String(t.schema) : undefined,
        row_count:
          typeof t.row_count === "number" && Number.isFinite(t.row_count) && t.row_count >= 0 ? t.row_count : undefined,
        columns: typeof t.columns === "number" ? t.columns : undefined,
      }))
      .filter((t) => Boolean(String(t.name || "").trim()))
  }, [state?.blocking_reason?.available_tables, blockingDetails?.available_tables, blockingReasonFromEvents])

  // AI-suggested tables for the table selector. undefined (key absent from the
  // details payload) keeps the selector's self-fetch fallback alive for
  // late-arriving suggestions; once the key is present, this panel's own
  // /state polling keeps the prop fresh, so the selector must not poll
  // /state redundantly itself.
  const suggestedTables = useMemo(() => {
    const raw = blockingDetails?.suggested_tables
    return raw === undefined ? undefined : normalizeSuggestedTables(raw)
  }, [blockingDetails])

  const tableHitlNodeId = useMemo(() => {
    return String(blockingDetails?.node_id || "").trim()
  }, [blockingDetails])

  const isNodeInputHitl = useMemo(() => {
    const t =
      String(state?.blocking_reason?.type || "").trim() ||
      String(state?.blocking_reason_type || "").trim()
    return t.toLowerCase().includes("node_input") || Boolean(tableHitlNodeId)
  }, [state?.blocking_reason?.type, state?.blocking_reason_type, tableHitlNodeId])

  const hitlKind: HitlKind | null = useMemo(() => {
    if (state?.status !== "waiting_for_user") return null
    const t = String(state?.blocking_reason?.type || "").toLowerCase()
    const widget = String(blockingDetails?.ui_hints?.widget || "").toLowerCase()

    const hasTables =
      availableTables.length > 0 ||
      Array.isArray(blockingDetails?.available_tables) ||
      t.includes("table") ||
      widget === "table_selector"
    if (hasTables) return "table_selection"

    const hasConnections =
      Array.isArray(blockingDetails?.missing_connections) ||
      t.includes("connection") ||
      widget === "connection_configurator"
    if (hasConnections) return "connection_config"

    const hasConnectors =
      Array.isArray(blockingDetails?.missing_connectors) ||
      t.includes("connector") ||
      widget === "connector_generator"
    if (hasConnectors) return "connector_generation"

    if (t.includes("policy") || t.includes("approval")) return "policy_approval"

    return "generic"
  }, [state?.status, state?.blocking_reason?.type, blockingDetails, availableTables.length])

  const needsTables = useMemo(() => {
    // Allow the table selector to open even when the backend didn't include a table list.
    // The modal supports manual entry as a fallback.
    return state?.status === "waiting_for_user" && hitlKind === "table_selection"
  }, [state?.status, hitlKind])

  // If backend didn't provide `available_tables`, fetch schema index so the user can select tables
  // without having to manually type them (and without restarting the pipeline).
  useEffect(() => {
    if (!needsTables) return
    if (!tableDiscoveryConnectionId) return
    if (availableTables.length > 0) return

    const key = `${pipelineId}:${state?.execution_id || ""}:${tableDiscoveryConnectionId}`
    if (schemaIndexFetchedKeyRef.current === key) return
    schemaIndexFetchedKeyRef.current = key

    let cancelled = false
    const run = async () => {
      setSchemaIndexLoading(true)
      setSchemaIndexError(null)
      try {
        // Force refresh so we don't get stuck with an older cached empty schema index.
        const url = `${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/explorer/connections/${encodeURIComponent(
          tableDiscoveryConnectionId
        )}/schema-index?refresh=true`
        const res = await authFetch(url, { cache: "no-store" })
        if (!res.ok) {
          const text = await res.text().catch(() => "")
          throw new Error(text || `Failed to load schema index (${res.status})`)
        }
        const data = (await res.json()) as { tables?: Array<Record<string, unknown>> }
        const tables = Array.isArray(data?.tables) ? data.tables : []
        const normalized = tables
          .map((t) => ({
            name: String(t?.["name"] || "").trim(),
            schema: t?.["schema"] ? String(t["schema"]).trim() : undefined,
            row_count: typeof t?.["row_count"] === "number" ? (t["row_count"] as number) : undefined,
            columns: Array.isArray(t?.["columns"]) ? (t["columns"] as unknown[]).length : undefined,
          }))
          .filter((t) => Boolean(t.name))
        if (!cancelled) setSchemaIndexTables(normalized)
      } catch (e: any) {
        if (!cancelled) setSchemaIndexError(String(e?.message || e || "Failed to load tables"))
      } finally {
        if (!cancelled) setSchemaIndexLoading(false)
      }
    }
    void run()
    return () => {
      cancelled = true
    }
  }, [needsTables, tableDiscoveryConnectionId, availableTables.length, pipelineId, state?.execution_id])

  const needsConnections = useMemo(() => {
    return state?.status === "waiting_for_user" && hitlKind === "connection_config"
  }, [state?.status, hitlKind])

  const needsConnectors = useMemo(() => {
    return state?.status === "waiting_for_user" && hitlKind === "connector_generation"
  }, [state?.status, hitlKind])

  const needsGenericHitl = useMemo(() => {
    return state?.status === "waiting_for_user" && hitlKind === "generic"
  }, [state?.status, hitlKind])

  // Auto-open table selector when required (and keep a manual button too).
  useEffect(() => {
    const key = `${state?.execution_id || ""}:${state?.current_stage || ""}:${state?.blocking_reason?.type || ""}`
    if (!key.trim()) return

    // Only auto-open once per "blocked step" to avoid fighting the user if they close the modal.
    if (autoOpenedKeyRef.current === key) return

    if (needsTables) {
      setShowTableModal(true)
      autoOpenedKeyRef.current = key
      return
    }

    if (needsConnections) {
      setShowConnectionsModal(true)
      autoOpenedKeyRef.current = key
      return
    }
  }, [needsTables, needsConnections, state?.execution_id, state?.current_stage, state?.blocking_reason?.type])

  const hitl: HITLState | null = useMemo(() => {
    if (state?.status !== "waiting_for_user" || !state?.blocking_reason) return null
    const br = state.blocking_reason
    const stage = (state.current_stage || state.progress?.stage || "").trim() || "unknown"
    return {
      pipelineId,
      executionId: state.execution_id,
      stage,
      requiredAt: Date.now(),
      blockingReason: {
        type: String(br.type || "user_input_required"),
        description: String(br.description || state.message || "Action required"),
        details: blockingDetails || undefined,
      },
      metadata: blockingDetails || undefined,
    }
  }, [state?.status, state?.blocking_reason, state?.current_stage, state?.progress?.stage, state?.message, state?.execution_id, blockingDetails, pipelineId])

  const connectionRequirements = useMemo(() => {
    const missing = Array.isArray(blockingDetails?.missing_connections) ? blockingDetails.missing_connections : []
    // Backend payloads vary by producer:
    // - Temporal workflow emits { source_type, destination_type, missing_connections: [{type, direction}] }
    // - Legacy/orchestrator worker emits { source_connector, destination_connector, missing_connections: [{connector_type, direction}] }
    let sourceType = String(
      blockingDetails?.source_type || blockingDetails?.source_connector || blockingDetails?.required_connectors?.source || ""
    )
    let destType = String(
      blockingDetails?.destination_type ||
        blockingDetails?.destination_connector ||
        blockingDetails?.required_connectors?.destination ||
        ""
    )

    // `missing_connections` lists ONLY unresolved directions; an already-resolved
    // + auto-assigned direction is intentionally absent. Track which directions
    // are present so we don't render a bogus "Source: Unknown" picker for a side
    // the backend already wired up.
    const dirs: ("source" | "destination")[] = []
    for (const m of missing) {
      const dir = String(m?.direction || "").toLowerCase()
      const typ = String(m?.type || m?.connector_type || "")
      if (dir === "source") {
        if (!dirs.includes("source")) dirs.push("source")
        if (typ && !sourceType) sourceType = typ
      } else if (dir === "destination") {
        if (!dirs.includes("destination")) dirs.push("destination")
        if (typ && !destType) destType = typ
      }
    }
    const missingDirs = dirs.length > 0 ? dirs : undefined
    const needsSource = !missingDirs || missingDirs.includes("source")
    const needsDest = !missingDirs || missingDirs.includes("destination")

    return {
      source: { connector_type: sourceType || (needsSource ? "unknown" : "") },
      destination: { connector_type: destType || (needsDest ? "unknown" : "") },
      missing: missingDirs,
    }
  }, [blockingDetails])

  const connectionSuggestions = useMemo(() => {
    return Array.isArray(blockingDetails?.suggestions) ? blockingDetails.suggestions : []
  }, [blockingDetails])

  const userRequest = useMemo(() => {
    const s = blockingDetails?.user_request
    return typeof s === "string" ? s : undefined
  }, [blockingDetails])

  const connectionValidationErrors = useMemo(() => {
    const ve = (blockingDetails?.validation_errors && typeof blockingDetails.validation_errors === "object"
      ? blockingDetails.validation_errors
      : null) as Record<string, unknown> | null
    if (!ve) return []
    const out: Array<{ direction: "source" | "destination" | "unknown"; message: string }> = []
    for (const [k, v] of Object.entries(ve)) {
      const dir: "source" | "destination" | "unknown" = k === "source" || k === "destination" ? k : "unknown"
      const msg = String(v || "").trim()
      if (msg) out.push({ direction: dir, message: msg })
    }
    return out
  }, [blockingDetails])

  const missingConnections = useMemo(() => {
    const raw = Array.isArray(blockingDetails?.missing_connections) ? blockingDetails.missing_connections : []
    return raw
      .map((m) => ({
        direction: String((m as Record<string, unknown>)?.["direction"] || "").toLowerCase() || "unknown",
        connector_type: String((m as Record<string, unknown>)?.["connector_type"] || (m as Record<string, unknown>)?.["type"] || "").trim() || "unknown",
      }))
      .filter((m) => m.direction && m.connector_type)
  }, [blockingDetails])

  const missingConnectors = useMemo(() => {
    const raw = blockingDetails?.missing_connectors
    if (Array.isArray(raw)) {
      return raw
        .map((x: any) => (typeof x === "string" ? x : String(x?.type || x?.connector || x?.connector_type || "")))
        .map((s: string) => s.trim())
        .filter(Boolean)
    }
    if (raw && typeof raw === "object") {
      // Some payloads use { source: "...", destination: "..." } shapes.
      return Object.values(raw)
        .map((x: any) => (typeof x === "string" ? x : String(x?.type || x?.connector || x?.connector_type || "")))
        .map((s: string) => s.trim())
        .filter(Boolean)
    }
    return []
  }, [blockingDetails])

  async function resumeAfterConnectorGeneration() {
    if (!pipelineId) return
    setConnectorError(null)
    try {
      setConnectorSubmitting(true)
      await resumePipelineConnectors(pipelineId, {
        execution_id: state?.execution_id,
        connectors: missingConnectors,
        metadata: { source: "pipeline_detail" },
      })
      await fetchState()
    } catch (e: any) {
      setConnectorError(String(e?.message || e || "Failed to resume pipeline"))
    } finally {
      setConnectorSubmitting(false)
    }
  }

  const percent = Math.max(0, Math.min(100, Number(state?.progress?.percent ?? 0)))
  const isStale = !!state?.is_stale
  const cancelRecommended = !!state?.cancel_recommended

  // CDC pipelines can be continuous (streaming) and will not "complete".
  // In that case, backend percent (e.g., 88%) is just "setup progress" and is misleading.
  const isLiveStreaming = useMemo(() => {
    const mode =
      String(pipelineInfo?.data_loading_strategy?.effective_sync_mode || pipelineInfo?.sync_mode || "")
        .trim()
        .toLowerCase()
    if (mode !== "cdc") return false

    // The dependency-aware runtime is the source of truth for stream liveness. A dead
    // feed leaves /state frozen at "running" with a "streaming" message, which would
    // otherwise render a false "LIVE". If runtime reports the stream failed (deps
    // unhealthy) or idle (feed stale), it is NOT live — regardless of the stale /state.
    if (pipelineRuntime && (pipelineRuntime.phase === "failed" || pipelineRuntime.phase === "idle")) {
      return false
    }

    const cm =
      String(pipelineInfo?.data_loading_strategy?.effective_cdc_mode || pipelineInfo?.cdc_mode || "")
        .trim()
        .toLowerCase()
    const msg = String(state?.message || state?.summary || "").trim().toLowerCase()

    // streaming_only is explicitly continuous; also treat "Streaming pipeline started" as live.
    return (
      (state?.status || "").toLowerCase() === "running" &&
      (cm === "streaming_only" || msg.includes("streaming pipeline started") || msg.includes("streaming"))
    )
  }, [pipelineInfo, state?.message, state?.summary, state?.status, pipelineRuntime?.phase])

  async function stopPipeline() {
    try {
      // Was `authFetch` with no `.ok` check: a 403 from the role gate resolved
      // normally, so the catch never ran and the only feedback was a console
      // line the user cannot see. "Keep UI simple" made Stop indistinguishable
      // from a Stop that was refused.
      await authFetchOrThrow(API_ENDPOINTS.PIPELINES.STOP(pipelineId), { method: "POST" })
      toast.success("Pipeline stopped")
    } catch (e) {
      const err = classifyError(e, "pipeline.stop")
      toast.error(err.title, { description: err.hint ?? err.message })
    } finally {
      fetchState()
    }
  }

  return (
    <Card className="border-zinc-200 dark:border-zinc-800">
      <CardHeader className="space-y-1">
        <div className="flex items-start justify-between gap-3">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-5 w-5 text-zinc-500" />
              Live Execution
            </CardTitle>
            <CardDescription>
              {state?.execution_id ? (
                <>
                  Execution{" "}
                  <Link className="underline" href={`/executions/${state.execution_id}`}>
                    {state.execution_id.slice(0, 8)}
                  </Link>
                </>
              ) : (
                "No active execution yet"
              )}
            </CardDescription>
          </div>

          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                // Allow schema-index re-fetch after DB changes (common in dev/E2E when users seed tables).
                schemaIndexFetchedKeyRef.current = ""
                void fetchState()
                void fetchEvents()
              }}
              disabled={loading}
            >
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />
              Refresh
            </Button>
            {(cancelRecommended || state?.status === "processing") && (
              <Button variant="outline" size="sm" onClick={stopPipeline}>
                <StopCircle className="h-4 w-4 mr-2" />
                Stop
              </Button>
            )}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {statusEscalated && reconciledStatus === "failed" ? (
            // /runtime says the stream's dependencies are down while /state is still frozen
            // at "running" — show the same terminal verdict as the health header and the
            // status pill. A more specific /state verdict is left untouched (else branch).
            <Badge variant="destructive" className="gap-1">
              <AlertTriangle className="h-3 w-3" />
              Failed
            </Badge>
          ) : (
            <Badge variant="outline">
              {statusEscalated ? reconciledStatus : state?.status || (loading ? "loading" : "unknown")}
            </Badge>
          )}
          {isStale ? (
            <Badge variant="destructive" className="gap-1">
              <AlertTriangle className="h-3 w-3" />
              Stale
            </Badge>
          ) : null}
          {derivedStepInfo ? (
            <Badge variant="secondary">
              Step {derivedStepInfo.current_step}/{derivedStepInfo.total_steps}
            </Badge>
          ) : typeof state?.progress?.current_step === "number" && typeof state?.progress?.total_steps === "number" ? (
            <Badge variant="secondary">
              Step {state.progress.current_step}/{state.progress.total_steps}
            </Badge>
          ) : null}
        </div>
      </CardHeader>

      <CardContent className="space-y-4">
        {error ? (
          <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-200">
            {error}
          </div>
        ) : null}

        {hitl ? (
          <HITLPanel
            hitl={hitl}
            className={state?.status === "waiting_for_user" ? "animate-in fade-in-0 slide-in-from-top-2" : undefined}
          >
            {needsTables ? (
              <div className="space-y-3">
                <div className="text-sm text-zinc-700 dark:text-zinc-300">
                  {state?.blocking_reason?.description ||
                    (availableTables.length > 0
                      ? `Select table(s) to sync (${availableTables.length} available).`
                      : schemaIndexLoading
                        ? "Loading available tables…"
                        : schemaIndexTables.length > 0
                          ? `Select table(s) to sync (${schemaIndexTables.length} available).`
                          : "Select table(s) to sync (you can enter table names manually if the list is empty).")}
                </div>
                {schemaIndexError ? (
                  <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-100">
                    {schemaIndexError}
                  </div>
                ) : null}
                <div className="flex flex-wrap gap-2">
                  <Button size="sm" onClick={() => setShowTableModal(true)}>
                    Select tables
                  </Button>
                </div>
              </div>
            ) : needsConnections ? (
              <div className="space-y-3">
                <div className="text-sm text-zinc-700 dark:text-zinc-300">
                  {state?.blocking_reason?.description || "Configure the required source and destination connections."}
                </div>
                {connectionValidationErrors.length > 0 || state?.error_message ? (
                  <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-100">
                    <div className="font-medium">Connection check failed</div>
                    {connectionValidationErrors.length > 0 ? (
                      <ul className="mt-1 list-disc pl-5 space-y-1">
                        {connectionValidationErrors.map((e, idx) => (
                          <li key={`${e.direction}-${idx}`}>
                            <span className="font-medium">{e.direction === "unknown" ? "Connection" : e.direction}:</span>{" "}
                            <span className="font-mono text-xs">{e.message}</span>
                          </li>
                        ))}
                      </ul>
                    ) : null}
                    {state?.error_message ? (
                      <div className="mt-2 text-xs text-amber-900/90 dark:text-amber-100/90">
                        {state.error_message}
                      </div>
                    ) : null}
                    <div className="mt-2 text-xs text-amber-900/80 dark:text-amber-100/80">
                      Pick/repair the failing connection (often destination credentials/permissions), then submit to resume.
                    </div>
                  </div>
                ) : null}
                <div className="flex flex-wrap gap-2">
                  <Button size="sm" onClick={() => setShowConnectionsModal(true)}>
                    Configure connections
                  </Button>
                </div>
              </div>
            ) : needsConnectors ? (
              <div className="space-y-3">
                <div className="text-sm text-zinc-700 dark:text-zinc-300">
                  {state?.blocking_reason?.description ||
                    "One or more connectors are missing. Generate them, then resume the pipeline."}
                </div>
                {missingConnectors.length > 0 ? (
                  <div className="rounded-md border border-zinc-200 bg-white/60 p-3 text-sm text-zinc-800 dark:border-zinc-800 dark:bg-zinc-950/20 dark:text-zinc-200">
                    <div className="font-medium">Missing connectors</div>
                    <ul className="mt-2 list-disc pl-5 space-y-1">
                      {missingConnectors.map((c) => (
                        <li key={c} className="font-mono text-xs">
                          {c}
                        </li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                {connectorError ? (
                  <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-200">
                    {connectorError}
                  </div>
                ) : null}
                <div className="flex flex-wrap gap-2">
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      // Keep it simple: open generator, then user can return and click Resume.
                      window.location.href =
                        "/connectors/generate?return=" + encodeURIComponent(`/pipelines/${pipelineId}`)
                    }}
                  >
                    <ExternalLink className="h-4 w-4 mr-2" />
                    Open connector generator
                  </Button>
                  <Button size="sm" onClick={resumeAfterConnectorGeneration} disabled={connectorSubmitting}>
                    {connectorSubmitting ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
                    Resume pipeline
                  </Button>
                </div>
              </div>
            ) : needsGenericHitl ? (
              <div className="space-y-3">
                <div className="text-sm text-zinc-700 dark:text-zinc-300">
                  {state?.blocking_reason?.description || "Input required to continue."}
                </div>
                <UnsupportedHitlNotice blockingType={state?.blocking_reason?.type || state?.blocking_reason_type} />
                {blockingDetails?.input_schema ? (
                  // Read-only on purpose: the operator can see (and report) what was
                  // asked, but Submit stays disabled because this build has no endpoint
                  // for this request type. Omitting `onSubmit` is what puts the form in
                  // that state -- do not pass a no-op stub, that just re-creates the
                  // defect with the warning removed
                  // (KI-HITL-GENERIC-SUBMIT-WIRED-TO-CONSOLE-WARN).
                  <GenericHitlForm
                    schema={blockingDetails.input_schema as unknown as import("@/components/chat/GenericHitlForm").JsonSchema}
                  />
                ) : null}
              </div>
            ) : (
              // Every remaining HITL kind that reaches this chain (today: policy_approval,
              // and a generic park carrying no input_schema) has no form either. Say so
              // instead of rendering an HITLPanel with an empty body. A real branch for
              // any of them MUST be inserted above this fallback, or it will be shadowed.
              <UnsupportedHitlNotice blockingType={state?.blocking_reason?.type || state?.blocking_reason_type} />
            )}
          </HITLPanel>
        ) : null}

        {isStale ? (
          (() => {
            // Stage-aware stall diagnosis. A stall during data sync usually means
            // CDC lag or a large batch still draining — not the same as a stall
            // during preflight/connection (which usually means a config/auth
            // problem the user must act on). Tailor the heading + hint so the
            // user knows whether to wait or to intervene.
            const stageKey = (currentStageKey || state?.current_stage || state?.progress?.stage || "")
              .toString()
              .toLowerCase()
            const isSyncStall = /sync|stream|cdc|transfer|execut|load/.test(stageKey)
            const heading = isSyncStall
              ? "Data sync is taking longer than expected"
              : "This pipeline looks stuck"
            const hint = state?.stale_reason
              ? state.stale_reason
              : isSyncStall
                ? "No new rows have landed recently. For a large initial load this can be normal; for CDC it may mean the source has no new changes or replication is lagging."
                : `No recent heartbeat from the ${currentStageLabel || "current"} stage. This often points to a connection or configuration issue rather than slow progress.`
            return (
          <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200">
            <div className="font-medium">{heading}.</div>
            <div className="mt-1">
              {hint}
              {typeof state?.stale_elapsed_seconds === "number" ? ` (idle ${state.stale_elapsed_seconds}s)` : ""}
            </div>
            <div className="mt-2 flex gap-2">
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  schemaIndexFetchedKeyRef.current = ""
                  void fetchState()
                  void fetchEvents()
                }}
              >
                Refresh Status
              </Button>
              {cancelRecommended ? (
                <Button size="sm" variant="destructive" onClick={stopPipeline}>
                  Stop Pipeline
                </Button>
              ) : null}
            </div>
          </div>
            )
          })()
        ) : null}

        {state?.error_message ? (
          <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-200">
            <div className="font-medium">Error</div>
            <div className="mt-1">{state.error_message}</div>
            {(connectionValidationErrors.length > 0 || missingConnections.length > 0) && (
              <div className="mt-2 space-y-2">
                {connectionValidationErrors.length > 0 ? (
                  <div>
                    <div className="text-xs font-medium">Which connection failed</div>
                    <ul className="mt-1 list-disc pl-5 space-y-1">
                      {connectionValidationErrors.map((e, idx) => (
                        <li key={`${e.direction}-${idx}`}>
                          <span className="font-medium">{e.direction}:</span>{" "}
                          <span className="font-mono text-xs">{e.message}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                {missingConnections.length > 0 ? (
                  <div>
                    <div className="text-xs font-medium">Missing required connections</div>
                    <ul className="mt-1 list-disc pl-5 space-y-1">
                      {missingConnections.map((m, idx) => (
                        <li key={`${m.direction}-${m.connector_type}-${idx}`}>
                          <span className="font-medium">{m.direction}:</span>{" "}
                          <span className="font-mono text-xs">{m.connector_type}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                <div className="flex flex-wrap gap-2">
                  <Button size="sm" variant="outline" onClick={() => setShowConnectionsModal(true)}>
                    Configure connections
                  </Button>
                </div>
              </div>
            )}
          </div>
        ) : null}

        <div className="space-y-2">
          <div className="flex items-center justify-between text-sm">
            <span className="text-zinc-600 dark:text-zinc-400">
              {pipelineTerminal.isTerminal && currentStageLabel
                ? `${pipelineTerminal.status === "failed" ? "Failed during" : "Stopped during"}: ${currentStageLabel}`
                : isLiveStreaming
                  ? "Current stage: Streaming (CDC)"
                  : currentStageLabel
                    ? `Current stage: ${currentStageLabel}`
                  : state?.current_stage
                    ? `Current stage: ${state.current_stage}`
                    : "Current stage"}
            </span>
            {isLiveStreaming ? (
              <Badge variant="secondary" className="text-xs">
                LIVE
              </Badge>
            ) : (
              <span className="font-medium">{percent}%</span>
            )}
          </div>
          <Progress value={isLiveStreaming ? 100 : percent} />
          {state?.message ? <div className="text-sm text-zinc-600 dark:text-zinc-400">{state.message}</div> : null}
          {isLiveStreaming ? (
            <div className="text-xs text-zinc-500 dark:text-zinc-400">
              Streaming runs continuously and won’t reach “100% complete”. Use Stop/Pause when you want to end it.
            </div>
          ) : null}
        </div>

        {stages.length > 0 ? (
          <StageTimeline
            title="Agentic pipeline stages"
            stages={timelineStages}
            currentStageKey={timelineCurrentStageKey}
          />
        ) : (
          <div className="text-sm text-zinc-500">Stage timeline will appear once execution starts.</div>
        )}

        {eventsError ? (
          <div className="text-xs text-zinc-500">
            Unable to load event timeline: {eventsError}
          </div>
        ) : null}

        <PipelineTableSelector
          isOpen={showTableModal}
          autoDismissOnResolve
          onClose={() => setShowTableModal(false)}
          onResolved={() => fetchState()}
          onTablesSelected={async (tables, destCfg) => {
            const execId = state?.execution_id
            if (isNodeInputHitl && tableHitlNodeId) {
              await resumePipelineNodeInput(pipelineId, {
                execution_id: execId || undefined,
                node_id: tableHitlNodeId,
                config_patch: {
                  selected_tables: tables,
                  ...(destCfg ? { destination_config: destCfg } : {}),
                },
              })
            } else {
              await resumePipelineTables(pipelineId, {
                execution_id: execId || undefined,
                selected_tables: tables,
                ...(destCfg ? { destination_config: destCfg } : {}),
              })
            }
            await fetchState()
          }}
          pipelineId={pipelineId}
          executionId={state?.execution_id}
          sourceType={state?.blocking_reason?.details?.source_type}
          availableTables={availableTables.length > 0 ? availableTables : schemaIndexTables}
          suggestedTables={suggestedTables}
          initialSelectedTables={persistedSelectedTables}
          title="Select Tables to Sync"
          loading={schemaIndexLoading}
          destinationConfig={destinationConfig}
          destinationType={destinationType}
        />

        <PipelineConnectionSelector
          isOpen={showConnectionsModal}
          onClose={() => setShowConnectionsModal(false)}
          pipelineId={pipelineId}
          executionId={state?.execution_id}
          requirements={connectionRequirements}
          userRequest={userRequest}
          suggestions={connectionSuggestions}
          title="Configure Required Connections"
          returnTo={`/pipelines/${pipelineId}`}
        />
      </CardContent>
    </Card>
  )
}


