"use client"

import { useEffect, useState, useCallback, useMemo, useRef } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Loader2,
  RefreshCw,
  ArrowDown,
  GitBranch,
  Zap,
} from "lucide-react"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import {
  LinearTimeline,
  type ExecutionPlanStage,
  type ExecutionPlan,
} from "./DAGVisualization"
import { DAGVisualizationV2 } from "./DAGVisualizationV2"
import { StageDetailPanel } from "./StageDetailPanel"
import { PipelineInsightsBar } from "./PipelineInsightsBar"
import { PipelineCopilotDock } from "./PipelineCopilotDock"
import { SchemaEvolutionPanel } from "./SchemaEvolutionPanel"

interface StepsDAGTabProps {
  pipelineId: string
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
  payload?: Record<string, any>
}

type PipelineSummary = {
  id: string
  name?: string | null
  source_connection_id?: string | null
  destination_connection_id?: string | null
}

type ConnectionSummary = {
  id: string
  connector_type?: string | null
  type?: string | null // sometimes older shapes use "type"
}

function ts(s?: string): number {
  if (!s) return 0
  const t = new Date(s).getTime()
  return Number.isFinite(t) ? t : 0
}

function eventWeight(t: string): number {
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

function normalizeStageStatus(status: string | undefined): string {
  const s = String(status || "").toLowerCase()
  if (s === "completed") return "complete"
  return s || "pending"
}

function prettyConnectorLabel(connectorType: string): string {
  const s = String(connectorType || "").trim()
  if (!s) return ""
  const lc = s.toLowerCase()
  if (lc === "aws-s3") return "AWS S3"
  if (lc === "mysql") return "MySQL"
  if (lc === "postgresql") return "PostgreSQL"
  return lc
    .split(/[-_]/g)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ")
}

function statusFromEventType(eventType: string): string | null {
  switch (eventType) {
    case "STAGE_STARTED":
      return "running"
    case "STAGE_PROGRESS":
      return "running"
    case "PIPELINE_WAITING":
      return "waiting"
    case "STAGE_COMPLETED":
      return "complete"
    case "STAGE_FAILED":
      return "failed"
    default:
      return null
  }
}


// =============================================================================
// Main Component
// =============================================================================

export function StepsDAGTab({ pipelineId }: StepsDAGTabProps) {
  const [executionPlan, setExecutionPlan] = useState<ExecutionPlan | null>(null)
  const [executionId, setExecutionId] = useState<string | null>(null)
  const [events, setEvents] = useState<PipelineRunEvent[]>([])
  const [connectorOverrides, setConnectorOverrides] = useState<{ source?: string; destination?: string }>({})
  const [initialLoading, setInitialLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [viewMode, setViewMode] = useState<"auto" | "dag" | "timeline">("auto")
  const [selectedStageId, setSelectedStageId] = useState<string | null>(null)
  const [pipelineName, setPipelineName] = useState<string | undefined>(undefined)

  const lastUserInteractionAtRef = useRef<number>(0)
  const didInitConnectorOverridesRef = useRef<boolean>(false)
  const inFlightRef = useRef<boolean>(false)

  const markUserInteraction = useCallback(() => {
    lastUserInteractionAtRef.current = Date.now()
  }, [])

  useEffect(() => {
    // Capture page-level scroll too (wheel isn't the only way to scroll).
    const onScroll = () => markUserInteraction()
    const onKeyDown = () => markUserInteraction()
    window.addEventListener("scroll", onScroll, { passive: true })
    window.addEventListener("keydown", onKeyDown)
    return () => {
      window.removeEventListener("scroll", onScroll)
      window.removeEventListener("keydown", onKeyDown)
    }
  }, [markUserInteraction])

  const fetchExecutionPlan = useCallback(async (opts?: { background?: boolean }) => {
    const background = !!opts?.background
    if (inFlightRef.current) return
    inFlightRef.current = true
    if (background) {
      setRefreshing(true)
    } else {
      setInitialLoading(true)
      setError(null)
    }

    try {
      const [stateRes, eventsRes] = await Promise.all([
        authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, { cache: "no-store" }),
        authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/events?limit=250`, { cache: "no-store" }).catch(() => null),
      ])

      if (!stateRes.ok) {
        throw new Error(`Failed to fetch pipeline state: ${stateRes.status}`)
      }

      const stateData = await stateRes.json()
      const execId = (stateData?.execution_id as string) || null
      setExecutionId(execId)

      // execution_plan is in the state response
      if (stateData.execution_plan) {
        // It might be a JSON string or object
        const plan =
          typeof stateData.execution_plan === "string"
            ? JSON.parse(stateData.execution_plan)
            : stateData.execution_plan
        setExecutionPlan(plan)
      } else {
        setExecutionPlan(null)
      }

      // Events (optional; used to reconcile DAG node status)
      if (eventsRes?.ok) {
        const evData = await eventsRes.json().catch(() => null)
        const ev = Array.isArray(evData?.events) ? (evData.events as PipelineRunEvent[]) : []
        setEvents(ev)
      }

      // Connector overrides are relatively static; only fetch once (first successful load).
      if (!didInitConnectorOverridesRef.current) {
        didInitConnectorOverridesRef.current = true
        const pipelineRes = await authFetch(API_ENDPOINTS.PIPELINES.GET(pipelineId), { cache: "no-store" }).catch(() => null)
        const overrides: { source?: string; destination?: string } = {}
        if (pipelineRes?.ok) {
          const p = (await pipelineRes.json().catch(() => null)) as PipelineSummary | null
          if (p?.name) setPipelineName(p.name)
          const srcId = (p?.source_connection_id || "").trim()
          const dstId = (p?.destination_connection_id || "").trim()
          const connFetches: Array<Promise<Response | null>> = []
          const keys: Array<"source" | "destination"> = []

          if (srcId) {
            keys.push("source")
            connFetches.push(authFetch(API_ENDPOINTS.CONNECTIONS.GET(srcId), { cache: "no-store" }).catch(() => null))
          }
          if (dstId) {
            keys.push("destination")
            connFetches.push(authFetch(API_ENDPOINTS.CONNECTIONS.GET(dstId), { cache: "no-store" }).catch(() => null))
          }

          if (connFetches.length > 0) {
            const resps = await Promise.all(connFetches)
            for (let i = 0; i < resps.length; i++) {
              const r = resps[i]
              if (!r?.ok) continue
              const c = (await r.json().catch(() => null)) as ConnectionSummary | null
              const ctype = (c?.connector_type || c?.type || "").toString().trim().toLowerCase().replace(/_/g, "-")
              if (!ctype) continue
              overrides[keys[i]] = ctype
            }
          }
        }
        setConnectorOverrides(overrides)
      }

      // Clear any prior transient errors after a successful refresh.
      setError(null)
    } catch (err: any) {
      // If we already have a plan rendered, don't blow away the UI on transient poll failures.
      // Still surface the error (non-blocking) so the user can manually refresh.
      setError(err?.message || "Failed to load execution plan")
    } finally {
      inFlightRef.current = false
      if (background) {
        setRefreshing(false)
      } else {
        setInitialLoading(false)
      }
    }
  }, [pipelineId])

  useEffect(() => {
    fetchExecutionPlan({ background: false })
  }, [fetchExecutionPlan])

  // Poll while any stage is running
  useEffect(() => {
    if (!executionPlan?.stages) return

    const hasRunning = executionPlan.stages.some(
      (s) => s.status === "running" || s.status === "waiting"
    )
    if (!hasRunning) return

    const interval = setInterval(() => {
      // Don’t refresh while the user is actively scrolling/interacting with the DAG view.
      // This avoids scroll jank from frequent re-renders.
      const last = lastUserInteractionAtRef.current
      if (last && Date.now() - last < 1500) return
      if (typeof document !== "undefined" && document.visibilityState !== "visible") return
      fetchExecutionPlan({ background: true })
    }, 8000)
    return () => clearInterval(interval)
  }, [executionPlan, fetchExecutionPlan])

  // True DAG (as reported by backend) vs "graph-able" (we can render a graph even if deps are missing).
  const isTrueDAG = useMemo(() => {
    if (!executionPlan) return false
    if (executionPlan.metadata?.is_dag) return true
    return executionPlan.stages?.some((s) => Array.isArray(s.dependencies) && s.dependencies.length > 0) ?? false
  }, [executionPlan])

  const resolvedStages = useMemo(() => {
    const stages = executionPlan?.stages || []
    if (!Array.isArray(stages) || stages.length === 0) return []

    const execId = (executionId || "").trim()

    const relevantEvents = (events || []).filter((e) => {
      if (!e) return false
      if (execId && String(e.execution_id || "").trim() && String(e.execution_id || "").trim() !== execId) return false
      return true
    })

    const byStage = new Map<string, PipelineRunEvent[]>()
    for (const e of relevantEvents) {
      const sid = String(e.stage_id || "").trim()
      if (!sid) continue
      if (!byStage.has(sid)) byStage.set(sid, [])
      byStage.get(sid)!.push(e)
    }

    return stages.map((s) => {
      const stageId = String(s.id || "").trim()
      const nodeKind = (s.node_kind || s.metadata?.node_kind || "").toString().trim().toLowerCase()

      // Start with backend plan status, then reconcile from events (best-effort)
      let status = normalizeStageStatus(s.status)
      let startedAt = s.started_at
      let completedAt = s.completed_at

      const ev = byStage.get(stageId) || []
      if (ev.length > 0) {
        const sorted = [...ev].sort((a, b) => {
          const ta = ts(a.occurred_at || a.received_at)
          const tb = ts(b.occurred_at || b.received_at)
          if (ta !== tb) return ta - tb
          return eventWeight(String(a.event_type || "")) - eventWeight(String(b.event_type || ""))
        })
        const last = sorted[sorted.length - 1]
        const st = statusFromEventType(String(last.event_type || ""))
        if (st) status = st

        const firstStarted = sorted.find((e) => e.event_type === "STAGE_STARTED")
        const lastCompleted = [...sorted].reverse().find((e) => e.event_type === "STAGE_COMPLETED" || e.event_type === "STAGE_FAILED")
        if (firstStarted) startedAt = firstStarted.occurred_at || firstStarted.received_at || startedAt
        if (lastCompleted) completedAt = lastCompleted.occurred_at || lastCompleted.received_at || completedAt
      }

      // Connector type resolution for better labeling/logos
      const nodeConfig = (s.metadata?.node_config || s.metadata?.nodeConfig || {}) as Record<string, any>
      const rawConnector = (nodeConfig.connector_type || nodeConfig.connectorType || "").toString().trim().toLowerCase().replace(/_/g, "-")
      let resolvedConnector = rawConnector
      if (nodeKind === "source" && (!resolvedConnector || resolvedConnector === "database") && connectorOverrides.source) {
        resolvedConnector = connectorOverrides.source
      }
      if (nodeKind === "destination" && (!resolvedConnector || resolvedConnector === "storage") && connectorOverrides.destination) {
        resolvedConnector = connectorOverrides.destination
      }

      let displayName = s.display_name
      if (nodeKind === "source" && resolvedConnector) displayName = `Extract from ${prettyConnectorLabel(resolvedConnector)}`
      if (nodeKind === "destination" && resolvedConnector) displayName = `Load to ${prettyConnectorLabel(resolvedConnector)}`

      return {
        ...s,
        status,
        started_at: startedAt,
        completed_at: completedAt,
        display_name: displayName,
        metadata: {
          ...(s.metadata || {}),
          node_config: nodeConfig,
          resolved_connector_type: resolvedConnector || undefined,
        },
      } as ExecutionPlanStage
    })
  }, [executionPlan?.stages, events, executionId, connectorOverrides])

  // Synthesize infra_preflight stage from run events if the execution plan doesn't include it.
  // The orchestrator emits infra_preflight events to Kafka but they don't live in the
  // execution_plan JSON — so we reconstruct the stage here for the timeline/graph.
  const enrichedStages = useMemo(() => {
    if (!resolvedStages.length) return resolvedStages
    const hasInfra = resolvedStages.some((s) => s.id === "infra_preflight")
    if (hasInfra) return resolvedStages

    // Scope infra events to the CURRENT execution — otherwise a previous run's
    // infra_preflight events leak in and synthesize a stale node (notably on Reload,
    // where the new run starts with an executor-only plan and a different execution_id).
    const execId = (executionId || "").trim()
    const infraEvents = events.filter((e) => {
      if (String(e.stage_id || "").trim() !== "infra_preflight") return false
      if (execId && String(e.execution_id || "").trim() && String(e.execution_id || "").trim() !== execId) return false
      return true
    })
    if (infraEvents.length === 0) return resolvedStages

    const sorted = [...infraEvents].sort((a, b) => ts(a.occurred_at || a.received_at) - ts(b.occurred_at || b.received_at))
    const last = sorted[sorted.length - 1]
    const first = sorted[0]
    const lastStatus = statusFromEventType(String(last.event_type || "")) || "complete"
    const startedAt = first.occurred_at || first.received_at
    const completedAt = (last.event_type === "STAGE_COMPLETED" || last.event_type === "STAGE_FAILED")
      ? (last.occurred_at || last.received_at)
      : undefined

    const startMs = ts(startedAt)
    const endMs = ts(completedAt)
    const durationMs = startMs && endMs ? endMs - startMs : undefined

    const infraStage: ExecutionPlanStage = {
      id: "infra_preflight",
      display_name: "Infra Preflight",
      description: "Starting required MCP servers, Kafka Connect, and infra services",
      status: lastStatus,
      started_at: startedAt,
      completed_at: completedAt,
      // This was always milliseconds; it just used to share a key with the
      // adapter's seconds. Now it has its own.
      actual_duration_ms: durationMs,
      result_summary: lastStatus === "complete" ? "All services ready" : undefined,
      node_kind: "infra_preflight",
    }

    // Insert before executor stage; fall back to appending before last item.
    // Wire dependencies so the DAG layout places infra_preflight in the correct position:
    // the stage before it gets infra_preflight as its successor, and infra_preflight
    // depends on the stage that precedes it.
    const executorIdx = resolvedStages.findIndex((s) => s.id === "executor")
    const insertAt = executorIdx > 0 ? executorIdx : Math.max(resolvedStages.length - 1, 0)
    const prevStage = resolvedStages[insertAt - 1]
    const nextStage = resolvedStages[insertAt]

    const infraWithDeps: ExecutionPlanStage = {
      ...infraStage,
      dependencies: prevStage ? [prevStage.id] : [],
    }
    // Point the stage that follows (executor / last) to depend on infra_preflight
    const patchedNext = nextStage
      ? { ...nextStage, dependencies: [infraWithDeps.id, ...(nextStage.dependencies || []).filter((d) => d !== (prevStage?.id ?? ""))] }
      : null

    const copy = [...resolvedStages]
    copy.splice(insertAt, patchedNext ? 1 : 0, infraWithDeps, ...(patchedNext ? [patchedNext] : []))
    return copy
  }, [resolvedStages, events, executionId])

  // A "fast re-run" (Reload of an already-configured pipeline) skips the agent/planning
  // stages on the backend, so the execution plan carries only the executor stage. Replaying
  // the planning steps would be misleading — instead we show the DATA FLOW that actually runs:
  // extract(source) → executor → load(destination), reconstructed from the pipeline's known
  // connectors. This keeps the familiar left-to-right lineage without fabricating stages that
  // never ran. When connectors aren't loaded yet we fall back to the raw (infra + executor) view.
  const isFastRerun = useMemo(() => {
    const nonInfra = resolvedStages.filter((s) => s.id !== "infra_preflight")
    return nonInfra.length === 1 && (nonInfra[0]?.id || "") === "executor"
  }, [resolvedStages])

  const displayStages = useMemo(() => {
    if (!isFastRerun) return enrichedStages
    if (!connectorOverrides.source && !connectorOverrides.destination) return enrichedStages

    const out: ExecutionPlanStage[] = []
    let prevId = ""
    for (const s of enrichedStages) {
      if (s.id !== "executor") {
        out.push(s)
        prevId = s.id
        continue
      }
      if (connectorOverrides.source) {
        out.push({
          id: "source",
          display_name: `Extract from ${prettyConnectorLabel(connectorOverrides.source)}`,
          description: "Reading rows from the source connector",
          status: s.status,
          started_at: s.started_at,
          node_kind: "source",
          dependencies: prevId ? [prevId] : [],
          metadata: { resolved_connector_type: connectorOverrides.source },
        })
        prevId = "source"
      }
      out.push({ ...s, dependencies: prevId ? [prevId] : s.dependencies || [] })
      prevId = "executor"
      if (connectorOverrides.destination) {
        out.push({
          id: "destination",
          display_name: `Load to ${prettyConnectorLabel(connectorOverrides.destination)}`,
          description: "Writing rows to the destination connector",
          status: s.status,
          completed_at: s.completed_at,
          node_kind: "destination",
          dependencies: ["executor"],
          metadata: { resolved_connector_type: connectorOverrides.destination },
        })
        prevId = "destination"
      }
    }
    return out
  }, [isFastRerun, enrichedStages, connectorOverrides])

  const hasStages = displayStages.length > 0

  // Determine actual view mode
  const actualViewMode = useMemo(() => {
    if (viewMode === "auto") {
      // Default to Graph whenever we have 2+ stages (deps may be synthesized in DAGVisualization).
      return displayStages.length > 1 ? "dag" : "timeline"
    }
    return viewMode
  }, [viewMode, displayStages.length])

  const selectedStage = selectedStageId
    ? displayStages.find((s) => s.id === selectedStageId) ?? null
    : null

  return (
    <div className="space-y-6" onWheelCapture={markUserInteraction} onScrollCapture={markUserInteraction} onTouchStartCapture={markUserInteraction}>
      {hasStages && (
        <PipelineCopilotDock
          pipelineId={pipelineId}
          pipelineName={pipelineName}
          stages={displayStages}
          selectedStage={selectedStage}
        />
      )}
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold flex items-center gap-2">
            Pipeline Execution Steps
            {isFastRerun && (
              <Badge variant="outline" className="ml-2 border-amber-400/60 text-amber-600 dark:text-amber-400">
                <Zap className="h-3 w-3 mr-1" />
                Re-run
              </Badge>
            )}
            {isTrueDAG && (
              <Badge variant="outline" className="ml-2">
                <GitBranch className="h-3 w-3 mr-1" />
                DAG
              </Badge>
            )}
          </h3>
          <p className="text-sm text-zinc-500 dark:text-zinc-400">
            {isFastRerun
              ? "Re-run — planning skipped (configuration unchanged); re-syncing data from source to destination"
              : isTrueDAG
              ? "Directed Acyclic Graph workflow visualization"
              : "Visual representation of your pipeline's execution flow"}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {hasStages && (
            <div className="flex items-center gap-1 border rounded-md p-1">
              <Button
                variant={actualViewMode === "dag" ? "default" : "ghost"}
                size="sm"
                onClick={() => setViewMode("dag")}
              >
                <GitBranch className="h-4 w-4 mr-1" />
                Graph
              </Button>
              <Button
                variant={actualViewMode === "timeline" ? "default" : "ghost"}
                size="sm"
                onClick={() => setViewMode("timeline")}
              >
                <ArrowDown className="h-4 w-4 mr-1" />
                Timeline
              </Button>
            </div>
          )}
          <Button variant="outline" size="sm" onClick={() => fetchExecutionPlan({ background: false })} disabled={initialLoading || refreshing}>
            <RefreshCw className={`h-4 w-4 mr-2 ${refreshing ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        </div>
      </div>

      {/* Non-blocking poll error (keep UI stable while scrolling) */}
      {error && executionPlan && (
        <div className="text-xs text-zinc-500 dark:text-zinc-400">
          Live update paused due to an error: <span className="text-red-600 dark:text-red-400">{error}</span>
        </div>
      )}

      {initialLoading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-violet-600" />
        </div>
      ) : error && !executionPlan ? (
        <div className="flex flex-col items-center justify-center py-12">
          <div className="text-red-600 mb-4">{error}</div>
          <Button onClick={() => fetchExecutionPlan({ background: false })}>Retry</Button>
        </div>
      ) : !executionPlan || !executionPlan.stages || executionPlan.stages.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <div className="text-zinc-500">No execution plan available</div>
          <p className="text-xs text-zinc-400 mt-2">Run this pipeline to see execution steps</p>
        </div>
      ) : (
        <div className="space-y-4">
          {/* Schema evolution — pending DDL approvals from healer agent */}
          <SchemaEvolutionPanel pipelineId={pipelineId} />

          {/* Insights bar — quick-action chips for live pipeline observability */}
          <PipelineInsightsBar stages={displayStages} />

          {/* Render DAG or Timeline + detail panel side-by-side */}
          <div className="flex flex-col lg:flex-row gap-4">
            <div className="flex-1 min-w-0">
              {actualViewMode === "dag" ? (
                <DAGVisualizationV2
                  stages={displayStages}
                  selectedStageId={selectedStageId}
                  onStageClick={(id) => setSelectedStageId((prev) => (prev === id ? null : id))}
                />
              ) : (
                <LinearTimeline
                  stages={displayStages}
                  selectedStageId={selectedStageId}
                  onStageClick={(id) => setSelectedStageId((prev) => (prev === id ? null : id))}
                />
              )}
            </div>
            {selectedStageId && (
              <StageDetailPanel
                stage={selectedStage}
                allStages={displayStages}
                onClose={() => setSelectedStageId(null)}
                onSelectStage={(id) => setSelectedStageId(id)}
              />
            )}
          </div>

          {/* Summary */}
          <div className="flex items-center gap-4 pt-4 border-t text-sm text-zinc-600 dark:text-zinc-400">
            <span>
              Mode: <span className="font-medium">{executionPlan.mode || "batch"}</span>
            </span>
            {executionPlan.estimated_time && (
              <span>
                Est. Time:{" "}
                <span className="font-medium">{Math.round(executionPlan.estimated_time / 60)}min</span>
              </span>
            )}
            {executionPlan.metadata?.node_count && (
              <span>
                Nodes: <span className="font-medium">{executionPlan.metadata.node_count}</span>
              </span>
            )}
            {executionPlan.metadata?.edge_count && (
              <span>
                Edges: <span className="font-medium">{executionPlan.metadata.edge_count}</span>
              </span>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
