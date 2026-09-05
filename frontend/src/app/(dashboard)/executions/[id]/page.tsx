import { notFound, unstable_rethrow } from "next/navigation"
import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { 
  ArrowLeft,
  Clock,
  CheckCircle2,
  XCircle,
  Loader2,
  Ban,
  Timer,
  GitBranch,
  AlertTriangle,
  Calendar,
  Play,
  Layers,
} from "lucide-react"
import { formatDateTime, cn } from "@/lib/utils"
import { ExecutionDetailClient } from "@/components/executions/ExecutionDetailClient"
import { TransformExecutionLogsPanel, type TransformExecutionLog } from "@/components/transforms/TransformExecutionLogsPanel"
import { ExecutionComparison } from "@/components/executions/ExecutionComparison"
import { TableStatisticsPanel } from "@/components/pipeline/TableStatisticsPanel"
import { DiagnosisCard, type DiagnosisData } from "@/components/chat/DiagnosisCard"
import { API_ENDPOINTS } from "@/lib/config/api"
import { cookies } from "next/headers"
import { activeWorkspaceCookieHeader } from "@/lib/workspace/server-workspace"
import {
  executionStatusConfig as statusConfig,
  normalizeExecutionStatus,
  type ExecutionStatusConfig,
} from "@/lib/execution-status"

export const dynamic = "force-dynamic"

interface Props {
  params: Promise<{ id: string }>
}

// formatAge renders a coarse "how long ago" label for the freshness KPI. Computed
// at render (the page is force-dynamic), so it reflects staleness at page load.
function formatAge(ms: number): string {
  if (ms < 45_000) return "just now"
  const mins = Math.round(ms / 60_000)
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.round(ms / 3_600_000)
  if (hrs < 24) return `${hrs}h ago`
  const days = Math.round(ms / 86_400_000)
  return `${days}d ago`
}

export default async function ExecutionDetailPage({ params }: Props) {
  const { id } = await params
  
  const cookieStore = await cookies()
  const authToken = cookieStore.get("auth_token")?.value
  // API Gateway expects the raw token value (no Bearer prefix) in local dev.
  // Forward the active-workspace selection (cookie mirror) so all three fetches
  // below scope to the chosen workspace instead of the caller's personal one.
  const authHeaders: Record<string, string> = {
    ...(authToken ? { Authorization: authToken } : {}),
    ...activeWorkspaceCookieHeader(cookieStore),
  }

  let execution: any = null
  let duration = "0s"
  let nodeResults: any[] = []
  let transformLogs: TransformExecutionLog[] = []
  let diagnosis: DiagnosisData | null = null
  let config: ExecutionStatusConfig = statusConfig.pending
  let freshnessAge = "—"
  let freshnessAbs: string | null = null
  // Whether data actually LANDED. Only a clean success may claim "Landed …"; a
  // failed / silent-drop / still-running execution shows "Last activity" instead,
  // so the freshness card never contradicts a red "Execution Failed" banner.
  let freshnessLanded = false

  try {
    const res = await fetch(API_ENDPOINTS.EXECUTIONS.GET_INTERNAL(id), {
      cache: "no-store",
      headers: {
        ...authHeaders,
      },
    })

    if (!res.ok) {
      if (res.status === 404) notFound()
      throw new Error(`Failed to fetch execution: ${res.status}`)
    }

    const rawExecution: any = await res.json()
    
    // Safely transform data
    execution = {
      id: rawExecution.id || id,
      pipelineId: rawExecution.pipeline_id || "",
      status: normalizeExecutionStatus(rawExecution.status, rawExecution.error_message),
      startedAt: rawExecution.start_time ? new Date(rawExecution.start_time) : new Date(),
      finishedAt: rawExecution.end_time ? new Date(rawExecution.end_time) : null,
      error: rawExecution.error_message || null,
      nodeResults: Array.isArray(rawExecution.node_results) ? rawExecution.node_results : [],
      pipeline: rawExecution.pipeline || { 
        id: rawExecution.pipeline_id || "", 
        name: rawExecution.pipeline_name || "Unknown Pipeline", 
        status: "draft", 
        nodes: [], 
        description: "" 
      },
    }

    config = statusConfig[execution.status as keyof typeof statusConfig] || statusConfig.pending

    const calculateDuration = (startedAt: Date, finishedAt: Date | null) => {
      try {
        const start = new Date(startedAt)
        const end = finishedAt ? new Date(finishedAt) : new Date()
        
        if (isNaN(start.getTime()) || (finishedAt && isNaN(end.getTime()))) return "-"

        const diffMs = end.getTime() - start.getTime()
        
        if (diffMs < 0) return "0s"
        if (diffMs < 1000) return `${diffMs}ms`
        if (diffMs < 60000) return `${Math.round(diffMs / 1000)}s`
        if (diffMs < 3600000) return `${Math.round(diffMs / 60000)}m ${Math.round((diffMs % 60000) / 1000)}s`
        return `${Math.round(diffMs / 3600000)}h ${Math.round((diffMs % 3600000) / 60000)}m`
      } catch (e) {
        return "-"
      }
    }

    duration = calculateDuration(execution.startedAt, execution.finishedAt)
    nodeResults = execution.nodeResults

    try {
      const logsRes = await fetch(API_ENDPOINTS.EXECUTIONS.TRANSFORMS_INTERNAL(id), {
        cache: "no-store",
        headers: {
          ...authHeaders,
        },
      })
      if (logsRes.ok) {
        const logsJson: any = await logsRes.json()
        transformLogs = Array.isArray(logsJson?.logs) ? logsJson.logs : []
      }
    } catch {
      transformLogs = []
    }

    // Freshness KPI: how recently did this run's data land? Prefer the most recent
    // transform-log activity, fall back to the execution end_time. Derived at render
    // (force-dynamic) — no new endpoint. Absent when neither source has a timestamp.
    const landedTimes: number[] = []
    if (execution.finishedAt) landedTimes.push(new Date(execution.finishedAt).getTime())
    for (const l of transformLogs) {
      const ts = l.updated_at || l.created_at
      if (ts) {
        const m = new Date(ts).getTime()
        if (!Number.isNaN(m)) landedTimes.push(m)
      }
    }
    if (landedTimes.length) {
      const latest = Math.max(...landedTimes)
      freshnessAbs = formatDateTime(new Date(latest))
      freshnessAge = formatAge(Math.max(0, Date.now() - latest))
      freshnessLanded = execution.status === "completed" || execution.status === "success"
    }

    // Fetch the Phase 3 diagnosis for failed/silent-drop executions. Best-effort:
    // a missing endpoint or empty response leaves the section hidden rather than
    // breaking the page.
    const failureStatuses = new Set(["failed", "silent_drop_detected", "silent_partial_drop_detected", "waiting_for_credential_reauth", "waiting_for_user"])
    if (failureStatuses.has(execution.status)) {
      try {
        const diagRes = await fetch(API_ENDPOINTS.EXECUTIONS.DIAGNOSE_INTERNAL(id), {
          cache: "no-store",
          headers: { ...authHeaders },
        })
        if (diagRes.ok) {
          const diagJson: any = await diagRes.json()
          // chat_diagnose response wraps the payload in `data`
          if (diagJson && diagJson.data && typeof diagJson.data === "object") {
            const d = diagJson.data
            diagnosis = {
              execution_id: String(d.execution_id || id),
              pipeline_id: String(d.pipeline_id || ""),
              pipeline_name: String(d.pipeline_name || execution.pipeline?.name || ""),
              status: String(d.status || execution.status),
              category: String(d.category || ""),
              action: String(d.action || ""),
              action_label: String(d.action_label || ""),
              deep_link: String(d.deep_link || ""),
              confidence: typeof d.confidence === "number" ? d.confidence : 0,
              rationale: String(d.rationale || ""),
              source_rows: typeof d.source_rows === "number" ? d.source_rows : undefined,
              landed_rows: typeof d.landed_rows === "number" ? d.landed_rows : undefined,
            }
          }
        }
      } catch {
        // ignore
      }
    }
  } catch (error: any) {
    // notFound()/redirect() signal themselves by throwing. Next tags those errors
    // with a `digest` (`NEXT_HTTP_ERROR_FALLBACK;404`) — the old `message ===
    // 'NEXT_NOT_FOUND'` check never matched, so a 404 fell through to the generic
    // error card below and the not-found boundary was unreachable. unstable_rethrow
    // is Next's own guard: it re-throws framework control-flow errors and returns
    // for everything else.
    unstable_rethrow(error)
    console.error("Error loading execution details:", error)
    // Fallback error state or rethrow if critical
    // For now, render error state
    return (
      <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
        <AlertTriangle className="h-10 w-10 text-red-500" />
        <h2 className="text-xl font-semibold">Failed to load execution details</h2>
        <p className="text-zinc-500">{String(error)}</p>
        <Link href="/executions">
          <Button variant="outline">Back to Executions</Button>
        </Link>
      </div>
    )
  }

  const StatusIcon = config.icon

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Link href="/executions">
          <Button variant="ghost" size="icon">
            <ArrowLeft className="h-5 w-5" />
          </Button>
        </Link>
        <div className="flex-1">
          <div className="flex items-center gap-3">
            <h1 className="text-2xl font-bold text-zinc-900 dark:text-white">
              Execution Details
            </h1>
            <Badge variant={config.variant} className="text-sm">
              <StatusIcon className={cn("h-3.5 w-3.5 mr-1", config.animate && "animate-spin")} />
              {config.label}
            </Badge>
          </div>
          <p className="text-sm text-zinc-500 mt-1 font-mono">
            {execution.id}
          </p>
        </div>
      </div>

      {/* Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-violet-100 dark:bg-violet-900/30">
                <GitBranch className="h-5 w-5 text-violet-600 dark:text-violet-400" />
              </div>
              <div>
                <p className="text-xs text-zinc-500">Pipeline</p>
                <Link 
                  href={`/pipelines/${execution.pipeline.id}`}
                  className="text-sm font-semibold text-zinc-900 dark:text-white hover:text-violet-600 dark:hover:text-violet-400"
                >
                  {execution.pipeline.name}
                </Link>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-blue-100 dark:bg-blue-900/30">
                <Calendar className="h-5 w-5 text-blue-600 dark:text-blue-400" />
              </div>
              <div>
                <p className="text-xs text-zinc-500">Started At</p>
                <p className="text-sm font-semibold text-zinc-900 dark:text-white">
                  {formatDateTime(execution.startedAt)}
                </p>
                {execution.finishedAt && (
                  <p className="text-xs text-zinc-500 mt-1">
                    Ended {formatDateTime(execution.finishedAt)}
                  </p>
                )}
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-emerald-100 dark:bg-emerald-900/30">
                <Timer className="h-5 w-5 text-emerald-600 dark:text-emerald-400" />
              </div>
              <div>
                <p className="text-xs text-zinc-500">Duration</p>
                <p className="text-sm font-semibold text-zinc-900 dark:text-white">
                  {duration}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Batch runs report no per-node results (node_results is empty), so a
            "0 nodes" tile after an 8-transform run reads as a failure. Only show
            this stat when there are actually node results (agentic/DAG runs). */}
        {nodeResults.length > 0 && (
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-lg bg-amber-100 dark:bg-amber-900/30">
                  <Layers className="h-5 w-5 text-amber-600 dark:text-amber-400" />
                </div>
                <div>
                  <p className="text-xs text-zinc-500">Nodes Executed</p>
                  <p className="text-sm font-semibold text-zinc-900 dark:text-white">
                    {nodeResults.length} nodes
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-teal-100 dark:bg-teal-900/30">
                <Clock className="h-5 w-5 text-teal-600 dark:text-teal-400" />
              </div>
              <div>
                <p className="text-xs text-zinc-500">Data Freshness</p>
                <p className="text-sm font-semibold text-zinc-900 dark:text-white">
                  {freshnessAge}
                </p>
                {freshnessAbs && (
                  <p className="text-xs text-zinc-500 mt-1">
                    {freshnessLanded ? "Landed" : "Last activity"} {freshnessAbs}
                  </p>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Error Message */}
      {execution.error && (
        <Card className="border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20">
          <CardContent className="p-4">
            <div className="flex items-start gap-3">
              <AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-semibold text-red-700 dark:text-red-300">
                  {config.label === "Silent Drop" || config.label === "Partial Silent Drop"
                    ? `Execution: ${config.label}`
                    : "Execution Failed"}
                </p>
                <p className="text-sm text-red-600 dark:text-red-400 mt-1">
                  {execution.error}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Phase 3/4 Healer Diagnosis — auto-renders for failure statuses */}
      {diagnosis && (
        <div>
          <h2 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300 mb-2 flex items-center gap-2">
            <AlertTriangle className="h-4 w-4 text-amber-500" />
            Healer Diagnosis
          </h2>
          <DiagnosisCard data={diagnosis} />
        </div>
      )}

      {/* Node Results */}
      <Card>
        <CardHeader className="pb-4">
          <CardTitle className="text-lg flex items-center gap-2">
            <Layers className="h-5 w-5" />
            Transform execution logs
          </CardTitle>
        </CardHeader>
        <CardContent>
          <TransformExecutionLogsPanel logs={transformLogs} emptyLabel="No transform execution logs for this execution." />
        </CardContent>
      </Card>

      {/* Per-table sync stats for this execution. Sorted failures-first by default. */}
      {execution.pipelineId && (
        <TableStatisticsPanel
          pipelineId={execution.pipelineId}
          executionId={execution.id}
          pipelineStatus={execution.status}
        />
      )}

      {/* Comparison with previous run — uses /api/v1/pipelines/:id/compare
          (previously unused). Component renders nothing when no prior run
          exists, so this slot is invisible on the first execution. */}
      {execution.pipelineId && (
        <ExecutionComparison pipelineId={execution.pipelineId} executionId={execution.id} />
      )}

      <Card>
        <CardHeader className="pb-4">
          <CardTitle className="text-lg flex items-center gap-2">
            <Play className="h-5 w-5" />
            Execution Timeline
          </CardTitle>
        </CardHeader>
        <CardContent>
          {nodeResults.length === 0 ? (
            <div className="text-center py-8 text-zinc-500">
              <Layers className="h-10 w-10 mx-auto mb-3 opacity-50" />
              <p>No node results available</p>
            </div>
          ) : (
            <div className="space-y-4">
              {nodeResults.map((node: any, index: number) => {
                const nodeConfig = statusConfig[node?.status as keyof typeof statusConfig] || statusConfig.pending
                const NodeIcon = nodeConfig.icon
                
                return (
                  <div key={node?.node_id || index} className="relative">
                    {index < nodeResults.length - 1 && (
                      <div className="absolute left-[21px] top-12 bottom-0 w-0.5 bg-zinc-200 dark:bg-zinc-700" />
                    )}
                    
                    <div className="flex items-start gap-4">
                      <div className={cn(
                        "flex h-11 w-11 shrink-0 items-center justify-center rounded-full",
                        nodeConfig.bg
                      )}>
                        <NodeIcon className={cn(
                          "h-5 w-5",
                          nodeConfig.color,
                          nodeConfig.animate && "animate-spin"
                        )} />
                      </div>
                      
                      <div className="flex-1 min-w-0 pb-6">
                        <div className="flex items-center justify-between">
                          <div>
                            <p className="font-semibold text-zinc-900 dark:text-white">
                              {node?.node_name || `Node ${index + 1}`}
                            </p>
                            <p className="text-xs text-zinc-500 mt-0.5">
                              {node?.node_id}
                            </p>
                          </div>
                          <Badge variant={nodeConfig.variant}>
                            {nodeConfig.label}
                          </Badge>
                        </div>
                        
                        {node?.duration_ms !== undefined && (
                          <p className="text-sm text-zinc-500 mt-2">
                            Duration: {node.duration_ms}ms
                            {node.row_count !== undefined && ` • ${node.row_count} rows processed`}
                          </p>
                        )}
                        
                        {node?.error && (
                          <p className="text-sm text-red-600 dark:text-red-400 mt-2">
                            Error: {node.error}
                          </p>
                        )}
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Actions */}
      <ExecutionDetailClient 
        execution={{
          id: execution.id,
          status: execution.status,
          pipelineId: execution.pipelineId || "",
        }} 
      />
    </div>
  )
}
