"use client"

import { useState, useEffect, useCallback } from "react"
import Link from "next/link"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { 
  Activity,
  Clock,
  CheckCircle2,
  XCircle,
  Loader2,
  Ban,
  Search,
  Filter,
  ChevronDown,
  ExternalLink,
  StopCircle,
  RotateCcw,
  GitBranch,
  Play,
  Timer,
  AlertTriangle,
  ArrowRight,
  CalendarDays,
  Hand
} from "lucide-react"
import { formatDateTime, cn } from "@/lib/utils"
import { cancelExecution, listExecutionsResponse } from "@/lib/api/executions"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"

// re-exported as `normalizeExecutionStatus` for backwards-compat with this
// component's call sites. Source of truth: @/lib/execution-status.
import { normalizeExecutionStatus as _normalizeExecutionStatus, executionStatusConfig as _statusConfig, formatErrorMessage } from "@/lib/execution-status"
function normalizeExecutionStatus(status: string | null | undefined, errorMessage?: string | null): string {
  return _normalizeExecutionStatus(status, errorMessage)
}

type ScheduleMeta = {
  title: string
  timezone: string
  details?: string
}

function formatInterval(seconds: number): string {
  if (!seconds || seconds <= 0) return "every ?"
  if (seconds % 86400 === 0) {
    const d = seconds / 86400
    return `every ${d} day${d === 1 ? "" : "s"}`
  }
  if (seconds % 3600 === 0) {
    const h = seconds / 3600
    return `every ${h} hour${h === 1 ? "" : "s"}`
  }
  if (seconds % 60 === 0) {
    const m = seconds / 60
    return `every ${m} minute${m === 1 ? "" : "s"}`
  }
  return `every ${seconds} second${seconds === 1 ? "" : "s"}`
}

// Status config is shared with the detail page; defined in execution-status.ts.
const statusConfig = _statusConfig

interface ExecutionWithPipeline {
  id: string
  pipelineId: string
  status: string
  triggerSource?: string
  scheduleId?: string | null
  scheduledTime?: Date | null
  startedAt: Date
  finishedAt: Date | null
  error: string | null
  nodeResults: unknown
  inputData: unknown
  outputData: unknown
  userId: string
  pipeline: {
    id: string
    name: string
    status: string
  }
}

interface PipelineOption {
  id: string
  name: string
}

interface Props {
  initialExecutions: ExecutionWithPipeline[]
  pipelines: PipelineOption[]
  stats: {
    total: number
    running: number
    success: number
    failed: number
  }
}

export function ExecutionsListClient({ initialExecutions, pipelines, stats }: Props) {
  const [searchQuery, setSearchQuery] = useState("")
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [pipelineFilter, setPipelineFilter] = useState<string | null>(null)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [displayExecutions, setDisplayExecutions] = useState(initialExecutions)
  const [statsState, setStatsState] = useState(stats)
  const [pipelineOptions, setPipelineOptions] = useState<PipelineOption[]>(pipelines)
  const [scheduleMetaById, setScheduleMetaById] = useState<Record<string, ScheduleMeta>>({})
  const [scheduleMetaLoading, setScheduleMetaLoading] = useState<Record<string, boolean>>({})

  // Filter executions
  const filteredExecutions = displayExecutions.filter((execution) => {
    const matchesSearch = searchQuery === "" || 
      execution.pipeline.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      execution.id.toLowerCase().includes(searchQuery.toLowerCase())
    
    const matchesStatus = statusFilter === null || execution.status === statusFilter
    const matchesPipeline = pipelineFilter === null || execution.pipelineId === pipelineFilter
    
    return matchesSearch && matchesStatus && matchesPipeline
  })

  // Function to fetch fresh data
  const fetchExecutions = useCallback(async () => {
    try {
      // In dev/e2e environments we can accumulate many executions quickly.
      // Fetch a wider window so search-by-execution-id remains reliable.
      const resp = await listExecutionsResponse({ limit: 200 })

      // Map API response to component state shape
      // Handle potentially raw snake_case data from API
      const rawData = (resp.executions || []) as any[]
      const mapped = rawData.map((e) => ({
        id: e.id,
        pipelineId: e.pipelineId || e.pipeline_id,
        status: normalizeExecutionStatus(e.status, e.error_message || e.error),
        triggerSource: e.triggerSource || e.trigger_source,
        scheduleId: e.scheduleId ?? e.schedule_id ?? null,
        scheduledTime: (e.scheduledTime || e.scheduled_time) ? new Date(e.scheduledTime || e.scheduled_time) : null,
        startedAt: new Date(e.startedAt || e.start_time),
        finishedAt: (e.finishedAt || e.end_time) ? new Date(e.finishedAt || e.end_time) : null,
        error: e.error || e.error_message || null,
        nodeResults: e.nodeResults || e.node_results,
        inputData: e.inputData || e.input_data,
        outputData: e.outputData || e.output_data,
        userId: e.userId || e.user_id,
        pipeline: {
          id: e.pipelineId || e.pipeline_id,
          name: e.pipelineName || e.pipeline_name || "Unknown Pipeline",
          status: "active"
        }
      }))
      
      setDisplayExecutions(mapped)

      // Update stats from API response (fallback to derived)
      const running = resp.stats?.running ?? mapped.filter((e) => e.status === "running" || e.status === "pending").length
      const success = resp.stats?.success ?? mapped.filter((e) => e.status === "success").length
      const failed = resp.stats?.failed ?? mapped.filter((e) => e.status === "failed").length
      const total = typeof resp.total === "number" ? resp.total : mapped.length
      setStatsState({ total, running, success, failed })

      // Update pipeline dropdown options from executions if not provided
      const map = new Map<string, string>()
      mapped.forEach((ex) => {
        if (ex.pipeline?.id && ex.pipeline?.name) map.set(ex.pipeline.id, ex.pipeline.name)
      })
      const opts = Array.from(map.entries()).map(([id, name]) => ({ id, name }))
      if (opts.length > 0) setPipelineOptions(opts)
    } catch (error) {
      console.error("Failed to refresh executions:", error)
    }
  }, [])

  const ensureScheduleMeta = useCallback(async (pipelineId: string, scheduleId: string) => {
    if (!pipelineId || !scheduleId) return
    if (scheduleMetaById[scheduleId]) return
    if (scheduleMetaLoading[scheduleId]) return

    setScheduleMetaLoading((prev) => ({ ...prev, [scheduleId]: true }))

    try {
      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules`, {
        method: "GET",
        cache: "no-store",
      })
      if (!res.ok) {
        throw new Error(`schedule fetch failed: ${res.status}`)
      }

      const data: any = await res.json().catch(() => null)
      const schedules = Array.isArray(data?.schedules) ? data.schedules : []

      const updates: Record<string, ScheduleMeta> = {}
      for (const s of schedules) {
        const sid = s?.schedule_id
        if (!sid) continue
        const tz = s?.schedule_spec?.timezone || "UTC"
        if (s?.schedule_type === "interval") {
          const everySeconds = Number(s?.schedule_spec?.every_seconds || 0)
          updates[sid] = {
            title: "Interval schedule",
            timezone: tz,
            details: formatInterval(everySeconds),
          }
        } else if (s?.schedule_type === "cron") {
          const cron = String(s?.schedule_spec?.cron || "")
          updates[sid] = {
            title: "Cron schedule",
            timezone: tz,
            details: cron ? `cron: ${cron}` : "cron: ?",
          }
        } else {
          updates[sid] = { title: "Schedule", timezone: tz }
        }
      }

      if (Object.keys(updates).length > 0) {
        setScheduleMetaById((prev) => ({ ...prev, ...updates }))
      }
    } catch (e) {
      console.warn("Failed to load schedule metadata:", e)
    } finally {
      setScheduleMetaLoading((prev) => ({ ...prev, [scheduleId]: false }))
    }
  }, [scheduleMetaById, scheduleMetaLoading])

  // Re-sync local state when the server sends fresh props. useState only seeds on
  // first mount, so after a workspace switch — which calls router.refresh() and
  // re-runs the server component with the new workspace's executions — the list
  // would otherwise keep showing the previous workspace's data. The prop references
  // only change on an actual server re-render (not on client interval refreshes), so
  // this can't clobber fresher client-fetched data.
  useEffect(() => {
    setDisplayExecutions(initialExecutions)
    setStatsState(stats)
    setPipelineOptions(pipelines)
  }, [initialExecutions, stats, pipelines])

  // Auto-refresh for running executions
  useEffect(() => {
    const hasRunning = displayExecutions.some(e => e.status === "running" || e.status === "pending")
    if (!hasRunning) return

    const interval = setInterval(fetchExecutions, 5000)
    return () => clearInterval(interval)
  }, [displayExecutions, fetchExecutions])

  // Initial load: if SSR couldn't fetch (no user context on server), hydrate via client fetch.
  useEffect(() => {
    if (initialExecutions.length > 0) return
    fetchExecutions()
  }, [fetchExecutions, initialExecutions.length])

  const handleCancelExecution = async (id: string) => {
    try {
      await cancelExecution(id)
      // Optimistic update
      setDisplayExecutions(prev => 
        prev.map(e => e.id === id ? { ...e, status: "cancelled", finishedAt: new Date() } : e)
      )
      // Trigger refresh to confirm
      fetchExecutions()
    } catch (error) {
      console.error("Failed to cancel execution:", error)
    }
  }

  const handleRefresh = async () => {
    setIsRefreshing(true)
    await fetchExecutions()
    setTimeout(() => setIsRefreshing(false), 500)
  }

  const calculateDuration = (startedAt: Date, finishedAt: Date | null) => {
    const start = new Date(startedAt)
    const end = finishedAt ? new Date(finishedAt) : new Date()
    const diffMs = end.getTime() - start.getTime()
    
    if (diffMs < 1000) return `${diffMs}ms`
    if (diffMs < 60000) return `${Math.round(diffMs / 1000)}s`
    if (diffMs < 3600000) return `${Math.round(diffMs / 60000)}m ${Math.round((diffMs % 60000) / 1000)}s`
    return `${Math.round(diffMs / 3600000)}h ${Math.round((diffMs % 3600000) / 60000)}m`
  }

  return (
    <div className="space-y-6">
      {/* Stats Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatsCard 
          icon={Activity} 
          label="Total Runs" 
          value={statsState.total} 
          className="bg-gradient-to-br from-zinc-50 to-zinc-100/50 dark:from-zinc-900 dark:to-zinc-800/50 border-zinc-200 dark:border-zinc-800"
          iconClass="text-zinc-600 dark:text-zinc-400"
          bgClass="bg-zinc-200/50 dark:bg-zinc-700/50"
        />
        <StatsCard 
          icon={Loader2} 
          label="Running" 
          value={statsState.running} 
          className="bg-gradient-to-br from-blue-50 to-blue-100/50 dark:from-blue-950/30 dark:to-blue-900/20 border-blue-200 dark:border-blue-800"
          iconClass="text-blue-600 dark:text-blue-400 animate-spin"
          textClass="text-blue-600 dark:text-blue-400"
          bgClass="bg-blue-200/50 dark:bg-blue-700/30"
        />
        <StatsCard 
          icon={CheckCircle2} 
          label="Successful" 
          value={statsState.success} 
          className="bg-gradient-to-br from-emerald-50 to-emerald-100/50 dark:from-emerald-950/30 dark:to-emerald-900/20 border-emerald-200 dark:border-emerald-800"
          iconClass="text-emerald-600 dark:text-emerald-400"
          textClass="text-emerald-600 dark:text-emerald-400"
          bgClass="bg-emerald-200/50 dark:bg-emerald-700/30"
        />
        <StatsCard 
          icon={XCircle} 
          label="Failed" 
          value={statsState.failed} 
          className="bg-gradient-to-br from-red-50 to-red-100/50 dark:from-red-950/30 dark:to-red-900/20 border-red-200 dark:border-red-800"
          iconClass="text-red-600 dark:text-red-400"
          textClass="text-red-600 dark:text-red-400"
          bgClass="bg-red-200/50 dark:bg-red-700/30"
        />
      </div>

      {/* Filters */}
      <div className="flex flex-col sm:flex-row gap-3">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-400" />
          <Input
            placeholder="Search by pipeline name or execution ID..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-9 bg-white dark:bg-zinc-900"
          />
        </div>
        
        <div className="flex gap-2">
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" className="gap-2 bg-white dark:bg-zinc-900">
                <Filter className="h-4 w-4 text-zinc-500" />
                <span className="text-zinc-700 dark:text-zinc-300">
                  {statusFilter ? statusConfig[statusFilter as keyof typeof statusConfig]?.label : "All Status"}
                </span>
                <ChevronDown className="h-4 w-4 text-zinc-400" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onClick={() => setStatusFilter(null)}>
                All Status
              </DropdownMenuItem>
              {Object.entries(statusConfig).map(([key, config]) => {
                const Icon = config.icon
                return (
                  <DropdownMenuItem key={key} onClick={() => setStatusFilter(key)}>
                    <Icon className={cn("h-4 w-4 mr-2", config.color)} />
                    {config.label}
                  </DropdownMenuItem>
                )
              })}
            </DropdownMenuContent>
          </DropdownMenu>

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" className="gap-2 bg-white dark:bg-zinc-900">
                <GitBranch className="h-4 w-4 text-zinc-500" />
                <span className="text-zinc-700 dark:text-zinc-300">
                  {pipelineFilter 
                    ? pipelineOptions.find(p => p.id === pipelineFilter)?.name || "Pipeline"
                    : "All Pipelines"
                  }
                </span>
                <ChevronDown className="h-4 w-4 text-zinc-400" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="max-h-64 overflow-auto">
              <DropdownMenuItem onClick={() => setPipelineFilter(null)}>
                All Pipelines
              </DropdownMenuItem>
              {pipelineOptions.map((pipeline) => (
                <DropdownMenuItem key={pipeline.id} onClick={() => setPipelineFilter(pipeline.id)}>
                  {pipeline.name}
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>

          <Button 
            variant="outline" 
            size="icon" 
            onClick={handleRefresh}
            disabled={isRefreshing}
            className="bg-white dark:bg-zinc-900"
            aria-label="Refresh executions"
          >
            <RotateCcw className={cn("h-4 w-4 text-zinc-500", isRefreshing && "animate-spin")} />
          </Button>
        </div>
      </div>

      {/* Executions List */}
      {filteredExecutions.length === 0 ? (
        <EmptyState 
          isFiltered={!!(searchQuery || statusFilter || pipelineFilter)} 
          onClear={() => {
            setSearchQuery("")
            setStatusFilter(null)
            setPipelineFilter(null)
          }}
        />
      ) : (
        <div className="space-y-3">
          {filteredExecutions.map((execution) => {
            const config = statusConfig[execution.status as keyof typeof statusConfig] || statusConfig.pending
            const StatusIcon = config.icon
            const duration = calculateDuration(execution.startedAt, execution.finishedAt)
            const trigger = (execution.triggerSource || "manual").toLowerCase()

            return (
              <Card 
                key={execution.id} 
                className={cn(
                  "group hover:border-zinc-300 dark:hover:border-zinc-700 transition-all duration-200",
                  execution.status === "running" && "border-blue-300 dark:border-blue-700 shadow-sm shadow-blue-100 dark:shadow-blue-900/20"
                )}
              >
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4 flex-1 min-w-0">
                      <div className={cn(
                        "flex h-11 w-11 shrink-0 items-center justify-center rounded-lg shadow-sm",
                        config.bg
                      )}>
                        <StatusIcon className={cn(
                          "h-5 w-5",
                          config.color,
                          config.animate && "animate-spin"
                        )} />
                      </div>
                      
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-1.5">
                          <Link
                            href={`/executions/${execution.id}`}
                            className="text-base font-semibold text-zinc-900 dark:text-white hover:text-violet-600 dark:hover:text-violet-400 transition-colors truncate"
                          >
                            {execution.pipeline.name}
                          </Link>
                          <Badge variant={config.variant} className="shrink-0 px-2 py-0.5">
                            {config.label}
                          </Badge>
                          {trigger === "scheduled" && execution.scheduleId ? (
                            <Link
                              href={`/pipelines/${execution.pipelineId}?scheduleId=${encodeURIComponent(
                                execution.scheduleId
                              )}#schedules`}
                              className="shrink-0"
                              onMouseEnter={() => ensureScheduleMeta(execution.pipelineId, execution.scheduleId as string)}
                              title={(() => {
                                const sid = execution.scheduleId as string
                                const meta = scheduleMetaById[sid]
                                const loading = scheduleMetaLoading[sid]
                                if (meta) {
                                  const parts = [meta.title]
                                  if (meta.details) parts.push(meta.details)
                                  parts.push(`Timezone: ${meta.timezone}`)
                                  parts.push("Click to view schedule")
                                  return parts.join(" • ")
                                }
                                if (loading) return "Loading schedule…"
                                return "Triggered by schedule • Click to view schedule"
                              })()}
                            >
                              <Badge
                                variant="outline"
                                className="px-2 py-0.5 bg-zinc-50 dark:bg-zinc-900 hover:bg-zinc-100 dark:hover:bg-zinc-800 cursor-pointer"
                              >
                                <CalendarDays className="h-3.5 w-3.5 mr-1" />
                                Scheduled
                              </Badge>
                            </Link>
                          ) : (
                            <Badge
                              variant="outline"
                              className="shrink-0 px-2 py-0.5 bg-zinc-50 dark:bg-zinc-900"
                              title="Triggered manually"
                            >
                              <Hand className="h-3.5 w-3.5 mr-1" />
                              Manual
                            </Badge>
                          )}
                        </div>
                        
                        <div className="flex items-center flex-wrap gap-x-4 gap-y-2 text-xs text-zinc-500">
                          <span className="flex items-center gap-1.5" title="Start time">
                            <Clock className="h-3.5 w-3.5" />
                            {formatDateTime(execution.startedAt)}
                          </span>
                          <span className="flex items-center gap-1.5" title="Duration">
                            <Timer className="h-3.5 w-3.5" />
                            {duration}
                          </span>
                          <span className="flex items-center gap-1.5 font-mono text-zinc-400 bg-zinc-100 dark:bg-zinc-800 px-1.5 py-0.5 rounded text-[10px]" title="Execution ID">
                            #{execution.id.slice(0, 8)}
                          </span>
                        </div>
                        
                        {execution.error && (() => {
                          const humanErr = formatErrorMessage(execution.error)
                          return humanErr ? (
                            <div className="flex items-center gap-1.5 mt-2 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-950/30 px-2 py-1 rounded-md border border-red-100 dark:border-red-900/50 w-fit max-w-full">
                              <AlertTriangle className="h-3 w-3 shrink-0" />
                              <span className="truncate">{humanErr}</span>
                            </div>
                          ) : null
                        })()}
                      </div>
                    </div>
                    
                    <div className="flex items-center gap-2 ml-4">
                      {(execution.status === "running" || execution.status === "pending") && (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handleCancelExecution(execution.id)}
                          className="text-red-600 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-900/20 border-red-200 dark:border-red-900/50 h-9"
                        >
                          <StopCircle className="h-4 w-4 mr-1.5" />
                          Cancel
                        </Button>
                      )}
                      
                      <Link href={`/executions/${execution.id}`}>
                        <Button variant="ghost" size="sm" className="text-zinc-600 hover:text-zinc-900 h-9 group-hover:bg-zinc-100 dark:group-hover:bg-zinc-800">
                          Details
                          <ArrowRight className="h-4 w-4 ml-1.5 opacity-50 group-hover:opacity-100 transition-opacity" />
                        </Button>
                      </Link>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )
          })}
        </div>
      )}
    </div>
  )
}

function StatsCard({ icon: Icon, label, value, className, iconClass, textClass, bgClass }: any) {
  return (
    <Card className={cn(className)}>
      <CardContent className="p-4">
        <div className="flex items-center gap-3">
          <div className={cn("p-2 rounded-lg", bgClass)}>
            <Icon className={cn("h-5 w-5", iconClass)} />
          </div>
          <div>
            <p className={cn("text-2xl font-bold", textClass || "text-zinc-900 dark:text-white")}>{value}</p>
            <p className={cn("text-xs", textClass ? "opacity-80" : "text-zinc-500")}>{label}</p>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

function EmptyState({ isFiltered, onClear }: { isFiltered: boolean; onClear: () => void }) {
  return (
    <Card className="border-dashed border-2 bg-zinc-50/50 dark:bg-zinc-900/50">
      <CardContent className="flex flex-col items-center justify-center py-16">
        <div className="flex h-16 w-16 items-center justify-center rounded-full bg-white dark:bg-zinc-800 shadow-sm mb-4 border border-zinc-100 dark:border-zinc-700">
          <Activity className="h-8 w-8 text-zinc-400" />
        </div>
        <h3 className="text-lg font-semibold text-zinc-900 dark:text-white mb-1">
          {isFiltered ? "No matching executions" : "No executions yet"}
        </h3>
        <p className="text-sm text-zinc-500 dark:text-zinc-400 mb-6 text-center max-w-sm">
          {isFiltered 
            ? "Try adjusting your filters to find what you're looking for"
            : "Run a pipeline to see execution history and performance metrics here"
          }
        </p>
        {isFiltered ? (
          <Button variant="outline" onClick={onClear}>
            Clear Filters
          </Button>
        ) : (
          <Link href="/pipelines">
            <Button className="bg-violet-600 hover:bg-violet-700 text-white gap-2 shadow-lg shadow-violet-200 dark:shadow-violet-900/20">
              <Play className="h-4 w-4" />
              Run a Pipeline
            </Button>
          </Link>
        )}
      </CardContent>
    </Card>
  )
}
