"use client"

import { useEffect, useState, useCallback } from "react"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { useRouter } from "next/navigation"
import { PageHeader } from "@/components/layout/PageHeader"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent } from "@/components/ui/card"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Search, Filter, GitBranch, Sparkles, Loader2 } from "lucide-react"
import { PipelinesTable } from "@/components/pipeline/PipelinesTable"
import type { PipelineListItem } from "@/components/pipeline/PipelinesTable"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { captureWorkspace, onActiveWorkspaceChange } from "@/lib/workspace/active-workspace"
import { toast } from "sonner"
import { classifyError, type AppError } from "@/lib/utils/error-handling"
import { ErrorPage } from "@/components/ui/error-banner"
import type { ApiErrorBody } from "@/lib/api/types"
import { UpgradeModal } from "@/components/plan/UpgradeModal"
import {
  AssessmentRequiredError,
  PlanLimitError,
  executePipelineWithRunMode,
  type AssessmentReport,
  type PlanLimitPayload,
} from "@/lib/api/pipelines"
import { PreMigrationAssessmentModal } from "@/components/pipeline/PreMigrationAssessmentModal"

interface PipelinesPageState {
  pipelines: PipelineListItem[]
  pagination: {
    page: number
    per_page: number
    total: number
    total_pages: number
    has_next: boolean
    has_prev: boolean
  }
  loading: boolean
  error: AppError | null
}

export function PipelinesPageClient() {
  const router = useRouter()

  // State
  // Role gate: creating pipelines is member+ (mirrors the backend
  // requireWorkspaceRole on CreatePipeline). Viewers see the list read-only.
  const { can } = useWorkspaceRole()
  const canCreate = can("create_pipeline")
  const [isAnyMenuOpen, setIsAnyMenuOpen] = useState(false)
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [deleteTargetId, setDeleteTargetId] = useState<string | null>(null)
  const [planLimitPayload, setPlanLimitPayload] = useState<PlanLimitPayload | null>(null)

  // Pre-migration assessment modal state. handleRunPipeline goes through
  // executePipelineWithRunMode, which gates on a 422 assessment the same
  // way PipelineActions does; without this the run silently fails.
  const [assessmentReport, setAssessmentReport] = useState<AssessmentReport | null>(null)
  const [pendingRunId, setPendingRunId] = useState<string | null>(null)
  const [submittingProceed, setSubmittingProceed] = useState(false)
  const [state, setState] = useState<PipelinesPageState>({
    pipelines: [],
    pagination: {
      page: 1,
      per_page: 25,
      total: 0,
      total_pages: 0,
      has_next: false,
      has_prev: false,
    },
    loading: true,
    error: null,
  })

  // Filters
  const [searchQuery, setSearchQuery] = useState("")
  const [statusFilter, setStatusFilter] = useState("all")
  const [typeFilter, setTypeFilter] = useState("all")
  const [createdFrom, setCreatedFrom] = useState("")
  const [createdTo, setCreatedTo] = useState("")
  const [currentPage, setCurrentPage] = useState(1)

  // Panel state removed - redirecting to /chat instead

  // Fetch pipelines
  const fetchPipelines = useCallback(async () => {
    // Don't blank the table during background refresh/polling.
    setState((prev) => ({ ...prev, loading: prev.pipelines.length === 0, error: null }))

    // A switch mid-flight makes this response the previous workspace's rows.
    const isStale = captureWorkspace()

    try {
      const params = new URLSearchParams()
      params.set("page", String(currentPage))
      params.set("per_page", "25")

      if (searchQuery.trim()) params.set("q", searchQuery.trim())
      if (statusFilter !== "all") params.set("status", statusFilter)
      if (typeFilter !== "all") params.set("type", typeFilter)
      if (createdFrom) params.set("created_from", createdFrom)
      if (createdTo) params.set("created_to", createdTo)

      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.LIST}?${params}`, {
        cache: "no-store",
      })

      if (!res.ok) {
        const body = await res.json().catch(() => ({}))
        throw Object.assign(new Error(), { statusCode: res.status, message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || `HTTP ${res.status}` })
      }

      const data = await res.json()
      if (isStale()) return

      setState({
        pipelines: data.pipelines || [],
        pagination: data.pagination || {
          page: 1,
          per_page: 25,
          total: data.total || 0,
          total_pages: Math.ceil((data.total || 0) / 25),
          has_next: false,
          has_prev: false,
        },
        loading: false,
        error: null,
      })
    } catch (err) {
      if (isStale()) return
      setState((prev) => ({
        ...prev,
        loading: false,
        error: classifyError(err, "pipeline.load"),
      }))
    }
  }, [currentPage, searchQuery, statusFilter, typeFilter, createdFrom, createdTo])

  // Initial fetch and polling for running pipelines
  useEffect(() => {
    fetchPipelines()
  }, [fetchPipelines])

  // Aggregate stats from /api/v1/pipelines/stats. Backend already had
  // this endpoint live; the page was computing counts from the loaded
  // list which is wrong when pagination is active. Single GET on mount
  // gives accurate totals (the endpoint already returns global counts, so
  // it does not need to re-fetch when the loaded list length changes).
  type PipelineStats = {
    total?: number
    active?: number
    executions?: {
      running?: number
      completed?: number
      failed?: number
      total?: number
    }
  }
  const [pipelineStats, setPipelineStats] = useState<PipelineStats | null>(null)
  const fetchStats = useCallback(async () => {
    const isStale = captureWorkspace()
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.STATS, { cache: "no-store" })
      if (!res.ok) return
      const data = await res.json()
      if (isStale()) return
      setPipelineStats({
        total: typeof data?.pipelines?.total === "number" ? data.pipelines.total : undefined,
        active: typeof data?.pipelines?.active === "number" ? data.pipelines.active : undefined,
        executions: data?.executions || undefined,
      })
    } catch {
      /* best-effort */
    }
  }, [])
  // Fetch on mount; the stats endpoint's counts are independent of the
  // currently-loaded page, so it does not re-fetch on list change (ISSUE-007).
  useEffect(() => {
    void fetchStats()
  }, [fetchStats])

  // Poll while any pipeline is running OR parked on user input, so the list
  // refreshes when a HITL prompt is answered elsewhere and the run resumes.
  useEffect(() => {
    const hasRunning = state.pipelines.some(
      (p) => p.derived_status === "running" || p.derived_status === "waiting_for_user",
    )
    if (!hasRunning) return
    if (isAnyMenuOpen) return

    const interval = setInterval(fetchPipelines, 5000)

    return () => {
      clearInterval(interval)
    }
  }, [state.pipelines, fetchPipelines, isAnyMenuOpen])

  // Re-fetch list + stats when the active workspace changes (header switcher or
  // another tab). router.refresh() only re-runs server components; this is a
  // client page, so without this the list keeps showing the previous
  // workspace's pipelines until a manual reload. (R4)
  useEffect(
    () =>
      onActiveWorkspaceChange(() => {
        void fetchPipelines()
        void fetchStats()
      }),
    [fetchPipelines, fetchStats],
  )

  const handleCreateNew = () => {
    router.push("/chat")
  }

  // Pipeline actions. Runs via executePipelineWithRunMode so the 422
  // pre-migration assessment gate surfaces the modal (same as PipelineActions)
  // and 402 plan limits surface the UpgradeModal — a raw run POST silently
  // failed on both. Returns whether we're awaiting an ack so the modal's
  // Proceed callback can keep itself open on a fresh 422.
  const handleRunPipeline = async (
    id: string,
    ackWarnings = false,
    nominatedKeys?: Record<string, string[]>,
  ) => {
    try {
      await executePipelineWithRunMode(id, "resume", { ackWarnings, nominatedKeys })
      toast.success("Pipeline started")
      fetchPipelines()
      return { awaitingAck: false }
    } catch (err) {
      if (err instanceof AssessmentRequiredError) {
        setAssessmentReport(err.report)
        setPendingRunId(id)
        return { awaitingAck: true }
      }
      if (err instanceof PlanLimitError) {
        setPlanLimitPayload(err.payload)
        return { awaitingAck: false }
      }
      const e = classifyError(err, "pipeline.run")
      toast.error(e.title, { description: e.hint ?? e.message })
      return { awaitingAck: false }
    }
  }

  const handlePausePipeline = async (id: string) => {
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.PAUSE(id), { method: "POST" })
      if (!res.ok) {
        const body = await res.json().catch(() => ({}))
        const e = classifyError(Object.assign(new Error(), { statusCode: res.status, message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || res.statusText }), "pipeline.pause")
        toast.error(e.title, { description: e.hint ?? e.message })
        return
      }
      toast.success("Pipeline paused")
      fetchPipelines()
    } catch (err) {
      const e = classifyError(err, "pipeline.pause")
      toast.error(e.title, { description: e.hint ?? e.message })
    }
  }

  const handleResumePipeline = async (id: string) => {
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.RESUME(id), { method: "POST" })
      if (!res.ok) {
        const body = await res.json().catch(() => ({}))
        const e = classifyError(Object.assign(new Error(), { statusCode: res.status, message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || res.statusText }), "pipeline.resume")
        toast.error(e.title, { description: e.hint ?? e.message })
        return
      }
      toast.success("Pipeline resumed")
      fetchPipelines()
    } catch (err) {
      const e = classifyError(err, "pipeline.resume")
      toast.error(e.title, { description: e.hint ?? e.message })
    }
  }

  const handleEditPipeline = (id: string) => {
    // TODO: Create draft from pipeline and open panel
    router.push(`/pipelines/${id}`)
  }

  const handleDeletePipeline = async (id: string) => {
    setDeleteTargetId(id)
    setDeleteDialogOpen(true)
  }

  const confirmDeletePipeline = async () => {
    const id = deleteTargetId
    if (!id) return

    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.DELETE(id), {
        method: "DELETE",
      })

      if (!res.ok) {
        const body = await res.json().catch(() => ({}))
        const e = classifyError(Object.assign(new Error(), { statusCode: res.status, message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || res.statusText }), "pipeline.delete")
        toast.error(e.title, { description: e.hint ?? e.message })
        return
      }

      toast.success("Pipeline deleted")
      setDeleteDialogOpen(false)
      setDeleteTargetId(null)
      fetchPipelines()
    } catch (err) {
      const e = classifyError(err, "pipeline.delete")
      toast.error(e.title, { description: e.hint ?? e.message })
    }
  }

  const handleExportConfig = async (id: string) => {
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.GET(id))
      if (!res.ok) {
        const body = await res.json().catch(() => ({}))
        const e = classifyError(Object.assign(new Error(), { statusCode: res.status, message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || res.statusText }), "pipeline.export")
        toast.error(e.title, { description: e.hint ?? e.message })
        return
      }
      const data = await res.json()
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" })
      const url = URL.createObjectURL(blob)
      const a = document.createElement("a")
      a.href = url
      a.download = `pipeline-${id}.json`
      a.click()
      URL.revokeObjectURL(url)
    } catch (err) {
      const e = classifyError(err, "pipeline.export")
      toast.error(e.title, { description: e.hint ?? e.message })
    }
  }

  const clearFilters = () => {
    setSearchQuery("")
    setStatusFilter("all")
    setTypeFilter("all")
    setCreatedFrom("")
    setCreatedTo("")
    setCurrentPage(1)
  }

  const hasActiveFilters =
    searchQuery || statusFilter !== "all" || typeFilter !== "all" || createdFrom || createdTo

  return (
    <div className="space-y-6">
      <PageHeader
        heading="Pipelines"
        description="Create and manage your data pipelines with AI assistance"
      />

      {/* Aggregate stats from /api/v1/pipelines/stats. Backend ships the
          endpoint already; UI was computing counts from the current page
          which under-reports with pagination. */}
      {pipelineStats && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-3">
            <div className="text-[11px] uppercase tracking-wide text-zinc-500">Pipelines</div>
            <div className="text-xl font-semibold text-zinc-900 dark:text-zinc-100">
              {pipelineStats.total ?? 0}
            </div>
          </div>
          <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-3">
            <div className="text-[11px] uppercase tracking-wide text-zinc-500">Running</div>
            <div className="text-xl font-semibold text-blue-700 dark:text-blue-300">
              {pipelineStats.executions?.running ?? 0}
            </div>
          </div>
          <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-3">
            <div className="text-[11px] uppercase tracking-wide text-zinc-500">Successful runs</div>
            <div className="text-xl font-semibold text-emerald-700 dark:text-emerald-300">
              {pipelineStats.executions?.completed ?? 0}
            </div>
          </div>
          <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-3">
            <div className="text-[11px] uppercase tracking-wide text-zinc-500">Failed runs</div>
            <div className="text-xl font-semibold text-red-700 dark:text-red-300">
              {pipelineStats.executions?.failed ?? 0}
            </div>
          </div>
        </div>
      )}

      {/* Filters & Actions Bar */}
      <Card>
        <CardContent className="p-4">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div className="flex flex-col gap-3 md:flex-row md:flex-wrap md:items-center">
              {/* Search */}
              <div className="relative w-full md:w-64">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-400" />
                <Input
                  placeholder="Search pipelines..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10"
                />
              </div>

              {/* Status + Type filters side by side on mobile */}
              <div className="flex gap-2">
                <Select value={statusFilter} onValueChange={setStatusFilter}>
                  <SelectTrigger className="w-full sm:w-[160px]">
                    <SelectValue placeholder="All Statuses" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Statuses</SelectItem>
                    <SelectItem value="passed">Completed</SelectItem>
                    <SelectItem value="failed">Failed</SelectItem>
                    <SelectItem value="running">Running</SelectItem>
                    <SelectItem value="paused">Paused</SelectItem>
                    <SelectItem value="stopped">Stopped</SelectItem>
                    <SelectItem value="scheduled">Scheduled</SelectItem>
                    <SelectItem value="idle">Idle</SelectItem>
                  </SelectContent>
                </Select>

                <Select value={typeFilter} onValueChange={setTypeFilter}>
                  <SelectTrigger className="w-full sm:w-[120px]">
                    <SelectValue placeholder="All Types" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Types</SelectItem>
                    <SelectItem value="etl">ETL</SelectItem>
                    <SelectItem value="cdc">CDC</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {/* Date Range */}
              <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2">
                <input
                  type="date"
                  value={createdFrom}
                  onChange={(e) => setCreatedFrom(e.target.value)}
                  className="w-full sm:w-auto px-3 py-2 border border-zinc-200 dark:border-zinc-800 rounded-md text-sm bg-transparent"
                  placeholder="From"
                />
                <span className="hidden sm:block text-zinc-400 text-sm">to</span>
                <input
                  type="date"
                  value={createdTo}
                  onChange={(e) => setCreatedTo(e.target.value)}
                  className="w-full sm:w-auto px-3 py-2 border border-zinc-200 dark:border-zinc-800 rounded-md text-sm bg-transparent"
                  placeholder="To"
                />
              </div>

              {hasActiveFilters && (
                <Button variant="outline" size="sm" onClick={clearFilters}>
                  <Filter className="h-4 w-4 mr-2" />
                  Clear
                </Button>
              )}
            </div>

            {/* Primary action */}
            {canCreate && (
              <div className="flex justify-end">
                <Button
                  onClick={handleCreateNew}
                  className="gap-2 bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-700 hover:to-indigo-700"
                >
                  <Sparkles className="h-4 w-4" />
                  Create New
                </Button>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Pipelines Table */}
      <Card>
        <CardContent className="p-0">
          {state.loading ? (
            <div className="flex items-center justify-center py-16">
              <Loader2 className="h-8 w-8 animate-spin text-violet-600" />
            </div>
          ) : state.error ? (
            <ErrorPage error={state.error} onRetry={fetchPipelines} />
          ) : state.pipelines.length === 0 && !hasActiveFilters ? (
            // Empty state: no pipelines at all
            <div className="flex flex-col items-center justify-center py-16">
              <div className="rounded-full p-4 bg-zinc-100 dark:bg-zinc-800 mb-4">
                <GitBranch className="h-8 w-8 text-zinc-400" />
              </div>
              <h3 className="text-lg font-semibold mb-2">No pipelines yet</h3>
              <p className="text-sm text-zinc-500 dark:text-zinc-400 text-center max-w-sm mb-6">
                Create your first data pipeline to start syncing data between your sources and
                destinations.
              </p>
              {canCreate ? (
                <Button
                  onClick={handleCreateNew}
                  className="gap-2 bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-700 hover:to-indigo-700"
                >
                  <Sparkles className="h-4 w-4" />
                  Create with AI
                </Button>
              ) : (
                <p className="text-sm text-zinc-500 dark:text-zinc-400">
                  You have view-only access to this workspace.
                </p>
              )}
            </div>
          ) : state.pipelines.length === 0 && hasActiveFilters ? (
            // Empty state: filters returned no results
            <div className="flex flex-col items-center justify-center py-16">
              <div className="text-zinc-500 mb-4">No pipelines match your filters</div>
              <Button variant="outline" onClick={clearFilters}>
                Clear Filters
              </Button>
            </div>
          ) : (
            <>
              <PipelinesTable
                pipelines={state.pipelines}
                onMenuOpenChange={setIsAnyMenuOpen}
                onRunPipeline={handleRunPipeline}
                onPausePipeline={handlePausePipeline}
                onResumePipeline={handleResumePipeline}
                onEditPipeline={handleEditPipeline}
                onDeletePipeline={handleDeletePipeline}
                onExportConfig={handleExportConfig}
              />

              {/* Pagination */}
              {state.pagination.total_pages > 1 && (
                <div className="flex items-center justify-between border-t p-4">
                  <div className="text-sm text-zinc-600 dark:text-zinc-400">
                    Page {state.pagination.page} of {state.pagination.total_pages} (
                    {state.pagination.total} total)
                  </div>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={!state.pagination.has_prev}
                      onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                    >
                      Previous
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={!state.pagination.has_next}
                      onClick={() => setCurrentPage((p) => p + 1)}
                    >
                      Next
                    </Button>
                  </div>
                </div>
              )}

              <AlertDialog
                open={deleteDialogOpen}
                onOpenChange={(open) => {
                  setDeleteDialogOpen(open)
                  if (!open) setDeleteTargetId(null)
                }}
              >
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle>Delete pipeline?</AlertDialogTitle>
                    <AlertDialogDescription>
                      This will cancel any running execution, delete all history, and remove associated schedules. This
                      cannot be undone.
                    </AlertDialogDescription>
                  </AlertDialogHeader>
                  <AlertDialogFooter>
                    <AlertDialogCancel>Cancel</AlertDialogCancel>
                    <AlertDialogAction
                      className="bg-red-600 hover:bg-red-700 focus:ring-red-600"
                      onClick={() => void confirmDeletePipeline()}
                    >
                      Delete
                    </AlertDialogAction>
                  </AlertDialogFooter>
                </AlertDialogContent>
              </AlertDialog>
            </>
          )}
        </CardContent>
      </Card>

      <UpgradeModal
        payload={planLimitPayload}
        onClose={() => setPlanLimitPayload(null)}
      />

      <PreMigrationAssessmentModal
        open={!!assessmentReport}
        onOpenChange={(open) => {
          if (!open) {
            setAssessmentReport(null)
            setPendingRunId(null)
            setSubmittingProceed(false)
          }
        }}
        report={assessmentReport}
        submitting={submittingProceed}
        onProceed={async (nominatedKeys) => {
          if (!pendingRunId) return
          setSubmittingProceed(true)
          const result = await handleRunPipeline(pendingRunId, true, nominatedKeys)
          // Close on success or non-ack errors; keep open on a fresh
          // 422 (the modal report will have been replaced in state).
          if (!result.awaitingAck) {
            setAssessmentReport(null)
            setPendingRunId(null)
          }
          setSubmittingProceed(false)
        }}
      />
    </div>
  )
}

