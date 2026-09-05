"use client"

import { useEffect, useState, useCallback } from "react"
import { useRouter } from "next/navigation"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Loader2, Clock, RefreshCw, Eye, RotateCcw } from "lucide-react"
import { formatRelativeTime, formatDateTime } from "@/lib/utils"
import { listExecutionsResponse } from "@/lib/api/executions"
import {
  executePipelineWithRunMode,
  AssessmentRequiredError,
  type AssessmentReport,
} from "@/lib/api/pipelines"
import { PreMigrationAssessmentModal } from "@/components/pipeline/PreMigrationAssessmentModal"
import {
  ExecutionDetailDialog,
  statusPresentation,
  type ExecutionRow as Execution,
} from "@/components/pipeline/ExecutionDetailDialog"
import { toast } from "sonner"

interface ExecutionHistoryTabProps {
  pipelineId: string
}

function formatDuration(startTime: string, endTime?: string | null): string {
  const start = new Date(startTime).getTime()
  const end = endTime ? new Date(endTime).getTime() : Date.now()
  const durationMs = end - start
  
  const seconds = Math.floor(durationMs / 1000)
  const minutes = Math.floor(seconds / 60)
  const hours = Math.floor(minutes / 60)
  
  if (hours > 0) {
    return `${hours}h ${minutes % 60}m`
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`
  }
  return `${seconds}s`
}

export function ExecutionHistoryTab({ pipelineId }: ExecutionHistoryTabProps) {
  const router = useRouter()
  const [executions, setExecutions] = useState<Execution[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [statusFilter, setStatusFilter] = useState("all")
  const [selectedExecution, setSelectedExecution] = useState<Execution | null>(null)
  const [currentPage, setCurrentPage] = useState(1)
  const perPage = 25

  // Pre-migration assessment modal state. The first POST /run returns
  // 422 with an assessment payload when warnings need acking. We hold
  // the report + the run mode the user originally clicked so the
  // Proceed button can re-issue with ack_warnings: true.
  const [assessmentReport, setAssessmentReport] = useState<AssessmentReport | null>(null)
  const [pendingRunMode, setPendingRunMode] = useState<"resume" | "reload" | null>(null)
  const [submittingProceed, setSubmittingProceed] = useState(false)

  // Try a run; if the gateway returns the assessment envelope, surface
  // the modal instead of toasting an error. ackWarnings=false on first
  // attempt; true on the Proceed retry.
  const tryRun = useCallback(
    async (
      runMode: "resume" | "reload",
      ackWarnings: boolean,
      nominatedKeys?: Record<string, string[]>,
    ) => {
      try {
        const res = await executePipelineWithRunMode(pipelineId, runMode, { ackWarnings, nominatedKeys })
        toast.success(`Pipeline re-run started (${runMode})`)
        router.push(`/executions/${res.execution_id}`)
        return { ok: true as const }
      } catch (e: any) {
        if (e instanceof AssessmentRequiredError) {
          setAssessmentReport(e.report)
          setPendingRunMode(runMode)
          return { ok: false as const, awaitingAck: true }
        }
        toast.error(e?.message || `Failed to retry (${runMode})`)
        return { ok: false as const, awaitingAck: false }
      }
    },
    [pipelineId, router],
  )

  const fetchExecutions = useCallback(async () => {
    setLoading(true)
    setError(null)

    try {
      const resp = await listExecutionsResponse({
        pipelineId,
        status: statusFilter !== "all" ? statusFilter : undefined,
        // Keep a wide window for accurate pagination/search within the tab.
        limit: 250,
      })

      const raw = (resp.executions || []) as any[]
      const normalized: Execution[] = raw.map((e) => ({
        id: String(e.id || ""),
        pipeline_id: String(e.pipeline_id || e.pipelineId || pipelineId),
        status: String(e.status || "pending"),
        start_time: String(e.start_time || e.startedAt || ""),
        end_time: e.end_time ?? e.finishedAt ?? null,
        error_message: e.error_message ?? e.error ?? null,
        metrics: (e.metrics ?? null) as Execution['metrics'],
        // The handler emits these four (`pipelines.go:3602-3621`); the mapper used
        // to drop them on the floor, so the dialog could not have shown how a run
        // was triggered even though the answer was already in the response.
        trigger_source: e.trigger_source ?? null,
        schedule_id: e.schedule_id ?? null,
        scheduled_time: e.scheduled_time ?? null,
        pipeline_name: e.pipeline_name ?? null,
      }))

      setExecutions(normalized.filter((e) => Boolean(e.id)))
    } catch (err: any) {
      setError(err?.message || "Failed to load execution history")
    } finally {
      setLoading(false)
    }
  }, [pipelineId, statusFilter])

  useEffect(() => {
    fetchExecutions()
  }, [fetchExecutions])

  const filteredExecutions = executions.slice((currentPage - 1) * perPage, currentPage * perPage)
  const totalPages = Math.ceil(executions.length / perPage)

  // The next-older run, for the dialog's delta. The list is newest-first, so it
  // is simply the following index — and it is already in state, so comparing two
  // runs costs no extra request. Undefined for the oldest run in the window,
  // which is the honest answer: we have nothing to compare it against.
  const selectedIndex = selectedExecution
    ? executions.findIndex((e) => e.id === selectedExecution.id)
    : -1
  const previousExecution =
    selectedIndex >= 0 ? executions[selectedIndex + 1] ?? null : null

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex items-center gap-4">
        {/*
          Reset to page 1 with the filter. The fetch callback depends on the
          filter but the page did not, and the pager only renders when
          totalPages > 1 — so filtering to a sub-page-sized result while on page
          3 left an empty slice rendering "No execution history yet", with no
          control on screen to get back. The rows existed; the UI denied it.
        */}
        <Select
          value={statusFilter}
          onValueChange={(value) => {
            setStatusFilter(value)
            setCurrentPage(1)
          }}
        >
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="All Statuses" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Statuses</SelectItem>
            <SelectItem value="success">Success</SelectItem>
            <SelectItem value="completed">Completed</SelectItem>
            <SelectItem value="failed">Failed</SelectItem>
            <SelectItem value="running">Running</SelectItem>
            <SelectItem value="cancelled">Cancelled</SelectItem>
          </SelectContent>
        </Select>

        <Button variant="outline" size="sm" onClick={fetchExecutions}>
          <RefreshCw className="h-4 w-4 mr-2" />
          Refresh
        </Button>
      </div>

      {/* Table */}
      {loading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-violet-600" />
        </div>
      ) : error ? (
        <div className="flex flex-col items-center justify-center py-12">
          <div className="text-red-600 mb-4">{error}</div>
          <Button onClick={fetchExecutions}>Retry</Button>
        </div>
      ) : filteredExecutions.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <Clock className="h-12 w-12 text-zinc-400 mb-4" />
          <p className="text-sm font-medium text-zinc-900 dark:text-zinc-100">
            No execution history yet
          </p>
          <p className="text-xs text-zinc-500 dark:text-zinc-400 mt-1">
            Run this pipeline to see execution results
          </p>
        </div>
      ) : (
        <>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Execution ID</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Started</TableHead>
                <TableHead>Duration</TableHead>
                <TableHead>Records</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredExecutions.map((execution) => {
                const statusConfig = statusPresentation(execution.status)
                const StatusIcon = statusConfig.icon

                return (
                  <TableRow key={execution.id}>
                    <TableCell className="font-mono text-xs">
                      {execution.id.slice(0, 8)}...
                    </TableCell>

                    <TableCell>
                      <Badge className={`gap-1 ${statusConfig.color}`} variant="outline">
                        <StatusIcon className={`h-3 w-3 ${execution.status === "running" ? "animate-spin" : ""}`} />
                        {statusConfig.label}
                      </Badge>
                    </TableCell>

                    <TableCell>
                      <div className="text-sm">
                        {formatRelativeTime(new Date(execution.start_time))}
                      </div>
                      <div className="text-xs text-zinc-500">
                        {formatDateTime(new Date(execution.start_time))}
                      </div>
                    </TableCell>

                    <TableCell>
                      {execution.end_time ? (
                        formatDuration(execution.start_time, execution.end_time)
                      ) : execution.status === "running" ? (
                        <span className="text-zinc-400">In progress</span>
                      ) : (
                        <span className="text-zinc-400">—</span>
                      )}
                    </TableCell>

                    <TableCell>
                      {execution.metrics?.records_processed !== undefined
                        ? execution.metrics.records_processed.toLocaleString()
                        : "—"}
                    </TableCell>

                    <TableCell className="text-right">
                      <div className="flex items-center justify-end gap-2">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setSelectedExecution(execution)}
                        >
                          <Eye className="h-4 w-4 mr-1" />
                          View
                        </Button>
                        {execution.status === "failed" && (
                          <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                              {/*
                                Named for what it does. This control starts a new
                                run of the PIPELINE — `tryRun` closes over
                                pipelineId and the selected execution is never an
                                input — and there is no execution-scoped retry
                                endpoint to call. Labelled "Retry" inside a row it
                                cannot reach, it promised to re-run that run.
                              */}
                              <Button variant="ghost" size="sm">
                                <RotateCcw className="h-4 w-4 mr-1" />
                                Re-run pipeline
                              </Button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end">
                              <DropdownMenuItem
                                onClick={() => void tryRun("resume", false)}
                              >
                                Resume from the pipeline&apos;s checkpoints
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                onClick={() => void tryRun("reload", false)}
                              >
                                Reload everything from scratch
                              </DropdownMenuItem>
                            </DropdownMenuContent>
                          </DropdownMenu>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between pt-4">
              <div className="text-sm text-zinc-600 dark:text-zinc-400">
                Page {currentPage} of {totalPages}
              </div>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={currentPage === 1}
                  onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                >
                  Previous
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={currentPage === totalPages}
                  onClick={() => setCurrentPage((p) => p + 1)}
                >
                  Next
                </Button>
              </div>
            </div>
          )}
        </>
      )}

      {/*
        The detail dialog is its own component because it now asks the API
        questions the list row cannot answer. Previously it re-rendered the row
        object it was handed — so no amount of layout work could have made it
        show more than the row already showed.
      */}
      <ExecutionDetailDialog
        execution={selectedExecution}
        pipelineId={pipelineId}
        previousExecution={previousExecution}
        onClose={() => setSelectedExecution(null)}
        onRerun={(mode) => {
          // Resume the PIPELINE from the PIPELINE's checkpoints (the safe
          // default). Not this execution's — `tryRun` never sees the selected
          // execution, and for a week-old failure the pipeline's checkpoints are
          // somewhere else entirely.
          setSelectedExecution(null)
          void tryRun(mode, false)
        }}
        onOpenFullDetail={(executionId) => {
          // Capture the id before closing — clearing the selection unmounts the
          // dialog and with it the value we are routing to.
          setSelectedExecution(null)
          router.push(`/executions/${executionId}`)
        }}
      />

      <PreMigrationAssessmentModal
        open={!!assessmentReport}
        onOpenChange={(open) => {
          if (!open) {
            setAssessmentReport(null)
            setPendingRunMode(null)
            setSubmittingProceed(false)
          }
        }}
        report={assessmentReport}
        submitting={submittingProceed}
        onProceed={async (nominatedKeys) => {
          if (!pendingRunMode) return
          setSubmittingProceed(true)
          const result = await tryRun(pendingRunMode, true, nominatedKeys)
          // On success or non-assessment error, close the modal.
          // (tryRun already cleared/changed state in the error path.)
          if (!result.awaitingAck) {
            setAssessmentReport(null)
            setPendingRunMode(null)
          }
          setSubmittingProceed(false)
        }}
      />
    </div>
  )
}
