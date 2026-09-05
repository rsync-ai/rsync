"use client"

import { useCallback, useEffect, useState } from "react"
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
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import {
  AlertTriangle,
  Check,
  CheckCircle2,
  Clock,
  Copy,
  Eye,
  HelpCircle,
  Loader2,
  RotateCcw,
  XCircle,
} from "lucide-react"
import { formatDateTime, formatRelativeTime } from "@/lib/utils"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { formatDuration } from "./DAGVisualization"
import {
  buildRunTimeline,
  computeRunDelta,
  dataMovementVerdict,
  formatBytes,
  movementHeadline,
  planStagesFromEvents,
  rollupFromSummary,
  rowsWrittenForTable,
  type RunTimeline,
  type TableStatRow,
  type TableStatsSummaryPayload,
} from "./executionSummary"

/**
 * The shape the executions list actually returns.
 *
 * The Go handler emits `trigger_source`, `scheduled_time` and `pipeline_name`
 * (`pipelines.go:3602-3621`); the previous interface declared six fields and the
 * mapper silently discarded the rest — the same unmarshal→remarshal drop that
 * bites `ValidateConnector`. Fields the server sends are declared here so a
 * reader can use them.
 */
export type ExecutionRow = {
  id: string
  pipeline_id: string
  status: string
  start_time: string
  end_time?: string | null
  error_message?: string | null
  metrics?: {
    records_processed?: number
    [key: string]: unknown
  } | null
  trigger_source?: string | null
  schedule_id?: string | null
  scheduled_time?: string | null
  pipeline_name?: string | null
}

type Diagnosis = {
  category: string
  action_label: string
  rationale: string
  confidence: number
  source_rows?: number
  landed_rows?: number
}

/** Matches the set the execution detail page fetches a diagnosis for (`[id]/page.tsx:173`). */
const FAILURE_STATUSES = new Set([
  "failed",
  "silent_drop_detected",
  "silent_partial_drop_detected",
  "waiting_for_credential_reauth",
  "waiting_for_user",
])

export function statusPresentation(status: string) {
  const configs: Record<string, { icon: React.ElementType; color: string; label: string }> = {
    running: { icon: Loader2, color: "text-blue-600", label: "Running" },
    success: { icon: CheckCircle2, color: "text-green-600", label: "Success" },
    completed: { icon: CheckCircle2, color: "text-green-600", label: "Completed" },
    failed: { icon: XCircle, color: "text-red-600", label: "Failed" },
    cancelled: { icon: XCircle, color: "text-zinc-400", label: "Cancelled" },
    pending: { icon: Clock, color: "text-zinc-500", label: "Pending" },
  }
  return configs[status] || configs.pending
}

/** A section that failed to load says so. Rendering nothing would read as "no data". */
function LoadFailure({ what }: { what: string }) {
  return (
    <div className="text-xs text-amber-700 dark:text-amber-400">
      Could not load {what}. The run may still have produced it — this is a fetch failure, not
      an empty result.
    </div>
  )
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-xs font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
      {children}
    </div>
  )
}

function TimeBreakdown({ timeline }: { timeline: RunTimeline }) {
  const timed = timeline.phases.filter((p) => p.ms !== null)
  if (timed.length === 0) return null

  const rows = [...timed]
  if (timeline.unaccountedMs !== null && timeline.unaccountedMs > 0) {
    rows.push({
      id: "__unaccounted__",
      // Named for what it is rather than hidden. On the measured prod run this is
      // ~15s of queueing and teardown — a third of the wall clock that no stage
      // claims, and previously invisible on every screen.
      label: "Queueing, startup and teardown",
      ms: timeline.unaccountedMs,
      inferred: true,
    })
  }

  const max = Math.max(...rows.map((r) => r.ms as number), 1)

  return (
    <div className="space-y-1.5">
      {rows.map((row) => (
        <div key={row.id} className="flex items-center gap-3">
          <div className="w-44 shrink-0 truncate text-xs text-zinc-700 dark:text-zinc-300">
            {row.label}
            {row.inferred && <span className="ml-1 text-zinc-400">(inferred)</span>}
          </div>
          <div className="h-2 flex-1 overflow-hidden rounded bg-zinc-100 dark:bg-zinc-800">
            <div
              className={`h-full rounded ${
                row.inferred
                  ? "bg-zinc-300 dark:bg-zinc-600"
                  : row.status === "failed"
                    ? "bg-red-500"
                    : "bg-violet-500"
              }`}
              style={{ width: `${Math.max(2, ((row.ms as number) / max) * 100)}%` }}
            />
          </div>
          <div className="w-16 shrink-0 text-right font-mono text-xs text-zinc-600 dark:text-zinc-400">
            {formatDuration(row.ms as number)}
          </div>
        </div>
      ))}
    </div>
  )
}

type Props = {
  execution: ExecutionRow | null
  pipelineId: string
  /** The next-older run in the list, used for the delta. Undefined when this is the oldest. */
  previousExecution?: ExecutionRow | null
  onClose: () => void
  onRerun?: (mode: "resume" | "reload") => void
  onOpenFullDetail?: (executionId: string) => void
}

export function ExecutionDetailDialog({
  execution,
  pipelineId,
  previousExecution,
  onClose,
  onRerun,
  onOpenFullDetail,
}: Props) {
  const executionId = execution?.id ?? null

  const [detail, setDetail] = useState<ExecutionRow | null>(null)
  const [detailFailed, setDetailFailed] = useState(false)
  const [tables, setTables] = useState<TableStatRow[] | null>(null)
  const [tableTotal, setTableTotal] = useState(0)
  const [summary, setSummary] = useState<TableStatsSummaryPayload | null>(null)
  const [statsFailed, setStatsFailed] = useState(false)
  const [stages, setStages] = useState<ReturnType<typeof planStagesFromEvents>>([])
  const [eventsFailed, setEventsFailed] = useState(false)
  const [diagnosis, setDiagnosis] = useState<Diagnosis | null>(null)
  // Keyed by execution rather than a bare boolean, so opening a different run
  // resets the "Copied" label without an effect that exists only to undo state.
  const [copiedId, setCopiedId] = useState<string | null>(null)
  const copied = copiedId !== null && copiedId === executionId
  // Load state is keyed the same way, and deliberately split in two. `/diagnose`
  // is best-effort, fetched for failures only, and nothing above it depends on
  // it — while it shared one flag with the other three requests, the outcome,
  // timing and table sections reported ITS progress instead of their own and
  // rendered "Loading…" with their data already in hand. Keying rather than a
  // bare boolean also makes a newly-opened execution read as unloaded on the
  // first paint, before the effect fires; a stale `true` there is precisely what
  // lets a dialog state a verdict it has not fetched yet.
  const [loadedFor, setLoadedFor] = useState<string | null>(null)
  const [diagnosingFor, setDiagnosingFor] = useState<string | null>(null)
  const coreLoaded = loadedFor !== null && loadedFor === executionId
  const diagnosing = diagnosingFor !== null && diagnosingFor === executionId

  const load = useCallback(async () => {
    if (!executionId || !execution) return
    setLoadedFor(null)
    setDiagnosingFor(null)
    setDetailFailed(false)
    setStatsFailed(false)
    setEventsFailed(false)
    setDiagnosis(null)

    const statsParams = new URLSearchParams({
      execution_id: executionId,
      sort: "qualified_name",
      limit: "50",
      offset: "0",
    })
    const eventParams = new URLSearchParams({ execution_id: executionId, limit: "250" })

    const [detailRes, statsRes, eventsRes] = await Promise.all([
      authFetch(API_ENDPOINTS.EXECUTIONS.GET(executionId), { cache: "no-store" }).catch(() => null),
      authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/table-stats?${statsParams}`, {
        cache: "no-store",
      }).catch(() => null),
      authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/events?${eventParams}`, {
        cache: "no-store",
      }).catch(() => null),
    ])

    if (detailRes?.ok) {
      setDetail((await detailRes.json()) as ExecutionRow)
    } else {
      // The list row we were opened from is still a real answer for the fields it
      // carries, so fall back to it rather than blanking the dialog.
      setDetail(null)
      setDetailFailed(true)
    }

    if (statsRes?.ok) {
      const data = await statsRes.json()
      setTables(Array.isArray(data?.tables) ? (data.tables as TableStatRow[]) : [])
      setSummary((data?.summary ?? null) as TableStatsSummaryPayload | null)
      setTableTotal(Number(data?.total || 0))
    } else {
      setTables(null)
      setSummary(null)
      setStatsFailed(true)
    }

    if (eventsRes?.ok) {
      const data = await eventsRes.json()
      setStages(planStagesFromEvents(Array.isArray(data?.events) ? data.events : []))
    } else {
      setStages([])
      setEventsFailed(true)
    }

    // Everything the outcome, timing and table sections render is now in state, so
    // they are done. They must not wait on the diagnosis below: it answers a
    // different question, it is fetched for failures only, and it is allowed never
    // to arrive.
    setLoadedFor(executionId)

    // Best-effort, failure statuses only — mirrors the execution detail page.
    if (FAILURE_STATUSES.has(execution.status)) {
      setDiagnosingFor(executionId)
      try {
        const res = await authFetch(API_ENDPOINTS.EXECUTIONS.DIAGNOSE(executionId), {
          cache: "no-store",
        })
        if (res.ok) {
          const json = await res.json()
          const d = json?.data
          if (d && typeof d === "object") {
            setDiagnosis({
              category: String(d.category || ""),
              action_label: String(d.action_label || ""),
              rationale: String(d.rationale || ""),
              confidence: typeof d.confidence === "number" ? d.confidence : 0,
              source_rows: typeof d.source_rows === "number" ? d.source_rows : undefined,
              landed_rows: typeof d.landed_rows === "number" ? d.landed_rows : undefined,
            })
          }
        }
      } catch {
        // A missing diagnosis hides the panel; it is not an error worth showing.
      } finally {
        setDiagnosingFor(null)
      }
    }
  }, [executionId, execution, pipelineId])

  useEffect(() => {
    if (executionId) void load()
  }, [executionId, load])

  if (!execution) return null

  const run = detail ?? execution
  const status = statusPresentation(run.status)
  const StatusIcon = status.icon

  const rollup = rollupFromSummary(summary)
  // `records_processed` is the server's own per-table sum over EVERY table, so it
  // survives the /table-stats page limit. The rollup only fills in when the
  // metric is absent, which the handler's `> 0` guard makes mean exactly zero.
  const serverRows = run.metrics?.records_processed
  const rowsWritten =
    typeof serverRows === "number" ? serverRows : rollup ? rollup.rowsWritten : null
  const verdict = dataMovementVerdict(
    rollup ? { ...rollup, rowsWritten: rowsWritten ?? rollup.rowsWritten } : null,
  )
  const headline = movementHeadline(verdict)

  const timeline = buildRunTimeline(stages, run.start_time, run.end_time)
  const delta = computeRunDelta(
    rowsWritten,
    run,
    previousExecution?.metrics?.records_processed ?? null,
    previousExecution,
  )

  const headlineTone =
    headline.tone === "alarm"
      ? "border-red-200 bg-red-50 dark:border-red-900 dark:bg-red-950/20"
      : headline.tone === "unknown"
        ? "border-amber-200 bg-amber-50 dark:border-amber-900 dark:bg-amber-950/20"
        : "border-zinc-200 bg-zinc-50 dark:border-zinc-800 dark:bg-zinc-900/40"

  const HeadlineIcon =
    headline.tone === "alarm" ? AlertTriangle : headline.tone === "unknown" ? HelpCircle : CheckCircle2

  const bytes = formatBytes(
    tables?.reduce<number | null>((acc, t) => {
      if (typeof t.bytes_committed !== "number") return acc
      return (acc ?? 0) + t.bytes_committed
    }, null) ?? null,
  )

  return (
    <Dialog open={!!execution} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-h-[85vh] max-w-3xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <StatusIcon
              className={`h-4 w-4 ${status.color} ${run.status === "running" ? "animate-spin" : ""}`}
            />
            <span>
              {status.label} run
              {run.pipeline_name ? ` of ${run.pipeline_name}` : ""}
            </span>
            <Badge variant="outline" className="font-normal">
              {run.trigger_source === "scheduled" ? "Scheduled" : "Manual"}
            </Badge>
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-5">
          {/* Identity. The full id, copyable — the old title truncated it to 12
              characters, which is the one thing you cannot paste into a query. */}
          <div className="flex items-center gap-2">
            <code className="rounded bg-zinc-100 px-2 py-1 font-mono text-xs dark:bg-zinc-800">
              {run.id}
            </code>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                void navigator.clipboard?.writeText(run.id).then(() => setCopiedId(run.id))
              }}
            >
              {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
              <span className="ml-1 text-xs">{copied ? "Copied" : "Copy ID"}</span>
            </Button>
          </div>

          {/* The outcome, as a sentence. */}
          <div className={`rounded-md border p-4 ${headlineTone}`}>
            <div className="flex items-start gap-2">
              <HeadlineIcon
                className={`mt-0.5 h-4 w-4 shrink-0 ${
                  headline.tone === "alarm"
                    ? "text-red-600"
                    : headline.tone === "unknown"
                      ? "text-amber-600"
                      : "text-green-600"
                }`}
              />
              <div>
                <div className="text-sm font-medium">
                  {!coreLoaded ? "Reading table statistics…" : headline.title}
                </div>
                <div className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                  {!coreLoaded ? "" : headline.detail}
                </div>
              </div>
            </div>
            {statsFailed && (
              <div className="mt-2">
                <LoadFailure what="table statistics" />
              </div>
            )}
          </div>

          {/* Facts that need no derivation. */}
          <div className="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-4">
            <div>
              <div className="text-xs text-zinc-500">Started</div>
              <div className="text-sm font-medium">
                {formatRelativeTime(new Date(run.start_time))}
              </div>
              <div className="text-xs text-zinc-500">
                {formatDateTime(new Date(run.start_time))}
              </div>
            </div>
            <div>
              <div className="text-xs text-zinc-500">Wall clock</div>
              <div className="text-sm font-medium">
                {timeline.totalMs !== null
                  ? formatDuration(timeline.totalMs)
                  : run.status === "running"
                    ? "In progress"
                    : "—"}
              </div>
            </div>
            <div>
              <div className="text-xs text-zinc-500">Tables</div>
              <div className="text-sm font-medium">
                {rollup ? rollup.tableCount.toLocaleString() : "—"}
                {rollup && rollup.failedTables > 0 && (
                  <span className="ml-1 text-red-600">({rollup.failedTables} failed)</span>
                )}
              </div>
            </div>
            <div>
              <div className="text-xs text-zinc-500">Dropped (DLQ)</div>
              <div
                className={`text-sm font-medium ${
                  rollup && rollup.dlqRows > 0 ? "text-red-600" : ""
                }`}
              >
                {rollup ? rollup.dlqRows.toLocaleString() : "—"}
              </div>
            </div>
          </div>

          {/* Comparison to the run before it. A single run in isolation rarely
              says whether anything is wrong. */}
          {delta && (delta.rowsDelta !== null || delta.durationDeltaMs !== null) && (
            <div className="rounded-md border border-zinc-200 p-3 text-xs dark:border-zinc-800">
              <SectionTitle>Compared with the previous run</SectionTitle>
              <div className="mt-2 flex flex-wrap gap-x-6 gap-y-1 text-zinc-700 dark:text-zinc-300">
                {delta.rowsDelta !== null && (
                  <span>
                    Rows:{" "}
                    {delta.sameRows ? (
                      <span className="font-medium">unchanged</span>
                    ) : (
                      <span
                        className={`font-medium ${delta.rowsDelta > 0 ? "text-green-600" : "text-amber-600"}`}
                      >
                        {delta.rowsDelta > 0 ? "+" : ""}
                        {delta.rowsDelta.toLocaleString()}
                      </span>
                    )}
                  </span>
                )}
                {delta.durationDeltaMs !== null && (
                  <span>
                    Duration:{" "}
                    <span className="font-medium">
                      {delta.durationDeltaMs >= 0 ? "+" : "−"}
                      {formatDuration(Math.abs(delta.durationDeltaMs))}
                    </span>
                  </span>
                )}
                <span className="font-mono text-zinc-500">
                  vs {delta.previousId.slice(0, 8)}
                </span>
              </div>
            </div>
          )}

          {/* Where the wall clock went. */}
          <div className="space-y-2">
            <SectionTitle>Where the time went</SectionTitle>
            {eventsFailed ? (
              <LoadFailure what="the run's event stream" />
            ) : timeline.phases.some((p) => p.ms !== null) ? (
              <TimeBreakdown timeline={timeline} />
            ) : (
              <div className="text-xs text-zinc-500">
                {!coreLoaded
                  ? "Loading…"
                  : "No stage timings were recorded for this run, so the wall clock cannot be broken down."}
              </div>
            )}
          </div>

          {/* Per-table detail. Suppressed entirely when the stats fetch failed:
              the outcome block above already says so, and repeating the same
              amber sentence twice reads as two separate failures. */}
          {!statsFailed && (
          <div className="space-y-2">
            <SectionTitle>
              Tables
              {tables && tableTotal > tables.length && (
                <span className="ml-1 font-normal normal-case text-zinc-400">
                  (showing {tables.length} of {tableTotal})
                </span>
              )}
              {bytes && <span className="ml-2 font-normal normal-case">· {bytes} committed</span>}
            </SectionTitle>
            {tables && tables.length > 0 ? (
              <div className="max-h-64 overflow-y-auto rounded-md border dark:border-zinc-800">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Table</TableHead>
                      <TableHead className="text-right">Read</TableHead>
                      <TableHead className="text-right">Written</TableHead>
                      <TableHead className="text-right">Dropped</TableHead>
                      <TableHead>Status</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {tables.map((t) => {
                      const written = rowsWrittenForTable(t)
                      const read = typeof t.read_rows === "number" ? t.read_rows : null
                      const stalled = read !== null && read > 0 && written === 0
                      return (
                        <TableRow key={`${t.mode || ""}:${t.qualified_name}`}>
                          <TableCell className="font-mono text-xs">
                            {t.qualified_name}
                            {/* qualified_name is the SOURCE-side name for CDC. Showing
                                only it answered "where did my data go?" with the source
                                database. Render the destination when the sink reported
                                one; stay silent rather than guess when it did not. */}
                            {t.destination_qualified_name &&
                              t.destination_qualified_name !== t.qualified_name && (
                                <span className="block text-[10px] font-normal text-zinc-500">
                                  → {t.destination_qualified_name}
                                </span>
                              )}
                          </TableCell>
                          <TableCell className="text-right text-xs">
                            {read === null ? "—" : read.toLocaleString()}
                          </TableCell>
                          <TableCell
                            className={`text-right text-xs ${stalled ? "font-medium text-red-600" : ""}`}
                          >
                            {written.toLocaleString()}
                          </TableCell>
                          <TableCell
                            className={`text-right text-xs ${(t.dlq_rows ?? 0) > 0 ? "font-medium text-red-600" : ""}`}
                          >
                            {(t.dlq_rows ?? 0).toLocaleString()}
                          </TableCell>
                          <TableCell className="text-xs">{t.status || "—"}</TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>
            ) : (
              <div className="text-xs text-zinc-500">
                {!coreLoaded ? "Loading…" : "No per-table statistics were recorded for this run."}
              </div>
            )}
          </div>
          )}

          {/* Failure detail. */}
          {run.error_message && (
            <div className="rounded-md border border-red-200 bg-red-50 p-4 dark:border-red-900 dark:bg-red-950/20">
              <div className="mb-2 text-sm font-medium text-red-900 dark:text-red-100">
                Error message
              </div>
              <div className="whitespace-pre-wrap font-mono text-xs text-red-700 dark:text-red-300">
                {run.error_message}
              </div>
            </div>
          )}

          {/* The diagnosis is the one thing here that is genuinely still in flight
              once the sections above have resolved. Say so, rather than letting a
              verdict panel appear from nowhere a second later. */}
          {diagnosing && !diagnosis && (
            <div className="rounded-md border border-zinc-200 bg-zinc-50 p-4 text-xs text-zinc-500 dark:border-zinc-800 dark:bg-zinc-900/40">
              Looking for a diagnosis…
            </div>
          )}

          {diagnosis && (
            <div className="rounded-md border border-amber-200 bg-amber-50 p-4 dark:border-amber-900 dark:bg-amber-950/20">
              <div className="mb-1 flex items-center gap-2 text-sm font-medium">
                Diagnosis
                <Badge variant="outline" className="font-normal">
                  {diagnosis.category || "unknown"}
                </Badge>
                <span className="text-xs font-normal text-zinc-500">
                  confidence {(diagnosis.confidence * 100).toFixed(0)}%
                </span>
              </div>
              {diagnosis.rationale && (
                <div className="text-xs text-zinc-700 dark:text-zinc-300">{diagnosis.rationale}</div>
              )}
              {diagnosis.action_label && (
                <div className="mt-2 text-xs font-medium">
                  Suggested: {diagnosis.action_label}
                </div>
              )}
              {typeof diagnosis.source_rows === "number" &&
                typeof diagnosis.landed_rows === "number" && (
                  <div className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                    {diagnosis.source_rows.toLocaleString()} rows at source ·{" "}
                    {diagnosis.landed_rows.toLocaleString()} landed
                  </div>
                )}
            </div>
          )}

          {detailFailed && <LoadFailure what="the full execution record" />}

          {/* Actions. */}
          <div className="flex flex-col items-end gap-2 border-t pt-4 dark:border-zinc-800">
            {run.status === "failed" && onRerun && (
              <p className="text-right text-xs text-muted-foreground">
                Starts a new run of the pipeline from its current checkpoints — it does not
                re-run this execution, and there is no way to.
              </p>
            )}
            <div className="flex justify-end gap-2">
              {run.status === "failed" && onRerun && (
                <Button size="sm" variant="outline" onClick={() => onRerun("resume")}>
                  <RotateCcw className="mr-2 h-4 w-4" />
                  Re-run pipeline
                </Button>
              )}
              <Button
                size="sm"
                variant="outline"
                onClick={() => onOpenFullDetail?.(run.id)}
                disabled={!onOpenFullDetail}
              >
                <Eye className="mr-2 h-4 w-4" />
                Open full detail
              </Button>
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  )
}
