"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table"
import {
  CheckCircle2,
  XCircle,
  Loader2,
  Download,
  RefreshCw,
  Search,
  Database,
  AlertTriangle,
} from "lucide-react"
import { API_ENDPOINTS } from "@/lib/config/api"
import { toast } from "sonner"
import { authFetch } from "@/lib/api/auth-fetch"
import { classifyError } from "@/lib/utils/error-handling"

type TableStat = {
  schema_name?: string
  table_name: string
  qualified_name: string
  mode: "batch" | "cdc"
  status: string
  
  // Batch fields
  read_rows?: number
  inserted_rows?: number
  
  // CDC fields
  inserts?: number
  updates?: number
  deletes?: number
  total_events?: number
  last_event_ts?: string

  // CDC applied fields (destination-truth)
  applied_inserts?: number
  applied_updates?: number
  applied_deletes?: number
  applied_total_events?: number
  last_applied_ts?: string

  // Rows the destination will never receive — parked in the sink's dead-letter queue.
  // Every other counter here reports what landed; this is the only one that reports
  // what did not. Always present (the API does not omit a zero).
  dlq_rows?: number

  // Timestamps
  started_at?: string
  completed_at?: string
  updated_at: string
}

type TableStatsSummary = {
  mode: string // "batch" | "cdc" | "mixed"
  total_tables: number
  tables_completed: number
  tables_failed: number
  tables_running: number
  tables_degraded?: number
  
  // Batch aggregates
  total_read_rows?: number
  total_inserted_rows?: number
  
  // CDC aggregates
  total_inserts?: number
  total_updates?: number
  total_deletes?: number
  total_cdc_events?: number

  // CDC applied aggregates
  total_applied_inserts?: number
  total_applied_updates?: number
  total_applied_deletes?: number
  total_applied_cdc_events?: number

  // Dropped-row aggregates (both modes)
  total_dlq_rows?: number
  tables_with_dlq?: number
}

type Props = {
  pipelineId: string
  executionId?: string
  pipelineStatus?: string
  blockingReasonType?: string
  mode?: "batch" | "cdc"
}

/** Rendered where a metric exists but has no value yet — never for a real 0. */
const NO_VALUE = "—"

function formatNumber(num?: number | null): string {
  // `!num` also caught 0, so "the destination rejected every row" and "this
  // column was never populated" printed the same character. They call for
  // opposite responses from an operator, so they get different glyphs. Callers
  // that genuinely mean zero say so (`table.dlq_rows ?? 0`).
  if (num === undefined || num === null) return NO_VALUE
  return new Intl.NumberFormat("en-US").format(num)
}

/**
 * Placeholder cells for the columns belonging to the mode this row is NOT.
 *
 * In a mixed pipeline the header carries batch columns AND CDC columns
 * (`isBatch`/`isCDC` are both true when the summary mode is "mixed"), so every
 * body row has to emit both groups. Emitting only its own left the row short,
 * and every value after the gap rendered under a heading that belonged to the
 * other mode — "Dropped" appearing beneath "Captured I". The numbers were real;
 * the column was a lie.
 */
function notApplicableCells(count: number, keyPrefix: string) {
  return Array.from({ length: count }, (_, i) => (
    <TableCell key={`${keyPrefix}-${i}`} className="text-right text-sm text-muted-foreground">
      {NO_VALUE}
    </TableCell>
  ))
}

function formatTimestamp(ts?: string): string {
  if (!ts) return ""
  try {
    return new Date(ts).toISOString().slice(11, 19)
  } catch {
    return ""
  }
}

function formatElapsed(start?: string, end?: string): string {
  if (!start) return ""
  try {
    const startMs = new Date(start).getTime()
    const endMs = end ? new Date(end).getTime() : Date.now()
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) return ""
    const totalSec = Math.floor((endMs - startMs) / 1000)
    const h = Math.floor(totalSec / 3600)
    const m = Math.floor((totalSec % 3600) / 60)
    const s = totalSec % 60
    if (h > 0) return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
    return `${m}:${String(s).padStart(2, "0")}`
  } catch {
    return ""
  }
}

function splitQualifiedName(q?: string): { schema?: string; name: string } {
  const raw = String(q || "").trim()
  if (!raw) return { name: "" }
  let parts = raw.split(".").map((p) => p.trim()).filter(Boolean)
  // Normalize common duplication patterns (e.g. "db.db.table") so UI doesn't show schema twice.
  while (parts.length >= 2 && parts[0] && parts[0] === parts[1]) {
    parts = [parts[0], ...parts.slice(2)]
  }
  if (parts.length <= 1) return { name: raw }
  return { schema: parts.slice(0, -1).join("."), name: parts[parts.length - 1] }
}

function StatusBadge({ status }: { status: string }) {
  switch (status) {
    case "completed":
      return (
        <Badge variant="outline" className="gap-1">
          <CheckCircle2 className="h-3 w-3 text-green-600" />
          Completed
        </Badge>
      )
    case "failed":
      return (
        <Badge variant="destructive" className="gap-1">
          <XCircle className="h-3 w-3" />
          Failed
        </Badge>
      )
    case "running":
      return (
        <Badge variant="default" className="gap-1">
          <Loader2 className="h-3 w-3 animate-spin" />
          Running
        </Badge>
      )
    case "degraded":
      return (
        <Badge variant="outline" className="gap-1 border-amber-400 bg-amber-50 text-amber-700 dark:border-amber-600 dark:bg-amber-950/30 dark:text-amber-400">
          <AlertTriangle className="h-3 w-3" />
          Degraded
        </Badge>
      )
    default:
      return <Badge variant="outline">{status}</Badge>
  }
}

export function TableStatisticsPanel({ pipelineId, executionId, pipelineStatus, blockingReasonType, mode }: Props) {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [summary, setSummary] = useState<TableStatsSummary | null>(null)
  const [tables, setTables] = useState<TableStat[]>([])
  const [total, setTotal] = useState(0)
  const [search, setSearch] = useState("")
  const [sortBy, setSortBy] = useState("qualified_name")
  const [offset, setOffset] = useState(0)
  const limit = 50

  const fetchStats = useCallback(async () => {
    try {
      setError(null)
      const params = new URLSearchParams()
      if (executionId) params.set("execution_id", executionId)
      if (mode) params.set("mode", mode)
      if (search) params.set("q", search)
      params.set("sort", sortBy)
      params.set("limit", String(limit))
      params.set("offset", String(offset))

      const url = `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/table-stats?${params.toString()}`
      const res = await authFetch(url, { cache: "no-store" })
      if (!res.ok) {
        throw new Error(`Failed to load table stats (${res.status})`)
      }
      const data = await res.json()
      setSummary(data.summary || null)
      const rawTables: TableStat[] = Array.isArray(data.tables) ? data.tables : []
      // Deduplicate by (mode, qualified_name) to prevent React key collisions.
      const seen = new Set<string>()
      const uniq: TableStat[] = []
      for (const t of rawTables) {
        const key = `${String(t?.mode || "")}:${String(t?.qualified_name || "")}`
        if (!t?.qualified_name || seen.has(key)) continue
        seen.add(key)
        uniq.push(t)
      }
      setTables(uniq)
      setTotal(Number(data.total || 0))
    } catch (e: any) {
      setError(String(e?.message || e || "Failed to load table stats"))
    } finally {
      setLoading(false)
    }
  }, [pipelineId, executionId, mode, search, sortBy, offset])

  useEffect(() => {
    fetchStats()
  }, [fetchStats])

  const exportCSV = useCallback(async () => {
    const params = new URLSearchParams()
    if (executionId) params.set("execution_id", executionId)
    if (mode) params.set("mode", mode)
    params.set("export", "csv")
    params.set("limit", "10000") // Export all

    const url = `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/table-stats?${params.toString()}`
    try {
      const res = await authFetch(url, {
        method: "GET",
        cache: "no-store",
        headers: {
          Accept: "text/csv",
        },
      })
      if (!res.ok) {
        throw new Error(`Export failed (${res.status})`)
      }
      const blob = await res.blob()
      const objectUrl = URL.createObjectURL(blob)
      const link = document.createElement("a")
      link.href = objectUrl
      link.download = `table-stats-${pipelineId}.csv`
      link.click()
      URL.revokeObjectURL(objectUrl)
    } catch (e) {
      // A download that produces no file and no message is read as "the button
      // is broken" — or worse, as "there was nothing to export".
      const err = classifyError(e, "pipeline.export")
      toast.error("Could not export table statistics", { description: err.hint ?? err.message })
    }
  }, [pipelineId, executionId, mode])

  const resolvedMode = summary?.mode || mode || "batch"
  const isBatch = resolvedMode === "batch" || resolvedMode === "mixed"
  const isCDC = resolvedMode === "cdc" || resolvedMode === "mixed"

  if (loading) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading table statistics…
          </div>
        </CardContent>
      </Card>
    )
  }

  if (error) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="text-sm text-red-600">{error}</div>
        </CardContent>
      </Card>
    )
  }

  if (total === 0) {
    const waitingForTables =
      pipelineStatus === "waiting_for_user" && blockingReasonType === "table_selection"
    return (
      <Card>
        <CardContent className="p-6">
          <div className="text-sm text-muted-foreground">
            {waitingForTables
              ? "This run is waiting for table selection. Select one or more tables to start execution — table statistics will appear as tables are processed."
              : resolvedMode === "cdc"
                ? "No CDC activity captured yet. Make an INSERT/UPDATE/DELETE on a selected source table — stats will appear once events are captured/applied."
                : "No table statistics available for this run yet. Statistics will appear as tables are processed (or after you re-run older executions)."}
          </div>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-4">
      {/* Summary Section (DMS-like) */}
      {summary && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Database className="h-4 w-4" />
              Summary
            </CardTitle>
            <CardDescription>
              {resolvedMode === "batch" && "Batch pipeline table migration status"}
              {resolvedMode === "cdc" && "CDC pipeline real-time activity by table"}
              {resolvedMode === "mixed" && "Mixed batch and CDC activity"}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {/* Failed tables alert banner */}
            {summary.tables_failed > 0 && (
              <div className="mb-4 flex items-start gap-2 rounded-lg border border-red-300 bg-red-50 px-3 py-2.5 text-sm dark:border-red-700 dark:bg-red-950/30">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-red-600 dark:text-red-400" />
                <div>
                  <span className="font-medium text-red-800 dark:text-red-300">
                    {summary.tables_failed} table{summary.tables_failed !== 1 ? "s" : ""} failed to sync
                  </span>
                  <span className="ml-1 text-red-700 dark:text-red-400">
                    — check the table rows below for error details and review connector logs.
                  </span>
                </div>
              </div>
            )}

            {/* Dropped-rows banner. Deliberately ABOVE (and visually louder than) the
                degraded banner: a degraded table means rows are behind, this means rows
                are GONE. Nothing else on this page reports it — every other counter is a
                count of what landed, so a dropped row leaves captured/applied reconciling
                perfectly with Failed at 0. */}
            {Boolean(summary.total_dlq_rows) && (
              <div className="mb-4 flex items-start gap-2 rounded-lg border border-red-300 bg-red-50 px-3 py-2.5 text-sm dark:border-red-700 dark:bg-red-950/30">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-red-600 dark:text-red-400" />
                <div>
                  <span className="font-medium text-red-800 dark:text-red-300">
                    {formatNumber(summary.total_dlq_rows)} row
                    {summary.total_dlq_rows !== 1 ? "s" : ""} dropped across{" "}
                    {summary.tables_with_dlq ?? 0} table
                    {(summary.tables_with_dlq ?? 0) !== 1 ? "s" : ""}
                  </span>
                  <span className="ml-1 text-red-700 dark:text-red-400">
                    — the destination rejected these rows after every retry, so they were
                    parked in the dead-letter queue and the stream moved on. They are not
                    counted anywhere else on this page and will not arrive on their own.
                    Inspect the <code>.dlq</code> topic for the rejected records and the
                    reason.
                  </span>
                </div>
              </div>
            )}

            {/* Degraded warning banner */}
            {Boolean(summary.tables_degraded) && (
              <div className="mb-4 flex items-start gap-2 rounded-lg border border-amber-300 bg-amber-50 px-3 py-2.5 text-sm dark:border-amber-700 dark:bg-amber-950/30">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
                <div>
                  <span className="font-medium text-amber-800 dark:text-amber-300">
                    {summary.tables_degraded} table{summary.tables_degraded !== 1 ? "s" : ""} degraded
                  </span>
                  <span className="ml-1 text-amber-700 dark:text-amber-400">
                    — rows were read from source but not written to destination. Check destination connector logs or credentials.
                  </span>
                </div>
              </div>
            )}

            <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
              <div>
                <div className="text-xs text-muted-foreground">Total Tables</div>
                <div className="text-xl font-semibold">{summary.total_tables}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Completed</div>
                <div className="text-xl font-semibold text-green-600">{summary.tables_completed}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Running</div>
                <div className="text-xl font-semibold text-blue-600">{summary.tables_running}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Degraded</div>
                <div className="text-xl font-semibold text-amber-600">{summary.tables_degraded ?? 0}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Failed</div>
                <div className="text-xl font-semibold text-red-600">{summary.tables_failed}</div>
              </div>

              {isBatch && (summary.total_read_rows !== undefined || summary.total_inserted_rows !== undefined) && (
                <>
                  <div>
                    <div className="text-xs text-muted-foreground">Total Rows Read</div>
                    <div className="text-xl font-semibold">{formatNumber(summary.total_read_rows)}</div>
                  </div>
                  <div>
                    <div className="text-xs text-muted-foreground">Total Rows Written</div>
                    <div className="text-xl font-semibold">{formatNumber(summary.total_inserted_rows)}</div>
                  </div>
                </>
              )}

              {isCDC && (
                <>
                  {summary.total_inserts !== undefined && (
                    <div>
                      <div className="text-xs text-muted-foreground">Captured Inserts</div>
                      <div className="text-xl font-semibold">{formatNumber(summary.total_inserts)}</div>
                    </div>
                  )}
                  {summary.total_updates !== undefined && (
                    <div>
                      <div className="text-xs text-muted-foreground">Captured Updates</div>
                      <div className="text-xl font-semibold">{formatNumber(summary.total_updates)}</div>
                    </div>
                  )}
                  {summary.total_deletes !== undefined && (
                    <div>
                      <div className="text-xs text-muted-foreground">Captured Deletes</div>
                      <div className="text-xl font-semibold">{formatNumber(summary.total_deletes)}</div>
                    </div>
                  )}
                  {summary.total_applied_inserts !== undefined && (
                    <div>
                      <div className="text-xs text-muted-foreground">Applied Inserts</div>
                      <div className="text-xl font-semibold">{formatNumber(summary.total_applied_inserts)}</div>
                    </div>
                  )}
                  {summary.total_applied_updates !== undefined && (
                    <div>
                      <div className="text-xs text-muted-foreground">Applied Updates</div>
                      <div className="text-xl font-semibold">{formatNumber(summary.total_applied_updates)}</div>
                    </div>
                  )}
                  {summary.total_applied_deletes !== undefined && (
                    <div>
                      <div className="text-xs text-muted-foreground">Applied Deletes</div>
                      <div className="text-xl font-semibold">{formatNumber(summary.total_applied_deletes)}</div>
                    </div>
                  )}
                </>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Table List */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between gap-3">
            <div>
              <CardTitle className="text-base">Table Statistics ({total})</CardTitle>
              <CardDescription>Per-table activity and status</CardDescription>
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={fetchStats} disabled={loading}>
                <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />
                Refresh
              </Button>
              <Button variant="outline" size="sm" onClick={exportCSV}>
                <Download className="h-4 w-4 mr-2" />
                Export CSV
              </Button>
            </div>
          </div>
          <div className="flex items-center gap-2 mt-2">
            <div className="relative flex-1">
              <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Search tables…"
                value={search}
                onChange={(e) => {
                  setSearch(e.target.value)
                  setOffset(0)
                }}
                className="pl-8"
              />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {/* 
            Use a single explicit horizontal scroll container for the stats table.
            This avoids nested scroll wrappers (our shadcn Table component wraps in overflow-auto)
            and makes horizontal scrolling reliable inside other scroll containers.
          */}
          <div className="border rounded-md overflow-x-auto">
            <table className="min-w-full w-max caption-bottom text-sm [&_th]:whitespace-nowrap [&_td]:whitespace-nowrap">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[200px]">Table</TableHead>
                  <TableHead className="w-[100px]">Status</TableHead>
                  <TableHead>Start</TableHead>
                  <TableHead>End</TableHead>
                  <TableHead>Elapsed</TableHead>
                  {isBatch && (
                    <>
                      <TableHead className="text-right">Read</TableHead>
                      <TableHead className="text-right">Written</TableHead>
                    </>
                  )}
                  {isCDC && (
                    <>
                      <TableHead className="text-right">Captured I</TableHead>
                      <TableHead className="text-right">Captured U</TableHead>
                      <TableHead className="text-right">Captured D</TableHead>
                      <TableHead className="text-right">Captured Total</TableHead>
                      <TableHead>Last Captured</TableHead>
                      <TableHead className="text-right">Applied I</TableHead>
                      <TableHead className="text-right">Applied U</TableHead>
                      <TableHead className="text-right">Applied D</TableHead>
                      <TableHead className="text-right">Applied Total</TableHead>
                      <TableHead>Last Applied</TableHead>
                    </>
                  )}
                  {/* Shown in both modes — a dropped row is not a CDC-only concept. */}
                  <TableHead className="text-right">Dropped</TableHead>
                  <TableHead>Updated</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {tables.map((table) => (
                  <TableRow key={`${table.mode}:${table.qualified_name}`}>
                    <TableCell className="font-mono text-sm">
                      {(() => {
                        const { schema, name } = splitQualifiedName(table.qualified_name)
                        if (schema) {
                          return (
                            <>
                              <span className="text-muted-foreground">{schema}.</span>
                              {name}
                            </>
                          )
                        }
                        // Fallback for older rows that might not have qualified_name populated.
                        return table.schema_name ? (
                          <>
                            <span className="text-muted-foreground">{table.schema_name}.</span>
                            {table.table_name}
                          </>
                        ) : (
                          <>{table.table_name}</>
                        )
                      })()}
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-col gap-1">
                        <StatusBadge status={table.status} />
                        {table.status === "degraded" && table.mode === "batch" && (
                          <span className="text-xs text-amber-600 dark:text-amber-400">
                            {(table.read_rows ?? 0) > 0 && (table.inserted_rows ?? 0) === 0
                              ? "Destination wrote 0 rows"
                              : `${formatNumber(table.inserted_rows)} of ${formatNumber(table.read_rows)} written`}
                          </span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {formatTimestamp(table.started_at)}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {formatTimestamp(table.completed_at)}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground font-mono">
                      {formatElapsed(table.started_at, table.completed_at)}
                    </TableCell>
                    {isBatch &&
                      (table.mode === "batch" ? (
                        <>
                          <TableCell className="text-right font-mono text-sm">
                            {formatNumber(table.read_rows)}
                          </TableCell>
                          <TableCell className="text-right font-mono text-sm">
                            {formatNumber(table.inserted_rows)}
                          </TableCell>
                        </>
                      ) : (
                        notApplicableCells(2, `${table.mode}:${table.qualified_name}:batch`)
                      ))}
                    {isCDC &&
                      (table.mode === "cdc" ? (
                        <>
                          <TableCell className="text-right font-mono text-sm">
                            {formatNumber(table.inserts)}
                          </TableCell>
                          <TableCell className="text-right font-mono text-sm">
                            {formatNumber(table.updates)}
                          </TableCell>
                          <TableCell className="text-right font-mono text-sm">
                            {formatNumber(table.deletes)}
                          </TableCell>
                          <TableCell className="text-right font-mono text-sm">
                            {formatNumber(table.total_events)}
                          </TableCell>
                          <TableCell className="text-xs text-muted-foreground">
                            {formatTimestamp(table.last_event_ts)}
                          </TableCell>
                          <TableCell className="text-right font-mono text-sm">
                            {formatNumber(table.applied_inserts)}
                          </TableCell>
                          <TableCell className="text-right font-mono text-sm">
                            {formatNumber(table.applied_updates)}
                          </TableCell>
                          <TableCell className="text-right font-mono text-sm">
                            {formatNumber(table.applied_deletes)}
                          </TableCell>
                          <TableCell className="text-right font-mono text-sm">
                            {formatNumber(table.applied_total_events)}
                          </TableCell>
                          <TableCell className="text-xs text-muted-foreground">
                            {formatTimestamp(table.last_applied_ts)}
                          </TableCell>
                        </>
                      ) : (
                        notApplicableCells(10, `${table.mode}:${table.qualified_name}:cdc`)
                      ))}
                    <TableCell
                      className={`text-right font-mono text-sm ${
                        (table.dlq_rows ?? 0) > 0
                          ? "font-semibold text-red-600 dark:text-red-400"
                          : "text-muted-foreground"
                      }`}
                      title={
                        (table.dlq_rows ?? 0) > 0
                          ? "Rows the destination rejected after every retry. Parked in the dead-letter queue; they will not arrive on their own."
                          : "No rows dropped"
                      }
                    >
                      {formatNumber(table.dlq_rows ?? 0)}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {formatTimestamp(table.updated_at)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </table>
          </div>

          {/* Pagination */}
          {total > limit && (
            <div className="flex items-center justify-between mt-4">
              <div className="text-sm text-muted-foreground">
                Showing {offset + 1}–{Math.min(offset + limit, total)} of {total} tables
              </div>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setOffset((o) => Math.max(0, o - limit))}
                  disabled={offset === 0}
                >
                  Previous
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setOffset((o) => o + limit)}
                  disabled={offset + limit >= total}
                >
                  Next
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
