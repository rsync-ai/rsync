"use client"

/**
 * Per-table resume positions: "where did this pipeline get to on each table?"
 *
 * GET /api/v1/pipelines/:id/checkpoints has been served (and workspace-gated)
 * since migration 026 with no UI calling it. Without this card the answer to
 * "will a restart replay from the beginning, and from where?" was only in the
 * database.
 *
 * `position` is free-form JSONB by design — a Postgres source stores an LSN, a
 * MongoDB source a resume token, an API connector a `watermark.value` timestamp
 * — so the rendering has to stay shape-agnostic. It prints the leaf key/value
 * pairs it finds rather than assuming any one engine's vocabulary.
 *
 * Only the batch export loop writes this table (cdc.SaveCheckpoint, called
 * once per batch in executor.go). A CDC stream resumes from Kafka offsets
 * instead: the Debezium connector's, which Kafka Connect keeps in its offsets
 * topic, and the sink's, committed by its consumer group. So
 * on a CDC pipeline the list is normally empty, and the batch wording — "a
 * restart would start from the beginning" — was telling a streaming pipeline's
 * operator the opposite of what a restart does.
 */

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { ChevronRight, RefreshCw } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { cn, formatRelativeTime } from "@/lib/utils"

type Checkpoint = {
  id: string
  pipeline_id: string
  connection_id: string
  source_table: string
  position: Record<string, unknown> | null
  created_at: string
  updated_at: string
}

// Keys whose raw name misreads on a per-table row. The batch executor stamps the
// whole run's running totals into each table's checkpoint (executor.go, the
// shared totalRows/totalBytes), so "rows_so_far: 1200" beside public.orders
// read as that table's count when it was every table's (#13).
const POSITION_LABELS: Record<string, string> = {
  rows_so_far: "run rows so far (all tables)",
  bytes_so_far: "run bytes so far (all tables)",
}

/**
 * The position's leaf key/value pairs, one level deep into nested objects so
 * `{"watermark":{"value":"2026-09-01T…"}}` reads as `watermark.value` instead
 * of as raw JSON. The run-wide keys carry their POSITION_LABELS wording.
 */
export function positionEntries(position: Record<string, unknown> | null | undefined): [string, string][] {
  if (!position || typeof position !== "object") return []
  const out: [string, string][] = []
  for (const [k, v] of Object.entries(position)) {
    if (v && typeof v === "object" && !Array.isArray(v)) {
      for (const [k2, v2] of Object.entries(v as Record<string, unknown>)) {
        out.push([`${k}.${k2}`, String(v2)])
      }
    } else {
      out.push([POSITION_LABELS[k] ?? k, Array.isArray(v) ? JSON.stringify(v) : String(v)])
    }
  }
  return out
}

/** `positionEntries` joined as "key: value · key: value". */
export function formatPosition(position: Record<string, unknown> | null | undefined): string {
  if (!position || typeof position !== "object") return "—"
  const parts = positionEntries(position).map(([k, v]) => `${k}: ${v}`)
  // An empty object is a real state — the row exists but carries no position —
  // and is not the same as having no row at all.
  return parts.length > 0 ? parts.join(" · ") : "(empty)"
}

function asNumber(v: unknown): number | null {
  const n = typeof v === "string" && v.trim() !== "" ? Number(v) : v
  return typeof n === "number" && Number.isFinite(n) ? n : null
}

function present(v: unknown): boolean {
  return v !== null && v !== undefined && v !== ""
}

export type CheckpointSummary = {
  /** null when the position is not the batch executor's shape — no claim made. */
  state: "complete" | "in_progress" | null
  /** One short line: where a restart resumes this table from. */
  resume: string
  /** This table's own row count (table_rows_so_far), when the executor wrote one. */
  tableRows: number | null
}

/**
 * summarizeCheckpoint reads the batch executor's position (executor.go
 * checkpointPosition: batch_idx, table_complete, cursor_column/cursor, offset,
 * table_rows_so_far, watermark) into one scannable line. Any other shape — a
 * Postgres LSN, a Mongo resume token — keeps the full formatPosition text: it
 * is short, and guessing at an unknown engine's vocabulary would misread it.
 */
export function summarizeCheckpoint(position: Record<string, unknown> | null | undefined): CheckpointSummary {
  const isBatch = !!position && ("batch_idx" in position || "table_complete" in position)
  if (!position || !isBatch) {
    return { state: null, resume: formatPosition(position), tableRows: null }
  }
  const complete = position.table_complete === true
  const parts: string[] = []
  const batch = asNumber(position.batch_idx)
  if (complete) {
    parts.push("sweep finished")
  } else if (batch !== null) {
    parts.push(`after batch ${batch.toLocaleString("en-US")}`)
  }
  if (!complete) {
    if (present(position.cursor)) {
      const col = present(position.cursor_column) ? String(position.cursor_column) : "cursor"
      parts.push(`${col} > ${String(position.cursor)}`)
    } else if (asNumber(position.offset)) {
      parts.push(`offset ${asNumber(position.offset)!.toLocaleString("en-US")}`)
    }
  }
  const wm = position.watermark
  if (wm && typeof wm === "object" && present((wm as Record<string, unknown>).value)) {
    parts.push(`since ${String((wm as Record<string, unknown>).value)}`)
  }
  return {
    state: complete ? "complete" : "in_progress",
    resume: parts.length > 0 ? parts.join(" · ") : formatPosition(position),
    tableRows: asNumber(position.table_rows_so_far),
  }
}

export type RunTotals = { rows: number | null; bytes: number | null; executionId: string | null; at: string }

/**
 * The run-wide totals every batch checkpoint repeats (rows_so_far/bytes_so_far
 * are the whole run's, #13), stated once for the latest run. The latest run is
 * the one whose checkpoint was committed last; its figure is the LARGEST any of
 * its tables carries, not the last-committed one: each table goroutine
 * snapshots the shared totals and then saves, so a table that snapshotted early
 * can commit after a sibling that saw more (0.1.7-rc1 showed 32 of 42 rows).
 * Within one run the totals only grow, so the largest is the newest. Rows from
 * an earlier run stay in the table until that table is swept again, so they
 * are left out.
 */
export function latestRunTotals(checkpoints: Checkpoint[]): RunTotals | null {
  const runOf = (cp: Checkpoint) => {
    const exec = cp.position?.execution_id
    return present(exec) ? String(exec) : null
  }
  const batch = checkpoints.filter(
    (cp) => cp.position && ("rows_so_far" in cp.position || "bytes_so_far" in cp.position)
  )
  let last: Checkpoint | null = null
  for (const cp of batch) {
    if (!last || new Date(cp.updated_at).getTime() > new Date(last.updated_at).getTime()) last = cp
  }
  if (!last) return null
  const run = runOf(last)
  let rows: number | null = null
  let bytes: number | null = null
  for (const cp of batch) {
    if (runOf(cp) !== run) continue
    const r = asNumber(cp.position?.rows_so_far)
    const b = asNumber(cp.position?.bytes_so_far)
    if (r !== null && (rows === null || r > rows)) rows = r
    if (b !== null && (bytes === null || b > bytes)) bytes = b
  }
  return { rows, bytes, executionId: run, at: last.updated_at }
}

const exactCount = new Intl.NumberFormat("en-US")

function fmtBytes(n: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"]
  let v = n
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${i === 0 ? v : v.toFixed(1)} ${units[i]}`
}

function fmtWhen(iso: string): string {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleString()
}

function fmtAgo(iso: string): string {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return formatRelativeTime(d)
}

type StateFilter = "all" | "in_progress" | "complete"

// Past this many tables the filter row appears; below it, it is clutter.
const FILTER_THRESHOLD = 8

export function PipelineCheckpointsCard({
  pipelineId,
  pipelineType,
}: {
  pipelineId: string
  pipelineType?: "etl" | "cdc"
}) {
  const isCdc = pipelineType === "cdc"
  const [checkpoints, setCheckpoints] = useState<Checkpoint[] | null>(null)
  const [loading, setLoading] = useState(true)
  // A failed read is a THIRD outcome, not a variant of "no checkpoints yet":
  // an empty list is a claim that the pipeline has not committed a position
  // anywhere, which is exactly the wrong thing to say when the read failed.
  const [error, setError] = useState<string | null>(null)

  const [query, setQuery] = useState("")
  const [stateFilter, setStateFilter] = useState<StateFilter>("all")
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())

  const rows = useMemo(() => {
    const list = (checkpoints ?? []).map((cp) => ({ cp, summary: summarizeCheckpoint(cp.position) }))
    // Unfinished tables first — they are the ones a restart resumes mid-way —
    // then by name, so a long list reads in a stable order across refreshes.
    list.sort((a, b) => {
      const ra = a.summary.state === "in_progress" ? 0 : 1
      const rb = b.summary.state === "in_progress" ? 0 : 1
      return ra - rb || a.cp.source_table.localeCompare(b.cp.source_table)
    })
    return list
  }, [checkpoints])

  const counts = useMemo(() => {
    let complete = 0
    let inProgress = 0
    for (const r of rows) {
      if (r.summary.state === "complete") complete++
      else if (r.summary.state === "in_progress") inProgress++
    }
    return { complete, inProgress }
  }, [rows])

  const runTotals = useMemo(() => latestRunTotals(checkpoints ?? []), [checkpoints])

  const visible = useMemo(() => {
    const q = query.trim().toLowerCase()
    return rows.filter(
      (r) =>
        (stateFilter === "all" || r.summary.state === stateFilter) &&
        (q === "" || r.cp.source_table.toLowerCase().includes(q))
    )
  }, [rows, query, stateFilter])

  const toggle = useCallback((id: string) => {
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }, [])

  // A Refresh clicked while an earlier read is in flight must not let that older
  // response land last and revert the list.
  const requestSeq = useRef(0)
  const load = useCallback(async () => {
    const seq = ++requestSeq.current
    setLoading(true)
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.CHECKPOINTS(pipelineId), { cache: "no-store" })
      if (seq !== requestSeq.current) return
      if (!res.ok) {
        setError(`Could not load checkpoints (HTTP ${res.status})`)
        return
      }
      const data = (await res.json().catch(() => null)) as { checkpoints?: Checkpoint[] } | null
      if (seq !== requestSeq.current) return
      setCheckpoints(Array.isArray(data?.checkpoints) ? data!.checkpoints : [])
      setError(null)
    } catch {
      if (seq !== requestSeq.current) return
      setError("Could not load checkpoints — the API is unreachable")
    } finally {
      if (seq === requestSeq.current) setLoading(false)
    }
  }, [pipelineId])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <Card>
      <CardHeader className="flex flex-row items-start justify-between gap-4 space-y-0">
        <div>
          <CardTitle className="text-sm font-medium">Checkpoints</CardTitle>
          <CardDescription className="text-xs">
            {isCdc
              ? "Positions written by batch runs only. A CDC stream resumes from Kafka offsets — the connector's in Kafka Connect, the sink's in its consumer group — not from this list."
              : "The last position committed per table — where a restart resumes from. Row and byte counts are the whole run's running total when this table last committed; per-table counts are in Table statistics."}
          </CardDescription>
        </div>
        <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
          <RefreshCw className={`h-3 w-3 mr-2 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </CardHeader>
      <CardContent>
        {loading && checkpoints === null ? (
          <div className="text-sm text-muted-foreground">Loading…</div>
        ) : error ? (
          <div className="rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300">
            {error}
          </div>
        ) : !checkpoints || checkpoints.length === 0 ? (
          isCdc ? (
            <div className="text-sm text-muted-foreground">
              No batch checkpoints — expected for a CDC pipeline. This does not mean a restart starts over: the stream
              resumes from the Kafka offsets its connector and sink have committed.
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">
              No checkpoint recorded yet. A restart would start this pipeline&apos;s tables from the beginning.
            </div>
          )
        ) : (
          <div className="space-y-3">
            <div
              data-testid="checkpoints-summary"
              className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground"
            >
              <span>
                <span className="font-medium text-foreground">{exactCount.format(rows.length)}</span>{" "}
                {rows.length === 1 ? "table" : "tables"}
              </span>
              {counts.inProgress + counts.complete > 0 && (
                <>
                  <span aria-hidden="true">·</span>
                  <span>{exactCount.format(counts.inProgress)} mid-table</span>
                  <span aria-hidden="true">·</span>
                  <span>{exactCount.format(counts.complete)} finished</span>
                </>
              )}
              {runTotals && (
                <span
                  data-testid="checkpoints-run-totals"
                  title={`The whole run's running total when a table last committed (${fmtWhen(runTotals.at)}).`}
                  className="sm:ml-auto"
                >
                  Run total at last commit:{" "}
                  {runTotals.rows !== null && <>{exactCount.format(runTotals.rows)} rows</>}
                  {runTotals.rows !== null && runTotals.bytes !== null && " · "}
                  {runTotals.bytes !== null && fmtBytes(runTotals.bytes)}
                  {runTotals.executionId && (
                    <span className="font-mono"> · run {runTotals.executionId.slice(0, 8)}</span>
                  )}
                </span>
              )}
            </div>

            {rows.length > FILTER_THRESHOLD && (
              <div className="flex flex-wrap items-center gap-2">
                <Input
                  type="search"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Filter tables…"
                  aria-label="Filter checkpoints by table name"
                  className="h-8 w-full text-xs sm:w-64"
                />
                <div role="group" aria-label="Filter by table state" className="flex gap-1">
                  {(
                    [
                      ["all", "All"],
                      ["in_progress", "Mid-table"],
                      ["complete", "Finished"],
                    ] as const
                  ).map(([value, label]) => (
                    <Button
                      key={value}
                      type="button"
                      size="sm"
                      variant={stateFilter === value ? "secondary" : "ghost"}
                      aria-pressed={stateFilter === value}
                      className="h-8 px-2 text-xs"
                      onClick={() => setStateFilter(value)}
                    >
                      {label}
                    </Button>
                  ))}
                </div>
                {visible.length !== rows.length && (
                  <span className="text-xs text-muted-foreground">
                    {exactCount.format(visible.length)} of {exactCount.format(rows.length)}
                  </span>
                )}
              </div>
            )}

            <div className="max-h-[28rem] overflow-auto rounded-md border">
              <table className="w-full text-sm">
                <thead className="sticky top-0 z-10 bg-card">
                  <tr className="border-b text-left text-xs text-muted-foreground">
                    <th className="w-8 py-2 pl-2" aria-label="Details" />
                    <th className="py-2 pr-4 font-medium">Table</th>
                    <th className="py-2 pr-4 font-medium">State</th>
                    <th className="py-2 pr-4 font-medium">Resumes from</th>
                    <th className="py-2 pr-4 text-right font-medium">Table rows</th>
                    <th className="py-2 pr-3 font-medium">Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {visible.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="py-4 text-center text-xs text-muted-foreground">
                        No table matches this filter.
                      </td>
                    </tr>
                  ) : (
                    visible.map(({ cp, summary }) => {
                      const open = expanded.has(cp.id)
                      const detailsId = `checkpoint-details-${cp.id}`
                      return (
                        <Fragment key={cp.id}>
                          <tr className={cn("border-b align-top", open && "bg-muted/40")}>
                            <td className="py-2 pl-2">
                              <button
                                type="button"
                                aria-expanded={open}
                                aria-controls={detailsId}
                                aria-label={`${open ? "Hide" : "Show"} the full position for ${cp.source_table}`}
                                onClick={() => toggle(cp.id)}
                                className="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
                              >
                                <ChevronRight className={cn("h-4 w-4 transition-transform", open && "rotate-90")} />
                              </button>
                            </td>
                            <td className="max-w-[16rem] break-words py-2 pr-4 font-medium">{cp.source_table}</td>
                            <td className="whitespace-nowrap py-2 pr-4">
                              {summary.state === "complete" ? (
                                <Badge
                                  variant="outline"
                                  className="border-emerald-300 text-emerald-700 dark:border-emerald-800 dark:text-emerald-400"
                                >
                                  Finished
                                </Badge>
                              ) : summary.state === "in_progress" ? (
                                <Badge
                                  variant="outline"
                                  className="border-sky-300 text-sky-700 dark:border-sky-800 dark:text-sky-400"
                                >
                                  Mid-table
                                </Badge>
                              ) : (
                                <span className="text-xs text-muted-foreground">—</span>
                              )}
                            </td>
                            <td className="wrap-anywhere py-2 pr-4 font-mono text-xs">{summary.resume}</td>
                            <td className="whitespace-nowrap py-2 pr-4 text-right text-xs tabular-nums">
                              {summary.tableRows !== null ? exactCount.format(summary.tableRows) : "—"}
                            </td>
                            <td
                              className="whitespace-nowrap py-2 pr-3 text-xs text-muted-foreground"
                              title={fmtWhen(cp.updated_at)}
                            >
                              {fmtAgo(cp.updated_at)}
                            </td>
                          </tr>
                          {open && (
                            <tr id={detailsId} className="border-b bg-muted/40">
                              <td />
                              <td colSpan={5} className="pb-3 pr-3">
                                {positionEntries(cp.position).length === 0 ? (
                                  <span className="font-mono text-xs text-muted-foreground">
                                    {formatPosition(cp.position)}
                                  </span>
                                ) : (
                                  <dl className="grid grid-cols-1 gap-x-6 gap-y-1 font-mono text-xs sm:grid-cols-[max-content_1fr]">
                                    {positionEntries(cp.position).map(([k, v]) => (
                                      <div key={k} className="contents">
                                        <dt className="text-muted-foreground">{k}</dt>
                                        <dd className="break-all">{v}</dd>
                                      </div>
                                    ))}
                                  </dl>
                                )}
                              </td>
                            </tr>
                          )}
                        </Fragment>
                      )
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
