"use client"

import { useEffect, useRef, useState } from "react"
import Link from "next/link"
import { Activity, CheckCircle2, ChevronRight, HeartPulse, Users } from "lucide-react"

import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import {
  usePipelineRuntime,
  type PipelineRuntime,
  type RuntimeDep,
  type RuntimeHealth,
} from "@/lib/hooks/usePipelineRuntime"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { DiagnosePanel } from "@/components/pipeline/DiagnosePanel"
import { LoadStatusBadge } from "@/components/pipeline/LoadStatusBadge"
import { describeLoadStatus, type LoadStatus } from "@/lib/pipeline/loadStatus"
import { tableStatusBreakdown } from "./executionSummary"
import { cn, formatDateTime } from "@/lib/utils"
import { formatAge } from "@/lib/transform-format"
import { dependencyStatusLabel, dependencyStreak } from "@/lib/pipeline/dependencyStatus"

// ---------------------------------------------------------------------------
// Shared shapes — mirror the api-gateway responses described in the task.
// ---------------------------------------------------------------------------
interface TableStatsSummary {
  mode?: "batch" | "cdc"
  total_tables?: number
  tables_completed?: number
  tables_failed?: number
  tables_degraded?: number
  tables_running?: number
  // A selected CDC table that has reported nothing yet (table_stats.go). It is
  // not in tables_running, so the footer must show it or the counts don't add up.
  tables_waiting_for_data?: number
  total_read_rows?: number | null
  total_inserted_rows?: number | null
  total_inserts?: number | null
  total_updates?: number | null
  total_deletes?: number | null
  total_cdc_events?: number | null
  total_applied_inserts?: number | null
  total_applied_updates?: number | null
  total_applied_deletes?: number | null
  total_applied_cdc_events?: number | null
  // Rows the initial load (and any re-snapshot) read and wrote. NOT part of the
  // counters above: total_events is inserts + updates + deletes captured from the
  // log, and excludes snapshot reads by design (table_stats.go capturedInsertsSQL).
  // Omitted when nothing measured them.
  total_snapshot_rows?: number | null
  total_applied_snapshot_rows?: number | null
}

// Only the two fields the Flow card reads; the full row is TableStatisticsPanel's.
// Both are the newest event each side has recorded, kept as a running max by the
// writers (cdcstats for captured, the gateway's projector for applied).
interface TableStatsRow {
  last_event_ts?: string | null
  last_applied_ts?: string | null
}

interface TableStatsResponse {
  summary: TableStatsSummary
  tables?: TableStatsRow[]
  /** Rows matching, across all pages; `tables` is one page (50 by default). */
  total?: number
}

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------
function statusDot(status: RuntimeHealth): string {
  switch (status) {
    case "healthy":
      return "bg-emerald-500"
    case "degraded":
      return "bg-amber-500"
    case "unhealthy":
      return "bg-red-500"
    default:
      return "bg-muted-foreground/40"
  }
}

const DEP_LABELS: Record<string, string> = {
  mcp_source: "Source connector",
  mcp_dest: "Destination connector",
  debezium_task: "CDC source stream",
  kafka_sink_worker: "Sink writer",
}

function depLabel(dep: RuntimeDep): string {
  return DEP_LABELS[dep.kind] || dep.kind
}

function fmtNum(v: number | null | undefined): string {
  if (v === null || v === undefined) return "–"
  return v.toLocaleString()
}

function num(v: number | null | undefined): number {
  return typeof v === "number" ? v : 0
}

// Generic 5s-polling JSON fetch hook used for table-stats.
//
// A 404 is an ordinary failure here, not an empty result. The endpoint answers
// "nothing to report" with a 200 and an empty body (`table_stats.go:436`), and
// returns 404 from only two places — an id that does not parse as a UUID, and
// `requirePipelineWorkspaceRole`, which 404s a pipeline that does not exist OR
// sits outside the caller's ACTIVE workspace (`pipeline_ownership.go:21`,
// deliberately indistinguishable, so the response cannot be used to probe for
// pipelines in other tenants).
//
// This used to set a `disabledRef` on 404 and return: no error, no cleared data,
// and every later tick a no-op. The card then showed its empty state — the same
// pixels a healthy pipeline with no rows yet shows — and never recovered, because
// the ref is only reset when `url` changes. Switching workspaces back was not
// enough; only a page reload was.
//
// Nothing here needs a per-call "treat 404 as empty" option. That would only be
// justified by an endpoint whose 404 means absence, and this one has none.
// `CDCLagAlertsPanel` hides itself on 404 for a genuinely different endpoint
// (`MONITORING.SENTINEL_ISSUES`, whose 404 means the feature is not enabled) —
// that precedent is about that endpoint, not about the status code.
//
// `at` is when the last successful read landed. A rate needs a sample per poll,
// not per change: a counter that holds still is the reading that proves "0 / min".
function usePolledJson<T>(url: string | null): { data: T | null; error: string | null; at: number | null } {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [at, setAt] = useState<number | null>(null)
  const inflightRef = useRef<AbortController | null>(null)

  // What this state is about. When the URL changes the answer it holds is about
  // something else — another pipeline, another run — so it has to go, or the
  // card shows the previous pipeline's numbers until the first response for the
  // new one lands (see the same reset in usePipelineRuntime). Cleared during
  // render, because an effect would paint that frame first. A failed poll on an
  // unchanged URL is the opposite case and keeps its data, below.
  const [subject, setSubject] = useState(url)
  if (url !== subject) {
    setSubject(url)
    setData(null)
    setError(null)
    setAt(null)
  }

  useEffect(() => {
    if (!url) return
    let cancelled = false

    const fetchOnce = async () => {
      inflightRef.current?.abort()
      const ac = new AbortController()
      inflightRef.current = ac
      try {
        const res = await authFetch(url, { cache: "no-store", signal: ac.signal })
        if (!res.ok) {
          // Deliberately NOT clearing `data`: a single failed poll must not
          // retract numbers that were read successfully a moment ago. The cards
          // say so themselves when both are present.
          if (!cancelled) setError(`${res.status}`)
          return
        }
        const json = (await res.json()) as T
        if (!cancelled) {
          setData(json)
          setError(null)
          setAt(Date.now())
        }
      } catch (e) {
        if ((e as { name?: string })?.name === "AbortError") return
        if (!cancelled) setError(String((e as Error)?.message ?? e))
      }
    }

    void fetchOnce()
    const t = window.setInterval(() => void fetchOnce(), 5000)
    return () => {
      cancelled = true
      window.clearInterval(t)
      inflightRef.current?.abort()
    }
  }, [url])

  return { data, error, at }
}

// ---------------------------------------------------------------------------
// Cards.
// ---------------------------------------------------------------------------
function DependenciesCard({
  runtime,
  error,
}: {
  runtime: PipelineRuntime | null
  // The read either failed or it didn't. Without this the card cannot tell
  // "this pipeline has no registered dependencies" apart from "we could not
  // ask", and it rendered the former for both.
  error?: string | null
}) {
  const deps = runtime?.dependencies ?? []
  const aggregate: RuntimeHealth = runtime?.health ?? "unknown"

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center justify-between text-base">
          <span className="flex items-center gap-2">
            <HeartPulse className="h-4 w-4" />
            Dependencies
          </span>
          <span className="flex items-center gap-1.5 text-xs font-normal text-muted-foreground capitalize">
            <span className={cn("h-2 w-2 rounded-full", statusDot(aggregate))} />
            {aggregate}
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {error ? (
          <p className="text-xs text-amber-600 dark:text-amber-400">
            Couldn&apos;t load dependencies ({error}). Nothing is claimed about their health
            below — this is a failed read, not an empty list. Retrying.
          </p>
        ) : deps.length === 0 ? (
          <p className="text-xs text-muted-foreground">No dependencies reported.</p>
        ) : (
          <ul className="space-y-1.5">
            {deps.map((dep) => {
              const streak = dependencyStreak(dep, formatAge)
              return (
              <li key={`${dep.kind}:${dep.identifier}`} className="flex items-start gap-2 text-xs">
                <span className={cn("mt-1.5 h-2 w-2 rounded-full shrink-0", statusDot(dep.status))} />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center justify-between gap-2">
                    <span className="truncate text-foreground">
                      {depLabel(dep)}
                      {dep.identifier ? (
                        <span className="ml-1.5 text-muted-foreground">{dep.identifier}</span>
                      ) : null}
                    </span>
                    <span className="text-muted-foreground capitalize shrink-0">{dependencyStatusLabel(dep)}</span>
                  </div>
                  {streak ? (
                    <p
                      className="text-[11px] text-muted-foreground mt-0.5"
                      data-testid={`dependency-streak-${dep.kind}`}
                      title={dep.last_healthy_at ? `last healthy ${formatDateTime(dep.last_healthy_at)}` : undefined}
                    >
                      {streak}
                    </p>
                  ) : null}
                  {dep.status !== "healthy" && dep.last_error ? (
                    <p className="text-[11px] text-muted-foreground mt-0.5 break-words">{dep.last_error}</p>
                  ) : null}
                </div>
              </li>
              )
            })}
          </ul>
        )}
      </CardContent>
    </Card>
  )
}

/**
 * A rate, derived from a cumulative counter that is polled.
 *
 * The Throughput card's numbers are LIFETIME totals out of
 * pipeline_run_table_stats — "captured 72,670 since the stream started". Under a
 * heading that means "per unit time" they read as a rate and are not one, which is
 * why a busy pipeline and an idle one look identical on this card: both show a big
 * number that does not move perceptibly between glances. The fix is not to rename
 * the totals away; it is to show the rate as well, computed from what changes
 * between two readings.
 *
 * A window of samples rather than the last two: the poll is every 5s, and a
 * five-second delta on a batching sink is mostly quantisation noise (one flush
 * lands or it does not). Spanning ~a minute smooths that without turning into a
 * lifetime average, which would never return to zero after a burst.
 *
 * Returns null while there is not yet a second reading far enough apart to divide
 * by — "not enough data yet" is an honest answer and must not render as 0/min,
 * which is a claim that nothing is moving.
 */
const RATE_WINDOW_MS = 60_000
const RATE_MIN_SPAN_MS = 8_000

export function computeRatePerMin(samples: { t: number; v: number }[]): number | null {
  if (samples.length < 2) return null
  const first = samples[0]
  const last = samples[samples.length - 1]
  const spanMs = last.t - first.t
  if (spanMs < RATE_MIN_SPAN_MS) return null
  const delta = last.v - first.v
  // A counter that went backwards is a reset (a new run, or a re-seeded stats
  // row), not negative throughput.
  if (delta < 0) return null
  return (delta / spanMs) * 60_000
}

type RateSample = { t: number; v: number }

/**
 * The rate window plus the first reading this page took (`origin`), so the card
 * can say how much moved while the reader watched. The server serves lifetime
 * totals only, with no history, so this is the one honest window
 * longer than a minute.
 */
export type RateWindow = { at: number | null; samples: RateSample[]; origin: RateSample | null }

export const EMPTY_RATE_WINDOW: RateWindow = { at: null, samples: [], origin: null }

export function nextRateWindow(w: RateWindow, total: number | null | undefined, at: number | null): RateWindow {
  const samples = at === null ? [] : appendRateSample(w.samples, total, at)
  const last = samples[samples.length - 1]
  let origin = w.origin
  // Unmeasured drops the origin with the samples; a counter reset starts it again,
  // so a re-seeded counter never reads as a negative move.
  if (!last) origin = null
  else if (!origin || last.v < origin.v) origin = samples[0]
  return { at, samples, origin }
}

/** How far the counter moved since the page's first reading, and over how long. */
export function movedSinceOpen(w: RateWindow): { delta: number; spanMs: number } | null {
  const last = w.samples[w.samples.length - 1]
  if (!w.origin || !last) return null
  return { delta: last.v - w.origin.v, spanMs: last.t - w.origin.t }
}

/**
 * The sample window after one more reading of the counter.
 *
 * Called once per successful poll, whether or not the value moved. The hook this
 * replaces sampled in an effect keyed on the value, so a counter that held still
 * never got a second sample and the card read "measuring…" for as long as the
 * page stayed open — exactly when the honest answer is "0 / min".
 */
export function appendRateSample(
  samples: RateSample[],
  total: number | null | undefined,
  at: number,
): RateSample[] {
  // Nothing measured this counter. Drop the history rather than keep dividing by
  // a stale one; "not measured" is not "zero".
  if (typeof total !== "number") return []
  const previous = samples[samples.length - 1]
  // Counter reset: start again from here rather than reporting a negative or a
  // huge fictitious catch-up.
  if (previous && total < previous.v) return [{ t: at, v: total }]
  const next = [...samples, { t: at, v: total }]
  // Keep one sample older than the window so the span can reach it.
  while (next.length > 2 && at - next[1].t > RATE_WINDOW_MS) next.shift()
  return next
}

// Sampled while rendering, keyed on the poll time, so each successful read adds
// exactly one sample (the codebase's adjust-state-during-render idiom; an effect
// would paint the stale rate for a frame and trips set-state-in-effect).
function useCounterRate(
  total: number | null | undefined,
  at: number | null,
): { rate: number | null; sinceOpen: { delta: number; spanMs: number } | null } {
  const [window_, setWindow] = useState<RateWindow>(EMPTY_RATE_WINDOW)
  let w = window_
  if (at !== window_.at) {
    w = nextRateWindow(window_, total, at)
    setWindow(w)
  }
  return { rate: computeRatePerMin(w.samples), sinceOpen: movedSinceOpen(w) }
}

/** Sum of two optional counters; unmeasured only when BOTH are. */
function sumMeasured(a: number | null | undefined, b: number | null | undefined): number | undefined {
  if (typeof a !== "number" && typeof b !== "number") return undefined
  return num(a) + num(b)
}

/** A rate, worded so 0 reads as "nothing moving now" rather than as a missing value. */
export function formatRatePerMin(rate: number | null): string {
  if (rate === null) return "measuring…"
  if (rate === 0) return "0 / min"
  if (rate < 1) return "<1 / min"
  return `${Math.round(rate).toLocaleString()} / min`
}

function ThroughputRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium tabular-nums text-foreground">{value}</span>
    </div>
  )
}

// Scope to the current run. pipeline_run_table_stats has one row per
// (pipeline_id, execution_id, qualified_name), so an unscoped query sums every
// run this pipeline has ever made and counts each table once per execution —
// which is how a 3-table pipeline reported "9 / 9 tables" after three runs.
// The handler already accepts these filters
// (api-gateway/internal/handlers/table_stats.go:117-128); only the caller was
// missing them.
//
// CDC does NOT key on the Temporal execution id: its counters live under the
// stable key execution_id == pipeline_id (table_stats.go:123-128), so passing
// the runtime execution id would match zero rows. Send mode=cdc instead and
// let the handler substitute the stable key — one source of truth for that
// mapping, on the server.
//
// Before either is known the URL is null and usePolledJson no-ops, rather than
// falling back to lifetime totals.
function tableStatsUrl(pipelineId: string, executionId?: string, mode?: "batch" | "cdc"): string | null {
  const base = API_ENDPOINTS.PIPELINES.TABLE_STATS(pipelineId)
  if (mode === "cdc") return `${base}?mode=cdc`
  return executionId ? `${base}?execution_id=${encodeURIComponent(executionId)}` : null
}

/**
 * When each side last moved, for a card that reads "Nothing moving": the newest
 * last_event_ts (captured) and last_applied_ts (written) across the tables. A 0 /
 * min rate alone cannot tell a stream that went quiet a minute ago from one that
 * has been idle since yesterday.
 *
 * Says nothing when the response is one page of several: the newest time on page
 * one is not the pipeline's newest, and a wrong "last written 2h ago" is worse
 * than no line.
 */
export function lastMovedDetail(
  tables: TableStatsRow[] | undefined,
  total: number | undefined,
  age: (iso: string) => string,
): string | null {
  if (!tables || tables.length === 0) return null
  if (typeof total === "number" && total > tables.length) return null
  const newest = (pick: (t: TableStatsRow) => string | null | undefined) => {
    let best: string | null = null
    let bestMs = -Infinity
    for (const t of tables) {
      const v = pick(t)
      const ms = v ? Date.parse(v) : NaN
      if (v && Number.isFinite(ms) && ms > bestMs) {
        best = v
        bestMs = ms
      }
    }
    return best
  }
  const captured = newest((t) => t.last_event_ts)
  const written = newest((t) => t.last_applied_ts)
  if (!captured && !written) return null
  return [
    captured ? `last captured ${age(captured)}` : "nothing captured yet",
    written ? `last written ${age(written)}` : "nothing written yet",
  ].join(", ")
}

/**
 * "Since this page opened (N min): +X captured · +Y written" — only once both
 * counters have been read for at least a minute, so it never reads "(0 min)".
 *
 * Each side counts from its own first reading, and the two can differ: a written
 * counter still unmeasured when the page opened, or one side reset. One shared
 * "(N min)" would then put 6 minutes of captured rows under "1 min", so unequal
 * spans are printed per side.
 */
export function sinceOpenLine(
  labels: [string, string],
  moved: [{ delta: number; spanMs: number } | null, { delta: number; spanMs: number } | null],
): string | null {
  const [a, b] = moved
  if (!a || !b) return null
  const minsA = Math.floor(a.spanMs / 60_000)
  const minsB = Math.floor(b.spanMs / 60_000)
  if (minsA < 1 || minsB < 1) return null
  const moveA = `+${a.delta.toLocaleString()} ${labels[0]}`
  const moveB = `+${b.delta.toLocaleString()} ${labels[1]}`
  if (a.spanMs === b.spanMs) return `Since this page opened (${minsA} min): ${moveA} · ${moveB}`
  return `Since this page opened: ${moveA} in ${minsA} min · ${moveB} in ${minsB} min`
}

function RateRows({
  first,
  second,
  rates,
  idleDetail,
  sinceOpen,
}: {
  first: string
  second: string
  rates: [number | null, number | null]
  idleDetail?: string | null
  sinceOpen?: string | null
}) {
  const since = sinceOpen ? (
    <p className="text-[11px] text-muted-foreground" data-testid="throughput-since-open">
      {sinceOpen}
    </p>
  ) : null
  // A measured standstill on both sides is one line, not "0 / min" twice. Until
  // both are measured, each side keeps its own row ("measuring…").
  if (rates[0] === 0 && rates[1] === 0) {
    return (
      <div className="border-t border-border/60 pt-2 space-y-1" data-testid="throughput-rate">
        <ThroughputRow label="Rate now" value="Nothing moving" />
        {idleDetail && (
          <p className="text-[11px] text-muted-foreground" data-testid="throughput-idle-detail">
            {idleDetail}
          </p>
        )}
        {since}
      </div>
    )
  }
  return (
    <div className="border-t border-border/60 pt-2 space-y-1" data-testid="throughput-rate">
      <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Rate now</p>
      <ThroughputRow label={first} value={formatRatePerMin(rates[0])} />
      <ThroughputRow label={second} value={formatRatePerMin(rates[1])} />
      {since}
    </div>
  )
}

/** Batch: one run's totals. A CDC pipeline gets FlowCard instead. */
function ThroughputCard({ pipelineId, executionId }: { pipelineId: string; executionId?: string }) {
  const url = tableStatsUrl(pipelineId, executionId, "batch")
  const { data, error, at } = usePolledJson<TableStatsResponse>(url)
  const s = data?.summary

  // Derived from the cumulative counters, so undefined exactly when those are —
  // "not measured" never becomes a confident 0 / min.
  const readRate = useCounterRate(s?.total_read_rows, at).rate
  const writtenRate = useCounterRate(s?.total_inserted_rows, at).rate

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Activity className="h-4 w-4" />
          Throughput
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {!url ? (
          <p className="text-xs text-muted-foreground">
            No execution yet — throughput appears once this pipeline runs.
          </p>
        ) : error && !s ? (
          // "Loading throughput…" forever is the same pixels as a run that has
          // simply not reported yet. A failed read has to say so.
          <p className="text-xs text-amber-600 dark:text-amber-400">
            Couldn&apos;t load throughput ({error}). Retrying.
          </p>
        ) : !s ? (
          <p className="text-xs text-muted-foreground">Loading throughput…</p>
        ) : (
          <>
            <p className="text-[11px] text-muted-foreground">Totals for this run</p>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-1.5">
                <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Read</p>
                <ThroughputRow label="Rows read" value={fmtNum(s.total_read_rows)} />
              </div>
              <div className="space-y-1.5">
                <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Applied</p>
                <ThroughputRow label="Rows inserted" value={fmtNum(s.total_inserted_rows)} />
              </div>
            </div>
            <RateRows first="Read" second="Written" rates={[readRate, writtenRate]} />
            <div className="border-t border-border/60 pt-2">
              <ThroughputRow
                label="Tables completed"
                value={`${num(s.tables_completed)} / ${num(s.total_tables)}`}
              />
              {/* Every table not completed lands in exactly one row, so the rows
                  below plus "completed" add up to the total. */}
              {tableStatusBreakdown(s).rows.map((row) => (
                <ThroughputRow key={row.key} label={row.label} value={row.count.toLocaleString()} />
              ))}
            </div>
          </>
        )}
      </CardContent>
    </Card>
  )
}

function FlowCell({ value }: { value: number | null | undefined }) {
  return <td className="py-0.5 pl-3 text-right font-medium tabular-nums text-foreground">{fmtNum(value)}</td>
}

/**
 * CDC: what has moved from the source to the destination, captured vs written.
 *
 * This replaced a "Throughput" card that read 0 in every cell on a pipeline with
 * 16,909 rows in its destination (prod, 600b012e, 2026-09-25). All of them were
 * rows the initial load had read, and the card only drew the change counters —
 * `total_events` excludes snapshot reads by design (table_stats.go
 * capturedInsertsSQL). The full load is now its own line, and the rate counts it.
 *
 * Per-table detail stays on Table statistics; this card is the pipeline-wide
 * answer, so it links there rather than repeating it.
 *
 * It opens with where the pipeline is, the way AWS DMS reports a task ("Load
 * completed, replication ongoing"), from the initial load the orchestrator
 * recorded. No record, no line: it never infers a finished load from row counts.
 */
function FlowCard({ pipelineId, loadStatus }: { pipelineId: string; loadStatus: LoadStatus | null }) {
  const { data, error, at } = usePolledJson<TableStatsResponse>(tableStatsUrl(pipelineId, undefined, "cdc"))
  const s = data?.summary

  const captured = useCounterRate(sumMeasured(s?.total_cdc_events, s?.total_snapshot_rows), at)
  const written = useCounterRate(sumMeasured(s?.total_applied_cdc_events, s?.total_applied_snapshot_rows), at)

  // Tables: completed and running both mean "this table is replicating" on a
  // stream — a CDC table is never done — so they are one line here. "Tables
  // completed 0 / 3" on a healthy stream read as a stuck one. Every other status
  // keeps its own line, so the lines still add up to the total.
  // Every change counter measured and 0: the stream has only loaded, and four
  // rows of 0 / 0 say what one line does. An unmeasured counter keeps the rows,
  // so it still reads "–" rather than "none".
  const noChanges =
    !!s &&
    [
      s.total_inserts,
      s.total_updates,
      s.total_deletes,
      s.total_cdc_events,
      s.total_applied_inserts,
      s.total_applied_updates,
      s.total_applied_deletes,
      s.total_applied_cdc_events,
    ].every((v) => v === 0)

  const breakdown = s ? tableStatusBreakdown(s) : null
  const replicating = s ? num(s.tables_completed) + num(s.tables_running) : 0
  const attention = breakdown ? breakdown.rows.filter((r) => r.key !== "running") : []

  return (
    <Card data-testid="flow-card">
      <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Activity className="h-4 w-4" />
          Flow
        </CardTitle>
        <Link
          href="?tab=table-stats"
          className="shrink-0 text-xs text-primary underline-offset-2 hover:underline"
        >
          Table statistics →
        </Link>
      </CardHeader>
      <CardContent className="space-y-3">
        {loadStatus && (
          <div data-testid="flow-load-status" className="space-y-1 text-xs">
            <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
              <LoadStatusBadge status={loadStatus} />
              {loadStatus.since && (
                <span className="text-muted-foreground">
                  Full load {loadStatus.since.label} {formatDateTime(loadStatus.since.at)}
                </span>
              )}
            </div>
            {/* The chip's tooltip is mouse-only; the reason a load failed is
                worth reading, so the card says it in words. */}
            {loadStatus.title && <p className="text-muted-foreground">{loadStatus.title}</p>}
          </div>
        )}
        {error && !s ? (
          <p className="text-xs text-amber-600 dark:text-amber-400">
            Couldn&apos;t load throughput ({error}). Retrying.
          </p>
        ) : !s ? (
          <p className="text-xs text-muted-foreground">Loading throughput…</p>
        ) : (
          <>
            <table className="w-full text-xs">
              <caption className="sr-only">Rows captured from the source and written to the destination</caption>
              <thead>
                <tr className="text-[11px] uppercase tracking-wide text-muted-foreground">
                  <th scope="col" className="pb-1 text-left font-normal">
                    <span className="sr-only">Kind</span>
                  </th>
                  <th scope="col" className="pb-1 pl-3 text-right font-normal">Captured</th>
                  <th scope="col" className="pb-1 pl-3 text-right font-normal">Written</th>
                </tr>
              </thead>
              <tbody>
                <tr data-testid="flow-row-load">
                  <th scope="row" className="py-0.5 text-left font-normal text-muted-foreground">Full load rows</th>
                  <FlowCell value={s.total_snapshot_rows} />
                  <FlowCell value={s.total_applied_snapshot_rows} />
                </tr>
                {noChanges ? (
                  <tr data-testid="flow-row-changes-none">
                    <th scope="row" className="py-0.5 text-left font-normal text-muted-foreground">
                      Changes since the load
                    </th>
                    <td colSpan={2} className="py-0.5 pl-3 text-right text-muted-foreground">
                      None yet
                    </td>
                  </tr>
                ) : (
                  <>
                    <tr>
                      <th
                        scope="colgroup"
                        colSpan={3}
                        className="pt-2 pb-0.5 text-left text-[11px] font-normal uppercase tracking-wide text-muted-foreground"
                      >
                        Changes since the load
                      </th>
                    </tr>
                    <tr>
                      <th scope="row" className="py-0.5 text-left font-normal text-muted-foreground">Inserts</th>
                      <FlowCell value={s.total_inserts} />
                      <FlowCell value={s.total_applied_inserts} />
                    </tr>
                    <tr>
                      <th scope="row" className="py-0.5 text-left font-normal text-muted-foreground">Updates</th>
                      <FlowCell value={s.total_updates} />
                      <FlowCell value={s.total_applied_updates} />
                    </tr>
                    <tr>
                      <th scope="row" className="py-0.5 text-left font-normal text-muted-foreground">Deletes</th>
                      <FlowCell value={s.total_deletes} />
                      <FlowCell value={s.total_applied_deletes} />
                    </tr>
                    <tr className="border-t border-border/40">
                      <th scope="row" className="py-0.5 text-left font-normal text-muted-foreground">All changes</th>
                      <FlowCell value={s.total_cdc_events} />
                      <FlowCell value={s.total_applied_cdc_events} />
                    </tr>
                  </>
                )}
              </tbody>
            </table>
            <RateRows
              first="Captured"
              second="Written"
              rates={[captured.rate, written.rate]}
              idleDetail={lastMovedDetail(data?.tables, data?.total, formatAge)}
              sinceOpen={sinceOpenLine(["captured", "written"], [captured.sinceOpen, written.sinceOpen])}
            />
            <div className="border-t border-border/60 pt-2 space-y-1" data-testid="flow-tables">
              <ThroughputRow label="Tables" value={num(s.total_tables).toLocaleString()} />
              {replicating > 0 && <ThroughputRow label="Replicating" value={replicating.toLocaleString()} />}
              {attention.map((row) => (
                <ThroughputRow key={row.key} label={row.label} value={row.count.toLocaleString()} />
              ))}
            </div>
          </>
        )}
      </CardContent>
    </Card>
  )
}

function DiagnoseCard({ pipelineId }: { pipelineId: string }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <CheckCircle2 className="h-4 w-4" />
          Diagnose
        </CardTitle>
      </CardHeader>
      <CardContent>
        <DiagnosePanel pipelineId={pipelineId} />
      </CardContent>
    </Card>
  )
}

/**
 * The pipeline's Kafka consumers and how far behind each one is.
 *
 * This is NOT the consumer table on Admin -> Health. That one reads
 * sentinel_component_health, which is keyed by topic and written from a hardcoded
 * list of the topics the orchestrator process itself consumes -- the PLATFORM's
 * own workers, admin-only, with no workspace column and
 * not one pipeline topic in it. This card reads the pipeline's own census
 * (`GET /pipelines/:id/consumers`, Viewer, no feature flag) and shows the consumers
 * that actually move this customer's rows to the destination.
 *
 * It lives in MonitorTab rather than in MonitoringOverviewTab on purpose: the
 * Overview sub-tab is hidden entirely unless NEXT_PUBLIC_FEATURE_MONITORING_OVERVIEW
 * is set (PipelineMonitoringPanel renders neither its trigger nor its content), and
 * staging sets that to false. A consumer view that vanished with an infrastructure
 * flag would repeat the mistake that hid the lag alerts.
 */
interface ConsumerTopicLag {
  topic: string
  table?: string
  // No omitempty on the server: 0 means "drained", and must not be confused with
  // a field that was not sent.
  lag: number
  committed: number
}

interface PipelineConsumer {
  group: string
  role: string
  /** Absent when the broker could not be asked -- render as unknown, never Stable. */
  state?: string
  members?: number
  total_lag: number
  topics: ConsumerTopicLag[]
  measured_at: string
}

interface ConsumersResponse {
  consumers: PipelineConsumer[]
  /** false = nothing has measured yet (no census table, or the Sentinel has not run). */
  measured: boolean
}

const CONSUMER_ROLE_LABELS: Record<string, string> = {
  sink: "Writes changes to the destination",
  sink_batch: "Loads existing rows (backfill)",
  sink_stream: "Writes changes to the destination (no snapshot)",
}

/**
 * How a group's state reads next to its lag.
 *
 * "Empty" is the one that earns this function. A group with lag 0 and no members
 * is not caught up -- nobody is consuming it. Lag alone cannot say that, which is
 * the same blind spot that let a dead producer read as healthy.
 */
export function consumerStateTone(state: string | undefined, members: number | undefined):
  { label: string; tone: "ok" | "warn" | "bad" | "neutral"; detail?: string } {
  if (!state) return { label: "Unknown", tone: "neutral", detail: "the broker could not be asked" }
  switch (state) {
    case "Stable":
      return { label: "Stable", tone: "ok" }
    case "Empty":
      return { label: "Empty", tone: "bad", detail: "no consumer is reading this — nothing is being written" }
    case "Dead":
      return { label: "Dead", tone: "bad", detail: "the group is gone" }
    case "PreparingRebalance":
    case "CompletingRebalance":
      return { label: "Rebalancing", tone: "warn", detail: "members are being reassigned; lag is a moving target" }
    default:
      return { label: state, tone: members && members > 0 ? "ok" : "neutral" }
  }
}

const CONSUMER_TONE_CLASS: Record<"ok" | "warn" | "bad" | "neutral", string> = {
  ok: "text-green-600 dark:text-green-400",
  warn: "text-amber-600 dark:text-amber-400",
  bad: "text-red-600 dark:text-red-400",
  neutral: "text-muted-foreground",
}

function fmtLag(n: number): string {
  return n === 0 ? "0" : n.toLocaleString()
}

/**
 * What a committed offset is, said where the number is shown. It is the sink's
 * position on the topic, summed over partitions: every message it has consumed,
 * the initial load's rows and delete tombstones included. Not a row count, so it
 * is never labelled as one.
 */
const COMMITTED_OFFSET_TITLE =
  "The sink's committed Kafka offset on this topic (all partitions): a position, not a row count. It counts every message consumed, the initial load's rows and delete tombstones included."

const offsetKey = (group: string, topic: string) => `${group}\u0000${topic}`

/**
 * Each topic's committed offset as this page first saw it, so a row can say how far
 * the sink has moved since the reader opened the page. The server keeps no history,
 * so "since the page opened" is the only honest window.
 *
 * Returns the next baselines, or null when nothing changed. A topic seen for the
 * first time is baselined at its current offset; an offset BELOW its baseline (the
 * group was reset or replaced) is re-baselined rather than shown as a negative move.
 */
export function advanceOffsetBaselines(
  prev: Readonly<Record<string, number>>,
  consumers: PipelineConsumer[],
): Record<string, number> | null {
  let next: Record<string, number> | null = null
  for (const c of consumers) {
    for (const t of c.topics) {
      if (typeof t.committed !== "number") continue
      const k = offsetKey(c.group, t.topic)
      const base = (next ?? prev)[k]
      if (base === undefined || t.committed < base) {
        next = next ?? { ...prev }
        next[k] = t.committed
      }
    }
  }
  return next
}

/** How far a topic's committed offset moved since its baseline; 0 when unmeasured. */
export function offsetMovedSince(baselines: Readonly<Record<string, number>>, group: string, t: ConsumerTopicLag): number {
  const base = baselines[offsetKey(group, t.topic)]
  if (typeof t.committed !== "number" || base === undefined) return 0
  return Math.max(0, t.committed - base)
}

function useOffsetBaselines(consumers: PipelineConsumer[] | undefined): Record<string, number> {
  const [baselines, setBaselines] = useState<Record<string, number>>({})
  // Adjusted during render, the pattern useCounterRate uses: an effect would
  // paint one frame with every new topic missing its baseline.
  const next = consumers ? advanceOffsetBaselines(baselines, consumers) : null
  if (next) {
    setBaselines(next)
    return next
  }
  return baselines
}

/** The oldest reading on the card: the one line that says how fresh all of it is. */
function oldestMeasurement(consumers: PipelineConsumer[]): string | undefined {
  let oldest: string | undefined
  let oldestMs = Infinity
  for (const c of consumers) {
    const ms = Date.parse(c.measured_at)
    if (Number.isFinite(ms) && ms < oldestMs) {
      oldestMs = ms
      oldest = c.measured_at
    }
  }
  return oldest
}

function ConsumerRow({ c, baselines }: { c: PipelineConsumer; baselines: Readonly<Record<string, number>> }) {
  const state = consumerStateTone(c.state, c.members)
  const behind = c.total_lag > 0
  // "tables" only when every topic resolved to one of this pipeline's tables;
  // otherwise the list below shows raw topics, and says so.
  const noun = c.topics.every((t) => t.table) ? "table" : "topic"
  const movedTotal = c.topics.reduce((sum, t) => sum + offsetMovedSince(baselines, c.group, t), 0)
  return (
    <li className="py-2.5 first:pt-0 last:pb-0" data-testid={`consumer-${c.group}`}>
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-0.5">
        <p className="text-sm font-medium text-foreground">{CONSUMER_ROLE_LABELS[c.role] ?? c.role}</p>
        <div className="flex items-baseline gap-3 text-xs">
          <span className={cn("font-medium", CONSUMER_TONE_CLASS[state.tone])} title={state.detail}>
            {state.label}
            {typeof c.members === "number" && ` · ${c.members} member${c.members === 1 ? "" : "s"}`}
          </span>
          <span className={cn("font-medium tabular-nums", behind ? "text-amber-600 dark:text-amber-400" : "text-foreground")}>
            {fmtLag(c.total_lag)} behind
          </span>
        </div>
      </div>
      <p className="truncate font-mono text-[11px] text-muted-foreground" title={c.group}>
        {c.group}
      </p>
      {state.detail && state.tone !== "ok" && (
        <p className={cn("mt-1 text-[11px]", CONSUMER_TONE_CLASS[state.tone])}>{state.detail}</p>
      )}
      {c.topics.length > 0 && (
        // Mostly a list of zeros, so it stays folded until something is behind.
        // `open` only seeds the element: a reader who opens it keeps it open
        // across polls, because React re-applies the prop only when it changes.
        <details open={behind} className="group mt-1.5">
          <summary className="flex cursor-pointer list-none items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground [&::-webkit-details-marker]:hidden">
            <ChevronRight className="h-3 w-3 transition-transform group-open:rotate-90" aria-hidden="true" />
            {c.topics.length} {noun}
            {c.topics.length === 1 ? "" : "s"}
            {movedTotal > 0 && (
              <span className="text-emerald-700 dark:text-emerald-400" title={COMMITTED_OFFSET_TITLE}>
                {" "}
                · offsets +{movedTotal.toLocaleString()} since this page opened
              </span>
            )}
          </summary>
          <table className="mt-1 w-full text-xs">
            {/* One such table per group; the caption says whose it is. */}
            <caption className="sr-only">Committed offsets for {c.group}</caption>
            <thead>
              <tr className="text-[10px] uppercase tracking-wide text-muted-foreground">
                <th scope="col" className="pl-4 text-left font-normal">
                  {noun}
                </th>
                <th scope="col" className="pl-3 text-right font-normal" title={COMMITTED_OFFSET_TITLE}>
                  Committed offset
                </th>
                <th scope="col" className="pl-3 text-right font-normal">
                  Behind
                </th>
              </tr>
            </thead>
            <tbody>
              {c.topics.map((t) => {
                const moved = offsetMovedSince(baselines, c.group, t)
                return (
                  <tr key={t.topic} data-testid={`consumer-topic-${t.topic}`}>
                    {/* The table name when the topic is one of this pipeline's
                        table topics, else the raw topic — never a guess. */}
                    <td className="w-full max-w-0 truncate pl-4 text-muted-foreground" title={t.topic}>
                      {t.table || t.topic}
                    </td>
                    <td className="whitespace-nowrap pl-3 text-right tabular-nums text-muted-foreground" title={COMMITTED_OFFSET_TITLE}>
                      {typeof t.committed === "number" ? t.committed.toLocaleString() : "—"}
                      {moved > 0 && (
                        <span className="ml-1 text-emerald-700 dark:text-emerald-400">+{moved.toLocaleString()}</span>
                      )}
                    </td>
                    <td
                      className={cn(
                        "whitespace-nowrap pl-3 text-right tabular-nums",
                        t.lag > 0 ? "text-amber-600 dark:text-amber-400" : "text-muted-foreground",
                      )}
                    >
                      {fmtLag(t.lag)}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </details>
      )}
    </li>
  )
}

function ConsumersCard({ pipelineId }: { pipelineId: string }) {
  const { data, error } = usePolledJson<ConsumersResponse>(API_ENDPOINTS.PIPELINES.CONSUMERS(pipelineId))
  const measuredAt = data?.measured ? oldestMeasurement(data.consumers) : undefined
  const baselines = useOffsetBaselines(data?.measured ? data.consumers : undefined)

  return (
    <Card data-testid="consumers-card">
      <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Users className="h-4 w-4" />
          Consumers
        </CardTitle>
        {/* formatAge already ends in "ago". */}
        {measuredAt && <span className="text-[11px] text-muted-foreground">measured {formatAge(measuredAt)}</span>}
      </CardHeader>
      <CardContent className="space-y-3">
        {error && !data ? (
          // A read that failed is not "no consumers". Saying so is the point:
          // an empty list here reads as "nothing is moving your data".
          <p className="text-xs text-amber-600 dark:text-amber-400">
            Couldn&apos;t load consumers ({error}). Retrying.
          </p>
        ) : !data ? (
          <p className="text-xs text-muted-foreground">Loading consumers…</p>
        ) : !data.measured ? (
          <p className="text-xs text-muted-foreground">
            No consumer readings yet — this pipeline&apos;s sink has not been measured.
          </p>
        ) : data.consumers.length === 0 ? (
          <p className="text-xs text-muted-foreground">
            No consumers running yet. They appear once the pipeline&apos;s sink starts reading.
          </p>
        ) : (
          <>
            {error && (
              <p className="text-xs text-amber-600 dark:text-amber-400">
                These readings may be out of date — the last refresh failed ({error}).
              </p>
            )}
            <ul className="divide-y divide-border/60">
              {data.consumers.map((c) => (
                <ConsumerRow key={c.group} c={c} baselines={baselines} />
              ))}
            </ul>
          </>
        )}
      </CardContent>
    </Card>
  )
}

export function MonitorTab({ pipelineId }: { pipelineId: string }) {
  // One runtime poller for the whole tab: DependenciesCard needs the dependency
  // list and health, the flow cards need the mode and execution id.
  const { runtime, error: runtimeError } = usePipelineRuntime(pipelineId)
  const isCDC = runtime?.mode === "cdc"

  return (
    <div className="space-y-4">
      {isCDC ? (
        <FlowCard pipelineId={pipelineId} loadStatus={describeLoadStatus(runtime)} />
      ) : (
        <ThroughputCard pipelineId={pipelineId} executionId={runtime?.execution_id} />
      )}
      {/* CDC only: a batch pipeline moves its rows through the executor, not
          through a consumer group, so it has nothing to show here. */}
      {isCDC && <ConsumersCard pipelineId={pipelineId} />}
      <div className="grid gap-4 lg:grid-cols-2">
        <DiagnoseCard pipelineId={pipelineId} />
        <DependenciesCard runtime={runtime} error={runtimeError} />
      </div>
    </div>
  )
}
