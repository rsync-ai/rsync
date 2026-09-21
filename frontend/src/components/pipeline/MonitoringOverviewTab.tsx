"use client"

/**
 * Monitoring → Overview: "is this pipeline healthy right now, and if not, where?"
 *
 * Four tiles (two for a batch pipeline), a short list of the tables that need
 * attention, one line about automated decisions. The
 * Throughput and Dependencies cards below it stay the detail view.
 *
 * Three reads, each independent so one failure never blanks the others:
 *   - /runtime            — mode, phase, pending_events, the latest run's id
 *   - /monitoring/overview — the newest Kafka lag reading and agent decisions
 *   - /table-stats        — per-table status, failed rows and last write
 * A failed poll keeps the last good numbers and says so on the tile (the F-242
 * lesson: never let a read error look like an empty, healthy pipeline).
 *
 * This replaced two cards that answered the wrong question: agent decisions in
 * the last hour (usually "0") and a one-hour data-plane rollup whose "CDC lag"
 * was the oldest reading of the hour, printed as raw ms. The page also never
 * refreshed.
 */

import Link from "next/link"
import { useEffect, useRef, useState, type Dispatch, type SetStateAction } from "react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import type { PipelineRuntime, RuntimePhase } from "@/lib/hooks/usePipelineRuntime"
import { formatAbsoluteTime, formatAge, formatCount } from "@/lib/transform-format"
import { cn } from "@/lib/utils"
import type { TableStatsSummaryPayload } from "./executionSummary"

export const OVERVIEW_POLL_MS = 30_000

// A write older than this is "behind" only while changes are waiting: a quiet
// source with nothing pending is idle, not stalled (KI-CDC-QUIET-STREAM-REPORTS-IDLE).
const STALE_WRITE_MS = 5 * 60_000

const ATTENTION_LIMIT = 5

// The overview's agent query stops at 20 rows (monitoring.go fetchAgentReasoning),
// so a count of 20 means "20 or more".
const AGENT_DECISIONS_CAP = 20

const BATCH_IN_FLIGHT: ReadonlySet<RuntimePhase> = new Set<RuntimePhase>([
  "initializing",
  "planning",
  "validating",
  "syncing",
])

// ---------------------------------------------------------------------------
// Payloads (the fields this tab reads).
// ---------------------------------------------------------------------------

type OverviewPayload = {
  agent_reasoning?: {
    total_decisions: number
    recent_decisions?: Array<{ confidence?: number | null }>
  }
  agent_reasoning_error?: string
  data_plane?: {
    /** Changes in Kafka the sink has not read yet, from the newest status poll. */
    sink_lag_messages?: number | null
    lag_measured_at?: string | null
  }
  data_plane_error?: string
}

export type AttentionTable = {
  qualified_name: string
  mode?: string
  status?: string
  read_rows?: number | null
  inserted_rows?: number | null
  total_events?: number | null
  applied_total_events?: number | null
  last_applied_ts?: string | null
  dlq_rows?: number | null
  completed_at?: string | null
}

type TableStatsPayload = {
  summary?: TableStatsSummaryPayload
  tables?: AttentionTable[]
  total?: number
}

// ---------------------------------------------------------------------------
// Pure derivations (exported for tests).
// ---------------------------------------------------------------------------

export type Tone = "ok" | "warn" | "bad" | "neutral"

export type TileState = {
  value: string
  detail?: string
  tone: Tone
  /** Absolute time behind a relative value, for the hover title. */
  title?: string
}

function plural(n: number, one: string, many = `${one}s`) {
  return `${formatCount(n)} ${n === 1 ? one : many}`
}

function newestIso(values: Array<string | null | undefined>): string | null {
  let best: string | null = null
  let bestMs = -Infinity
  for (const v of values) {
    if (!v) continue
    const ms = Date.parse(v)
    if (!Number.isNaN(ms) && ms > bestMs) {
      best = v
      bestMs = ms
    }
  }
  return best
}

/** CDC: when the destination last received a change. Staleness is judged as of the table read. */
export function cdcFreshness(tables: AttentionTable[], backlogged: boolean, readAtMs: number): TileState {
  const newest = newestIso(tables.map((t) => t.last_applied_ts))
  if (!newest) {
    return {
      value: "Nothing written yet",
      detail: "no change has reached the destination",
      tone: backlogged ? "warn" : "neutral",
    }
  }
  const stale = readAtMs - Date.parse(newest) > STALE_WRITE_MS
  return {
    value: formatAge(newest),
    detail: backlogged && stale ? "since the last write, with changes waiting" : "since the last change was written",
    tone: backlogged && stale ? "warn" : "ok",
    title: formatAbsoluteTime(newest),
  }
}

/** Batch: when the latest run finished, or that one is running. */
export function batchFreshness(phase: string | undefined, tables: AttentionTable[], hasRun: boolean): TileState {
  if (phase && BATCH_IN_FLIGHT.has(phase as RuntimePhase)) return { value: "Run in progress", tone: "neutral" }
  if (!hasRun) return { value: "No run yet", tone: "neutral" }
  const newest = newestIso(tables.map((t) => t.completed_at))
  const failed = phase === "failed"
  if (!newest) {
    return {
      value: failed ? "Last run failed" : "—",
      detail: "the last run reported no finish time",
      tone: failed ? "bad" : "neutral",
    }
  }
  return {
    value: formatAge(newest),
    detail: failed ? "since the last run ended in failure" : "since the last run finished",
    tone: failed ? "bad" : "ok",
    title: formatAbsoluteTime(newest),
  }
}

export function backlogTile(pending: number | undefined): TileState {
  if (typeof pending !== "number") return { value: "—", detail: "not reported by this server", tone: "neutral" }
  if (pending <= 0) return { value: "Caught up", detail: "nothing waiting to be written", tone: "ok" }
  return { value: plural(pending, "change"), detail: "read by the sink, not yet written", tone: "warn" }
}

export function kafkaTile(messages: number | null | undefined): TileState {
  if (typeof messages !== "number") {
    return { value: "No reading", detail: "no lag reading in the last 24h", tone: "neutral" }
  }
  if (messages <= 0) return { value: "Caught up", detail: "nothing waiting in Kafka", tone: "ok" }
  return { value: plural(messages, "change"), detail: "in Kafka, not yet read by the sink", tone: "warn" }
}

export function errorsTile(summary: TableStatsSummaryPayload | undefined, isCdc: boolean): TileState {
  const window = isCdc ? "since the stream started" : "in the last run"
  const failed = summary?.tables_failed ?? 0
  const dlq = summary?.total_dlq_rows ?? 0
  if (failed > 0) {
    return {
      value: `${plural(failed, "table")} failed`,
      detail: dlq > 0 ? `${plural(dlq, "row")} failed to load ${window}` : window,
      tone: "bad",
    }
  }
  if (dlq > 0) return { value: `${plural(dlq, "failed row")}`, detail: `sent to the dead-letter queue ${window}`, tone: "warn" }
  return { value: "None", detail: `no failed tables or rows ${window}`, tone: "ok" }
}

export type AttentionItem = { name: string; reason: string; tone: Tone }

/**
 * Tables worth a look, worst first: failed, then with failed rows, then
 * degraded, then (CDC) the furthest behind — ties go to the one whose last
 * write is oldest. A table that is merely quiet is not listed.
 */
export function tablesNeedingAttention(tables: AttentionTable[], isCdc: boolean): AttentionItem[] {
  const behind = (t: AttentionTable) => Math.max(0, (t.total_events ?? 0) - (t.applied_total_events ?? 0))
  const dlq = (t: AttentionTable) => t.dlq_rows ?? 0
  const rank = (t: AttentionTable) => {
    if (t.status === "failed") return 0
    if (dlq(t) > 0) return 1
    if (t.status === "degraded") return 2
    if (isCdc && behind(t) > 0) return 3
    return -1
  }
  const appliedMs = (t: AttentionTable) => {
    const ms = t.last_applied_ts ? Date.parse(t.last_applied_ts) : NaN
    return Number.isNaN(ms) ? -Infinity : ms
  }

  return tables
    .filter((t) => rank(t) >= 0)
    .sort(
      (a, b) =>
        rank(a) - rank(b) ||
        dlq(b) - dlq(a) ||
        behind(b) - behind(a) ||
        appliedMs(a) - appliedMs(b) ||
        a.qualified_name.localeCompare(b.qualified_name)
    )
    .map((t) => {
      const r = rank(t)
      const failedRows = dlq(t) > 0 ? `${plural(dlq(t), "row")} failed to load` : ""
      if (r === 0) return { name: t.qualified_name, reason: ["Failed", failedRows].filter(Boolean).join(" · "), tone: "bad" as Tone }
      if (r === 1) return { name: t.qualified_name, reason: failedRows, tone: "warn" as Tone }
      if (r === 2) {
        const reason =
          t.mode === "batch" && (t.read_rows ?? 0) > 0
            ? (t.inserted_rows ?? 0) === 0
              ? "Degraded · destination wrote 0 rows"
              : `Degraded · ${formatCount(t.inserted_rows)} of ${formatCount(t.read_rows)} rows written`
            : "Degraded"
        return { name: t.qualified_name, reason, tone: "warn" as Tone }
      }
      const last = t.last_applied_ts ? ` · last write ${formatAge(t.last_applied_ts)}` : " · nothing written yet"
      return { name: t.qualified_name, reason: `${plural(behind(t), "change")} waiting${last}`, tone: "warn" as Tone }
    })
}

/** "3 automated decisions in the last 24h, lowest confidence 62%", or null for none. */
export function agentDecisionsLine(reasoning: OverviewPayload["agent_reasoning"]): string | null {
  const n = reasoning?.total_decisions ?? 0
  if (n <= 0) return null
  const count = n >= AGENT_DECISIONS_CAP ? `At least ${AGENT_DECISIONS_CAP} automated decisions` : `${plural(n, "automated decision")}`
  const confidences = (reasoning?.recent_decisions ?? [])
    .map((d) => d.confidence)
    .filter((c): c is number => typeof c === "number")
  const lowest = confidences.length ? `, lowest confidence ${Math.round(Math.min(...confidences) * 100)}%` : ""
  return `${count} in the last 24h${lowest}`
}

// ---------------------------------------------------------------------------
// Reads.
// ---------------------------------------------------------------------------

// `pid` is the pipeline the reading belongs to: after a switch to another
// pipeline the old one's numbers read as empty until its own first read lands.
type Source<T> = { data: T | null; error: string | null; at: number | null; pid?: string }

const EMPTY: Source<never> = { data: null, error: null, at: null }

type ReadResult<T> = { data: T } | { error: string } | { aborted: true }

async function readJson<T>(url: string, signal: AbortSignal): Promise<ReadResult<T>> {
  try {
    const res = await authFetch(url, { cache: "no-store", signal })
    if (!res.ok) return { error: `HTTP ${res.status}` }
    return { data: (await res.json()) as T }
  } catch (e) {
    if ((e as { name?: string })?.name === "AbortError" || signal.aborted) return { aborted: true }
    return { error: "network error" }
  }
}

// A failed read keeps the last good data; only a success replaces it.
function settle<T>(set: Dispatch<SetStateAction<Source<T>>>, r: ReadResult<T>, pid: string) {
  if ("aborted" in r) return
  if ("error" in r) set((prev) => ({ ...(prev.pid === pid ? prev : EMPTY), error: r.error, pid }))
  else set({ data: r.data, error: null, at: Date.now(), pid })
}

/**
 * CDC counters live under the stable key execution_id == pipeline_id, which the
 * handler substitutes for mode=cdc; batch is scoped to the latest run, or there
 * is nothing to read yet. sort=status puts failed and degraded tables first, so
 * a pipeline with more than 1000 tables still returns its problem tables.
 */
function tableStatsUrl(pipelineId: string, rt: PipelineRuntime | null): string | null {
  if (!rt) return null
  const base = `${API_ENDPOINTS.PIPELINES.TABLE_STATS(pipelineId)}?limit=1000&sort=status`
  if (rt.mode === "cdc") return `${base}&mode=cdc`
  return rt.execution_id ? `${base}&execution_id=${encodeURIComponent(rt.execution_id)}` : null
}

function useOverviewSources(pipelineId: string) {
  const [runtime, setRuntime] = useState<Source<PipelineRuntime>>(EMPTY)
  const [overview, setOverview] = useState<Source<OverviewPayload>>(EMPTY)
  const [tables, setTables] = useState<Source<TableStatsPayload>>(EMPTY)
  const lastRuntimeRef = useRef<PipelineRuntime | null>(null)

  useEffect(() => {
    lastRuntimeRef.current = null
    let cancelled = false
    let inflight: AbortController | null = null

    const tick = async () => {
      // Nobody is looking: skip, and catch up as soon as the tab is shown again.
      if (document.visibilityState === "hidden") return
      inflight?.abort()
      const ac = new AbortController()
      inflight = ac

      const [rt, ov] = await Promise.all([
        readJson<PipelineRuntime>(API_ENDPOINTS.PIPELINES.RUNTIME(pipelineId), ac.signal),
        readJson<OverviewPayload>(`${API_ENDPOINTS.PIPELINES.MONITORING_OVERVIEW(pipelineId)}?range=last_24h`, ac.signal),
      ])
      if (cancelled || ac.signal.aborted) return
      settle(setRuntime, rt, pipelineId)
      settle(setOverview, ov, pipelineId)
      if ("data" in rt) lastRuntimeRef.current = rt.data

      const url = tableStatsUrl(pipelineId, lastRuntimeRef.current)
      if (!url) return
      const ts = await readJson<TableStatsPayload>(url, ac.signal)
      if (cancelled || ac.signal.aborted) return
      settle(setTables, ts, pipelineId)
    }

    void tick()
    const timer = window.setInterval(() => void tick(), OVERVIEW_POLL_MS)
    const onVisibility = () => {
      if (document.visibilityState === "visible") void tick()
    }
    document.addEventListener("visibilitychange", onVisibility)
    return () => {
      cancelled = true
      window.clearInterval(timer)
      document.removeEventListener("visibilitychange", onVisibility)
      inflight?.abort()
    }
  }, [pipelineId])

  const current = <T,>(s: Source<T>): Source<T> => (s.pid === pipelineId ? s : EMPTY)
  return { runtime: current(runtime), overview: current(overview), tables: current(tables) }
}

// ---------------------------------------------------------------------------
// View.
// ---------------------------------------------------------------------------

const TONE_BORDER: Record<Tone, string> = {
  ok: "border-l-emerald-500",
  warn: "border-l-amber-500",
  bad: "border-l-red-500",
  neutral: "border-l-muted-foreground/30",
}

const TONE_TEXT: Record<Tone, string> = {
  ok: "text-emerald-700 dark:text-emerald-400",
  warn: "text-amber-700 dark:text-amber-400",
  bad: "text-red-700 dark:text-red-400",
  neutral: "text-foreground",
}

function clock(ms: number | string | null | undefined): string {
  if (ms == null) return ""
  const d = new Date(ms)
  if (Number.isNaN(d.getTime())) return ""
  return d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit", second: "2-digit" })
}

function readError(error: string | null, hasData: boolean): string | null {
  if (!error) return null
  return hasData ? `Couldn't refresh (${error}); showing the last reading` : `Couldn't load (${error})`
}

function Tile(props: {
  id: string
  label: string
  state: TileState | null
  asOf?: number | string | null
  error?: string | null
}) {
  const { id, label, state, asOf, error } = props
  const tone: Tone = state?.tone ?? "neutral"
  const asOfText = clock(asOf)
  return (
    <div
      data-testid={`overview-tile-${id}`}
      data-tone={tone}
      className={cn("flex flex-col rounded-lg border border-l-4 bg-card p-3", TONE_BORDER[tone])}
    >
      <div className="text-xs font-medium text-muted-foreground">{label}</div>
      <div className={cn("mt-1 text-lg font-semibold leading-tight", TONE_TEXT[tone])} title={state?.title || undefined}>
        {state ? state.value : error ? "—" : "Loading…"}
      </div>
      {state?.detail && <div className="mt-0.5 text-xs text-muted-foreground">{state.detail}</div>}
      {error && <div className="mt-1 text-xs text-amber-700 dark:text-amber-400">{error}</div>}
      {asOfText && <div className="mt-auto pt-2 text-[11px] text-muted-foreground">as of {asOfText}</div>}
    </div>
  )
}

export function MonitoringOverviewTab({
  pipelineId,
  onOpenActivity,
}: {
  pipelineId: string
  /** Switches the Monitoring card to its Activity sub-tab. */
  onOpenActivity?: () => void
}) {
  const { runtime, overview, tables } = useOverviewSources(pipelineId)
  const rt = runtime.data
  const isCdc = rt?.mode === "cdc"
  const rows = tables.data?.tables ?? []
  const summary = tables.data?.summary
  const dataPlane = overview.data?.data_plane
  const pending = rt?.liveness?.pending_events
  const kafkaLag = dataPlane?.sink_lag_messages
  const backlogged = (pending ?? 0) > 0 || (kafkaLag ?? 0) > 0
  const hasRun = Boolean(rt?.execution_id)

  // Table-derived tiles wait for their own read; a batch pipeline with no run
  // has nothing to read and says so instead of "Loading…".
  const tablesKnown = tables.data !== null
  const tablesError = readError(tables.error, tablesKnown)

  const freshness: TileState | null = !rt
    ? null
    : isCdc
      ? tablesKnown
        ? cdcFreshness(rows, backlogged, tables.at ?? 0)
        : null
      : !hasRun || tablesKnown || BATCH_IN_FLIGHT.has(rt.phase)
        ? batchFreshness(rt.phase, rows, hasRun)
        : null
  const errors: TileState | null = tablesKnown
    ? errorsTile(summary, isCdc)
    : rt && !isCdc && !hasRun
      ? { value: "None", detail: "no run yet", tone: "neutral" }
      : null
  const overviewError = readError(overview.error ?? dataPlaneError(overview.data), overview.data !== null)

  const attention = tablesNeedingAttention(rows, isCdc)
  const tableCount = summary?.total_tables ?? tables.data?.total ?? rows.length
  const decisions = agentDecisionsLine(overview.data?.agent_reasoning)

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-sm font-semibold">Health at a glance</h3>
        <span className="text-xs text-muted-foreground">Refreshes every 30 s</span>
      </div>

      {!rt ? (
        runtime.error ? (
          <div className="rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300">
            Couldn&apos;t load this pipeline&apos;s status ({runtime.error}). Retrying every 30 s.
          </div>
        ) : (
          <div className="text-sm text-muted-foreground">Loading…</div>
        )
      ) : (
        <>
          {runtime.error && (
            <p className="text-xs text-amber-700 dark:text-amber-400">{readError(runtime.error, true)}</p>
          )}
          <div className={cn("grid grid-cols-1 gap-3 sm:grid-cols-2", isCdc && "lg:grid-cols-4")}>
            <Tile id="freshness" label="Freshness" state={freshness} asOf={tables.at ?? runtime.at} error={tablesError} />
            {isCdc && (
              <Tile
                id="backlog"
                label="Backlog"
                state={backlogTile(pending)}
                asOf={runtime.at}
                error={readError(runtime.error, true)}
              />
            )}
            {isCdc && (
              <Tile
                id="kafka"
                label="Waiting in Kafka"
                state={overview.data ? kafkaTile(kafkaLag) : null}
                asOf={dataPlane?.lag_measured_at}
                error={overviewError}
              />
            )}
            <Tile id="errors" label="Errors" state={errors} asOf={tables.at} error={tablesError} />
          </div>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Tables needing attention</CardTitle>
            </CardHeader>
            <CardContent>
              {!tablesKnown ? (
                <p className="text-sm text-muted-foreground">
                  {tablesError ?? (!isCdc && !hasRun ? "No run yet." : "Loading…")}
                </p>
              ) : attention.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  {isCdc
                    ? `All ${plural(tableCount, "table")} caught up.`
                    : `No problems in the last run's ${plural(tableCount, "table")}.`}{" "}
                  <Link
                    href={`/pipelines/${pipelineId}?tab=table-stats`}
                    className="text-blue-600 hover:underline dark:text-blue-400"
                  >
                    Table statistics →
                  </Link>
                </p>
              ) : (
                <div className="space-y-2">
                  <ul className="divide-y rounded-md border" data-testid="overview-attention">
                    {attention.slice(0, ATTENTION_LIMIT).map((item) => (
                      <li key={item.name} className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-0.5 px-3 py-2">
                        <span className="break-all font-mono text-xs">{item.name}</span>
                        <span className={cn("text-xs", TONE_TEXT[item.tone])}>{item.reason}</span>
                      </li>
                    ))}
                  </ul>
                  <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
                    <span>
                      {attention.length > ATTENTION_LIMIT
                        ? `+${formatCount(attention.length - ATTENTION_LIMIT)} more`
                        : `${plural(attention.length, "table")} of ${formatCount(tableCount)}`}
                    </span>
                    <Link
                      href={`/pipelines/${pipelineId}?tab=table-stats`}
                      className="text-blue-600 hover:underline dark:text-blue-400"
                    >
                      Table statistics →
                    </Link>
                  </div>
                </div>
              )}
              {tablesKnown && tablesError && <p className="mt-2 text-xs text-amber-700 dark:text-amber-400">{tablesError}</p>}
              {tablesKnown && tableCount > rows.length && (
                <p className="mt-2 text-xs text-muted-foreground">
                  Checked the first {formatCount(rows.length)} of {formatCount(tableCount)} tables, problem tables first.
                </p>
              )}
            </CardContent>
          </Card>
        </>
      )}

      {decisions && (
        <div
          data-testid="overview-decisions"
          className="flex flex-wrap items-center justify-between gap-2 rounded-lg border px-3 py-2 text-sm"
        >
          <span>{decisions}</span>
          {onOpenActivity && (
            <Button variant="outline" size="sm" onClick={onOpenActivity}>
              View in Activity
            </Button>
          )}
        </div>
      )}
      {!decisions && (overview.data?.agent_reasoning_error || (!overview.data && overview.error)) && (
        <p className="text-xs text-muted-foreground">
          Couldn&apos;t load automated decisions ({overview.data?.agent_reasoning_error ? "server error" : overview.error}).
        </p>
      )}
    </div>
  )
}

// The server reports a failed data-plane query beside an otherwise good payload.
function dataPlaneError(o: OverviewPayload | null): string | null {
  return o?.data_plane_error ? "server error" : null
}
