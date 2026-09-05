"use client"

import { useEffect, useRef, useState } from "react"
import { Activity, AlertTriangle, CheckCircle2, HeartPulse } from "lucide-react"

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
import { eventDetail, eventLabel, eventStageLabel, isNoiseEvent } from "./eventDisplay"
import { cn } from "@/lib/utils"

// ---------------------------------------------------------------------------
// Shared shapes — mirror the api-gateway responses described in the task.
// ---------------------------------------------------------------------------
interface TableStatsSummary {
  mode?: "batch" | "cdc"
  total_tables?: number
  tables_completed?: number
  tables_failed?: number
  tables_running?: number
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
}

interface TableStatsResponse {
  summary: TableStatsSummary
  tables?: unknown[]
}

interface PipelineRunEvent {
  event_id: string
  seq?: number
  event_type: string
  stage_id?: string
  stage_group?: string
  severity?: string
  occurred_at?: string
  received_at: string
  payload: Record<string, unknown>
}

interface EventsResponse {
  events: PipelineRunEvent[]
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

function relTime(iso: string | undefined, now: number): string {
  if (!iso) return ""
  const secs = (now - new Date(iso).getTime()) / 1000
  const s = Math.max(0, Math.floor(secs))
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  return `${Math.floor(h / 24)}d ago`
}

function severityDot(severity: string | undefined): string {
  switch ((severity || "").toLowerCase()) {
    case "error":
    case "critical":
      return "bg-red-500"
    case "warning":
    case "warn":
      return "bg-amber-500"
    default:
      return "bg-muted-foreground/40"
  }
}

// Generic 5s-polling JSON fetch hook used for table-stats and events.
//
// A 404 is an ordinary failure here, not an empty result. Both endpoints this
// hook is pointed at answer "nothing to report" with a 200 and an empty body
// (`table_stats.go:436`, `pipeline_events.go:331`); each returns 404 from only
// two places — an id that does not parse as a UUID, and `requirePipelineWorkspaceRole`,
// which 404s a pipeline that does not exist OR sits outside the caller's ACTIVE
// workspace (`pipeline_ownership.go:21`, deliberately indistinguishable, so the
// response cannot be used to probe for pipelines in other tenants).
//
// This used to set a `disabledRef` on 404 and return: no error, no cleared data,
// and every later tick a no-op. The card then showed its empty state — the same
// pixels a healthy pipeline with no rows yet shows — and never recovered, because
// the ref is only reset when `url` changes. Switching workspaces back was not
// enough; only a page reload was.
//
// Nothing here needs a per-call "treat 404 as empty" option. That would only be
// justified by an endpoint whose 404 means absence, and neither of these two has
// one. `CDCLagAlertsPanel` hides itself on 404 for a genuinely different endpoint
// (`MONITORING.SENTINEL_ISSUES`, whose 404 means the feature is not enabled) —
// that precedent is about that endpoint, not about the status code.
function usePolledJson<T>(url: string | null): { data: T | null; error: string | null } {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  const inflightRef = useRef<AbortController | null>(null)

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

  return { data, error }
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
            {deps.map((dep) => (
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
                    <span className="text-muted-foreground capitalize shrink-0">{dep.status}</span>
                  </div>
                  {dep.status !== "healthy" && dep.last_error ? (
                    <p className="text-[11px] text-muted-foreground mt-0.5 break-words">{dep.last_error}</p>
                  ) : null}
                </div>
              </li>
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  )
}

function ThroughputRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium tabular-nums text-foreground">{value}</span>
    </div>
  )
}

function ThroughputCard({
  pipelineId,
  executionId,
  mode,
}: {
  pipelineId: string
  executionId?: string
  mode?: "batch" | "cdc"
}) {
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
  const base = API_ENDPOINTS.PIPELINES.TABLE_STATS(pipelineId)
  const url =
    mode === "cdc"
      ? `${base}?mode=cdc`
      : executionId
        ? `${base}?execution_id=${encodeURIComponent(executionId)}`
        : null
  const { data, error } = usePolledJson<TableStatsResponse>(url)
  const s = data?.summary
  const isCdc = s?.mode === "cdc"

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
        ) : isCdc ? (
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Captured</p>
              <ThroughputRow label="Inserts" value={fmtNum(s.total_inserts)} />
              <ThroughputRow label="Updates" value={fmtNum(s.total_updates)} />
              <ThroughputRow label="Deletes" value={fmtNum(s.total_deletes)} />
              <ThroughputRow label="CDC events" value={fmtNum(s.total_cdc_events)} />
            </div>
            <div className="space-y-1.5">
              <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Applied</p>
              <ThroughputRow label="Inserts" value={fmtNum(s.total_applied_inserts)} />
              <ThroughputRow label="Updates" value={fmtNum(s.total_applied_updates)} />
              <ThroughputRow label="Deletes" value={fmtNum(s.total_applied_deletes)} />
              <ThroughputRow label="CDC events" value={fmtNum(s.total_applied_cdc_events)} />
            </div>
          </div>
        ) : (
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
        )}

        {s && (
          <div className="border-t border-border/60 pt-2">
            <ThroughputRow
              label="Tables completed"
              value={`${num(s.tables_completed)} / ${num(s.total_tables)}`}
            />
            {num(s.tables_failed) > 0 && (
              <ThroughputRow label="Tables failed" value={fmtNum(s.tables_failed)} />
            )}
            {num(s.tables_running) > 0 && (
              <ThroughputRow label="Tables running" value={fmtNum(s.tables_running)} />
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function EventRow({ ev, now }: { ev: PipelineRunEvent; now: number }) {
  const detail = eventDetail(ev)
  const stage = eventStageLabel(ev)
  return (
    <li className="flex items-start gap-2 text-xs">
      <span className={cn("mt-1.5 h-2 w-2 rounded-full shrink-0", severityDot(ev.severity))} />
      <div className="min-w-0 flex-1">
        <div className="flex items-center justify-between gap-2">
          <span className="truncate font-medium text-foreground">
            {eventLabel(ev)}
            {stage ? <span className="ml-1 font-normal text-muted-foreground">· {stage}</span> : null}
          </span>
          <span className="shrink-0 text-[11px] text-muted-foreground">
            {relTime(ev.occurred_at || ev.received_at, now)}
          </span>
        </div>
        {detail ? <p className="text-[11px] text-muted-foreground break-words">{detail}</p> : null}
      </div>
    </li>
  )
}

// Exported so the card can be rendered on its own in tests. MonitorTab itself
// pulls in the runtime poller and the diagnose panel, neither of which this card
// uses.
export function LiveEventsCard({ pipelineId }: { pipelineId: string }) {
  // 120, not 60. Roughly a fifth of the rows on this stream are heartbeats and
  // metrics ticks (345 of 1614 measured on prod), and they are now hidden — so a
  // 60-row page would have rendered ~47 lines and silently dropped the oldest
  // real ones off the end of the window.
  const { data, error } = usePolledJson<EventsResponse>(
    `${API_ENDPOINTS.PIPELINES.EVENTS(pipelineId)}?limit=120`,
  )
  const events = data?.events ?? []
  const [showRoutine, setShowRoutine] = useState(false)
  // Tick so relative times stay current without calling Date.now() in render.
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(t)
  }, [])

  const notable = events.filter((ev) => !isNoiseEvent(ev))
  const routineCount = events.length - notable.length

  // A read that failed is not an empty stream. Keep whatever the last good poll
  // returned — retracting the feed on one bad tick would be its own defect — but
  // never let "no rows" and "could not ask" share a rendering.
  const failedRead = error !== null && events.length === 0

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <AlertTriangle className="h-4 w-4" />
          Live events
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-2">
        {failedRead ? (
          <p className="text-xs text-muted-foreground">
            Could not load recent events ({error}). Retrying…
          </p>
        ) : events.length === 0 ? (
          <p className="text-xs text-muted-foreground">No recent events.</p>
        ) : (
          <>
            {error && (
              <p className="text-[11px] text-amber-700 dark:text-amber-400">
                These events may be out of date — could not refresh ({error}).
              </p>
            )}
            {notable.length > 0 && (
              <ul className="max-h-80 overflow-auto space-y-1.5 pr-1">
                {notable.map((ev) => (
                  <EventRow key={ev.event_id} ev={ev} now={now} />
                ))}
              </ul>
            )}
            {routineCount > 0 && (
              <>
                {/* Hiding rows without saying so is its own small lie. The count
                    is the honest part, and the rows stay one click away. */}
                <button
                  type="button"
                  onClick={() => setShowRoutine((v) => !v)}
                  className="text-[11px] text-muted-foreground underline underline-offset-2 hover:text-foreground"
                >
                  {showRoutine ? "Hide" : "Show"} {routineCount} routine progress update
                  {routineCount === 1 ? "" : "s"}
                </button>
                {showRoutine && (
                  <ul className="max-h-60 overflow-auto space-y-1.5 pr-1 opacity-70">
                    {events
                      .filter((ev) => isNoiseEvent(ev))
                      .map((ev) => (
                        <EventRow key={ev.event_id} ev={ev} now={now} />
                      ))}
                  </ul>
                )}
              </>
            )}
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

export function MonitorTab({ pipelineId }: { pipelineId: string }) {
  // One runtime poller for the whole tab: DependenciesCard needs the dependency
  // list and health, ThroughputCard needs the execution id to scope its totals.
  const { runtime, error: runtimeError } = usePipelineRuntime(pipelineId)

  return (
    <div className="space-y-4">
      <DependenciesCard runtime={runtime} error={runtimeError} />
      <ThroughputCard
        pipelineId={pipelineId}
        executionId={runtime?.execution_id}
        mode={runtime?.mode}
      />
      <LiveEventsCard pipelineId={pipelineId} />
      <DiagnoseCard pipelineId={pipelineId} />
    </div>
  )
}
