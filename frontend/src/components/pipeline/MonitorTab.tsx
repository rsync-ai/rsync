"use client"

import { useEffect, useRef, useState } from "react"
import { Activity, CheckCircle2, HeartPulse } from "lucide-react"

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
import { tableStatusBreakdown } from "./executionSummary"
import { cn } from "@/lib/utils"
import { dependencyStatusLabel } from "@/lib/pipeline/dependencyStatus"

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
}

interface TableStatsResponse {
  summary: TableStatsSummary
  tables?: unknown[]
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
function usePolledJson<T>(url: string | null): { data: T | null; error: string | null } {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
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
                    <span className="text-muted-foreground capitalize shrink-0">{dependencyStatusLabel(dep)}</span>
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
            {/* Every table not completed lands in exactly one row, so the rows
                below plus "completed" add up to the total. This used to list
                failed and running only; degraded and no-data-yet tables were
                counted in the total and shown nowhere. */}
            {tableStatusBreakdown(s).rows.map((row) => (
              <ThroughputRow key={row.key} label={row.label} value={row.count.toLocaleString()} />
            ))}
          </div>
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
      <ThroughputCard
        pipelineId={pipelineId}
        executionId={runtime?.execution_id}
        mode={runtime?.mode}
      />
      <DiagnoseCard pipelineId={pipelineId} />
      <DependenciesCard runtime={runtime} error={runtimeError} />
    </div>
  )
}
