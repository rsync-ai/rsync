"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { AlertTriangle, RefreshCw } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { authFetch } from "@/lib/api/auth-fetch"
import { cn, formatAbsoluteTime } from "@/lib/utils"

/**
 * What the Sentinel agent last recorded for each worker, one row per component
 * (GET /api/v1/monitoring/sentinel/health, admin only; api-gateway monitoring.go
 * GetSentinelHealth). The table has no workspace column, so it lives on admin/health.
 *
 * Two facts about the writer (backend-orchestrator sentinel/health_monitor.go) shape
 * this table:
 *  - Infrastructure, MCP-connector and Kafka-consumer rows are rewritten on a 30 s tick,
 *    agent rows on every heartbeat. A row stops being rewritten when its check can't run
 *    (the Sentinel is down, or it can't read a consumer group's lag), so every row shows
 *    its age, and one the Sentinel has not rewritten for STALE_AFTER_MS is marked.
 *  - Only agents report message and error counts, and only agents and consumers report
 *    lag. Every other type stores 0 because nothing measured it, so it renders "—".
 */

export interface ComponentHealth {
  component_id: string
  component_type: string
  status: string
  last_heartbeat: string
  messages_processed: number
  error_count: number
  consumer_lag?: number
  last_error?: string
  metadata?: Record<string, unknown>
  updated_at: string
}

/** Five missed 30 s checks: long enough that a slow sweep over many connectors is not flagged. */
export const STALE_AFTER_MS = 5 * 60 * 1000

const STATUS_ORDER = ["dead", "unhealthy", "degraded", "unknown", "healthy"]

const statusDots: Record<string, string> = {
  healthy: "bg-green-500",
  degraded: "bg-yellow-500",
  unhealthy: "bg-red-500",
  dead: "bg-zinc-900 dark:bg-zinc-100",
  // Hollow, as on the service cards above: "unknown" is the absence of a reading.
  unknown: "bg-transparent ring-2 ring-inset ring-zinc-400",
}

type BadgeVariant = React.ComponentProps<typeof Badge>["variant"]

const statusBadges: Record<string, BadgeVariant> = {
  healthy: "success",
  degraded: "warning",
  unhealthy: "destructive",
  dead: "destructive",
  unknown: "outline",
}

const typeLabels: Record<string, string> = {
  agent: "Agent",
  kafka_consumer: "Kafka consumer",
  mcp_connector: "MCP connector",
  infrastructure: "Infrastructure",
  cdc_pipeline: "CDC pipeline",
  batch_pipeline: "Batch pipeline",
}

export function typeLabel(type: string): string {
  return typeLabels[type] ?? type
}

function statusRank(status: string): number {
  const i = STATUS_ORDER.indexOf(status)
  // A status this page has never heard of sorts with "unknown": it is not a known-good answer.
  return i === -1 ? STATUS_ORDER.indexOf("unknown") : i
}

function time(iso: string): number {
  const t = new Date(iso).getTime()
  return Number.isNaN(t) ? 0 : t
}

/** Worst status first; within a status, the component heard from longest ago first. */
export function sortComponents(components: ComponentHealth[]): ComponentHealth[] {
  return [...components].sort(
    (a, b) =>
      statusRank(a.status) - statusRank(b.status) ||
      time(a.last_heartbeat) - time(b.last_heartbeat) ||
      a.component_id.localeCompare(b.component_id),
  )
}

export function isStale(c: ComponentHealth, nowMs: number): boolean {
  const updated = time(c.updated_at)
  return updated > 0 && nowMs - updated > STALE_AFTER_MS
}

/**
 * "12m ago", measured against the time the rows were fetched rather than the render, so
 * the label matches the data it describes. "—" for a missing or zero timestamp (Go's zero
 * time serialises as year 1).
 */
export function ageLabel(iso: string, nowMs: number): string {
  const t = time(iso)
  if (t <= 0 || new Date(iso).getUTCFullYear() < 2000) return "—"
  const seconds = Math.max(0, Math.floor((nowMs - t) / 1000))
  if (seconds < 60) return "just now"
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 48) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
}

/** A count the component reported, or "—" when its type never measures it. */
function metric(value: number | undefined, measured: boolean): string {
  if (value && value > 0) return value.toLocaleString()
  return measured ? "0" : "—"
}

type Load =
  | { state: "loading" }
  | { state: "ready"; components: ComponentHealth[]; fetchedAt: number; refreshFailed: boolean }
  | { state: "off" }
  | { state: "error" }

export function ComponentHealthTable({ refreshToken }: { refreshToken: number }) {
  const [load, setLoad] = useState<Load>({ state: "loading" })
  const [typeFilter, setTypeFilter] = useState<string>("all")
  const [retryToken, setRetryToken] = useState(0)

  const fetchHealth = useCallback(async (): Promise<Load | null> => {
    try {
      const res = await authFetch("/api/v1/monitoring/sentinel/health", { method: "GET" })
      // The handler answers 404 while FEATURE_MONITORING_INFRA is off.
      if (res.status === 404) return { state: "off" }
      if (!res.ok) return null
      const json = await res.json()
      const components = Array.isArray(json?.components) ? (json.components as ComponentHealth[]) : []
      return { state: "ready", components, fetchedAt: Date.now(), refreshFailed: false }
    } catch {
      return null
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    void fetchHealth().then((next) => {
      if (cancelled) return
      setLoad((prev) => {
        if (next) return next
        // Keep the last good rows on a failed refresh and say so, rather than blanking a
        // table an admin may be reading.
        return prev.state === "ready" ? { ...prev, refreshFailed: true } : { state: "error" }
      })
    })
    return () => {
      cancelled = true
    }
  }, [fetchHealth, refreshToken, retryToken])

  const components = useMemo(() => (load.state === "ready" ? load.components : []), [load])
  const sorted = useMemo(() => sortComponents(components), [components])
  const types = useMemo(() => {
    const counts = new Map<string, number>()
    for (const c of components) counts.set(c.component_type, (counts.get(c.component_type) ?? 0) + 1)
    return [...counts.entries()].sort((a, b) => typeLabel(a[0]).localeCompare(typeLabel(b[0])))
  }, [components])

  // A filter for a type that stopped reporting would hide every row behind a button that
  // is no longer drawn.
  const activeFilter = typeFilter !== "all" && types.some(([t]) => t === typeFilter) ? typeFilter : "all"
  const visible = activeFilter === "all" ? sorted : sorted.filter((c) => c.component_type === activeFilter)

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Worker health</CardTitle>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          What the Sentinel agent last recorded for each agent, Kafka consumer, MCP connector and
          infrastructure check.
        </p>
      </CardHeader>
      <CardContent>
        {load.state === "loading" ? (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading worker health…</p>
        ) : load.state === "off" ? (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">
            Worker health is off. Set <code className="font-mono text-xs">FEATURE_MONITORING_INFRA=true</code> on
            the api-gateway to turn it on.
          </p>
        ) : load.state === "error" ? (
          <div className="flex flex-wrap items-center gap-3 text-sm">
            <span className="text-red-600 dark:text-red-400">Could not load worker health.</span>
            <Button variant="outline" size="sm" onClick={() => setRetryToken((n) => n + 1)}>
              <RefreshCw className="mr-1.5 h-3.5 w-3.5" />
              Retry
            </Button>
          </div>
        ) : components.length === 0 ? (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">
            No component has reported yet. Rows appear once the orchestrator&apos;s Sentinel agent runs its
            first checks.
          </p>
        ) : (
          <ReadyTable
            components={components}
            visible={visible}
            types={types}
            activeFilter={activeFilter}
            onFilter={setTypeFilter}
            fetchedAt={load.fetchedAt}
            refreshFailed={load.refreshFailed}
          />
        )}
      </CardContent>
    </Card>
  )
}

function ReadyTable({
  components,
  visible,
  types,
  activeFilter,
  onFilter,
  fetchedAt,
  refreshFailed,
}: {
  components: ComponentHealth[]
  visible: ComponentHealth[]
  types: [string, number][]
  activeFilter: string
  onFilter: (type: string) => void
  fetchedAt: number
  refreshFailed: boolean
}) {
  const counts = STATUS_ORDER.map((s) => [s, components.filter((c) => c.status === s).length] as const)
  const other = components.filter((c) => !STATUS_ORDER.includes(c.status)).length
  const staleCount = components.filter((c) => isStale(c, fetchedAt)).length

  return (
    <div className="space-y-3">
      {refreshFailed && (
        <p role="status" className="text-xs text-amber-700 dark:text-amber-400">
          Could not refresh. Showing rows fetched at {new Date(fetchedAt).toLocaleTimeString()}.
        </p>
      )}

      <p aria-label="Status summary" className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm">
        {counts
          .filter(([, n]) => n > 0)
          .map(([s, n]) => (
            <span key={s} className="flex items-center gap-1.5">
              <span aria-hidden className={cn("inline-block h-2 w-2 rounded-full", statusDots[s])} />
              <span className="font-medium tabular-nums">{n}</span> {s}
            </span>
          ))}
        {other > 0 && (
          <span>
            <span className="font-medium tabular-nums">{other}</span> other
          </span>
        )}
        {staleCount > 0 && (
          <span className="flex items-center gap-1 text-amber-700 dark:text-amber-400">
            <AlertTriangle aria-hidden className="h-3.5 w-3.5" />
            <span className="font-medium tabular-nums">{staleCount}</span> not updated in 5 min
          </span>
        )}
      </p>

      {types.length > 1 && (
        <div role="group" aria-label="Filter by type" className="flex flex-wrap gap-1.5">
          {[["all", components.length] as [string, number], ...types].map(([t, n]) => (
            <Button
              key={t}
              size="sm"
              variant={activeFilter === t ? "secondary" : "ghost"}
              aria-pressed={activeFilter === t}
              className="h-7 px-2.5 text-xs"
              onClick={() => onFilter(t)}
            >
              {t === "all" ? "All" : typeLabel(t)}
              <span className="ml-1 tabular-nums text-zinc-500 dark:text-zinc-400">{n}</span>
            </Button>
          ))}
        </div>
      )}

      <div className="overflow-x-auto">
        <table className="w-full min-w-[760px] text-sm">
          <thead>
            <tr className="border-b text-left text-xs text-zinc-500 dark:text-zinc-400">
              <th scope="col" className="py-2 pr-3 font-medium">Component</th>
              <th scope="col" className="py-2 pr-3 font-medium">Status</th>
              <th scope="col" className="py-2 pr-3 font-medium">Last heartbeat</th>
              <th scope="col" className="py-2 pr-3 text-right font-medium">Processed</th>
              <th scope="col" className="py-2 pr-3 text-right font-medium">Errors</th>
              <th scope="col" className="py-2 pr-3 text-right font-medium">Consumer lag</th>
              <th scope="col" className="py-2 font-medium">Last error</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((c) => {
              const stale = isStale(c, fetchedAt)
              const isAgent = c.component_type === "agent"
              const lagMeasured =
                isAgent || (c.component_type === "kafka_consumer" && c.status === "healthy")
              return (
                <tr key={c.component_id} data-component={c.component_id} className="border-b align-top last:border-0">
                  <td className="py-2 pr-3">
                    <div className="max-w-[18rem] break-all font-mono text-xs">{c.component_id}</div>
                    <div className="text-xs text-zinc-500 dark:text-zinc-400">{typeLabel(c.component_type)}</div>
                  </td>
                  <td className="py-2 pr-3">
                    <span className="flex items-center gap-1.5">
                      <span
                        aria-hidden
                        data-status-dot={c.status}
                        className={cn("inline-block h-2.5 w-2.5 shrink-0 rounded-full", statusDots[c.status] ?? "bg-zinc-400")}
                      />
                      <Badge variant={statusBadges[c.status] ?? "secondary"}>{c.status}</Badge>
                    </span>
                  </td>
                  <td className="whitespace-nowrap py-2 pr-3">
                    <span title={formatAbsoluteTime(c.last_heartbeat)}>{ageLabel(c.last_heartbeat, fetchedAt)}</span>
                    {stale && (
                      <div
                        className="text-xs text-amber-700 dark:text-amber-400"
                        title="The Sentinel re-checks every 30 s, so a row it has not rewritten in 5 minutes is out of date."
                      >
                        Row not updated {ageLabel(c.updated_at, fetchedAt)}
                      </div>
                    )}
                  </td>
                  <td className="py-2 pr-3 text-right tabular-nums">{metric(c.messages_processed, isAgent)}</td>
                  <td
                    className={cn(
                      "py-2 pr-3 text-right tabular-nums",
                      c.error_count > 0 && "font-medium text-red-600 dark:text-red-400",
                    )}
                  >
                    {metric(c.error_count, isAgent)}
                  </td>
                  <td className="py-2 pr-3 text-right tabular-nums">{metric(c.consumer_lag, lagMeasured)}</td>
                  <td className="py-2">
                    {c.last_error ? (
                      <span
                        title={c.last_error}
                        className={cn(
                          "line-clamp-2 max-w-[22rem] break-words text-xs",
                          c.status === "healthy" ? "text-zinc-500 dark:text-zinc-400" : "text-red-600 dark:text-red-400",
                        )}
                      >
                        {c.last_error}
                      </span>
                    ) : (
                      <span className="text-zinc-400">—</span>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
