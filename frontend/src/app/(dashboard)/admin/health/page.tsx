"use client"

import { useEffect, useState, useCallback, useRef } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { authFetch } from "@/lib/api/auth-fetch"
import { AccessDeniedState, LoadingState, RateLimitExceededState } from "@/components/admin/AdminStates"
import { ComponentHealthTable } from "@/components/admin/ComponentHealthTable"
import { DeployedCommitCard } from "@/components/admin/DeployedCommitCard"
import { toast } from "sonner"
import { RefreshCw, Database, Server, Radio, Clock, Timer, Workflow, Cable, ArrowDownToLine } from "lucide-react"
import type { ServiceHealth } from "@/lib/api/admin"
import { formatAbsoluteTime } from "@/lib/utils"

const serviceIcons: Record<string, React.ElementType> = {
  postgresql: Database,
  redis: Server,
  kafka: Radio,
  temporal: Clock,
  "explorer-freshness-sweep": Timer,
  orchestrator: Workflow,
  "kafka-connect": Cable,
  "kafka-mcp-sink": ArrowDownToLine,
}

// Names that do not read well with only their first letter capitalised.
const serviceLabels: Record<string, string> = {
  "explorer-freshness-sweep": "Explorer freshness sweep",
  orchestrator: "Orchestrator",
  "kafka-connect": "Kafka Connect",
  "kafka-mcp-sink": "CDC sink (kafka-mcp-sink)",
}

const statusColors: Record<string, string> = {
  up: "bg-green-500",
  down: "bg-red-500",
  degraded: "bg-yellow-500",
  // Hollow, because "unknown" is the absence of a reading — nothing was asked — and a
  // filled grey dot is also what a status this page has never heard of falls back to.
  unknown: "bg-transparent ring-2 ring-inset ring-zinc-400",
}

type BadgeVariant = React.ComponentProps<typeof Badge>["variant"]

const statusBadgeVariants: Record<string, BadgeVariant> = {
  up: "default",
  down: "destructive",
  degraded: "warning",
  unknown: "outline",
}

function humanizeDetailKey(key: string): string {
  const words = key.replace(/_/g, " ")
  return words.charAt(0).toUpperCase() + words.slice(1)
}

function formatDetailValue(value: unknown): string {
  if (value === null || value === undefined || value === "") return "—"
  if (typeof value === "boolean") return value ? "yes" : "no"
  if (typeof value === "number" || typeof value === "string") return String(value)
  if (Array.isArray(value)) return value.map(formatDetailValue).join(", ")
  if (typeof value === "object") {
    return Object.entries(value as Record<string, unknown>)
      .map(([k, v]) => `${formatDetailValue(v)} ${k.replace(/_/g, " ")}`)
      .join(", ")
  }
  return String(value)
}

/** 300 → "5 min", 5400 → "1 h 30 min", 45 → "45 s". */
function formatInterval(seconds: unknown): string {
  if (typeof seconds !== "number" || !Number.isFinite(seconds) || seconds <= 0) return formatDetailValue(seconds)
  if (seconds < 60) return `${seconds} s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes} min`
  const hours = Math.floor(minutes / 60)
  return minutes % 60 ? `${hours} h ${minutes % 60} min` : `${hours} h`
}

type DetailRow = { key: string; label: string; value: string }

/**
 * The freshness sweep's keys in words (#53: the card showed "effective_interval_seconds
 * 300" and a raw ISO timestamp). `detail_available` only says whether the rest exists,
 * so it is not a row of its own; ticks_per_run folds into ticks_this_run ("7 of 288").
 */
const knownDetail: Record<string, ((detail: Record<string, unknown>) => Omit<DetailRow, "key"> | null) | null> = {
  detail_available: null,
  effective_interval_seconds: (d) => ({ label: "Runs every", value: formatInterval(d.effective_interval_seconds) }),
  ticks_this_run: (d) => ({
    label: "Sweeps since the workflow last restarted",
    value:
      typeof d.ticks_per_run === "number"
        ? `${formatDetailValue(d.ticks_this_run)} of ${d.ticks_per_run}`
        : formatDetailValue(d.ticks_this_run),
  }),
  ticks_per_run: (d) =>
    "ticks_this_run" in d ? null : { label: "Sweeps per workflow run", value: formatDetailValue(d.ticks_per_run) },
  swept_this_run: (d) => ({ label: "Swept since the restart", value: formatDetailValue(d.swept_this_run) }),
  last_sweep_at: (d) => ({
    label: "Last sweep",
    value:
      typeof d.last_sweep_at === "string" && d.last_sweep_at
        ? formatAbsoluteTime(d.last_sweep_at) || d.last_sweep_at
        : "never",
  }),
  last_sweep_ok: (d) => ({ label: "Last sweep succeeded", value: formatDetailValue(d.last_sweep_ok) }),
  last_result: (d) => ({ label: "Last sweep's models", value: formatDetailValue(d.last_result) }),
}

function detailRows(detail: Record<string, unknown>): DetailRow[] {
  const rows: DetailRow[] = []
  for (const [key, value] of Object.entries(detail)) {
    if (key in knownDetail) {
      const known = knownDetail[key]
      const row = known ? known(detail) : null
      if (row) rows.push({ key, ...row })
    } else {
      rows.push({ key, label: humanizeDetailKey(key), value: formatDetailValue(value) })
    }
  }
  return rows
}

/**
 * A check's detail. Keys this page knows get words; any other key is still shown,
 * humanised, so a check that starts reporting a new fact appears here without this
 * page learning its name first.
 */
function HealthDetail({ detail }: { detail: Record<string, unknown> }) {
  const rows = detailRows(detail)
  if (rows.length === 0) return null
  return (
    // A pair shares a line when it fits and the value drops under its label when it does
    // not. A label column would take most of a narrow card and split every value mid-word.
    <dl className="mt-3 space-y-1 text-xs">
      {rows.map((row) => (
        <div key={row.key} className="flex flex-wrap gap-x-2">
          <dt className="text-zinc-500 dark:text-zinc-400">{row.label}</dt>
          <dd className="min-w-0 break-words text-zinc-700 dark:text-zinc-300">{row.value}</dd>
        </div>
      ))}
    </dl>
  )
}

export default function AdminHealthPage() {
  const [services, setServices] = useState<ServiceHealth[]>([])
  const [loading, setLoading] = useState(true)
  const [pageStatus, setPageStatus] = useState<number | null>(null)
  const [retryAfter, setRetryAfter] = useState<number | null>(null)
  const [lastChecked, setLastChecked] = useState<Date | null>(null)
  // Bumped by Refresh and the 30 s tick so the worker table re-reads with the cards; it
  // reads once on its own when it mounts.
  const [refreshToken, setRefreshToken] = useState(0)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const load = useCallback(async () => {
    try {
      const res = await authFetch("/api/v1/admin/health", { method: "GET" })
      if (res.status === 401) { window.location.href = `/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`; return }
      if (res.status === 403) { setPageStatus(403); setLoading(false); return }
      if (res.status === 429) {
        setPageStatus(429)
        setRetryAfter(parseInt(res.headers.get("Retry-After") || "", 10))
        setLoading(false)
        return
      }
      const json = await res.json()
      setServices(json.services || [])
      setLastChecked(new Date())
    } catch {
      toast.error("Failed to check health")
    }
    setLoading(false)
  }, [])

  const refresh = useCallback(() => {
    setRefreshToken((n) => n + 1)
    void load()
  }, [load])

  useEffect(() => {
    load()
    intervalRef.current = setInterval(refresh, 30000)
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [load, refresh])

  return (
    <div className="space-y-6">
      <PageHeader heading="Admin" description="System health" />
      <AdminNav />

      {loading && services.length === 0 ? (
        <LoadingState />
      ) : pageStatus === 403 ? (
        <AccessDeniedState />
      ) : pageStatus === 429 ? (
        <RateLimitExceededState retryAfterSeconds={retryAfter} />
      ) : (
        <>
          <div className="flex items-center justify-between">
            <div className="text-sm text-zinc-500 dark:text-zinc-400">
              {lastChecked && `Last checked: ${lastChecked.toLocaleTimeString()}`}
              <span className="ml-2 text-xs">(auto-refreshes every 30s)</span>
            </div>
            <Button variant="outline" onClick={refresh}>
              <RefreshCw className="h-4 w-4 mr-1.5" />
              Refresh
            </Button>
          </div>

          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            {services.map((svc) => {
              const Icon = serviceIcons[svc.service] || Server
              return (
                <Card key={svc.service}>
                  <CardHeader className="pb-2">
                    <CardTitle className="flex items-center justify-between text-base">
                      <span className="flex items-center gap-2">
                        <Icon className="h-4 w-4 shrink-0 text-zinc-500 dark:text-zinc-400" />
                        {serviceLabels[svc.service] ??
                          svc.service.charAt(0).toUpperCase() + svc.service.slice(1)}
                      </span>
                      <span className="flex shrink-0 items-center gap-1.5">
                        <span
                          aria-hidden
                          data-status-dot={svc.status}
                          className={`inline-block h-2.5 w-2.5 rounded-full ${statusColors[svc.status] || "bg-zinc-400"}`}
                        />
                        <Badge variant={statusBadgeVariants[svc.status] ?? "secondary"}>
                          {svc.status}
                        </Badge>
                      </span>
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="text-sm text-zinc-500 dark:text-zinc-400">
                      Latency: <span className="font-mono">{svc.latency_ms}ms</span>
                    </div>
                    {svc.error && (
                      // An `up` row can carry a caveat — "the sweep is running but did not
                      // answer this query" — which qualifies a healthy answer rather than
                      // reporting a failure. In red it would read as the latter.
                      <div
                        className={`mt-2 text-xs break-all ${
                          svc.status === "up" ? "text-zinc-500 dark:text-zinc-400" : "text-red-600 dark:text-red-400"
                        }`}
                      >
                        {svc.error}
                      </div>
                    )}
                    {svc.detail && <HealthDetail detail={svc.detail} />}
                  </CardContent>
                </Card>
              )
            })}
          </div>

          <DeployedCommitCard refreshToken={refreshToken} />

          <ComponentHealthTable refreshToken={refreshToken} />
        </>
      )}
    </div>
  )
}
