"use client"

import { useEffect, useState } from "react"
import { Gauge } from "lucide-react"
import { PageHeader } from "@/components/layout/PageHeader"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table"
import { LoadingState } from "@/components/admin/AdminStates"
import { authFetch } from "@/lib/api/auth-fetch"
import { captureWorkspace, onActiveWorkspaceChange } from "@/lib/workspace/active-workspace"
import type { WorkspaceUsage } from "@/lib/api/usage"

const nf = new Intl.NumberFormat()
const fmt = (n: number) => nf.format(n)

function fmtDate(iso?: string | null): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" })
}

function daysLeft(iso?: string | null): number | null {
  if (!iso) return null
  const ms = new Date(iso).getTime() - Date.now()
  if (Number.isNaN(ms)) return null
  return Math.max(0, Math.ceil(ms / 86_400_000))
}

function planBadgeVariant(plan: string): "default" | "secondary" | "outline" | "success" | "warning" | "info" {
  switch (plan) {
    case "pro":
      return "info"
    case "starter":
      return "default"
    case "trial":
      return "warning"
    case "free":
      return "outline"
    default:
      return "secondary"
  }
}

// Maps the reconciled run-state tokens the /usage API now returns (aligned with
// the pipelines list's derived_status) to human labels, so the same pipeline
// reads the same on the Usage page and the Pipelines list. A raw pipeline
// status (never-run pipelines fall through the backend reconcile) is title-cased
// via the fallback. (R2)
function usageStatusLabel(s: string): string {
  const labels: Record<string, string> = {
    running: "Running",
    passed: "Passed",
    failed: "Failed",
    stopped: "Stopped",
    paused: "Paused",
    scheduled: "Scheduled",
    idle: "Idle",
    waiting_for_user: "Needs input",
  }
  if (labels[s]) return labels[s]
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : "—"
}

export default function UsagePage() {
  const [data, setData] = useState<WorkspaceUsage | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [noWorkspace, setNoWorkspace] = useState(false)

  async function load() {
    setLoading(true)
    setError(null)
    setNoWorkspace(false)

    // A switch mid-flight makes these the previous workspace's meters.
    const isStale = captureWorkspace()

    const res = await authFetch("/api/v1/usage", { method: "GET" }).catch(() => null)
    if (isStale()) return
    if (!res) {
      setError("Network error")
      setLoading(false)
      return
    }
    if (res.status === 401) {
      window.location.href = `/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`
      return
    }
    if (res.status === 400) {
      // No active workspace selected.
      setNoWorkspace(true)
      setLoading(false)
      return
    }
    if (!res.ok) {
      const txt = await res.text().catch(() => "")
      setError(txt || `HTTP ${res.status}`)
      setLoading(false)
      return
    }
    const json = (await res.json().catch(() => null)) as WorkspaceUsage | null
    if (isStale()) return
    setData(json)
    setLoading(false)
  }

  useEffect(() => {
    load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Re-fetch when the active workspace changes (header switcher). This is a client
  // page, so router.refresh() alone won't re-run load() — subscribe explicitly so
  // the meters never show the previous workspace's usage after a switch.
  useEffect(() => {
    return onActiveWorkspaceChange(() => {
      load()
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const limit = data?.pipeline_limit ?? null
  const used = data?.pipelines_used ?? 0
  const unlimited = limit == null
  const expiryDays = daysLeft(data?.plan_expires_at)
  const gbUsed = data?.transfer?.gb ?? 0
  const gbLimit = data?.transfer?.gb_limit ?? null
  const queriesUsed = data?.queries_used ?? 0
  const queriesLimit = data?.queries_limit ?? null

  return (
    <div className="space-y-6">
      <PageHeader
        heading="Usage"
        description="This workspace's consumption against your plan limits."
      />

      {loading ? (
        <LoadingState />
      ) : noWorkspace ? (
        <Card className="p-6 text-sm text-zinc-500">
          No workspace selected. Pick a workspace from the switcher in the header to see its usage.
        </Card>
      ) : error ? (
        <Card className="border border-red-200 bg-red-50 p-4 dark:border-red-900/40 dark:bg-red-950/20">
          <div className="font-semibold text-red-800 dark:text-red-200">Failed to load usage</div>
          <div className="mt-1 text-sm text-red-700 dark:text-red-300">{error}</div>
          <div className="mt-3">
            <Button variant="outline" onClick={load}>
              Retry
            </Button>
          </div>
        </Card>
      ) : !data ? (
        <LoadingState label="No data" />
      ) : (
        <div className="space-y-6">
          {/* Plan + pipeline meter */}
          <Card className="p-5">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div className="flex items-center gap-2">
                <Gauge className="h-5 w-5 text-violet-600" />
                <div>
                  <div className="text-sm text-zinc-500">Current plan</div>
                  <div className="mt-0.5 flex items-center gap-2">
                    <Badge variant={planBadgeVariant(data.plan)} className="capitalize">
                      {data.plan}
                    </Badge>
                    {data.blocked ? (
                      <span className="text-xs font-medium text-red-600 dark:text-red-400">Expired</span>
                    ) : !data.can_run ? (
                      <span className="text-xs font-medium text-amber-600 dark:text-amber-400">Runs paused</span>
                    ) : null}
                  </div>
                </div>
              </div>
              <div className="text-right text-sm">
                <div className="text-zinc-500">Plan renews / expires</div>
                <div className="font-medium text-zinc-900 dark:text-white">
                  {data.plan_expires_at
                    ? `${fmtDate(data.plan_expires_at)}${expiryDays != null ? ` · ${expiryDays} day${expiryDays === 1 ? "" : "s"} left` : ""}`
                    : "Never expires"}
                </div>
              </div>
            </div>

            <div className="mt-5">
              <div className="mb-1.5 flex items-center justify-between text-sm">
                <span className="font-medium text-zinc-700 dark:text-zinc-300">Pipelines</span>
                <span className="text-zinc-500">
                  {unlimited ? `${fmt(used)} used · unlimited` : `${fmt(used)} / ${fmt(limit!)} used`}
                </span>
              </div>
              {unlimited ? (
                <Progress value={1} max={1} className="opacity-40" />
              ) : (
                <Progress value={used} max={Math.max(limit!, 1)} />
              )}
            </div>

            <div className="mt-4">
              <div className="mb-1.5 flex items-center justify-between text-sm">
                <span className="font-medium text-zinc-700 dark:text-zinc-300">Data transfer (this month)</span>
                <span className="text-zinc-500">
                  {gbLimit == null
                    ? `${gbUsed.toFixed(2)} GB used · unlimited`
                    : `${gbUsed.toFixed(2)} / ${fmt(gbLimit)} GB`}
                </span>
              </div>
              {gbLimit == null ? (
                <Progress value={1} max={1} className="opacity-40" />
              ) : (
                <Progress value={gbUsed} max={Math.max(gbLimit, 0.001)} />
              )}
            </div>

            <div className="mt-4">
              <div className="mb-1.5 flex items-center justify-between text-sm">
                <span className="font-medium text-zinc-700 dark:text-zinc-300">NL→SQL queries (this month)</span>
                <span className="text-zinc-500">
                  {queriesLimit == null
                    ? `${fmt(queriesUsed)} used · unlimited`
                    : `${fmt(queriesUsed)} / ${fmt(queriesLimit)} queries`}
                </span>
              </div>
              {queriesLimit == null ? (
                <Progress value={1} max={1} className="opacity-40" />
              ) : (
                <Progress value={queriesUsed} max={Math.max(queriesLimit, 1)} />
              )}
            </div>
          </Card>

          {/* Data transfer (rows today; GB coming with Phase 3) */}
          <Card className="p-5">
            <div className="flex items-center justify-between gap-4">
              <div className="font-semibold text-zinc-900 dark:text-white">Data transfer (to-date)</div>
              <Button variant="outline" size="sm" onClick={load}>
                Refresh
              </Button>
            </div>

            <div className="mt-4 grid gap-4 sm:grid-cols-3">
              <div className="rounded-lg border border-zinc-200 p-4 dark:border-zinc-800">
                <div className="text-sm text-zinc-500">Records processed</div>
                <div className="mt-1 text-2xl font-semibold text-zinc-900 dark:text-white">
                  {fmt(data.transfer.records_processed)}
                </div>
              </div>
              <div className="rounded-lg border border-zinc-200 p-4 dark:border-zinc-800">
                <div className="text-sm text-zinc-500">Rows read (source)</div>
                <div className="mt-1 text-2xl font-semibold text-zinc-900 dark:text-white">
                  {fmt(data.transfer.rows_read)}
                </div>
              </div>
              <div className="rounded-lg border border-zinc-200 p-4 dark:border-zinc-800">
                <div className="text-sm text-zinc-500">Rows written (dest)</div>
                <div className="mt-1 text-2xl font-semibold text-zinc-900 dark:text-white">
                  {fmt(data.transfer.rows_written)}
                </div>
              </div>
            </div>

            {data.retention_enabled ? (
              <div className="mt-3 text-xs text-zinc-500">
                Totals cover the last {data.retention_days} days (run-stat retention is enabled).
              </div>
            ) : null}
          </Card>

          {/* Per-pipeline breakdown */}
          <Card className="p-0">
            <div className="border-b border-zinc-200 p-4 dark:border-zinc-800">
              <div className="font-semibold text-zinc-900 dark:text-white">Per-pipeline transfer</div>
              <div className="text-sm text-zinc-500">Cumulative rows moved by each pipeline in this workspace.</div>
            </div>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="text-zinc-600 dark:text-zinc-400">Pipeline</TableHead>
                    <TableHead className="text-zinc-600 dark:text-zinc-400">Status</TableHead>
                    <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Records</TableHead>
                    <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Rows read</TableHead>
                    <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Rows written</TableHead>
                    <TableHead className="text-right text-zinc-600 dark:text-zinc-400">CDC (I/U/D)</TableHead>
                    <TableHead className="text-zinc-600 dark:text-zinc-400">Last activity</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.pipelines.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={7} className="py-8 text-center text-sm text-zinc-500">
                        No pipelines in this workspace yet.
                      </TableCell>
                    </TableRow>
                  ) : (
                    data.pipelines.map((p) => (
                      <TableRow key={p.id}>
                        <TableCell className="font-medium text-zinc-900 dark:text-white">
                          {p.name || p.id}
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline">
                            {usageStatusLabel(p.status)}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-right tabular-nums">{fmt(p.records_processed)}</TableCell>
                        <TableCell className="text-right tabular-nums">{fmt(p.rows_read)}</TableCell>
                        <TableCell className="text-right tabular-nums">{fmt(p.rows_written)}</TableCell>
                        <TableCell className="text-right tabular-nums text-zinc-500">
                          {fmt(p.cdc_inserts)}/{fmt(p.cdc_updates)}/{fmt(p.cdc_deletes)}
                        </TableCell>
                        <TableCell className="text-zinc-500">{fmtDate(p.last_activity)}</TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </Card>
        </div>
      )}
    </div>
  )
}
