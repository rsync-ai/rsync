"use client"

import { useEffect, useState } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table"
import { AccessDeniedState, LoadingState, RateLimitExceededState } from "@/components/admin/AdminStates"
import { authFetch } from "@/lib/api/auth-fetch"
import type { AdminUsageResponse } from "@/lib/api/usage"

const nf = new Intl.NumberFormat()
const fmt = (n: number) => nf.format(n)

function fmtDate(iso?: string | null): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" })
}

type Tab = "workspaces" | "users"

export default function AdminUsagePage() {
  const [data, setData] = useState<AdminUsageResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [status, setStatus] = useState<number | null>(null)
  const [retryAfter, setRetryAfter] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [tab, setTab] = useState<Tab>("workspaces")

  async function load() {
    setLoading(true)
    setError(null)
    setStatus(null)
    setRetryAfter(null)

    const res = await authFetch("/api/v1/admin/usage", { method: "GET" }).catch(() => null)
    if (!res) {
      setError("Network error")
      setLoading(false)
      return
    }
    if (res.status === 401) {
      window.location.href = `/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`
      return
    }
    if (res.status === 403) {
      setStatus(403)
      setLoading(false)
      return
    }
    if (res.status === 429) {
      setStatus(429)
      const ra = parseInt(res.headers.get("Retry-After") || "", 10)
      setRetryAfter(Number.isFinite(ra) ? ra : null)
      setLoading(false)
      return
    }
    if (!res.ok) {
      const txt = await res.text().catch(() => "")
      setError(txt || `HTTP ${res.status}`)
      setLoading(false)
      return
    }
    const json = (await res.json().catch(() => null)) as AdminUsageResponse | null
    setData(json)
    setLoading(false)
  }

  useEffect(() => {
    load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  return (
    <div className="space-y-6">
      <PageHeader heading="Platform admin" description="Consumption across all workspaces and users (beta)" />
      <AdminNav />

      <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800 dark:border-amber-900/50 dark:bg-amber-950/20 dark:text-amber-200">
        Platform-wide consumption across all users and workspaces — not scoped to the workspace selected in the
        header. GB is destination-committed data transfer for the current month (the billed dimension); rows are
        cumulative to-date. Display-only — no enforcement yet.
      </div>

      {loading ? (
        <LoadingState />
      ) : status === 403 ? (
        <AccessDeniedState />
      ) : status === 429 ? (
        <RateLimitExceededState retryAfterSeconds={retryAfter} />
      ) : error ? (
        <Card className="border border-red-200 bg-red-50 p-4 dark:border-red-900/40 dark:bg-red-950/20">
          <div className="font-semibold text-red-800 dark:text-red-200">Failed to load</div>
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
        <div className="space-y-4">
          <div className="flex items-center justify-between gap-4">
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => setTab("workspaces")}
                className={
                  "rounded-md px-3 py-1.5 text-sm border transition-colors " +
                  (tab === "workspaces"
                    ? "bg-zinc-100 text-zinc-900 border-zinc-200 dark:bg-zinc-800 dark:text-white dark:border-zinc-700"
                    : "bg-white text-zinc-600 border-zinc-200 hover:bg-zinc-50 dark:bg-zinc-950 dark:text-zinc-300 dark:border-zinc-800")
                }
              >
                By workspace ({data.workspaces.length})
              </button>
              <button
                type="button"
                onClick={() => setTab("users")}
                className={
                  "rounded-md px-3 py-1.5 text-sm border transition-colors " +
                  (tab === "users"
                    ? "bg-zinc-100 text-zinc-900 border-zinc-200 dark:bg-zinc-800 dark:text-white dark:border-zinc-700"
                    : "bg-white text-zinc-600 border-zinc-200 hover:bg-zinc-50 dark:bg-zinc-950 dark:text-zinc-300 dark:border-zinc-800")
                }
              >
                By user ({data.users.length})
              </button>
            </div>
            <Button variant="outline" onClick={load}>
              Refresh
            </Button>
          </div>

          {tab === "workspaces" ? (
            <Card className="p-0">
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="text-zinc-600 dark:text-zinc-400">Workspace</TableHead>
                      <TableHead className="text-zinc-600 dark:text-zinc-400">Plan</TableHead>
                      <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Pipelines</TableHead>
                      <TableHead className="text-right text-zinc-600 dark:text-zinc-400">GB (mo)</TableHead>
                      <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Queries (mo)</TableHead>
                      <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Records</TableHead>
                      <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Rows read</TableHead>
                      <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Rows written</TableHead>
                      <TableHead className="text-zinc-600 dark:text-zinc-400">Last activity</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {data.workspaces.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={9} className="py-8 text-center text-sm text-zinc-500">
                          No workspaces found.
                        </TableCell>
                      </TableRow>
                    ) : (
                      data.workspaces.map((w) => (
                        <TableRow key={w.workspace_id}>
                          <TableCell className="font-medium text-zinc-900 dark:text-white">
                            {w.name || w.workspace_id}
                            {w.is_personal ? (
                              <span className="ml-2 text-xs text-zinc-400">personal</span>
                            ) : null}
                          </TableCell>
                          <TableCell>
                            <Badge variant="outline" className="capitalize">
                              {w.plan || "—"}
                            </Badge>
                            {/* The stored plan has lapsed and something else is
                                being enforced. Show both — the stored value is
                                what an admin edits, the effective one is what
                                the limit beside it refers to. */}
                            {w.effective_plan && w.effective_plan !== w.plan ? (
                              <span className="ml-1 text-xs text-amber-600 dark:text-amber-500">
                                → <span className="capitalize">{w.effective_plan}</span>
                              </span>
                            ) : null}
                            <span className="ml-2 text-xs text-zinc-500">
                              {w.plan_limit == null ? "∞" : `${fmt(w.pipelines)}/${fmt(w.plan_limit)}`}
                            </span>
                            {/* An override is invisible otherwise: the number
                                to its left is the override's, and an admin
                                reading the table could not tell which
                                workspaces had been granted one. */}
                            {w.pipeline_limit_override != null ? (
                              <span
                                className="ml-1 text-xs text-sky-600 dark:text-sky-400"
                                title="Pipeline limit override — this workspace's limit is set per-workspace, not by its plan"
                              >
                                override
                              </span>
                            ) : null}
                          </TableCell>
                          <TableCell className="text-right tabular-nums">{fmt(w.pipelines)}</TableCell>
                          <TableCell className="text-right tabular-nums font-medium">{w.transfer_gb.toFixed(2)}</TableCell>
                          <TableCell className="text-right tabular-nums">{fmt(w.queries)}</TableCell>
                          <TableCell className="text-right tabular-nums">{fmt(w.records_processed)}</TableCell>
                          <TableCell className="text-right tabular-nums">{fmt(w.rows_read)}</TableCell>
                          <TableCell className="text-right tabular-nums">{fmt(w.rows_written)}</TableCell>
                          <TableCell className="text-zinc-500">{fmtDate(w.last_activity)}</TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </Card>
          ) : (
            <Card className="p-0">
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="text-zinc-600 dark:text-zinc-400">User</TableHead>
                      <TableHead className="text-zinc-600 dark:text-zinc-400">Plan</TableHead>
                      <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Pipelines</TableHead>
                      <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Records</TableHead>
                      <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Rows read</TableHead>
                      <TableHead className="text-right text-zinc-600 dark:text-zinc-400">Rows written</TableHead>
                      <TableHead className="text-zinc-600 dark:text-zinc-400">Last activity</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {data.users.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={7} className="py-8 text-center text-sm text-zinc-500">
                          No users with activity found.
                        </TableCell>
                      </TableRow>
                    ) : (
                      data.users.map((u) => (
                        <TableRow key={u.user_id}>
                          <TableCell className="font-medium text-zinc-900 dark:text-white">
                            {u.email || u.user_id}
                          </TableCell>
                          <TableCell>
                            <Badge variant="outline" className="capitalize">
                              {u.plan || "—"}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-right tabular-nums">{fmt(u.pipelines)}</TableCell>
                          <TableCell className="text-right tabular-nums font-medium">{fmt(u.records_processed)}</TableCell>
                          <TableCell className="text-right tabular-nums">{fmt(u.rows_read)}</TableCell>
                          <TableCell className="text-right tabular-nums">{fmt(u.rows_written)}</TableCell>
                          <TableCell className="text-zinc-500">{fmtDate(u.last_activity)}</TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </Card>
          )}

          {data.retention_enabled ? (
            <div className="text-xs text-zinc-500">
              Transfer totals cover the last {data.retention_days} days (run-stat retention is enabled).
            </div>
          ) : null}
        </div>
      )}
    </div>
  )
}
