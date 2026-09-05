"use client"

import { useEffect, useState } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { authFetch } from "@/lib/api/auth-fetch"
import { AccessDeniedState, LoadingState, RateLimitExceededState } from "@/components/admin/AdminStates"

type OverviewResponse = {
  counts: {
    users: number
    pipelines: number
    executions: number
    connections: number
  }
  recent_failed_executions: Array<{
    id: string
    pipeline_id: string
    pipeline_name: string
    created_by_email: string
    status: string
    start_time: string
    end_time?: string | null
    error_message?: string | null
  }>
}

export default function AdminOverviewPage() {
  const [data, setData] = useState<OverviewResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [status, setStatus] = useState<number | null>(null)
  const [retryAfter, setRetryAfter] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function load() {
    setLoading(true)
    setError(null)
    setStatus(null)
    setRetryAfter(null)

    const res = await authFetch("/api/v1/admin/overview", { method: "GET" }).catch(() => null)
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

    const json = (await res.json().catch(() => null)) as OverviewResponse | null
    setData(json)
    setLoading(false)
  }

  useEffect(() => {
    load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  return (
    <div className="space-y-6">
      <PageHeader heading="Platform admin" description="Operator-wide overview (beta)" />
      <AdminNav />

      {/* These counts come from /admin/overview, which aggregates the WHOLE platform
          — all users and all workspaces. They are deliberately NOT scoped to the
          workspace selected in the header switcher, so say so to avoid confusion. */}
      <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800 dark:border-amber-900/50 dark:bg-amber-950/20 dark:text-amber-200">
        These metrics are platform-wide across all users and workspaces — they are not scoped to the
        workspace selected in the header.
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
        <div className="space-y-6">
          <div className="grid gap-4 md:grid-cols-4">
            <Card className="p-4">
              <div className="text-sm text-zinc-500">Users</div>
              <div className="text-2xl font-semibold">{data.counts.users}</div>
            </Card>
            <Card className="p-4">
              <div className="text-sm text-zinc-500">Pipelines</div>
              <div className="text-2xl font-semibold">{data.counts.pipelines}</div>
            </Card>
            <Card className="p-4">
              <div className="text-sm text-zinc-500">Executions</div>
              <div className="text-2xl font-semibold">{data.counts.executions}</div>
            </Card>
            <Card className="p-4">
              <div className="text-sm text-zinc-500">Connections</div>
              <div className="text-2xl font-semibold">{data.counts.connections}</div>
            </Card>
          </div>

          <Card className="p-4">
            <div className="flex items-center justify-between gap-4">
              <div>
                <div className="font-semibold text-zinc-900 dark:text-white">Recent failures (24h)</div>
                <div className="text-sm text-zinc-500">Last 20 failed/error executions</div>
              </div>
              <Button variant="outline" onClick={load}>
                Refresh
              </Button>
            </div>

            <div className="mt-4 space-y-2">
              {data.recent_failed_executions.length === 0 ? (
                <div className="text-sm text-zinc-500">No recent failures</div>
              ) : (
                data.recent_failed_executions.map((e) => (
                  <div
                    key={e.id}
                    className="rounded-md border border-zinc-200 bg-white p-3 text-sm dark:border-zinc-800 dark:bg-zinc-900"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="font-mono text-xs text-zinc-500">{e.id}</div>
                      <Badge variant="outline">{e.status}</Badge>
                    </div>
                    <div className="mt-1">
                      <span className="font-medium">{e.pipeline_name || e.pipeline_id}</span>
                      {e.created_by_email ? (
                        <span className="ml-2 text-xs text-zinc-500">({e.created_by_email})</span>
                      ) : null}
                    </div>
                    {e.error_message ? (
                      <div className="mt-1 text-xs text-red-700 dark:text-red-300">{e.error_message}</div>
                    ) : null}
                  </div>
                ))
              )}
            </div>
          </Card>
        </div>
      )}
    </div>
  )
}

