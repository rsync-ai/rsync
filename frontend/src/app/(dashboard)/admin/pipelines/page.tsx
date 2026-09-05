"use client"

import { useEffect, useMemo, useState } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { authFetch } from "@/lib/api/auth-fetch"
import { AccessDeniedState, LoadingState, RateLimitExceededState } from "@/components/admin/AdminStates"

type PipelinesResponse = {
  data: Array<{
    id: string
    name: string
    status: string
    created_at: string
    updated_at: string
    created_by: string
    created_by_email: string
  }>
  total: number
  limit: number
  offset: number
}

export default function AdminPipelinesPage() {
  const [q, setQ] = useState("")
  const [offset, setOffset] = useState(0)
  const [limit] = useState(50)

  const [data, setData] = useState<PipelinesResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [status, setStatus] = useState<number | null>(null)
  const [retryAfter, setRetryAfter] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)

  const queryString = useMemo(() => {
    const params = new URLSearchParams()
    params.set("limit", String(limit))
    params.set("offset", String(offset))
    if (q.trim()) params.set("q", q.trim())
    return params.toString()
  }, [limit, offset, q])

  async function load() {
    setLoading(true)
    setError(null)
    setStatus(null)
    setRetryAfter(null)

    const res = await authFetch(`/api/v1/admin/pipelines?${queryString}`, { method: "GET" }).catch(() => null)
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

    const json = (await res.json().catch(() => null)) as PipelinesResponse | null
    setData(json)
    setLoading(false)
  }

  useEffect(() => {
    load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryString])

  const canPrev = offset > 0
  const canNext = !!data && offset + limit < (data.total || 0)

  return (
    <div className="space-y-6">
      <PageHeader heading="Admin: Pipelines" description="Pipelines across all users" />
      <AdminNav />

      <Card className="p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <form
            className="flex w-full max-w-xl items-center gap-2"
            onSubmit={(e) => {
              e.preventDefault()
              setOffset(0)
              load()
            }}
          >
            <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search by name, pipeline id, or email…" />
            <Button type="submit" variant="outline">
              Search
            </Button>
          </form>

          <div className="flex items-center gap-2">
            <Button variant="outline" onClick={load}>
              Refresh
            </Button>
          </div>
        </div>
      </Card>

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
        <Card className="p-4">
          <div className="flex items-center justify-between text-sm text-zinc-600 dark:text-zinc-400">
            <div>
              Showing <span className="font-medium">{data.data.length}</span> of{" "}
              <span className="font-medium">{data.total}</span>
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" disabled={!canPrev} onClick={() => setOffset((v) => Math.max(0, v - limit))}>
                Prev
              </Button>
              <Button variant="outline" disabled={!canNext} onClick={() => setOffset((v) => v + limit)}>
                Next
              </Button>
            </div>
          </div>

          <div className="mt-4 space-y-2">
            {data.data.length === 0 ? (
              <div className="text-sm text-zinc-500">No pipelines found</div>
            ) : (
              data.data.map((p) => (
                <div
                  key={p.id}
                  className="rounded-md border border-zinc-200 bg-white p-3 text-sm hover:bg-zinc-50 cursor-pointer dark:border-zinc-800 dark:bg-zinc-900 dark:hover:bg-zinc-900/60"
                  role="button"
                  tabIndex={0}
                  onClick={() => {
                    window.location.href = `/pipelines/${p.id}`
                  }}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      window.location.href = `/pipelines/${p.id}`
                    }
                  }}
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="min-w-0">
                      <div className="font-medium text-zinc-900 dark:text-white truncate">{p.name || p.id}</div>
                      <div className="mt-1 text-xs text-zinc-500 font-mono">{p.id}</div>
                      {p.created_by_email ? (
                        <div className="mt-1 text-xs text-zinc-500">Owner: {p.created_by_email}</div>
                      ) : null}
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant="outline">{p.status}</Badge>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={(evt) => {
                          evt.stopPropagation()
                          window.location.href = `/pipelines/${p.id}`
                        }}
                      >
                        Open
                      </Button>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </Card>
      )}
    </div>
  )
}

