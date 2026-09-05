"use client"

import { useState } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { authFetch } from "@/lib/api/auth-fetch"
import { AccessDeniedState, RateLimitExceededState } from "@/components/admin/AdminStates"

type RawEventsResponse = {
  pipeline_id: string
  execution_id: string
  limit: number
  events: Array<{
    event_id: string
    event_type: string
    execution_id?: string
    seq?: number
    received_at: string
    payload: any
  }>
}

export default function AdminRawEventsPage() {
  const [pipelineId, setPipelineId] = useState("")
  const [executionId, setExecutionId] = useState("")
  const [limit, setLimit] = useState("50")
  const [justification, setJustification] = useState("")

  const [loading, setLoading] = useState(false)
  const [status, setStatus] = useState<number | null>(null)
  const [retryAfter, setRetryAfter] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<RawEventsResponse | null>(null)

  async function run() {
    setLoading(true)
    setStatus(null)
    setRetryAfter(null)
    setError(null)
    setData(null)

    const pid = pipelineId.trim()
    if (!pid) {
      setError("Pipeline ID is required.")
      setLoading(false)
      return
    }
    if (!justification.trim()) {
      setError("Justification is required.")
      setLoading(false)
      return
    }

    const params = new URLSearchParams()
    const lim = parseInt(limit.trim() || "50", 10)
    if (Number.isFinite(lim) && lim > 0) params.set("limit", String(lim))
    if (executionId.trim()) params.set("execution_id", executionId.trim())

    const res = await authFetch(`/api/v1/admin/pipelines/${encodeURIComponent(pid)}/events/raw?${params.toString()}`, {
      method: "POST",
      body: JSON.stringify({ justification: justification.trim() }),
    }).catch(() => null)

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
      const json = await res.json().catch(() => null)
      setError((json && (json.error || json.message)) || `HTTP ${res.status}`)
      setLoading(false)
      return
    }

    const json = (await res.json().catch(() => null)) as RawEventsResponse | null
    setData(json)
    setLoading(false)
  }

  return (
    <div className="space-y-6">
      <PageHeader
        heading="Admin: Raw events"
        description="Fetch less-redacted pipeline events. Requires justification and is audited."
      />
      <AdminNav />

      <Card className="p-4 space-y-3">
        <div className="grid gap-3 md:grid-cols-2">
          <div className="space-y-1">
            <div className="text-sm text-zinc-500">Pipeline ID</div>
            <Input value={pipelineId} onChange={(e) => setPipelineId(e.target.value)} placeholder="uuid" />
          </div>
          <div className="space-y-1">
            <div className="text-sm text-zinc-500">Execution ID (optional)</div>
            <Input value={executionId} onChange={(e) => setExecutionId(e.target.value)} placeholder="uuid" />
          </div>
        </div>

        <div className="grid gap-3 md:grid-cols-2">
          <div className="space-y-1">
            <div className="text-sm text-zinc-500">Limit (max 200)</div>
            <Input value={limit} onChange={(e) => setLimit(e.target.value)} placeholder="50" />
          </div>
          <div className="space-y-1">
            <div className="text-sm text-zinc-500">Justification</div>
            <Input
              value={justification}
              onChange={(e) => setJustification(e.target.value)}
              placeholder="e.g. debugging pipeline failure for beta user"
            />
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Button onClick={run} disabled={loading}>
            {loading ? "Fetching…" : "Fetch raw events"}
          </Button>
          <Button
            variant="outline"
            onClick={() => {
              setData(null)
              setError(null)
              setStatus(null)
              setRetryAfter(null)
            }}
          >
            Clear
          </Button>
        </div>
      </Card>

      {status === 403 ? <AccessDeniedState /> : null}
      {status === 429 ? <RateLimitExceededState retryAfterSeconds={retryAfter} /> : null}

      {error ? (
        <Card className="border border-red-200 bg-red-50 p-4 dark:border-red-900/40 dark:bg-red-950/20">
          <div className="font-semibold text-red-800 dark:text-red-200">Request failed</div>
          <div className="mt-1 text-sm text-red-700 dark:text-red-300">{error}</div>
        </Card>
      ) : null}

      {data ? (
        <Card className="p-4">
          <div className="flex items-center justify-between gap-4">
            <div className="text-sm text-zinc-600 dark:text-zinc-400">
              Pipeline: <span className="font-mono text-xs">{data.pipeline_id}</span>
              {data.execution_id ? (
                <>
                  {" "}
                  • Execution: <span className="font-mono text-xs">{data.execution_id}</span>
                </>
              ) : null}
            </div>
            <Badge variant="outline">{data.events?.length || 0} events</Badge>
          </div>

          <div className="mt-4 space-y-2">
            {(data.events || []).length === 0 ? (
              <div className="text-sm text-zinc-500">No events returned</div>
            ) : (
              data.events.map((e, idx) => (
                <details
                  key={`${e.event_id}-${idx}`}
                  className="rounded-md border border-zinc-200 bg-white p-3 text-sm dark:border-zinc-800 dark:bg-zinc-900"
                >
                  <summary className="cursor-pointer select-none">
                    <span className="font-medium">{e.event_type}</span>{" "}
                    <span className="ml-2 text-xs text-zinc-500 font-mono">{e.event_id}</span>
                  </summary>
                  <div className="mt-2 text-xs text-zinc-500">
                    received_at: <span className="font-mono">{e.received_at}</span>
                    {typeof e.seq === "number" ? (
                      <>
                        {" "}
                        • seq: <span className="font-mono">{e.seq}</span>
                      </>
                    ) : null}
                  </div>
                  <pre className="mt-2 overflow-auto rounded bg-zinc-50 p-2 text-xs text-zinc-800 dark:bg-zinc-950 dark:text-zinc-200">
                    {JSON.stringify(e.payload, null, 2)}
                  </pre>
                </details>
              ))
            )}
          </div>
        </Card>
      ) : null}
    </div>
  )
}

