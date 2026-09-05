"use client"

import { useEffect, useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { ArrowDown, ArrowUp, Minus } from "lucide-react"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"

interface Props {
  pipelineId: string
  executionId: string
}

interface ExecSummary {
  execution_id: string
  status: string
  start_time?: string
  end_time?: string | null
  duration_ms?: number | null
  rows_processed?: number | null
  bytes_processed?: number | null
  event_count?: number
  error_count?: number
  retry_count?: number
}

interface CompareResponse {
  execution_a: ExecSummary
  execution_b: ExecSummary
  deltas: {
    duration_delta_ms?: number | null
    duration_change_percent?: number | null
    event_count_delta?: number
    error_count_delta?: number
    retry_count_delta?: number
    rows_delta?: number | null
    bytes_delta?: number | null
  }
}

/**
 * Renders a side-by-side comparison of THIS execution vs the PREVIOUS run
 * of the same pipeline. Uses the backend's existing
 * /api/v1/pipelines/:id/compare endpoint (no UI consumer before this).
 *
 * Strategy:
 *   1. Resolve previous execution via /api/v1/pipelines/:id/trends
 *      (sibling endpoint, also previously unused on the detail page).
 *   2. If a previous run exists, fetch the comparison and render deltas.
 *   3. If no previous run, render nothing — first runs have nothing to
 *      compare against.
 */
export function ExecutionComparison({ pipelineId, executionId }: Props) {
  const [data, setData] = useState<CompareResponse | null>(null)
  const [previousId, setPreviousId] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    const load = async () => {
      try {
        // Fetch trends to find the previous execution. The endpoint already
        // exists at /api/v1/pipelines/:id/trends and returns recent_executions.
        const trendsRes = await authFetch(
          `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/trends?limit=5`,
          { cache: "no-store" },
        )
        if (!trendsRes.ok || cancelled) return
        const trends = await trendsRes.json()
        const recent: Array<{ execution_id: string }> = Array.isArray(trends?.recent_executions)
          ? trends.recent_executions
          : []
        const idx = recent.findIndex((e) => e?.execution_id === executionId)
        const prev = idx >= 0 ? recent[idx + 1] : recent[0]
        if (!prev || prev.execution_id === executionId) return
        if (cancelled) return
        setPreviousId(prev.execution_id)

        const cmpRes = await authFetch(
          `${API_ENDPOINTS.PIPELINES.COMPARE(pipelineId)}?execution_a=${encodeURIComponent(
            prev.execution_id,
          )}&execution_b=${encodeURIComponent(executionId)}`,
          { cache: "no-store" },
        )
        if (!cmpRes.ok || cancelled) return
        setData(await cmpRes.json())
      } catch {
        /* best-effort */
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [pipelineId, executionId])

  if (!data || !previousId) return null

  const fmtMs = (ms?: number | null) => {
    if (ms == null) return "—"
    if (ms < 1000) return `${ms}ms`
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
    return `${(ms / 60_000).toFixed(1)}m`
  }
  const fmtNum = (n?: number | null) => (n == null ? "—" : n.toLocaleString())
  const deltaIcon = (n: number | null | undefined, invert = false) => {
    if (n == null || n === 0) return <Minus className="h-3 w-3 text-zinc-400" />
    const positive = (n > 0) !== invert
    return positive ? (
      <ArrowUp className="h-3 w-3 text-emerald-600" />
    ) : (
      <ArrowDown className="h-3 w-3 text-red-600" />
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Comparison with previous run</CardTitle>
        <div className="text-[11px] text-zinc-500 font-mono">{previousId.slice(0, 8)} → {executionId.slice(0, 8)}</div>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <Metric
            label="Duration"
            a={fmtMs(data.execution_a.duration_ms)}
            b={fmtMs(data.execution_b.duration_ms)}
            delta={
              data.deltas.duration_change_percent != null
                ? `${data.deltas.duration_change_percent > 0 ? "+" : ""}${data.deltas.duration_change_percent.toFixed(1)}%`
                : null
            }
            icon={deltaIcon(data.deltas.duration_delta_ms, /*invert=*/ true)}
          />
          <Metric
            label="Rows"
            a={fmtNum(data.execution_a.rows_processed)}
            b={fmtNum(data.execution_b.rows_processed)}
            delta={data.deltas.rows_delta != null ? `${data.deltas.rows_delta > 0 ? "+" : ""}${fmtNum(data.deltas.rows_delta)}` : null}
            icon={deltaIcon(data.deltas.rows_delta)}
          />
          <Metric
            label="Errors"
            a={fmtNum(data.execution_a.error_count)}
            b={fmtNum(data.execution_b.error_count)}
            delta={data.deltas.error_count_delta != null ? `${data.deltas.error_count_delta > 0 ? "+" : ""}${data.deltas.error_count_delta}` : null}
            icon={deltaIcon(data.deltas.error_count_delta, /*invert=*/ true)}
          />
          <Metric
            label="Retries"
            a={fmtNum(data.execution_a.retry_count)}
            b={fmtNum(data.execution_b.retry_count)}
            delta={data.deltas.retry_count_delta != null ? `${data.deltas.retry_count_delta > 0 ? "+" : ""}${data.deltas.retry_count_delta}` : null}
            icon={deltaIcon(data.deltas.retry_count_delta, /*invert=*/ true)}
          />
        </div>
      </CardContent>
    </Card>
  )
}

function Metric({
  label,
  a,
  b,
  delta,
  icon,
}: {
  label: string
  a: string
  b: string
  delta: string | null
  icon: React.ReactNode
}) {
  return (
    <div className="rounded-md border border-zinc-200 dark:border-zinc-800 px-3 py-2">
      <div className="text-[10px] uppercase tracking-wide text-zinc-500">{label}</div>
      <div className="text-sm text-zinc-700 dark:text-zinc-300 font-mono">
        {a} → {b}
      </div>
      {delta && (
        <div className="mt-0.5 flex items-center gap-1 text-[11px]">
          {icon}
          <span className="text-zinc-600 dark:text-zinc-400">{delta}</span>
        </div>
      )}
    </div>
  )
}
