"use client"

/**
 * PipelineRecentRuns — "has this pipeline been reliable?" for a batch pipeline.
 *
 * One pill per recent run (oldest → newest, each linking to its execution) and
 * one sentence: "9 of the last 10 finished runs succeeded · average 4m 12s".
 * Data is GET /pipelines/:id/trends (pipeline_compare.go GetPipelineTrends),
 * whose success rate is succeeded / finished over the same window — a run still
 * in flight counts toward neither side.
 *
 * Batch only: a CDC pipeline has no discrete runs, and the health header already
 * answers "is it keeping up?" for it.
 */

import Link from "next/link"
import { useCallback, useEffect, useState } from "react"
import { History } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { formatAbsoluteTime, formatDuration } from "@/lib/transform-format"
import { cn } from "@/lib/utils"

const WINDOW = 10
const POLL_MS = 30_000
// Below this many finished runs a percentage overstates what we know; the
// "N of M" sentence still says it honestly.
const MIN_RUNS_FOR_RATE = 3

export interface TrendRun {
  execution_id: string
  status: string
  start_time?: string
  duration_ms?: number
}

export interface PipelineTrendsResponse {
  success_rate?: number
  finished_runs?: number
  succeeded_runs?: number
  avg_duration_ms?: number
  recent_executions?: TrendRun[]
}

export interface RecentRunsSummary {
  /** Oldest first, so the newest run sits at the right, where the eye ends. */
  runs: TrendRun[]
  finished: number
  succeeded: number
  running: number
  /** Null when there are too few finished runs for a percentage to mean much. */
  ratePercent: number | null
  avgDurationMs: number | null
}

function isSuccess(status: string) {
  return status === "completed" || status === "success"
}

export function summarizeRecentRuns(t: PipelineTrendsResponse): RecentRunsSummary {
  const newestFirst = Array.isArray(t.recent_executions) ? t.recent_executions : []
  const runs = [...newestFirst].reverse()
  // Prefer the gateway's counts; derive them from the list for an older gateway
  // that predates finished_runs / succeeded_runs.
  const derivedFinished = runs.filter((r) => isSuccess(r.status) || r.status === "failed").length
  const derivedSucceeded = runs.filter((r) => isSuccess(r.status)).length
  const finished = typeof t.finished_runs === "number" ? t.finished_runs : derivedFinished
  const succeeded = typeof t.succeeded_runs === "number" ? t.succeeded_runs : derivedSucceeded
  const running = runs.filter((r) => r.status === "running").length
  return {
    runs,
    finished,
    succeeded,
    running,
    ratePercent: finished >= MIN_RUNS_FOR_RATE ? Math.round((succeeded / finished) * 100) : null,
    avgDurationMs: typeof t.avg_duration_ms === "number" && t.avg_duration_ms > 0 ? t.avg_duration_ms : null,
  }
}

function pillClass(status: string) {
  if (isSuccess(status)) return "bg-emerald-500 hover:bg-emerald-600"
  if (status === "failed") return "bg-red-500 hover:bg-red-600"
  return "bg-blue-500 animate-pulse"
}

function statusLabel(status: string) {
  if (isSuccess(status)) return "Succeeded"
  if (status === "failed") return "Failed"
  if (status === "running") return "Running"
  return status
}

function rateTone(pct: number) {
  if (pct >= 90) return "border-emerald-300 text-emerald-700 dark:border-emerald-800 dark:text-emerald-300"
  if (pct >= 50) return "border-amber-300 text-amber-700 dark:border-amber-800 dark:text-amber-300"
  return "border-red-300 text-red-700 dark:border-red-800 dark:text-red-300"
}

function runDescription(r: TrendRun) {
  const parts = [statusLabel(r.status)]
  const started = formatAbsoluteTime(r.start_time)
  if (started) parts.push(`started ${started}`)
  if (typeof r.duration_ms === "number" && r.duration_ms > 0) parts.push(`took ${formatDuration(r.duration_ms)}`)
  return parts.join(" · ")
}

export function PipelineRecentRuns({ pipelineId }: { pipelineId: string }) {
  const [data, setData] = useState<PipelineTrendsResponse | null>(null)
  const [error, setError] = useState(false)

  const load = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const res = await authFetch(API_ENDPOINTS.PIPELINES.TRENDS(pipelineId, WINDOW), { cache: "no-store", signal })
        if (!res.ok) {
          setError(true)
          return
        }
        setData((await res.json()) as PipelineTrendsResponse)
        setError(false)
      } catch (e) {
        if ((e as { name?: string })?.name === "AbortError") return
        setError(true)
      }
    },
    [pipelineId]
  )

  useEffect(() => {
    const ac = new AbortController()
    void load(ac.signal)
    const t = window.setInterval(() => void load(ac.signal), POLL_MS)
    return () => {
      ac.abort()
      window.clearInterval(t)
    }
  }, [load])

  if (!data && !error) return null

  const summary = data ? summarizeRecentRuns(data) : null

  return (
    <Card data-testid="pipeline-recent-runs">
      <CardHeader className="py-3 px-4 pb-2">
        <div className="flex items-center justify-between gap-2">
          <CardTitle className="flex items-center gap-2 text-sm font-semibold">
            <History className="h-4 w-4 text-blue-600 dark:text-blue-400" />
            Recent runs
            {summary?.ratePercent != null && (
              <Badge variant="outline" className={cn("text-xs", rateTone(summary.ratePercent))}>
                {summary.ratePercent}% succeeded
              </Badge>
            )}
          </CardTitle>
          <Link
            href={`/pipelines/${pipelineId}?tab=history`}
            className="text-xs text-blue-600 hover:underline dark:text-blue-400"
          >
            View all history →
          </Link>
        </div>
      </CardHeader>
      <CardContent className="px-4 pb-3 pt-0">
        {!summary ? (
          <p className="text-sm text-muted-foreground">Couldn&apos;t load recent runs.</p>
        ) : summary.runs.length === 0 ? (
          <p className="text-sm text-muted-foreground">No runs yet. Runs appear here once the pipeline has run.</p>
        ) : (
          <>
            <ol className="flex items-center gap-1.5" aria-label="Recent runs, oldest first">
              {summary.runs.map((r) => {
                const label = runDescription(r)
                return (
                  <li key={r.execution_id}>
                    <Link
                      href={`/executions/${r.execution_id}`}
                      title={label}
                      aria-label={label}
                      data-status={r.status}
                      className={cn(
                        "block h-6 w-3 rounded-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1",
                        pillClass(r.status)
                      )}
                    />
                  </li>
                )
              })}
            </ol>
            <p className="mt-2 text-xs text-muted-foreground" data-testid="pipeline-recent-runs-summary">
              {summary.finished > 0
                ? `${summary.succeeded} of the last ${summary.finished} finished run${summary.finished === 1 ? "" : "s"} succeeded`
                : "No run has finished yet"}
              {summary.running > 0 && ` · ${summary.running} running`}
              {summary.avgDurationMs != null && ` · average ${formatDuration(summary.avgDurationMs)}`}
            </p>
          </>
        )}
      </CardContent>
    </Card>
  )
}
