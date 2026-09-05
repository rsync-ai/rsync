"use client"

import { useEffect, useState } from "react"
import { AlertTriangle, CheckCircle2, HelpCircle, RefreshCw } from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"

type LagIssue = {
  id: string
  type: string
  severity: string
  component_id: string
  component_type: string
  description: string
  detected_at: string
  resolved_at?: string
  occurrence_count: number
  last_occurrence: string
  metadata?: {
    lag_bytes?: number
    lag_mb?: number
    slot_name?: string
    total_lag?: number
    consumer_group?: string
    db_type?: string
  }
}

export function CDCLagAlertsPanel({ pipelineId }: { pipelineId: string }) {
  const [issues, setIssues] = useState<LagIssue[]>([])
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  // null = unknown/disabled, false = enabled + checked
  const [available, setAvailable] = useState<boolean | null>(null)
  // A read that failed is a THIRD outcome, not a variant of "no alerts". Empty
  // issues used to cover both, so a 500 rendered the green "source database is
  // keeping up" card — a positive claim about the database drawn from a read
  // that never came back, which tells the operator to stop looking at exactly
  // the moment they should not.
  const [loadError, setLoadError] = useState<string | null>(null)

  async function fetchIssues(isRefresh = false) {
    if (isRefresh) setRefreshing(true)
    else setLoading(true)
    try {
      const url = `${API_ENDPOINTS.MONITORING.SENTINEL_ISSUES}?component_type=cdc_pipeline&component_id=${pipelineId}&resolved=false&limit=10`
      const res = await authFetch(url, { cache: "no-store" })
      if (res.ok) {
        const data = await res.json()
        setAvailable(true)
        setIssues(data.issues ?? [])
        setLoadError(null)
      } else if (res.status === 404 || res.status === 403) {
        // Feature disabled or no permission — hide the panel entirely.
        // DELIBERATE: this is the sentinel API's answer for "not enabled here",
        // which is not a fault and must not surface as an error card. Keep this
        // branch ahead of the generic one; routing this call through a throwing
        // fetch helper would collapse it into the catch arm.
        setAvailable(false)
        setLoadError(null)
      } else {
        setAvailable(true)
        // Deliberately NOT clearing `issues`: if alerts were already on screen,
        // one failed poll must not silently retract them.
        setLoadError(`Could not check replication lag (HTTP ${res.status})`)
      }
    } catch {
      setAvailable(true)
      setLoadError("Could not check replication lag — the monitoring service is unreachable")
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }

  useEffect(() => {
    fetchIssues()
    // Re-check every 2 minutes (matches sentinel poll interval)
    const timer = setInterval(() => fetchIssues(true), 2 * 60 * 1000)
    return () => clearInterval(timer)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pipelineId])

  if (loading) return null
  if (available === false) return null

  // Could not read, and nothing known from a previous read to fall back on.
  // Muted grey, not green and not amber: this says "unknown", which is neither
  // an all-clear nor an alarm, and it offers the retry the operator needs.
  if (loadError && issues.length === 0) {
    return (
      <Card className="border-zinc-200 bg-zinc-50 dark:bg-zinc-900/40 dark:border-zinc-800">
        <CardContent className="py-3 px-4">
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2 text-zinc-600 dark:text-zinc-400 text-sm">
              <HelpCircle className="h-4 w-4 shrink-0" />
              <span>{loadError}</span>
            </div>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => fetchIssues(true)}
              disabled={refreshing}
              aria-label="Refresh replication lag alerts"
              className="h-7 px-2 text-xs text-zinc-600 hover:text-zinc-900 dark:hover:text-zinc-200"
            >
              <RefreshCw className={`h-3.5 w-3.5 mr-1 ${refreshing ? "animate-spin" : ""}`} />
              Retry
            </Button>
          </div>
        </CardContent>
      </Card>
    )
  }

  if (issues.length === 0) return (
    <Card className="border-green-200 bg-green-50 dark:bg-green-950/20 dark:border-green-800">
      <CardContent className="py-3 px-4">
        <div className="flex items-center gap-2 text-green-700 dark:text-green-400 text-sm">
          <CheckCircle2 className="h-4 w-4 shrink-0" />
          <span>No replication lag alerts — source database is keeping up</span>
        </div>
      </CardContent>
    </Card>
  )

  return (
    <Card className="border-amber-200 bg-amber-50 dark:bg-amber-950/20 dark:border-amber-800">
      <CardHeader className="py-3 px-4 pb-0">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-semibold text-amber-800 dark:text-amber-300 flex items-center gap-2">
            <AlertTriangle className="h-4 w-4" />
            Source Replication Lag
            <Badge variant="outline" className="ml-1 text-xs border-amber-400 text-amber-700 dark:text-amber-300">
              {issues.length} alert{issues.length !== 1 ? "s" : ""}
            </Badge>
          </CardTitle>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => fetchIssues(true)}
            disabled={refreshing}
            // Icon-only button: without a name it is unreachable by screen
            // reader and by name-based tests.
            aria-label="Refresh replication lag alerts"
            className="h-7 w-7 p-0 text-amber-600 hover:text-amber-800"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${refreshing ? "animate-spin" : ""}`} />
          </Button>
        </div>
      </CardHeader>
      <CardContent className="px-4 py-3 space-y-2">
        {/* Alerts survived a failed refresh — say so rather than presenting
            them as current, and never fall back to the green card. */}
        {loadError && (
          <p className="text-xs text-amber-700 dark:text-amber-400">
            These alerts may be out of date — {loadError.replace(/^Could not check/, "could not re-check")}
          </p>
        )}
        {issues.map((issue) => (
          <div key={issue.id} className="text-sm text-amber-800 dark:text-amber-200">
            <p>{issue.description}</p>
            <div className="flex gap-3 mt-1 text-xs text-amber-600 dark:text-amber-400">
              <span>Occurrences: {issue.occurrence_count}</span>
              {issue.metadata?.lag_mb != null && (
                <span>Lag: {issue.metadata.lag_mb.toFixed(1)} MB</span>
              )}
              {issue.metadata?.total_lag != null && (
                <span>Kafka lag: {issue.metadata.total_lag.toLocaleString()} events</span>
              )}
              <span>Last seen: {new Date(issue.last_occurrence).toLocaleTimeString()}</span>
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  )
}
