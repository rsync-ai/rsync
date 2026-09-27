"use client"

import { useCallback, useEffect, useState } from "react"
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

/**
 * What kind of alert this is, from the issue id the Sentinel keys it by.
 *
 * The three classes are deliberately distinct ids so neither lag resolver can
 * clear the other's alarm (cdc_sentinel.go sinkLagIssueID / connectorIssueID), and
 * they mean different things to whoever is reading:
 *
 *   cdc-lag-*             Debezium is behind the source's WAL/binlog
 *   cdc-sink-lag-*        changes are captured but not reaching the destination
 *   cdc-connector-down-*  the connector is down and restarts stopped working
 *
 * The panel used to render all of them under "Source Replication Lag", in one
 * shade of amber, which mislabelled two thirds of what it showed.
 */
export function alertClassLabel(id: string, type: string): string {
  if (id.startsWith("cdc-connector-down-")) return "Connector down"
  if (id.startsWith("cdc-sink-lag-")) return "Not reaching the destination"
  if (id.startsWith("cdc-lag-")) return "Source replication lag"
  return type.replace(/_/g, " ")
}

/**
 * Severity drives the colour. `critical` is what the terminal connector-down
 * escalation carries, and rendering it in the same amber as a warning is how a
 * "restarts have stopped working" alert reads like a transient backlog.
 */
export function alertIsCritical(severity: string): boolean {
  return severity.toLowerCase() === "critical"
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

  // useCallback on pipelineId: the effect below re-subscribes when the pipeline
  // changes, and exhaustive-deps can hold it to that instead of being switched off.
  const fetchIssues = useCallback(async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true)
    else setLoading(true)
    try {
      // The PIPELINE's own alerts route, not MONITORING.SENTINEL_ISSUES.
      //
      // That one is the admin infrastructure view: gated on FEATURE_MONITORING_INFRA
      // (default off) plus a platform power_user/admin role. Both answers land in the
      // `available = false` branch below, which hides this panel — so on a default
      // deployment, and for every ordinary workspace member, the Sentinel could
      // detect a stalled sink and the person who owns the pipeline was never told.
      // /pipelines/:id/alerts is Viewer-gated and unflagged.
      const url = `${API_ENDPOINTS.PIPELINES.ALERTS(pipelineId)}?resolved=false&limit=10`
      const res = await authFetch(url, { cache: "no-store" })
      if (res.ok) {
        const data = await res.json()
        setAvailable(true)
        setIssues(data.alerts ?? [])
        setLoadError(null)
      } else if (res.status === 404 || res.status === 403) {
        // Hide rather than alarm. On the new route a 404 means a gateway that
        // predates it (the route is unflagged, so "not enabled here" is no longer a
        // reason) and a 403 means this caller may not read this pipeline. Neither is
        // a fault to report on the pipeline page. Keep this branch ahead of the
        // generic one; routing this call through a throwing fetch helper would
        // collapse it into the catch arm.
        setAvailable(false)
        setLoadError(null)
      } else {
        setAvailable(true)
        // Deliberately NOT clearing `issues`: if alerts were already on screen,
        // one failed poll must not silently retract them.
        setLoadError(`Could not check this pipeline's alerts (HTTP ${res.status})`)
      }
    } catch {
      setAvailable(true)
      setLoadError("Could not check this pipeline's alerts — the monitoring service is unreachable")
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [pipelineId])

  useEffect(() => {
    fetchIssues()
    // Re-check every 2 minutes (matches sentinel poll interval)
    const timer = setInterval(() => fetchIssues(true), 2 * 60 * 1000)
    return () => clearInterval(timer)
  }, [fetchIssues])

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
              aria-label="Refresh pipeline alerts"
              className="h-7 px-2 text-xs text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-200"
            >
              <RefreshCw className={`h-3.5 w-3.5 mr-1 ${refreshing ? "animate-spin" : ""}`} />
              Retry
            </Button>
          </div>
        </CardContent>
      </Card>
    )
  }

  // One quiet line, not a green card: the all-clear is the usual state, and a
  // full-width banner above the tab's real content spent the first screen on it.
  if (issues.length === 0) return (
    <p
      data-testid="pipeline-alerts-clear"
      className="flex items-center gap-1.5 px-1 text-xs text-muted-foreground"
    >
      <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-green-600 dark:text-green-400" aria-hidden="true" />
      {/* Not "the source database is keeping up": this list also carries
          sink-drain lag (cdc-sink-lag-*) and a connector that is down
          (cdc-connector-down-*), so the all-clear must cover what was
          actually checked, not just the source. */}
      <span>No alerts for this pipeline — source, sink and connectors are keeping up</span>
    </p>
  )

  // The card takes the worst severity it is showing: one critical alert among
  // warnings must not be softened to the colour of the majority.
  const anyCritical = issues.some((i) => alertIsCritical(i.severity))

  return (
    <Card
      data-testid="pipeline-alerts"
      data-severity={anyCritical ? "critical" : "warning"}
      className={
        anyCritical
          ? "border-red-200 bg-red-50 dark:bg-red-950/20 dark:border-red-800"
          : "border-amber-200 bg-amber-50 dark:bg-amber-950/20 dark:border-amber-800"
      }
    >
      <CardHeader className="py-3 px-4 pb-0">
        <div className="flex items-center justify-between">
          <CardTitle
            className={`text-sm font-semibold flex items-center gap-2 ${
              anyCritical ? "text-red-800 dark:text-red-300" : "text-amber-800 dark:text-amber-300"
            }`}
          >
            <AlertTriangle className="h-4 w-4" />
            Pipeline alerts
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
            aria-label="Refresh pipeline alerts"
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
          <div
            key={issue.id}
            data-testid={`alert-${issue.id}`}
            className={`text-sm ${
              alertIsCritical(issue.severity)
                ? "text-red-800 dark:text-red-200"
                : "text-amber-800 dark:text-amber-200"
            }`}
          >
            <div className="flex items-center gap-2">
              <Badge
                variant="outline"
                className={`text-[10px] ${
                  alertIsCritical(issue.severity)
                    ? "border-red-400 text-red-700 dark:text-red-300"
                    : "border-amber-400 text-amber-700 dark:text-amber-300"
                }`}
              >
                {alertIsCritical(issue.severity) ? "Critical" : "Warning"}
              </Badge>
              <span className="text-xs font-medium">{alertClassLabel(issue.id, issue.type)}</span>
            </div>
            <p className="mt-1">{issue.description}</p>
            <div className="flex flex-wrap gap-3 mt-1 text-xs opacity-80">
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
