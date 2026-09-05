"use client"

import { useEffect, useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { ExternalLink, AlertTriangle, CheckCircle2, XCircle, TrendingUp } from "lucide-react"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"

type MonitoringOverview = {
  pipeline_id: string
  time_range: {
    since: string
    until: string
  }
  agent_reasoning?: {
    total_decisions: number
    high_confidence: number
    medium_confidence: number
    low_confidence: number
    avg_confidence: number
    recent_decisions?: Array<{
      event_id: string
      timestamp: string
      decision: string
      rationale?: string
      confidence?: number
      trace_id?: string
    }>
  }
  agent_reasoning_error?: string
  data_plane?: {
    total_rows_processed: number
    total_bytes_processed: number
    avg_throughput_rows_per_sec?: number
    cdc_lag_ms?: number
    error_count: number
    last_metric_time?: string
  }
  data_plane_error?: string
  infrastructure?: {
    components_monitored: number
    healthy_components: number
    degraded_components: number
    unhealthy_components: number
    active_issues: number
    critical_issues: number
  }
  infrastructure_error?: string
  correlations?: Correlation[]
  links: {
    signoz_trace?: string
    signoz_metrics?: string
    signoz_dashboard?: string
  }
}

type Correlation = {
  agent_decision: {
    decision?: string
  }
  infra_event?: {
    description?: string
  }
  confidence: number
  method: string
}

export function MonitoringOverviewTab({ pipelineId }: { pipelineId: string }) {
  const [overview, setOverview] = useState<MonitoringOverview | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function fetchOverview() {
      try {
        setLoading(true)
        setError(null)
        const res = await authFetch(
          `${API_ENDPOINTS.PIPELINES.MONITORING_OVERVIEW(pipelineId)}?range=last_hour`,
          {
            cache: "no-store",
          }
        )
        
        if (!res.ok) {
          if (res.status === 404) {
            setError("Monitoring overview not enabled")
          } else if (res.status === 403) {
            setError("Access denied")
          } else {
            setError(`Failed to load overview (${res.status})`)
          }
          return
        }
        
        const data = await res.json()
        setOverview(data)
      } catch {
        setError("Network error")
      } finally {
        setLoading(false)
      }
    }

    fetchOverview()
  }, [pipelineId])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-sm text-muted-foreground">Loading monitoring overview...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="rounded border border-amber-200 bg-amber-50 p-4 text-sm text-amber-700 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300">
        <div className="flex items-center gap-2">
          <AlertTriangle className="h-4 w-4" />
          {error}
        </div>
      </div>
    )
  }

  if (!overview) {
    return (
      <div className="text-sm text-muted-foreground">No monitoring data available</div>
    )
  }

  const formatNumber = (n: number) => {
    if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`
    if (n >= 1000) return `${(n / 1000).toFixed(1)}K`
    return n.toString()
  }

  const formatBytes = (bytes: number) => {
    if (bytes >= 1073741824) return `${(bytes / 1073741824).toFixed(2)} GB`
    if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(2)} MB`
    if (bytes >= 1024) return `${(bytes / 1024).toFixed(2)} KB`
    return `${bytes} B`
  }

  return (
    <div className="space-y-4">
      {/* Summary cards (keep focused on agent + data plane) */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Agent Reasoning */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium">Agent Reasoning</CardTitle>
            <CardDescription className="text-xs">Decision-making health</CardDescription>
          </CardHeader>
          <CardContent>
            {overview.agent_reasoning_error ? (
              <div className="text-xs text-amber-600 dark:text-amber-400">
                {overview.agent_reasoning_error}
              </div>
            ) : overview.agent_reasoning ? (
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Decisions</span>
                  <span className="text-lg font-semibold">{overview.agent_reasoning.total_decisions}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Avg Confidence</span>
                  <Badge variant={overview.agent_reasoning.avg_confidence >= 0.9 ? "default" : "secondary"}>
                    {(overview.agent_reasoning.avg_confidence * 100).toFixed(0)}%
                  </Badge>
                </div>
                <div className="flex gap-2 text-xs">
                  <div className="flex items-center gap-1">
                    <CheckCircle2 className="h-3 w-3 text-green-500" />
                    <span>{overview.agent_reasoning.high_confidence} high</span>
                  </div>
                  {overview.agent_reasoning.low_confidence > 0 && (
                    <div className="flex items-center gap-1">
                      <AlertTriangle className="h-3 w-3 text-amber-500" />
                      <span>{overview.agent_reasoning.low_confidence} low</span>
                    </div>
                  )}
                </div>
              </div>
            ) : (
              <div className="text-xs text-muted-foreground">Not available</div>
            )}
          </CardContent>
        </Card>

        {/* Data Plane */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium">Data Plane</CardTitle>
            <CardDescription className="text-xs">Pipeline performance</CardDescription>
          </CardHeader>
          <CardContent>
            {overview.data_plane_error ? (
              <div className="text-xs text-amber-600 dark:text-amber-400">
                {overview.data_plane_error}
              </div>
            ) : overview.data_plane ? (
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Rows</span>
                  <span className="text-lg font-semibold">{formatNumber(overview.data_plane.total_rows_processed)}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Data</span>
                  <span className="text-sm font-medium">{formatBytes(overview.data_plane.total_bytes_processed)}</span>
                </div>
                {overview.data_plane.avg_throughput_rows_per_sec && (
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-muted-foreground">Throughput</span>
                    <div className="flex items-center gap-1">
                      <TrendingUp className="h-3 w-3 text-green-500" />
                      <span className="text-xs">{formatNumber(overview.data_plane.avg_throughput_rows_per_sec)} rows/s</span>
                    </div>
                  </div>
                )}
                {overview.data_plane.cdc_lag_ms !== undefined && overview.data_plane.cdc_lag_ms !== null && (
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-muted-foreground">CDC Lag</span>
                    <Badge variant={overview.data_plane.cdc_lag_ms < 1000 ? "default" : "secondary"}>
                      {overview.data_plane.cdc_lag_ms}ms
                    </Badge>
                  </div>
                )}
              </div>
            ) : (
              <div className="text-xs text-muted-foreground">Not available yet</div>
            )}
          </CardContent>
        </Card>

      </div>

      {/* SigNoz deep links */}
      {(overview.links.signoz_trace || overview.links.signoz_dashboard) && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Deep Dive</CardTitle>
            <CardDescription className="text-xs">
              Explore detailed traces in SigNoz
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-2">
              {overview.links.signoz_trace && (
                <Button variant="outline" size="sm" asChild>
                  <a href={overview.links.signoz_trace} target="_blank" rel="noopener noreferrer">
                    <ExternalLink className="h-3 w-3 mr-2" />
                    View Trace
                  </a>
                </Button>
              )}
              {overview.links.signoz_dashboard && (
                <Button variant="outline" size="sm" asChild>
                  <a href={overview.links.signoz_dashboard} target="_blank" rel="noopener noreferrer">
                    <ExternalLink className="h-3 w-3 mr-2" />
                    SigNoz Dashboard
                  </a>
                </Button>
              )}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

