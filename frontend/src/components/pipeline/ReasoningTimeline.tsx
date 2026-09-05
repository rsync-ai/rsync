"use client"

import { useState, useMemo } from "react"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { 
  ChevronDown, 
  ChevronRight, 
  Search, 
  Filter,
  AlertCircle,
  CheckCircle2,
  Clock,
  Zap
} from "lucide-react"
import { EventNormalizer, type PipelineRunEvent, type StageGroup } from "@/lib/pipeline/eventNormalizer"

type Props = {
  events: PipelineRunEvent[]
  loading?: boolean
  emptyMessage?: string
}

function formatDuration(ms?: number): string {
  if (!ms) return ""
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${(ms / 60000).toFixed(1)}m`
}

function SeverityIcon({ severity }: { severity: string }) {
  switch (severity) {
    case "error":
      return <AlertCircle className="h-4 w-4 text-red-500" />
    case "warn":
      return <AlertCircle className="h-4 w-4 text-yellow-500" />
    default:
      return <CheckCircle2 className="h-4 w-4 text-green-500" />
  }
}

function StageStatusBadge({ status }: { status: StageGroup["status"] }) {
  type BadgeVariant = "default" | "secondary" | "outline" | "destructive"
  const variants: Record<StageGroup["status"], { variant: BadgeVariant; label: string }> = {
    pending: { variant: "secondary", label: "Pending" },
    running: { variant: "default", label: "Running" },
    completed: { variant: "outline", label: "Completed" },
    failed: { variant: "destructive", label: "Failed" },
  }
  const config = variants[status] || variants.pending
  return <Badge variant={config.variant}>{config.label}</Badge>
}

export function ReasoningTimeline({ events, loading, emptyMessage }: Props) {
  const [searchQuery, setSearchQuery] = useState("")
  const [severityFilter, setSeverityFilter] = useState<string[]>([])
  const [collapsedStages, setCollapsedStages] = useState<Set<string>>(new Set())
  const [expandedEvents, setExpandedEvents] = useState<Set<string>>(new Set())

  const stageGroups = useMemo(() => {
    return EventNormalizer.groupByStage(events)
  }, [events])

  const decisions = useMemo(() => {
    return EventNormalizer.extractDecisions(events)
  }, [events])

  const selfHeals = useMemo(() => {
    return EventNormalizer.extractSelfHeals(events)
  }, [events])

  const filteredGroups = useMemo(() => {
    return stageGroups.map((group) => {
      let filteredEvents = group.events

      // Apply search filter
      if (searchQuery.trim()) {
        const query = searchQuery.toLowerCase()
        filteredEvents = filteredEvents.filter(
          (e) =>
            e.title.toLowerCase().includes(query) ||
            e.description?.toLowerCase().includes(query) ||
            e.type.toLowerCase().includes(query)
        )
      }

      // Apply severity filter
      if (severityFilter.length > 0) {
        filteredEvents = filteredEvents.filter((e) => severityFilter.includes(e.severity))
      }

      return { ...group, events: filteredEvents }
    }).filter((group) => group.events.length > 0) // Hide empty groups
  }, [stageGroups, searchQuery, severityFilter])

  const toggleStage = (id: string) => {
    setCollapsedStages((prev) => {
      const next = new Set(prev)
      if (next.has(id)) {
        next.delete(id)
      } else {
        next.add(id)
      }
      return next
    })
  }

  const toggleEvent = (id: string) => {
    setExpandedEvents((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const toggleSeverityFilter = (severity: string) => {
    setSeverityFilter((prev) =>
      prev.includes(severity) ? prev.filter((s) => s !== severity) : [...prev, severity]
    )
  }

  if (loading) {
    return <div className="text-sm text-muted-foreground">Loading timeline…</div>
  }

  if (events.length === 0) {
    return <div className="text-sm text-muted-foreground">{emptyMessage ?? "No events to display yet."}</div>
  }

  return (
    <div className="space-y-3">
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative flex-1 min-w-[200px]">
          <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search events..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-8"
          />
        </div>
        <div className="flex items-center gap-1">
          <Filter className="h-4 w-4 text-muted-foreground" />
          <Button
            variant={severityFilter.includes("error") ? "default" : "outline"}
            size="sm"
            onClick={() => toggleSeverityFilter("error")}
          >
            Errors
          </Button>
          <Button
            variant={severityFilter.includes("warn") ? "default" : "outline"}
            size="sm"
            onClick={() => toggleSeverityFilter("warn")}
          >
            Warnings
          </Button>
        </div>
      </div>

      {/* Self-heal summary */}
      {selfHeals.length > 0 && (
        <Card className="border-purple-200 bg-purple-50 dark:border-purple-900 dark:bg-purple-950/30">
          <CardContent className="p-3">
            <div className="flex items-center gap-2 text-sm font-medium text-purple-700 dark:text-purple-300">
              <Zap className="h-4 w-4" />
              {selfHeals.length} self-healing action{selfHeals.length > 1 ? "s" : ""} detected
            </div>
          </CardContent>
        </Card>
      )}

      {/* Decision cards */}
      {decisions.length > 0 && (
        <div className="space-y-2">
          <div className="text-sm font-medium text-muted-foreground">Key Decisions</div>
          {decisions.slice(0, 3).map((decision) => (
            <Card key={decision.id} className="border-blue-200 bg-blue-50 dark:border-blue-900 dark:bg-blue-950/30">
              <CardContent className="p-3 space-y-2">
                <div className="text-sm font-medium text-blue-900 dark:text-blue-100">{decision.summary}</div>
                {decision.rationale && (
                  <div className="text-xs text-blue-700 dark:text-blue-300">{decision.rationale}</div>
                )}
                {decision.confidence !== undefined && (
                  <div className="text-xs text-muted-foreground">Confidence: {Math.round(decision.confidence * 100)}%</div>
                )}
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Stage-grouped timeline */}
      <div className="space-y-2">
        {filteredGroups.map((group) => {
          const isCollapsed = collapsedStages.has(group.id)

          return (
            <Card key={group.id} className="border-zinc-200 dark:border-zinc-800">
              <div
                className="flex items-center justify-between p-3 cursor-pointer hover:bg-zinc-50 dark:hover:bg-zinc-900/50"
                onClick={() => toggleStage(group.id)}
              >
                <div className="flex items-center gap-2">
                  {isCollapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                  <div className="text-sm font-medium">{group.name}</div>
                  <StageStatusBadge status={group.status} />
                  {group.duration && (
                    <Badge variant="outline" className="text-xs">
                      <Clock className="h-3 w-3 mr-1" />
                      {formatDuration(group.duration)}
                    </Badge>
                  )}
                </div>
                <div className="text-xs text-muted-foreground">{group.events.length} events</div>
              </div>

              {!isCollapsed && (
                <CardContent className="px-3 pb-3 space-y-2 border-t border-zinc-100 dark:border-zinc-800">
                  {group.events.map((event) => (
                    <div
                      key={event.id}
                      className="rounded border border-zinc-200 p-2 text-sm dark:border-zinc-700"
                    >
                      <div className="flex items-start justify-between gap-2">
                        <div className="flex items-start gap-2 flex-1">
                          <SeverityIcon severity={event.severity} />
                          <div className="flex-1 space-y-1">
                            <div className="font-medium">{event.title}</div>
                            {event.description && (
                              <div className="text-xs text-muted-foreground">{event.description}</div>
                            )}
                            <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                              <span className="font-mono">{event.type}</span>
                              {typeof event.seq === "number" ? (
                                <span className="font-mono">seq: {event.seq}</span>
                              ) : null}
                              {event.stage ? (
                                <span className="font-mono">stage: {event.stage}</span>
                              ) : null}
                              {event.stageGroup ? (
                                <span className="font-mono">group: {event.stageGroup}</span>
                              ) : null}
                              {event.traceId && (
                                <span className="font-mono">trace: {event.traceId.slice(0, 8)}</span>
                              )}
                            </div>
                          </div>
                        </div>
                        <div className="text-xs text-muted-foreground whitespace-nowrap">
                          {new Date(event.timestamp).toISOString().slice(11, 19)}
                        </div>
                      </div>

                      {/* Raw payload (prod-debug friendly) */}
                      {event.metadata ? (
                        <div className="mt-2">
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={() => toggleEvent(event.id)}
                          >
                            {expandedEvents.has(event.id) ? "Hide raw" : "Show raw"}
                          </Button>
                          {expandedEvents.has(event.id) ? (
                            <pre className="mt-2 max-h-72 overflow-auto rounded bg-zinc-50 p-2 text-[11px] text-zinc-800 dark:bg-zinc-900 dark:text-zinc-200">
                              {JSON.stringify(event.metadata, null, 2)}
                            </pre>
                          ) : null}
                        </div>
                      ) : null}
                    </div>
                  ))}
                </CardContent>
              )}
            </Card>
          )
        })}
      </div>

      {filteredGroups.length === 0 && (
        <div className="text-sm text-muted-foreground text-center py-8">
          No events match your filters.
        </div>
      )}
    </div>
  )
}

