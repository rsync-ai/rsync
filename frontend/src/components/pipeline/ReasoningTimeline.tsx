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
import {
  EventNormalizer,
  type NormalizedRunEvent,
  type PipelineRunEvent,
  type StageGroup,
} from "@/lib/pipeline/eventNormalizer"
import { LocalDateTime } from "@/components/ui/local-date-time"
// Floors to "21h 2m"; the local one printed a long stage as "1262.1m".
import { formatStageDuration } from "@/lib/duration"
import {
  eventDetail,
  eventLabel,
  eventStageLabel,
  isNoiseEvent,
  type DisplayEvent,
} from "@/components/pipeline/eventDisplay"

type Props = {
  events: PipelineRunEvent[]
  // Stage transitions fetched apart from the paged feed; they decide each
  // group's badge and duration (EventNormalizer.groupByStage).
  statusEvents?: PipelineRunEvent[]
  loading?: boolean
  emptyMessage?: string
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

function toDisplayEvent(e: NormalizedRunEvent): DisplayEvent {
  return { event_type: e.type, stage_id: e.stage, severity: e.severity, payload: e.metadata }
}

/**
 * What a row says, in words. The event code, sequence number, stage id, group
 * and trace id are for whoever is debugging the producer; they live behind the
 * row's Details toggle rather than on every line.
 */
type RowText = { label: string; stage: string; detail: string; extra: string; routine: boolean }

function rowText(e: NormalizedRunEvent): RowText {
  const d = toDisplayEvent(e)
  const detail = eventDetail(d)
  const extra = e.description && e.description.trim() !== detail ? e.description.trim() : ""
  return { label: eventLabel(d), stage: eventStageLabel(d), detail, extra, routine: isNoiseEvent(d) }
}

function StageStatusBadge({ status }: { status: StageGroup["status"] }) {
  type BadgeVariant = "default" | "secondary" | "outline" | "destructive"
  const variants: Record<Exclude<StageGroup["status"], "unknown">, { variant: BadgeVariant; label: string }> = {
    running: { variant: "default", label: "Running" },
    completed: { variant: "outline", label: "Completed" },
    failed: { variant: "destructive", label: "Failed" },
  }
  // No transition for this stage is known, so there is nothing true to badge.
  if (status === "unknown") return null
  const config = variants[status]
  if (!config) return null
  return <Badge variant={config.variant}>{config.label}</Badge>
}

export function ReasoningTimeline({ events, statusEvents, loading, emptyMessage }: Props) {
  const [searchQuery, setSearchQuery] = useState("")
  const [severityFilter, setSeverityFilter] = useState<string[]>([])
  const [collapsedStages, setCollapsedStages] = useState<Set<string>>(new Set())
  const [expandedEvents, setExpandedEvents] = useState<Set<string>>(new Set())
  // Routine rows (heartbeats, throughput and per-batch table-stat ticks) are
  // hidden by default and counted, never dropped: the toggle hands them back.
  const [showRoutine, setShowRoutine] = useState(false)

  const stageGroups = useMemo(() => {
    return EventNormalizer.groupByStage(events, statusEvents)
  }, [events, statusEvents])

  const decisions = useMemo(() => {
    return EventNormalizer.extractDecisions(events)
  }, [events])

  const selfHeals = useMemo(() => {
    return EventNormalizer.extractSelfHeals(events)
  }, [events])

  const textById = useMemo(() => {
    const m = new Map<string, RowText>()
    for (const g of stageGroups) for (const e of g.events) m.set(e.id, rowText(e))
    return m
  }, [stageGroups])

  const filtersActive = searchQuery.trim() !== "" || severityFilter.length > 0

  const { filteredGroups, routineCount } = useMemo(() => {
    const query = searchQuery.trim().toLowerCase()
    const counted = stageGroups.map((group) => {
      let filteredEvents = group.events

      // Apply search filter: the words on the row, plus the event code so a
      // search for a type someone pasted from a log still finds it.
      if (query) {
        filteredEvents = filteredEvents.filter((e) => {
          const t = textById.get(e.id)
          return [t?.label, t?.stage, t?.detail, t?.extra, e.title, e.type]
            .some((s) => s?.toLowerCase().includes(query))
        })
      }

      // Apply severity filter
      if (severityFilter.length > 0) {
        filteredEvents = filteredEvents.filter((e) => severityFilter.includes(e.severity))
      }

      // Count routine rows among what the filters kept, so the toggle's number
      // is exactly how many rows it would add.
      const kept = filteredEvents.filter((e) => !textById.get(e.id)?.routine)
      const routine = filteredEvents.length - kept.length
      if (!showRoutine) filteredEvents = kept

      return { group: { ...group, events: filteredEvents }, routine }
    })
    return {
      filteredGroups: counted.map((c) => c.group).filter((group) => group.events.length > 0), // Hide empty groups
      routineCount: counted.reduce((n, c) => n + c.routine, 0),
    }
  }, [stageGroups, textById, searchQuery, severityFilter, showRoutine])

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

      {routineCount > 0 && (
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>
            {showRoutine
              ? "Routine progress updates are shown."
              : "Routine progress updates (heartbeats, throughput and table-stat ticks) are hidden."}
          </span>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="h-7 px-2 text-xs"
            onClick={() => setShowRoutine((v) => !v)}
          >
            {showRoutine ? "Hide" : "Show"} {routineCount} routine progress update{routineCount === 1 ? "" : "s"}
          </Button>
        </div>
      )}

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
                  {/* "32s · 2 attempts", not "32s": the figure is working time
                      summed across attempts, so on a retried stage it is much
                      shorter than the wall clock between the lane's first and
                      last event. Saying how many attempts there were is what
                      makes the two numbers reconcilable. */}
                  {group.duration ? (
                    <Badge variant="outline" className="text-xs">
                      <Clock className="h-3 w-3 mr-1" />
                      {formatStageDuration({
                        activeMs: group.duration,
                        attempts: group.attempts ?? 1,
                        running: group.status === "running",
                      })}
                    </Badge>
                  ) : null}
                </div>
                <div className="text-xs text-muted-foreground">{group.events.length} {group.events.length === 1 ? "event" : "events"}</div>
              </div>

              {!isCollapsed && (
                <CardContent className="px-3 pb-3 space-y-2 border-t border-zinc-100 dark:border-zinc-800">
                  {group.events.map((event) => {
                    const t = textById.get(event.id) ?? rowText(event)
                    const open = expandedEvents.has(event.id)
                    const hasPayload = !!event.metadata && Object.keys(event.metadata).length > 0
                    return (
                      <div
                        key={event.id}
                        data-testid="activity-row"
                        className="rounded border border-zinc-200 p-2 text-sm dark:border-zinc-700"
                      >
                        <div className="flex items-start justify-between gap-2">
                          <div className="flex items-start gap-2 flex-1 min-w-0">
                            <SeverityIcon severity={event.severity} />
                            <div className="flex-1 min-w-0 space-y-1">
                              <div>
                                <span className="font-medium">{t.label}</span>
                                {t.stage ? (
                                  <span className="text-xs text-muted-foreground"> · {t.stage}</span>
                                ) : null}
                              </div>
                              {t.detail ? <div className="text-xs break-words">{t.detail}</div> : null}
                              {t.extra ? (
                                <div className="text-xs text-muted-foreground break-words">{t.extra}</div>
                              ) : null}
                            </div>
                          </div>
                          <div className="flex flex-col items-end gap-1 text-xs text-muted-foreground whitespace-nowrap">
                            {/* Was new Date(ts).toISOString().slice(11, 19): unlabelled UTC,
                                and a RangeError that took the whole timeline down when an
                                event carried an unparseable timestamp. */}
                            <LocalDateTime value={event.timestamp} fallback="Time unknown" />
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              className="h-6 px-2 text-xs"
                              aria-expanded={open}
                              onClick={() => toggleEvent(event.id)}
                            >
                              {open ? "Hide details" : "Details"}
                            </Button>
                          </div>
                        </div>

                        {/* Internal codes and the raw payload, for debugging a producer. */}
                        {open ? (
                          <div className="mt-2 space-y-2">
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
                              {event.traceId ? (
                                <span className="font-mono">trace: {event.traceId}</span>
                              ) : null}
                            </div>
                            {hasPayload ? (
                              <pre className="max-h-72 overflow-auto rounded bg-zinc-50 p-2 text-[11px] text-zinc-800 dark:bg-zinc-900 dark:text-zinc-200">
                                {JSON.stringify(event.metadata, null, 2)}
                              </pre>
                            ) : null}
                          </div>
                        ) : null}
                      </div>
                    )
                  })}
                </CardContent>
              )}
            </Card>
          )
        })}
      </div>

      {filteredGroups.length === 0 && (
        <div className="text-sm text-muted-foreground text-center py-8">
          {/* "No events" would be false here: there were events, all routine. */}
          {!filtersActive && routineCount > 0
            ? "Only routine progress updates so far — nothing that needs your attention."
            : "No events match your filters."}
        </div>
      )}
    </div>
  )
}

