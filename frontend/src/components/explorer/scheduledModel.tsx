import { AlertTriangle } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { describeUpstreamSet, type RunProvenanceFields } from "@/components/explorer/runProvenance"
import { describeCron } from "@/components/explorer/cronSentence"

// Shared by the Scheduled Queries list and the per-model page, so the two cannot
// disagree about what a schedule's status or a run's outcome is called.

export interface ScheduleSpec {
  cron?: string
  every_seconds?: number
  timezone?: string
}

export interface ScheduledQuery {
  schedule_id: string
  saved_query_id: string
  name: string
  description?: string
  connection_id: string
  connection_name?: string
  connector_type?: string
  schedule_type: string
  schedule_spec: ScheduleSpec
  status: string
  materialization: string
  target_table?: string
  statement_class?: string
  /** Resolved from connector_type server-side by the Explorer capability table. */
  supports_materialization?: boolean
  last_run_at?: string
  last_run_status?: string
  last_run_error?: string
  next_run_at?: string
  /**
   * Set only for schedule_type "after_upstream": every pipeline and model whose
   * completion wakes this one. Whether one landing is enough or all of them must have
   * landed is upstream_policy. Names are joined server-side and are empty for a producer
   * since deleted.
   */
  upstreams?: { kind: string; id: string; name?: string }[]
  /** after_upstream only: "any" rebuilds on each landing, "all" waits for every upstream. */
  upstream_policy?: "any" | "all"
  created_by: string
  created_at: string
  updated_at: string
  paused_at?: string
  paused_reason?: string
  auto_paused_at?: string
  auto_paused_reason?: string
}

export interface SavedQueryRun extends RunProvenanceFields {
  run_id: string
  saved_query_id: string
  schedule_id?: string
  trigger_source: string
  status: string
  target_table?: string
  rows_affected?: number
  error?: string
  auto_pause_reason?: string
  started_at: string
  finished_at: string
  duration_ms: number
}

/** What describeCadence reads. The model dialog's own schedule type carries the same fields. */
export type CadenceFields = Pick<ScheduledQuery, "schedule_type" | "schedule_spec" | "upstreams" | "upstream_policy">

export function describeCadence(s: CadenceFields): string {
  if (s.schedule_type === "after_upstream") {
    return describeUpstreamSet(s.upstreams, s.upstream_policy)
  }
  if (s.schedule_type === "interval") {
    const seconds = s.schedule_spec.every_seconds || 0
    if (seconds % 3600 === 0) return `Every ${seconds / 3600} hour${seconds === 3600 ? "" : "s"}`
    if (seconds % 60 === 0) return `Every ${seconds / 60} minute${seconds === 60 ? "" : "s"}`
    return `Every ${seconds} seconds`
  }
  const zone = s.schedule_spec.timezone || "UTC"
  return `${describeCron(s.schedule_spec.cron) ?? s.schedule_spec.cron} (${zone})`
}

/**
 * A cron schedule's expression as stored, and whether describeCadence put it into words.
 * Null for any other kind of schedule. Where it was worded, the expression is still worth
 * showing beside the sentence: it is what the Edit schedule form holds.
 */
export function cadenceCron(s: CadenceFields): { expression: string; worded: boolean } | null {
  if (s.schedule_type === "after_upstream" || s.schedule_type === "interval") return null
  const expression = s.schedule_spec.cron ?? ""
  return { expression, worded: describeCron(expression) !== null }
}

/**
 * The Next run of an active after_upstream schedule, which has no time by construction.
 * A fan-in on "all" is not woken by the next landing: the gateway rebuilds it only once
 * every upstream has completed since its last successful rebuild. Same rule as
 * describeUpstreamSet, so with one upstream the policy changes nothing.
 */
export function describeUpstreamNextRun(s: Pick<ScheduledQuery, "upstreams" | "upstream_policy">): string {
  return s.upstream_policy === "all" && (s.upstreams?.length ?? 0) > 1
    ? "When all its upstreams have run"
    : "When an upstream runs"
}

/**
 * Whether a fire of this schedule would actually do anything. A statement model has
 * no target table by design — it writes wherever its own SQL says — so the absence of
 * one is only a fault in table mode.
 */
export function runsDoSomething(s: ScheduledQuery): boolean {
  if (s.materialization === "statement") return true
  return s.materialization === "table" && !!s.target_table
}

/**
 * "48ms", "12.3s", "4m 5s", "1h 2m": precise where a run is short, readable where
 * it is not. A dash, not "0ms", for a run with no duration yet — this renders into
 * the schedule table and the duration chart's axis, where a zero would read as a
 * measurement. Both behaviours now come from `@/lib/duration`; the sub-second
 * branch is the one this copy existed for (16 ms and 48 ms must not both read
 * "0s", or the chart draws two very different runs the same).
 */
export { formatDurationOrDash as formatDuration } from "@/lib/duration"

export function statusBadge(s: ScheduledQuery) {
  // A schedule whose runs do nothing is `active` and firing, and every fire refuses.
  // The status alone would report that as healthy, so it is called out separately.
  if (s.status === "active" && !runsDoSomething(s)) {
    return (
      <Badge variant="outline" className="border-amber-500 text-amber-600">
        <AlertTriangle className="h-3 w-3 mr-1" />
        does nothing
      </Badge>
    )
  }
  if (s.status === "active") {
    return (
      <Badge variant="outline" className="border-emerald-500 text-emerald-600">
        active
      </Badge>
    )
  }
  return (
    <Badge variant="outline" className="text-zinc-500 dark:text-zinc-400">
      {s.status}
    </Badge>
  )
}

export function runStatusBadge(status: string) {
  if (status === "succeeded") {
    return <Badge variant="outline" className="border-emerald-500 text-emerald-600">succeeded</Badge>
  }
  if (status === "failed") {
    return <Badge variant="outline" className="border-red-500 text-red-600">failed</Badge>
  }
  if (status === "skipped") {
    return <Badge variant="outline" className="border-amber-500 text-amber-600">skipped</Badge>
  }
  return <Badge variant="outline" className="text-zinc-500 dark:text-zinc-400">{status}</Badge>
}
