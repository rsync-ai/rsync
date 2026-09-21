// The rules behind the schedules page's "Now" column, kept free of React so that every
// decision about what a row is allowed to claim can be tested without rendering it.
//
// One rule outranks the rest: nothing here produces "idle" unless the server asked a
// refresh loop and was told there was none. Idle is the reassuring answer — the one an
// operator reads and moves on from — so every way of not knowing (orchestration down, a
// failed request, a model the answer left out, a state this page has never seen) gets a
// cell of its own instead of falling through to it. The api-gateway keeps the same four
// answers apart for the same reason (saved_query_explorer_running.go, the runningState
// constants).

import { formatSpanCoarse } from "@/lib/duration"

/** The rebuild in flight (explorer_workflow_queries.go ModelRefreshCurrent). */
export interface ModelRefreshCurrent {
  schedule_id: string
  upstream_kind: string
  upstream_id: string
  execution_id?: string
  /** How many model-to-model hops the chain had already taken. */
  depth: number
  /** How many upstream completions this one rebuild is standing in for. */
  coalesced: number
  started_at: string
}

/** One refresh loop's answer (explorer_workflow_queries.go ModelRefreshState). */
export interface ModelRefreshState {
  saved_query_id: string
  phase: string
  queued_completions: number
  /** Null while waiting. */
  current: ModelRefreshCurrent | null
  refreshes_this_run: number
  refreshes_per_run: number
  last_error?: string
}

export interface RunningModelWork {
  saved_query_id: string
  name: string
  schedule_id?: string
  workflow_id: string
  state: string
  detail_available: boolean
  detail?: ModelRefreshState
  message?: string
}

export interface RunningModelsResponse {
  models: RunningModelWork[]
  count: number
  limit: number
  temporal_available: boolean
}

/** One recorded freshness miss (saved_query_freshness.go ModelFreshnessBreach). */
export interface ModelFreshnessBreach {
  breach_id: string
  saved_query_id: string
  name: string
  target_table: string
  deadline_seconds: number
  cause: string
  reference_at: string
  never_succeeded: boolean
  /** How far past the deadline the table was when the breach opened. Never changes. */
  stale_seconds: number
  /** How far past the deadline it is as the response was written. Open breaches only. */
  stale_seconds_now?: number
  detected_at: string
  resolved_at?: string
  resolution?: string
}

export interface ScheduleUpstream {
  kind: string
  id: string
  name?: string
}

export type Fetched<T> =
  /** `note` says why an answer that was on screen was set aside to wait for this one. */
  | { status: "loading"; note?: string }
  | { status: "error"; message: string }
  | { status: "ok"; data: T }

export type LiveCell =
  /** A clock schedule. It has no refresh loop, so there is nothing to ask. */
  | { kind: "not_applicable" }
  | { kind: "loading"; reason?: string }
  /** Nothing could be asked: the request failed, or orchestration is not available. */
  | { kind: "unavailable"; reason: string }
  /** The answer came back without this model in it. */
  | { kind: "not_checked"; reason: string }
  | { kind: "idle"; reason: string }
  | { kind: "rebuilding"; detail: ModelRefreshState }
  | { kind: "waiting"; detail: ModelRefreshState }
  | { kind: "running_no_detail"; reason: string }
  | { kind: "unreachable"; reason: string }
  | { kind: "unknown"; reason: string }

export const AFTER_UPSTREAM = "after_upstream"

export const ORCHESTRATION_UNAVAILABLE_REASON =
  "Orchestration is not available to the server, so no refresh loop was asked."

/**
 * Reads a /explorer/running body, or returns null for one this page cannot trust.
 *
 * `temporal_available` is required rather than defaulted: it decides whether any row may
 * read idle at all, so a body without it is not allowed to say anything.
 */
export function parseRunningModels(body: unknown): RunningModelsResponse | null {
  if (!body || typeof body !== "object") return null
  const b = body as Record<string, unknown>
  if (typeof b.temporal_available !== "boolean" || !Array.isArray(b.models)) return null
  return {
    models: b.models as RunningModelWork[],
    count: typeof b.count === "number" ? b.count : b.models.length,
    limit: typeof b.limit === "number" ? b.limit : Number.POSITIVE_INFINITY,
    temporal_available: b.temporal_available,
  }
}

/**
 * Reads a /explorer/freshness body into its OPEN breaches, or null if unreadable.
 *
 * The route already returns only open breaches unless asked for history. They are
 * filtered again here because a resolved breach rendered as a present-tense "Overdue"
 * is a false alarm on a table that has since been rebuilt.
 */
export function parseOpenBreaches(body: unknown): ModelFreshnessBreach[] | null {
  if (!body || typeof body !== "object") return null
  const breaches = (body as Record<string, unknown>).breaches
  if (!Array.isArray(breaches)) return null
  return (breaches as ModelFreshnessBreach[]).filter((b) => !b.resolved_at)
}

/** What the Now column says for one schedule row. */
export function liveCellFor(
  scheduleType: string,
  savedQueryId: string,
  running: Fetched<RunningModelsResponse>,
): LiveCell {
  // The route lists after_upstream schedules only. A clock schedule reading "Idle" would
  // be an answer to a question nobody asked.
  if (scheduleType !== AFTER_UPSTREAM) return { kind: "not_applicable" }
  if (running.status === "loading") return running.note ? { kind: "loading", reason: running.note } : { kind: "loading" }
  if (running.status === "error") return { kind: "unavailable", reason: running.message }

  const { data } = running
  // Checked before any row's own state: with no Temporal client nothing was asked, so no
  // row in this answer can be idle, whatever it says.
  if (!data.temporal_available) {
    return { kind: "unavailable", reason: ORCHESTRATION_UNAVAILABLE_REASON }
  }

  const work = data.models.find((m) => m.saved_query_id === savedQueryId)
  if (!work) {
    return {
      kind: "not_checked",
      reason:
        data.count >= data.limit
          ? `Only the first ${data.limit} upstream-triggered models, by name, are checked.`
          : "This model was not in the last answer. It may have changed since the page loaded.",
    }
  }

  switch (work.state) {
    case "idle":
      return { kind: "idle", reason: work.message || "No refresh loop is open for this model." }
    case "running":
      if (!work.detail_available || !work.detail) {
        return {
          kind: "running_no_detail",
          reason: work.message || "The refresh loop is running but did not say what it is doing.",
        }
      }
      if (work.detail.phase === "rebuilding") return { kind: "rebuilding", detail: work.detail }
      if (work.detail.phase === "waiting") return { kind: "waiting", detail: work.detail }
      return {
        kind: "running_no_detail",
        reason: `The refresh loop reported a phase this page does not recognise: "${work.detail.phase}".`,
      }
    case "unreachable":
      // The server's message is the raw query error ("context deadline exceeded"), which
      // on its own does not say what was being asked.
      return {
        kind: "unreachable",
        reason: work.message
          ? `The refresh loop could not be asked what it is doing (${work.message}).`
          : "The refresh loop could not be asked what it is doing.",
      }
    case "unknown":
      return { kind: "unknown", reason: work.message || ORCHESTRATION_UNAVAILABLE_REASON }
    default:
      return {
        kind: "unknown",
        reason: `The server reported a state this page does not recognise: "${work.state}".`,
      }
  }
}

/** The open breach for a model, if it has one. The database allows at most one. */
export function openBreachFor(
  savedQueryId: string,
  breaches: ModelFreshnessBreach[],
): ModelFreshnessBreach | undefined {
  return breaches.find((b) => b.saved_query_id === savedQueryId && !b.resolved_at)
}

/** How far past its deadline the table is now, falling back to when the miss was recorded. */
export function overdueSeconds(b: ModelFreshnessBreach): number {
  return b.stale_seconds_now ?? b.stale_seconds
}

/**
 * How stale, in words: "45s", "12m", "2h 5m", "3d 4h". Seconds in, because
 * every freshness figure the API sends is in seconds.
 *
 * The body was a copy — the same shape as the pipeline panels' formatter,
 * written separately — so "how overdue is this model" and "how long did this
 * stage take" could disagree about the same span. Both spans are now rendered
 * by `@/lib/duration`; this one keeps its coarse form there.
 */
export function formatSpan(seconds: number): string {
  return formatSpanCoarse(seconds)
}

/** Why nothing rebuilt the model, in words (saved_query_freshness.go classifyFreshnessCause). */
export function describeFreshnessCause(cause: string): string {
  switch (cause) {
    case "overdue":
      return "its schedule is active but has not rebuilt it"
    case "no_schedule":
      return "nothing is scheduled to rebuild it"
    case "schedule_paused":
      return "its schedule is paused"
    // Kept apart from a paused schedule because the fix is different: somebody chose
    // that one, and nobody chose this one.
    case "schedule_auto_paused":
      return "its schedule was paused automatically after failing"
    case "no_upstreams":
      return "it rebuilds after upstreams, but none are set"
    default:
      return cause ? `recorded cause "${cause}"` : "no cause was recorded"
  }
}

/** Names an upstream by the schedule's own joined name when it has one, else by id. */
export function describeUpstream(kind: string, id: string, upstreams?: ScheduleUpstream[]): string {
  const match = upstreams?.find((u) => u.kind === kind && u.id === id)
  const label = match?.name?.trim() || id
  return kind ? `${kind} ${label}` : label
}

function plural(n: number, word: string): string {
  return `${n} ${word}${n === 1 ? "" : "s"}`
}

/** The counts under an open refresh loop, as short clauses joined by the caller. */
export function describeRefreshFacts(d: ModelRefreshState): string[] {
  const facts = [`${plural(d.queued_completions, "completion")} queued`]
  // One rebuild writes one row to saved_query_runs however many completions it absorbed,
  // so this is the only place the rest of that batch is visible at all.
  if (d.current && d.current.coalesced > 1) {
    facts.push(`this rebuild stands in for ${d.current.coalesced} completions`)
  }
  if (d.current && d.current.depth > 0) {
    facts.push(`${plural(d.current.depth, "hop")} down a model chain`)
  }
  if (d.refreshes_per_run > 0) {
    facts.push(`refresh ${d.refreshes_this_run} of ${d.refreshes_per_run} this run`)
  }
  return facts
}
