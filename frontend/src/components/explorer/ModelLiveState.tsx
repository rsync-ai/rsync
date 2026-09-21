"use client"

import { useCallback, useEffect, useRef, useState, type ReactNode } from "react"
import { AlertTriangle, HelpCircle, Loader2 } from "lucide-react"
import { authFetch } from "@/lib/api/auth-fetch"
import { Badge } from "@/components/ui/badge"
import { formatAbsoluteTime } from "@/components/explorer/scheduleTime"
import {
  describeFreshnessCause,
  describeRefreshFacts,
  describeUpstream,
  formatSpan,
  ORCHESTRATION_UNAVAILABLE_REASON,
  overdueSeconds,
  parseOpenBreaches,
  parseRunningModels,
  type Fetched,
  type LiveCell,
  type ModelFreshnessBreach,
  type RunningModelsResponse,
  type ScheduleUpstream,
} from "@/components/explorer/liveState"

// Every model answered costs one Temporal query on pipeline-workflows, the task queue
// that carries real rebuilds (saved_query_explorer_running.go). Fifteen seconds is slow
// enough that a page left open is not load, and fast enough that a rebuild of any length
// worth watching shows up while someone is looking.
export const RUNNING_POLL_MS = 15_000
// The breach list is a Postgres read, and the sweep that writes it runs on a
// minutes-scale interval, so polling it faster would only re-read the same rows.
export const FRESHNESS_POLL_MS = 60_000

/**
 * Runs `load` now and then every `intervalMs` — only while the tab is visible, and never
 * two at a time. Returns a manual refresh that does nothing while disabled.
 *
 * A timeout chain rather than setInterval, for the last of those. A workspace at the
 * fifty-model cap can take ~14 s to answer (eight concurrent queries, two seconds each),
 * so a fixed interval would start the next poll on top of the one still running —
 * doubling the load on the rebuild queue exactly when it is slowest. The next poll is
 * scheduled only once the last one has settled.
 */
export function useVisiblePoll(
  load: () => Promise<void>,
  intervalMs: number,
  enabled: boolean,
): () => Promise<void> {
  // The check still out. The timer and Refresh both wait on it rather than start a second
  // one, so whichever of them meets it, the next check is timed from when it lands.
  const inFlight = useRef<Promise<void> | null>(null)

  const run = useCallback(() => {
    if (!inFlight.current) {
      inFlight.current = load().finally(() => {
        inFlight.current = null
      })
    }
    return inFlight.current
  }, [load])

  useEffect(() => {
    if (!enabled) return
    let timer: ReturnType<typeof setTimeout> | null = null
    // Whether this chain is waiting on a check, and so will schedule its own next one.
    let waiting = false
    let stopped = false

    const tick = async () => {
      timer = null
      // A hidden tab stops the chain outright; the listener below restarts it. A page
      // left open in a background tab overnight asks nothing.
      if (document.visibilityState !== "visible") return
      waiting = true
      try {
        await run()
      } finally {
        waiting = false
      }
      if (!stopped) timer = setTimeout(() => void tick(), intervalMs)
    }
    const onVisibility = () => {
      // Gated on this chain, not on whether a check is out: a check Refresh started
      // schedules nothing after itself, so deferring to it would stop the page for good.
      if (document.visibilityState === "visible" && timer === null && !waiting) {
        void tick()
      }
    }

    void tick()
    document.addEventListener("visibilitychange", onVisibility)
    return () => {
      stopped = true
      if (timer !== null) clearTimeout(timer)
      document.removeEventListener("visibilitychange", onVisibility)
    }
  }, [enabled, intervalMs, run])

  return useCallback(() => (enabled ? run() : Promise.resolve()), [enabled, run])
}

// One poll plus slack for a late timer. An answer older than that was not refreshed on
// schedule (the tab was hidden, or polling was off), so it is no longer the page's
// current answer. A tab switched away for a few seconds keeps its rows.
const STALE_AFTER_MS = RUNNING_POLL_MS + 5_000

/**
 * Polls GET /api/v1/explorer/running.
 *
 * A failed check replaces the last answer rather than sitting behind it: "Rebuilding"
 * carried past a poll that failed is a present-tense claim nothing is backing any more.
 * So does an answer too old to vouch for. A tab shown again after ten minutes reads
 * Checking, with that answer's age, until the new one lands — which at the fifty-model
 * cap can take ~14 s — rather than showing a ten-minute-old Rebuilding as current.
 */
export function useRunningModels(enabled: boolean) {
  const [running, setRunning] = useState<Fetched<RunningModelsResponse>>({ status: "loading" })
  const lastSettledAt = useRef<number | null>(null)

  const load = useCallback(async () => {
    const last = lastSettledAt.current
    if (last !== null && Date.now() - last > STALE_AFTER_MS) {
      const note = `The last answer is ${formatSpan((Date.now() - last) / 1000)} old, so it is not shown while the page asks again.`
      // An error claims nothing, and its banner already says so; only an answer is set aside.
      setRunning((prev) => (prev.status === "ok" ? { status: "loading", note } : prev))
    }
    try {
      const res = await authFetch("/api/v1/explorer/running", { cache: "no-store" })
      // authFetch does not throw on a non-2xx, and an error body has no models in it —
      // which, unchecked, would read as a workspace where nothing is running.
      if (!res.ok) {
        setRunning({ status: "error", message: `The live-state check failed (HTTP ${res.status}).` })
        return
      }
      const parsed = parseRunningModels(await res.json())
      setRunning(
        parsed
          ? { status: "ok", data: parsed }
          : { status: "error", message: "The live-state check returned something this page could not read." },
      )
    } catch {
      setRunning({ status: "error", message: "Could not reach the server to check live state." })
    } finally {
      lastSettledAt.current = Date.now()
    }
  }, [])

  const refresh = useVisiblePoll(load, RUNNING_POLL_MS, enabled)
  return { running, refresh }
}

/**
 * Polls GET /api/v1/explorer/freshness for the workspace's open breaches.
 *
 * Unlike live state, an old breach list is not set aside when the tab returns. A freshness
 * check that is out shows no Overdue badge at all, which reads as on time, so withdrawing
 * the badges while it asks would be the calmer claim, not the safer one.
 */
export function useOpenFreshnessBreaches(enabled: boolean) {
  const [freshness, setFreshness] = useState<Fetched<ModelFreshnessBreach[]>>({ status: "loading" })

  const load = useCallback(async () => {
    try {
      const res = await authFetch("/api/v1/explorer/freshness", { cache: "no-store" })
      if (!res.ok) {
        setFreshness({ status: "error", message: `The freshness check failed (HTTP ${res.status}).` })
        return
      }
      const parsed = parseOpenBreaches(await res.json())
      setFreshness(
        parsed
          ? { status: "ok", data: parsed }
          : { status: "error", message: "The freshness check returned something this page could not read." },
      )
    } catch {
      setFreshness({ status: "error", message: "Could not reach the server to check freshness." })
    }
  }, [])

  const refresh = useVisiblePoll(load, FRESHNESS_POLL_MS, enabled)
  return { freshness, refresh }
}

function queuedSuffix(n: number): string {
  return n > 0 ? ` · ${n} queued` : ""
}

/** The Now cell for one row. */
export function LiveStateCell({ cell }: { cell: LiveCell }) {
  switch (cell.kind) {
    case "not_applicable":
      return (
        <span
          className="text-zinc-400"
          title="Runs on a clock. It has no refresh loop, so there is no live state to show."
        >
          —
        </span>
      )
    case "loading":
      return (
        <span className="inline-flex items-center gap-1 text-zinc-400" title={cell.reason}>
          <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
          Checking…
        </span>
      )
    case "unavailable":
      return (
        <span className="inline-flex items-center gap-1 text-zinc-500 dark:text-zinc-400" title={cell.reason}>
          <HelpCircle className="h-3 w-3" aria-hidden />
          Unavailable
        </span>
      )
    case "not_checked":
      return (
        <span className="text-zinc-400" title={cell.reason}>
          Not checked
        </span>
      )
    case "idle":
      return (
        <span className="text-zinc-500 dark:text-zinc-400" title={cell.reason}>
          Idle
        </span>
      )
    case "rebuilding":
      return (
        <>
          <Badge variant="info" className="gap-1 font-medium whitespace-nowrap">
            <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
            {`Rebuilding${queuedSuffix(cell.detail.queued_completions)}`}
          </Badge>
          <RebuildFailedBadge error={cell.detail.last_error} />
        </>
      )
    case "waiting":
      return (
        <>
          <Badge
            variant="secondary"
            className="font-medium whitespace-nowrap"
            title="The refresh loop is open and waiting for the next upstream completion."
          >
            {`Waiting${queuedSuffix(cell.detail.queued_completions)}`}
          </Badge>
          <RebuildFailedBadge error={cell.detail.last_error} />
        </>
      )
    case "running_no_detail":
      return (
        <Badge variant="info" className="font-medium whitespace-nowrap" title={cell.reason}>
          Running · no detail
        </Badge>
      )
    case "unreachable":
      return (
        <Badge variant="warning" className="gap-1 font-medium whitespace-nowrap" title={cell.reason}>
          <HelpCircle className="h-3 w-3" aria-hidden />
          Unreachable
        </Badge>
      )
    case "unknown":
      return (
        <span className="inline-flex items-center gap-1 text-zinc-500 dark:text-zinc-400" title={cell.reason}>
          <HelpCircle className="h-3 w-3" aria-hidden />
          Unknown
        </span>
      )
  }
}

/**
 * A rebuild in this run of the refresh loop failed after its retries. The loop survives
 * that and never clears it within the run (model_refresh_workflow.go), so this says a
 * failure happened, not that the model is failing now. Without it a loop carrying one
 * reads as a calm grey Waiting until someone expands the row.
 */
function RebuildFailedBadge({ error }: { error?: string }) {
  if (!error) return null
  return (
    <Badge
      variant="destructive"
      className="gap-1 font-medium whitespace-nowrap"
      title={`A rebuild failed during this run of the refresh loop: ${error}. It is not cleared when a later rebuild succeeds.`}
    >
      <AlertTriangle className="h-3 w-3" aria-hidden />
      Rebuild failed this run
    </Badge>
  )
}

/** The marker for a model whose table is past its freshness deadline. */
export function OverdueBadge({ breach }: { breach: ModelFreshnessBreach }) {
  const by = formatSpan(overdueSeconds(breach))
  return (
    <Badge
      variant="outline"
      className="gap-1 border-amber-500 font-medium whitespace-nowrap text-amber-600 dark:text-amber-400"
      title={`Past its ${formatSpan(breach.deadline_seconds)} freshness deadline by ${by}: ${describeFreshnessCause(breach.cause)}.`}
    >
      <AlertTriangle className="h-3 w-3" aria-hidden />
      {`Overdue ${by}`}
    </Badge>
  )
}

/** What an expanded row says about the model right now, above its run history. */
export function LiveStateDetail({
  cell,
  breach,
  upstreams,
}: {
  cell: LiveCell
  breach?: ModelFreshnessBreach
  upstreams?: ScheduleUpstream[]
}) {
  let body: ReactNode = null
  switch (cell.kind) {
    case "rebuilding": {
      const cur = cell.detail.current
      body = (
        <>
          <p>
            {cur
              ? `Rebuilding since ${formatAbsoluteTime(cur.started_at)}, woken by ${describeUpstream(cur.upstream_kind, cur.upstream_id, upstreams)}`
              : "Rebuilding. The refresh loop did not say which upstream woke it."}
          </p>
          <p className="text-zinc-500 dark:text-zinc-400">{describeRefreshFacts(cell.detail).join(" · ")}</p>
        </>
      )
      break
    }
    case "waiting":
      body = (
        <>
          <p>Refresh loop open, waiting for the next upstream completion</p>
          <p className="text-zinc-500 dark:text-zinc-400">{describeRefreshFacts(cell.detail).join(" · ")}</p>
        </>
      )
      break
    case "loading":
      body = (
        <p className="text-zinc-500 dark:text-zinc-400">{cell.reason ? `Checking live state… ${cell.reason}` : "Checking live state…"}</p>
      )
      break
    case "idle":
      body = <p className="text-zinc-500 dark:text-zinc-400">Idle: {cell.reason}</p>
      break
    case "running_no_detail":
      body = <p className="text-zinc-500 dark:text-zinc-400">Running, detail unavailable: {cell.reason}</p>
      break
    case "unreachable":
      body = <p className="text-amber-700 dark:text-amber-400">Unreachable: {cell.reason}</p>
      break
    case "unknown":
      body = <p className="text-zinc-500 dark:text-zinc-400">Unknown: {cell.reason}</p>
      break
    case "unavailable":
      body = <p className="text-zinc-500 dark:text-zinc-400">Live state unavailable: {cell.reason}</p>
      break
    case "not_checked":
      body = <p className="text-zinc-500 dark:text-zinc-400">Not checked: {cell.reason}</p>
      break
    case "not_applicable":
      break
  }

  const lastError =
    cell.kind === "rebuilding" || cell.kind === "waiting" ? cell.detail.last_error : undefined

  if (!body && !breach) return null
  return (
    <div className="space-y-1 pt-2 text-xs text-zinc-700 dark:text-zinc-300">
      {body}
      {lastError && (
        <p className="text-red-600 dark:text-red-400 break-words">
          Last rebuild failure this run: {lastError}
        </p>
      )}
      {breach && (
        <p className="text-amber-700 dark:text-amber-400">
          {`Past its ${formatSpan(breach.deadline_seconds)} freshness deadline by ${formatSpan(overdueSeconds(breach))} — `}
          {breach.never_succeeded
            ? "it has never rebuilt successfully"
            : `last rebuilt successfully ${formatAbsoluteTime(breach.reference_at)}`}
          {`; ${describeFreshnessCause(breach.cause)}.`}
        </p>
      )}
    </div>
  )
}

/**
 * Everything the page cannot currently vouch for, above the table.
 *
 * Each of these is a way for a row to look calmer than the truth — a missing "Overdue",
 * a cell that is not "Rebuilding" — so each one is said out loud rather than left for the
 * reader to infer from an absence.
 */
export function LiveStateBanner({
  running,
  freshness,
  liveRelevant,
  unlistedBreaches,
}: {
  running: Fetched<RunningModelsResponse>
  freshness: Fetched<ModelFreshnessBreach[]>
  /** Whether any row is after_upstream, i.e. whether live state was asked for at all. */
  liveRelevant: boolean
  /** Open breaches for models that have no row on this page. */
  unlistedBreaches: ModelFreshnessBreach[]
}) {
  const notes: ReactNode[] = []

  if (liveRelevant && running.status === "error") {
    notes.push(
      <>
        <span className="font-semibold">Live state unavailable.</span> {running.message} Rows read
        Unavailable rather than Idle until a check succeeds.
      </>,
    )
  } else if (liveRelevant && running.status === "ok" && !running.data.temporal_available) {
    notes.push(
      <>
        <span className="font-semibold">Live state unavailable.</span> {ORCHESTRATION_UNAVAILABLE_REASON} Schedules
        are unaffected by this page, and no row is shown as Idle.
      </>,
    )
  } else if (liveRelevant && running.status === "ok" && running.data.count >= running.data.limit) {
    notes.push(
      <>
        Live state is checked for the first {running.data.limit} upstream-triggered models by name;
        any others read Not checked.
      </>,
    )
  }

  if (freshness.status === "error") {
    notes.push(
      <>
        <span className="font-semibold">Overdue markers unavailable.</span> {freshness.message} A row
        without one is not known to be on time.
      </>,
    )
  }

  if (unlistedBreaches.length > 0) {
    const names = unlistedBreaches.map((b) => b.name)
    const shown = names.slice(0, 5).join(", ")
    const more = names.length > 5 ? ` and ${names.length - 5} more` : ""
    notes.push(
      <>
        {unlistedBreaches.length === 1
          ? "1 model past its freshness deadline has no row on this page"
          : `${unlistedBreaches.length} models past their freshness deadline have no row on this page`}
        : {shown}
        {more}.
      </>,
    )
  }

  if (notes.length === 0) return null
  return (
    <div
      role="status"
      className="space-y-1 rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-200"
    >
      {notes.map((note, i) => (
        <p key={i} className="flex items-start gap-2">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          <span>{note}</span>
        </p>
      ))}
    </div>
  )
}
