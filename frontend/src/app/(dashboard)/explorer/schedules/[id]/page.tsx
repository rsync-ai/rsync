"use client"

import { Fragment, Suspense, useCallback, useEffect, useState } from "react"
import Link from "next/link"
import { useParams, usePathname, useRouter, useSearchParams } from "next/navigation"
import { toast } from "sonner"
import { authFetch } from "@/lib/api/auth-fetch"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  AlertTriangle,
  ChevronLeft,
  Clock,
  Loader2,
  Pause,
  Play,
  RefreshCw,
  SquarePen,
} from "lucide-react"
import { formatAbsoluteTime, formatNextRun } from "@/components/explorer/scheduleTime"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { useCurrentUser } from "@/contexts/CurrentUserContext"
import { canEditSavedQuery } from "@/lib/workspace/roles"
import { SavedQueryEditDialog } from "@/components/explorer/SavedQueryEditDialog"
import { SavedQueryModelDialog } from "@/components/explorer/SavedQueryModelDialog"
import { describeRunOrigin, describeSkipReason } from "@/components/explorer/runProvenance"
import { RunDurationChart } from "@/components/explorer/RunDurationChart"
import { ModelLineageGraph } from "@/components/explorer/ModelLineageGraph"
import { LineageCountValue, RunAsValue } from "@/components/explorer/ModelDetailRows"
import { ModelFreshnessDeadline } from "@/components/explorer/ModelFreshnessDeadline"
import {
  cadenceCron,
  describeCadence,
  describeUpstreamNextRun,
  formatDuration,
  runsDoSomething,
  runStatusBadge,
  statusBadge,
  type SavedQueryRun,
  type ScheduledQuery,
} from "@/components/explorer/scheduledModel"
import {
  LiveStateCell,
  LiveStateDetail,
  OverdueBadge,
  useOpenFreshnessBreaches,
  useRunningModels,
} from "@/components/explorer/ModelLiveState"
import { AFTER_UPSTREAM, liveCellFor, openBreachFor } from "@/components/explorer/liveState"

// One model, all of it on one page. The list's inline history showed the latest 50
// runs as lines of text with no way to reach the 51st, and every action on a model
// meant opening a dialog from a row. A model that has failed every night for a week
// needs its runs as a table you can filter to failures and page back through, and the
// controls to act on what that shows, next to it.

const PAGE_SIZE = 25

// The tab is in the address, so a link can open a page on its graph: each node of the
// graph links to its model's page that way, and walking a chain stays on the graph.
type PageTab = "runs" | "graph"
function tabFromParam(value: string | null | undefined): PageTab {
  return value === "graph" ? "graph" : "runs"
}

type RunFilter = "" | "succeeded" | "failed" | "skipped"
const RUN_FILTERS: { value: RunFilter; label: string }[] = [
  { value: "", label: "All" },
  { value: "failed", label: "Failed" },
  { value: "skipped", label: "Skipped" },
  { value: "succeeded", label: "Succeeded" },
]

// What both a scheduled model and a query without a schedule can say about themselves.
// The query's own row (GET /explorer/saved/:id) carries these under the same names; it
// has no cadence, status or upstreams, so nothing that reads those can be handed one.
type ModelBasics = Pick<
  ScheduledQuery,
  | "name"
  | "description"
  | "created_by"
  | "created_at"
  | "updated_at"
  | "materialization"
  | "target_table"
  | "last_run_at"
  | "last_run_status"
>

type ScheduleState =
  | { status: "loading" }
  | { status: "ok"; schedule: ScheduledQuery }
  | { status: "unscheduled"; query: ModelBasics }
  | { status: "missing" }
  | { status: "error"; message: string }

interface RunsPage {
  runs: SavedQueryRun[]
  nextCursor?: string
}

function DetailRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[96px_minmax(0,1fr)] gap-2 py-1.5 text-xs">
      <dt className="text-zinc-500 dark:text-zinc-400">{label}</dt>
      <dd className="min-w-0 text-zinc-800 dark:text-zinc-200">{children}</dd>
    </div>
  )
}

/**
 * A schema-qualified name that wraps after a dot before anywhere else. The Details column
 * is narrow, and break-all cut public.swipes_total into "public.swipes_tota" and "l".
 */
function QualifiedName({ name }: { name: string }) {
  return (
    <span className="font-mono break-words">
      {name.split(".").map((part, i) => (
        <Fragment key={i}>
          {i > 0 && (
            <>
              .<wbr />
            </>
          )}
          {part}
        </Fragment>
      ))}
    </span>
  )
}

function describeWhatItDoes(s: ModelBasics) {
  if (s.materialization === "statement") return <span>Runs its SQL as written</span>
  if (s.materialization === "table" && s.target_table) {
    return (
      <span>
        Rebuilds <QualifiedName name={s.target_table} />
      </span>
    )
  }
  return <span className="text-amber-600 dark:text-amber-400">Nothing: no target table is set</span>
}

function TriggerCadence({ s }: { s: ScheduledQuery }) {
  const cron = cadenceCron(s)
  if (!cron) return <span>{describeCadence(s)}</span>
  if (!cron.worded) return <span className="font-mono break-all">{describeCadence(s)}</span>
  // The sentence, and under it the expression it was read from: that is what the Edit
  // schedule form shows, and what anyone comparing against another tool has.
  return (
    <span className="block">
      <span className="block">{describeCadence(s)}</span>
      <span className="block font-mono text-zinc-500 dark:text-zinc-400 break-all">{cron.expression}</span>
    </span>
  )
}

function PausedNotice({ s }: { s: ScheduledQuery }) {
  if (s.status !== "paused") return null
  // An automatic pause is the one a reader most needs explained: nobody chose it, and
  // the schedule stays stopped until somebody resumes it.
  const auto = !!s.auto_paused_reason
  const reason = auto ? s.auto_paused_reason : s.paused_reason
  const at = auto ? s.auto_paused_at : s.paused_at
  return (
    <div
      role="status"
      className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-3 text-sm"
    >
      <Pause className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" aria-hidden />
      <div className="min-w-0 space-y-0.5">
        <p className="font-medium text-amber-800 dark:text-amber-200">
          {auto ? "Paused automatically" : "Paused"}
          {at ? ` on ${formatAbsoluteTime(at)}` : ""}
        </p>
        {reason && <p className="break-words text-amber-700 dark:text-amber-300">{reason}</p>}
        <p className="text-xs text-amber-700/80 dark:text-amber-300/80">
          Nothing runs on this schedule until it is resumed.
        </p>
      </div>
    </div>
  )
}

function NotScheduledNotice() {
  return (
    <div
      role="status"
      className="flex items-start gap-2 rounded-md border border-zinc-300 bg-zinc-50 p-3 text-sm dark:border-zinc-700 dark:bg-zinc-900"
    >
      <Clock className="mt-0.5 h-4 w-4 shrink-0 text-zinc-500 dark:text-zinc-400" aria-hidden />
      <div className="min-w-0 space-y-0.5">
        <p className="font-medium text-zinc-800 dark:text-zinc-200">Not scheduled</p>
        <p className="text-zinc-600 dark:text-zinc-400">
          Nothing runs this query automatically. Its past runs are in the Runs tab.
        </p>
      </div>
    </div>
  )
}

// useSearchParams needs a Suspense boundary above it; without one, Radix Tabs render
// every trigger with tabIndex=-1 (pipelines/[id]/page.tsx says the same of its tabs).
export default function ModelSchedulePage() {
  return (
    <Suspense
      fallback={
        <div className="flex items-center gap-2 py-12 text-sm text-zinc-500 dark:text-zinc-400">
          <Loader2 className="h-4 w-4 animate-spin" />
          Loading model…
        </div>
      }
    >
      <ModelSchedulePageBody />
    </Suspense>
  )
}

function ModelSchedulePageBody() {
  const params = useParams<{ id: string }>()
  const id = typeof params?.id === "string" ? params.id : ""

  const [state, setState] = useState<ScheduleState>({ status: "loading" })
  const [filter, setFilter] = useState<RunFilter>("")
  // A stack of `before` cursors, one per page visited. The API pages only backwards
  // (keyset on finished_at, run_id), so "Newer" is a pop, not a query of its own.
  const [cursors, setCursors] = useState<(string | undefined)[]>([undefined])
  const [runsTick, setRunsTick] = useState(0)
  // The answer to the last request, tagged with what was asked. Loading is then "the
  // answer on hand is for some other question", and the previous page stays on screen,
  // dimmed, instead of the table collapsing to a spinner on every page turn.
  const [runsResult, setRunsResult] = useState<{ key: string; page?: RunsPage; error?: string } | null>(null)
  const [runningNow, setRunningNow] = useState(false)
  const [scheduleBusy, setScheduleBusy] = useState(false)
  const [editQuery, setEditQuery] = useState(false)
  const [editSchedule, setEditSchedule] = useState(false)
  const [graphReloadTick, setGraphReloadTick] = useState(0)
  const [graphLoading, setGraphLoading] = useState(false)

  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()
  const urlTab = tabFromParam(searchParams?.get("tab"))
  // Clicking a tab shows it at once; the address follows. Back and forward change only
  // the address, so a tab the address moved to is taken up here, during render.
  const [tab, setTab] = useState<PageTab>(urlTab)
  const [urlTabSeen, setUrlTabSeen] = useState<PageTab>(urlTab)
  if (urlTab !== urlTabSeen) {
    setUrlTabSeen(urlTab)
    setTab(urlTab)
  }

  const { role: workspaceRole, can } = useWorkspaceRole()
  const { user } = useCurrentUser()
  const canSchedule = can("schedule_query")

  const schedule = state.status === "ok" ? state.schedule : null
  // The graph reads it for every model it draws, whatever this one's own schedule is.
  const { running, refresh: refreshRunning } = useRunningModels(
    schedule?.schedule_type === AFTER_UPSTREAM || tab === "graph",
  )
  const { freshness, refresh: refreshFreshness } = useOpenFreshnessBreaches(!!schedule)

  const loadSchedule = useCallback(async () => {
    if (!id) return
    try {
      const res = await authFetch(
        `/api/v1/explorer/schedules?saved_query_id=${encodeURIComponent(id)}`,
        { cache: "no-store" },
      )
      // 400 is a malformed id in the address bar: nothing by that id can exist.
      if (res.status === 400) {
        setState({ status: "missing" })
        return
      }
      if (!res.ok) {
        setState({ status: "error", message: `Could not load this model (HTTP ${res.status}).` })
        return
      }
      const data = await res.json()
      const found = Array.isArray(data?.schedules) ? (data.schedules[0] as ScheduledQuery | undefined) : undefined
      if (found) {
        setState({ status: "ok", schedule: found })
        return
      }
      // No live schedule. A query whose schedule was deleted keeps every run it made, and
      // those runs are what a link to this page was followed for — so ask for the query
      // itself before calling it missing. Its route answers 404 for a query that does not
      // exist and for another member's private one alike, and so does this page.
      const queryRes = await authFetch(`/api/v1/explorer/saved/${encodeURIComponent(id)}`, { cache: "no-store" })
      if (queryRes.status === 404 || queryRes.status === 403) {
        setState({ status: "missing" })
        return
      }
      if (!queryRes.ok) {
        setState({ status: "error", message: `Could not load this model (HTTP ${queryRes.status}).` })
        return
      }
      setState({ status: "unscheduled", query: (await queryRes.json()) as ModelBasics })
    } catch {
      setState({ status: "error", message: "Could not reach the server to load this model." })
    }
  }, [id])

  useEffect(() => {
    void loadSchedule()
  }, [loadSchedule])

  const before = cursors[cursors.length - 1]
  const hasModel = state.status === "ok" || state.status === "unscheduled"
  const runsKey = `${filter}|${before ?? ""}|${runsTick}`
  const runsLoading = runsResult?.key !== runsKey
  const runsError = runsLoading ? null : (runsResult?.error ?? null)
  const page = runsResult?.page ?? null
  useEffect(() => {
    if (!id || !hasModel) return
    let cancelled = false
    const query = new URLSearchParams({ limit: String(PAGE_SIZE) })
    if (filter) query.set("status", filter)
    if (before) query.set("before", before)
    ;(async () => {
      try {
        const res = await authFetch(`/api/v1/explorer/saved/${encodeURIComponent(id)}/runs?${query}`, {
          cache: "no-store",
        })
        if (cancelled) return
        // authFetch does not throw on a non-2xx, so this check is the only thing
        // between an error page and a confident, empty "no runs yet".
        if (!res.ok) {
          setRunsResult({ key: runsKey, error: `Could not load run history (HTTP ${res.status}).` })
          return
        }
        const data = await res.json()
        if (cancelled) return
        setRunsResult({
          key: runsKey,
          page: {
            runs: Array.isArray(data?.runs) ? data.runs : [],
            nextCursor: typeof data?.next_cursor === "string" ? data.next_cursor : undefined,
          },
        })
      } catch {
        if (!cancelled) setRunsResult({ key: runsKey, error: "Could not reach the server to load run history." })
      }
    })()
    return () => {
      cancelled = true
    }
  }, [id, hasModel, filter, before, runsKey])

  const showLatestRuns = () => {
    setCursors([undefined])
    setRunsTick((t) => t + 1)
  }

  const refreshAll = () => {
    void loadSchedule()
    showLatestRuns()
    setGraphReloadTick((t) => t + 1)
    // A no-op while nothing on the page polls it.
    void refreshRunning()
    if (schedule) void refreshFreshness()
  }

  const chooseTab = (value: string) => {
    const next = tabFromParam(value)
    setTab(next)
    const query = new URLSearchParams(searchParams?.toString() ?? "")
    if (next === "runs") query.delete("tab")
    else query.set("tab", next)
    const qs = query.toString()
    router.replace(qs ? `${pathname}?${qs}` : pathname, { scroll: false })
  }

  const chooseFilter = (next: RunFilter) => {
    setFilter(next)
    setCursors([undefined])
  }

  const handleRunNow = async () => {
    if (!schedule) return
    setRunningNow(true)
    try {
      const res = await authFetch(`/api/v1/explorer/saved/${encodeURIComponent(id)}/run`, { method: "POST" })
      const data = await res.json().catch(() => ({}))
      if (res.ok) {
        const rows = typeof data?.rows_affected === "number" ? (data.rows_affected as number) : null
        toast.success(
          schedule.materialization === "statement"
            ? rows === null
              ? "Statement ran"
              : `Statement ran — ${rows} row${rows === 1 ? "" : "s"} affected`
            : `Rebuilt ${data?.target_table || schedule.target_table || "the target table"}`,
        )
      } else {
        toast.error(data?.error || "The run did not complete")
      }
    } catch {
      toast.error("Could not run the model")
    } finally {
      setRunningNow(false)
      // Either outcome wrote a run row; the newest page is where it is.
      void loadSchedule()
      showLatestRuns()
    }
  }

  const setPaused = async (pause: boolean) => {
    if (scheduleBusy) return
    setScheduleBusy(true)
    try {
      const res = await authFetch(
        `/api/v1/explorer/saved/${encodeURIComponent(id)}/schedule/${pause ? "pause" : "resume"}`,
        {
          method: "POST",
          ...(pause ? { body: JSON.stringify({ reason: "Paused by user" }) } : {}),
        },
      )
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(data?.error || "The schedule change did not apply")
        return
      }
      toast.success(pause ? "Schedule paused" : "Schedule resumed")
      await loadSchedule()
    } catch {
      toast.error("The schedule change did not apply")
    } finally {
      setScheduleBusy(false)
    }
  }

  const backLink = (
    <Link
      href="/explorer/schedules"
      className="inline-flex items-center gap-1 text-xs text-zinc-500 dark:text-zinc-400 hover:text-zinc-800 dark:hover:text-zinc-200"
    >
      <ChevronLeft className="h-3.5 w-3.5" aria-hidden />
      Scheduled Queries
    </Link>
  )

  if (state.status !== "ok" && state.status !== "unscheduled") {
    return (
      <div className="space-y-4">
        {backLink}
        <Card className="flex flex-col items-center gap-2 px-4 py-12 text-center">
          {state.status === "loading" ? (
            <div className="flex items-center gap-2 text-sm text-zinc-500 dark:text-zinc-400">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading model…
            </div>
          ) : state.status === "missing" ? (
            <>
              <Clock className="h-5 w-5 text-zinc-400" />
              <p className="text-sm text-zinc-600 dark:text-zinc-400">This query could not be found.</p>
              <p className="max-w-md text-xs text-zinc-500 dark:text-zinc-400">
                It may have been deleted, or it is a private query of another member.
              </p>
            </>
          ) : (
            <>
              <AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400" />
              <p className="text-sm text-red-600 dark:text-red-400">{state.message}</p>
              <Button size="sm" variant="outline" onClick={() => void loadSchedule()}>
                Retry
              </Button>
            </>
          )}
        </Card>
      </div>
    )
  }

  // A query without a schedule still has a page: its runs are real history, and nothing
  // else in the product lists them. Everything that acts on a schedule is left off it.
  const s = state.status === "ok" ? state.schedule : null
  const m: ModelBasics = state.status === "ok" ? state.schedule : state.query
  const cell = s ? liveCellFor(s.schedule_type, s.saved_query_id, running) : null
  const breach = s ? openBreachFor(s.saved_query_id, freshness.status === "ok" ? freshness.data : []) : undefined
  const canEditQuery = canEditSavedQuery(workspaceRole, m.created_by, user?.id)
  const runDisabledReason = !s
    ? null
    : !canSchedule
      ? `Running a model needs the admin role or higher. Your role is ${workspaceRole || "unknown"}.`
      : s.supports_materialization === false
        ? `${s.connector_type || "This"} connections cannot back a materialized model yet`
        : !runsDoSomething(s)
          ? "Choose what a run does, and save it, first"
          : null
  const pageIndex = cursors.length - 1
  const runs = page?.runs ?? []

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        {backLink}
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0 space-y-1">
            <div className="flex flex-wrap items-center gap-2">
              <h1 className="break-words text-2xl font-bold tracking-tight text-zinc-900 dark:text-white">
                {m.name}
              </h1>
              {s ? (
                statusBadge(s)
              ) : (
                <Badge variant="outline" className="text-zinc-500 dark:text-zinc-400">
                  not scheduled
                </Badge>
              )}
            </div>
            {m.description && <p className="text-sm text-zinc-500 dark:text-zinc-400">{m.description}</p>}
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {s && (
              <>
                <Button
                  size="sm"
                  onClick={() => void handleRunNow()}
                  disabled={!!runDisabledReason || runningNow}
                  title={
                    runDisabledReason ||
                    (s.materialization === "statement" ? "Run the SQL now, as written" : "Rebuild the table now")
                  }
                >
                  {runningNow ? (
                    <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Play className="mr-1 h-3.5 w-3.5" />
                  )}
                  Run now
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!canSchedule || scheduleBusy || (s.status !== "active" && s.status !== "paused")}
                  title={
                    canSchedule
                      ? undefined
                      : `Pausing a schedule needs the admin role or higher. Your role is ${workspaceRole || "unknown"}.`
                  }
                  onClick={() => void setPaused(s.status === "active")}
                >
                  {s.status === "active" ? (
                    <>
                      <Pause className="mr-1 h-3.5 w-3.5" />
                      Pause
                    </>
                  ) : (
                    <>
                      <Play className="mr-1 h-3.5 w-3.5" />
                      Resume
                    </>
                  )}
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!canSchedule}
                  title={canSchedule ? "Change the cadence, what a run does, or delete the schedule" : undefined}
                  onClick={() => setEditSchedule(true)}
                >
                  <Clock className="mr-1 h-3.5 w-3.5" />
                  Edit schedule
                </Button>
              </>
            )}
            <Button
              size="sm"
              variant="outline"
              disabled={!canEditQuery}
              title={
                canEditQuery
                  ? "Edit this query's name, SQL or sharing"
                  : "Only the query's creator or a workspace admin can change it"
              }
              onClick={() => setEditQuery(true)}
            >
              <SquarePen className="mr-1 h-3.5 w-3.5" />
              Edit query
            </Button>
            <Button size="sm" variant="ghost" onClick={refreshAll} aria-label="Refresh">
              <RefreshCw className={`h-4 w-4 ${runsLoading || graphLoading ? "animate-spin" : ""}`} />
            </Button>
          </div>
        </div>
      </div>

      {s && cell ? (
        // Both render nothing for an active model with nothing to report; the wrapper
        // then collapses rather than adding a gap.
        <div className="space-y-4 empty:hidden">
          <PausedNotice s={s} />
          <LiveStateDetail cell={cell} breach={breach} upstreams={s.upstreams} />
        </div>
      ) : (
        <NotScheduledNotice />
      )}

      {/* The notices and the tab list sit above the grid, so the Runs card and the
          Details card start on the same line; the grid stretches both to the taller one. */}
      <Tabs value={tab} onValueChange={chooseTab} className="space-y-2">
        <TabsList>
          <TabsTrigger value="runs">Runs</TabsTrigger>
          <TabsTrigger value="graph">Graph</TabsTrigger>
        </TabsList>
        <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_320px]">
          <div className="min-w-0">
            <TabsContent value="runs" className="mt-0 h-full">
              <Card className="flex h-full flex-col overflow-hidden p-0" data-testid="model-runs-card">
                <div className="flex flex-wrap items-center justify-between gap-2 border-b px-4 py-3 dark:border-zinc-800">
                  <h2 className="text-sm font-semibold text-zinc-900 dark:text-white">Runs</h2>
                  <div role="group" aria-label="Filter runs by status" className="flex flex-wrap gap-1">
                    {RUN_FILTERS.map((f) => (
                      <Button
                        key={f.value || "all"}
                        size="sm"
                        variant={filter === f.value ? "secondary" : "ghost"}
                        className="h-7 px-2 text-xs"
                        aria-pressed={filter === f.value}
                        onClick={() => chooseFilter(f.value)}
                      >
                        {f.label}
                      </Button>
                    ))}
                  </div>
                </div>

                {runsError ? (
                  <div className="flex flex-col items-center gap-2 px-4 py-10 text-center">
                    <p className="text-sm text-red-600 dark:text-red-400">{runsError}</p>
                    <Button size="sm" variant="outline" onClick={() => setRunsTick((t) => t + 1)}>
                      Retry
                    </Button>
                  </div>
                ) : !page && runsLoading ? (
                  <div className="flex items-center justify-center gap-2 py-10 text-sm text-zinc-500 dark:text-zinc-400">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Loading runs…
                  </div>
                ) : runs.length === 0 ? (
                  <p className="px-4 py-10 text-center text-sm text-zinc-500 dark:text-zinc-400">
                    {filter ? `No ${filter} runs.` : "No runs recorded yet."}
                  </p>
                ) : (
                  <>
                    <RunDurationChart runs={runs} className={runsLoading ? "opacity-60" : ""} />
                    <div className={`overflow-x-auto ${runsLoading ? "opacity-60" : ""}`} aria-busy={runsLoading}>
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead className="whitespace-nowrap">Started</TableHead>
                            <TableHead>Duration</TableHead>
                            <TableHead>Trigger</TableHead>
                            <TableHead>Status</TableHead>
                            <TableHead>Result</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {runs.map((r) => {
                            const skip = describeSkipReason(r)
                            return (
                              <TableRow key={r.run_id}>
                                <TableCell className="whitespace-nowrap align-top text-xs text-zinc-600 dark:text-zinc-400">
                                  {formatAbsoluteTime(r.started_at)}
                                </TableCell>
                                <TableCell className="whitespace-nowrap align-top text-xs tabular-nums">
                                  {skip ? <span className="text-zinc-400">—</span> : formatDuration(r.duration_ms)}
                                </TableCell>
                                <TableCell className="min-w-[140px] align-top text-xs text-zinc-600 dark:text-zinc-400">
                                  {describeRunOrigin(r)}
                                </TableCell>
                                <TableCell className="align-top">{runStatusBadge(r.status)}</TableCell>
                                <TableCell className="min-w-[200px] max-w-[420px] align-top text-xs">
                                  <div className="space-y-0.5">
                                    {skip && <div className="text-amber-600 dark:text-amber-400">{skip}</div>}
                                    {/* A skip row's message explains a decision, not a failure: red
                                        would read as something broken. */}
                                    {r.error && (
                                      <div
                                        className={`line-clamp-3 break-words ${
                                          skip ? "text-zinc-500" : "text-red-600 dark:text-red-400"
                                        }`}
                                        title={r.error}
                                      >
                                        {r.error}
                                      </div>
                                    )}
                                    {r.auto_pause_reason && (
                                      <div className="text-amber-600 dark:text-amber-400">
                                        auto-paused: {r.auto_pause_reason}
                                      </div>
                                    )}
                                    {!skip && !r.error && typeof r.rows_affected === "number" && (
                                      <div className="text-zinc-500 dark:text-zinc-400">
                                        {r.rows_affected} row{r.rows_affected === 1 ? "" : "s"}
                                      </div>
                                    )}
                                    {/* A rebuild's statements are DDL, which report no row count
                                        (reportsRowsAffected), and only a table model has a target. */}
                                    {r.status === "succeeded" &&
                                      !skip &&
                                      !r.error &&
                                      typeof r.rows_affected !== "number" &&
                                      r.target_table && (
                                        <div className="text-zinc-500 dark:text-zinc-400" title={`Rebuilt ${r.target_table}`}>
                                          Table rebuilt
                                        </div>
                                      )}
                                  </div>
                                </TableCell>
                              </TableRow>
                            )
                          })}
                        </TableBody>
                      </Table>
                    </div>
                  </>
                )}

                {(pageIndex > 0 || page?.nextCursor) && !runsError && (
                  <div className="mt-auto flex items-center justify-between gap-2 border-t px-4 py-2 dark:border-zinc-800">
                    <span className="text-xs text-zinc-500 dark:text-zinc-400">Page {pageIndex + 1}</span>
                    <div className="flex gap-1">
                      <Button
                        size="sm"
                        variant="outline"
                        className="h-7 text-xs"
                        disabled={pageIndex === 0 || runsLoading}
                        onClick={() => setCursors((c) => c.slice(0, -1))}
                      >
                        Newer
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        className="h-7 text-xs"
                        disabled={!page?.nextCursor || runsLoading}
                        onClick={() => {
                          const next = page?.nextCursor
                          if (next) setCursors((c) => [...c, next])
                        }}
                      >
                        Older
                      </Button>
                    </div>
                  </div>
                )}
              </Card>
            </TabsContent>
            <TabsContent value="graph" className="mt-0">
              <ModelLineageGraph
                modelId={id}
                modelName={m.name}
                rootSchedule={s}
                canSchedule={canSchedule}
                running={running}
                reloadTick={graphReloadTick}
                onLoadingChange={setGraphLoading}
              />
            </TabsContent>
          </div>

          <Card className="p-4" data-testid="model-details-card">
            {/* Sticky, so a long run list does not scroll the details away. */}
            <div className="lg:sticky lg:top-4">
              <h2 className="mb-2 text-sm font-semibold text-zinc-900 dark:text-white">Details</h2>
              <dl className="divide-y divide-zinc-100 dark:divide-zinc-800">
                {s && cell ? (
                  <>
                    <DetailRow label="Now">
                      <div className="flex flex-wrap items-center gap-1.5">
                        <LiveStateCell cell={cell} />
                        {breach && <OverdueBadge breach={breach} />}
                      </div>
                    </DetailRow>
                    <DetailRow label="Trigger">
                      <TriggerCadence s={s} />
                    </DetailRow>
                    {s.schedule_type === AFTER_UPSTREAM && (s.upstreams?.length ?? 0) > 0 && (
                      <DetailRow label="Upstreams">
                        <ul className="space-y-0.5">
                          {s.upstreams!.map((u) => (
                            <li key={`${u.kind}:${u.id}`} className="break-words">
                              <span className="text-zinc-400">{u.kind} </span>
                              {!u.name ? (
                                <span className="font-mono text-zinc-500 dark:text-zinc-400" title="Deleted, or not visible to you">
                                  {u.id}
                                </span>
                              ) : u.kind === "model" ? (
                                <Link href={`/explorer/schedules/${u.id}`} className="hover:underline">
                                  {u.name}
                                </Link>
                              ) : u.kind === "pipeline" ? (
                                <Link href={`/pipelines/${u.id}`} className="hover:underline">
                                  {u.name}
                                </Link>
                              ) : (
                                u.name
                              )}
                            </li>
                          ))}
                        </ul>
                      </DetailRow>
                    )}
                    <DetailRow label="Next run">
                      {s.next_run_at ? (
                        <span title={formatAbsoluteTime(s.next_run_at)}>
                          {formatNextRun(s.next_run_at)} · {formatAbsoluteTime(s.next_run_at)}
                        </span>
                      ) : s.schedule_type === AFTER_UPSTREAM && s.status === "active" ? (
                        <span className="text-zinc-500 dark:text-zinc-400">{describeUpstreamNextRun(s)}</span>
                      ) : (
                        <span className="text-zinc-400">—</span>
                      )}
                    </DetailRow>
                    <DetailRow label="Run as">
                      <RunAsValue savedQueryId={id} scheduleId={s.schedule_id} />
                    </DetailRow>
                  </>
                ) : (
                  <DetailRow label="Trigger">
                    <span className="text-zinc-400">Not scheduled</span>
                  </DetailRow>
                )}
                {/* A model with no schedule of its own can still wake the models downstream of it. */}
                <DetailRow label="Lineage">
                  <LineageCountValue
                    modelId={id}
                    rootSchedule={s}
                    reloadTick={graphReloadTick}
                    onOpenGraph={() => chooseTab("graph")}
                  />
                </DetailRow>
                <DetailRow label="Last run">
                  {m.last_run_at ? (
                    <span className="flex flex-wrap items-center gap-1.5">
                      {formatAbsoluteTime(m.last_run_at)}
                      {m.last_run_status && runStatusBadge(m.last_run_status)}
                    </span>
                  ) : (
                    <span className="text-zinc-400">never run</span>
                  )}
                </DetailRow>
                <DetailRow label="Freshness">
                  <ModelFreshnessDeadline
                    savedQueryId={id}
                    materialization={m.materialization}
                    canEdit={canSchedule}
                    disabledReason={`Setting a freshness deadline needs the admin role or higher. Your role is ${workspaceRole || "unknown"}.`}
                    onSaved={() => {
                      // The PUT bumps updated_at; the sweep opens or closes a breach within a minute.
                      void loadSchedule()
                      if (schedule) void refreshFreshness()
                    }}
                  />
                </DetailRow>
                <DetailRow label="Does">{describeWhatItDoes(m)}</DetailRow>
                {s && (
                  <DetailRow label="Connection">
                    {s.connection_name || <span className="font-mono">{s.connection_id}</span>}
                    {s.connector_type && <span className="text-zinc-400"> · {s.connector_type}</span>}
                  </DetailRow>
                )}
                <DetailRow label="Created">
                  {formatAbsoluteTime(m.created_at)}
                  {user?.id && m.created_by === user.id && <span className="text-zinc-400"> · by you</span>}
                </DetailRow>
                <DetailRow label="Updated">{formatAbsoluteTime(m.updated_at)}</DetailRow>
              </dl>
            </div>
          </Card>
        </div>
      </Tabs>

      {editQuery && (
        <SavedQueryEditDialog
          key={`edit-query-${id}`}
          savedQueryId={id}
          open
          onOpenChange={(next) => {
            if (!next) setEditQuery(false)
          }}
          onSaved={() => void loadSchedule()}
        />
      )}

      {editSchedule && s && (
        <SavedQueryModelDialog
          key={`edit-schedule-${s.saved_query_id}`}
          savedQueryId={s.saved_query_id}
          savedQueryName={s.name}
          materialization={s.materialization}
          targetTable={s.target_table}
          statementClass={s.statement_class}
          supportsMaterialization={s.supports_materialization}
          connectorType={s.connector_type}
          lastRunStatus={s.last_run_status}
          lastRunError={s.last_run_error}
          open
          onOpenChange={(next) => {
            if (!next) setEditSchedule(false)
          }}
          onChanged={() => {
            void loadSchedule()
            showLatestRuns()
          }}
        />
      )}
    </div>
  )
}
