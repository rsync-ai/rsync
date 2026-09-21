"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { PageHeader } from "@/components/layout/PageHeader"
import { authFetch } from "@/lib/api/auth-fetch"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
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
  Clock,
  Loader2,
  RefreshCw,
  Search,
  SquarePen,
} from "lucide-react"
import {
  formatAbsoluteTime,
  formatNextRunOrDue,
  nextRunRefetchDelay,
} from "@/components/explorer/scheduleTime"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { useCurrentUser } from "@/contexts/CurrentUserContext"
import { canEditSavedQuery } from "@/lib/workspace/roles"
import { SavedQueryEditDialog } from "@/components/explorer/SavedQueryEditDialog"
import { SavedQueryModelDialog } from "@/components/explorer/SavedQueryModelDialog"
import {
  cadenceCron,
  describeCadence,
  describeUpstreamNextRun,
  runStatusBadge,
  statusBadge,
  type ScheduledQuery,
} from "@/components/explorer/scheduledModel"
import {
  LiveStateBanner,
  LiveStateCell,
  OverdueBadge,
  useOpenFreshnessBreaches,
  useRunningModels,
} from "@/components/explorer/ModelLiveState"
import { AFTER_UPSTREAM, liveCellFor, openBreachFor } from "@/components/explorer/liveState"

// Every other schedule route is keyed by a saved-query id, so before this page a
// schedule could only be found by opening the query that owns it — which meant
// "what is running on a schedule in this workspace?" had no answer in the product.
// That is the question this page exists to answer. A row opens the query's own
// page (/explorer/schedules/[id]) for everything else: every run, paged and
// filtered, what the refresh loop is doing now, and the controls. This list keeps
// only the last run's outcome, so a failure at 03:00 is still visible from here.

export default function ScheduledQueriesPage() {
  const [items, setItems] = useState<ScheduledQuery[]>([])
  const [loading, setLoading] = useState(true)
  // Distinct from `items.length === 0`, which asserts the workspace has no
  // schedules. This says the list could not be loaded, so their number is unknown —
  // and "you have nothing scheduled" is the worst possible thing to say wrongly here.
  const [error, setError] = useState<string | null>(null)
  const router = useRouter()
  // Before these, this page could only be read. A schedule found here had to be
  // matched back to a saved query in the Explorer panel to change anything about it —
  // including the cadence the row is showing.
  const [editQuery, setEditQuery] = useState<ScheduledQuery | null>(null)
  const [editSchedule, setEditSchedule] = useState<ScheduledQuery | null>(null)
  // This page is viewer-readable, so the Edit column has to gate per role. The
  // api-gateway 403s either mutation regardless; this keeps the UI from offering a
  // button that can only fail.
  const { role: workspaceRole, can } = useWorkspaceRole()
  const { user } = useCurrentUser()
  const canSchedule = can("schedule_query")

  // What each model is doing right now, and which tables are past their freshness
  // deadline: one page-level poll each, joined onto rows by saved_query_id, so no row
  // ever asks anything on its own. Live state is asked only when some row is
  // after_upstream — the route lists nothing else — so a workspace of clock schedules
  // never puts a query on the rebuild queue by being looked at.
  const hasUpstreamTriggered = items.some((s) => s.schedule_type === AFTER_UPSTREAM)
  const { running, refresh: refreshRunning } = useRunningModels(!error && hasUpstreamTriggered)
  const { freshness, refresh: refreshFreshness } = useOpenFreshnessBreaches(!error && items.length > 0)
  const openBreaches = freshness.status === "ok" ? freshness.data : []
  const unlistedBreaches = openBreaches.filter(
    (b) => !items.some((s) => s.saved_query_id === b.saved_query_id),
  )

  // Counts user-visible loads. A background refetch applies its rows only if no such
  // load started while it was in flight, so a timed refetch landing late cannot put
  // older rows back over the ones a Refresh just fetched.
  const foregroundLoads = useRef(0)
  // Bumped when a background refetch settles, success or not, so the timer below
  // re-arms even when a failed refetch left `items` untouched.
  const [backgroundRefetches, setBackgroundRefetches] = useState(0)

  // `background` is the timed refetch below: no spinner, and a failure keeps the rows
  // already on screen rather than replacing a working list with an error nobody asked
  // for. Every other caller passes nothing and behaves exactly as before.
  const load = useCallback(async (opts?: { background?: boolean }) => {
    const background = opts?.background === true
    const seq = background ? foregroundLoads.current : ++foregroundLoads.current
    if (!background) {
      setLoading(true)
      setError(null)
    }
    try {
      const res = await authFetch("/api/v1/explorer/schedules", { cache: "no-store" })
      if (!res.ok) {
        if (!background) setError(`Could not load scheduled queries (HTTP ${res.status}).`)
        return
      }
      const data = await res.json()
      if (background && seq !== foregroundLoads.current) return
      setItems(Array.isArray(data?.schedules) ? data.schedules : [])
      if (background) setError(null)
    } catch {
      if (!background) setError("Could not reach the server to load scheduled queries.")
    } finally {
      if (background) setBackgroundRefetches((n) => n + 1)
      else setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  // next_run_at is an instant computed server-side, so once it passes the row has
  // nothing true to show until the server is asked for the following one. Re-ask a
  // few seconds after the earliest comes due; the new rows re-arm this for the next.
  useEffect(() => {
    const delay = nextRunRefetchDelay(items.map((s) => s.next_run_at))
    if (delay === null) return
    const timer = setTimeout(() => void load({ background: true }), delay)
    return () => clearTimeout(timer)
  }, [items, load, backgroundRefetches])

  const refreshAll = useCallback(() => {
    void load()
    void refreshRunning()
    void refreshFreshness()
  }, [load, refreshRunning, refreshFreshness])

  return (
    <div className="space-y-6">
      <PageHeader
        heading="Scheduled Queries"
        description="Every saved query in this workspace that runs on a schedule, and how each one has been getting on."
      >
        <Button variant="outline" size="sm" onClick={refreshAll} disabled={loading}>
          <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </PageHeader>

      {!loading && !error && items.length > 0 && (
        <LiveStateBanner
          running={running}
          freshness={freshness}
          liveRelevant={hasUpstreamTriggered}
          unlistedBreaches={unlistedBreaches}
        />
      )}

      <Card className="p-0 overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center gap-2 py-12 text-sm text-zinc-500 dark:text-zinc-400">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading scheduled queries…
          </div>
        ) : error ? (
          <div className="flex flex-col items-center gap-2 py-12 text-center">
            <AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400" />
            <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              Any schedules you have are unaffected and may still be running.
            </p>
            <Button size="sm" variant="outline" onClick={() => void load()}>
              Retry
            </Button>
          </div>
        ) : items.length === 0 ? (
          <div className="flex flex-col items-center gap-2 py-12 text-center">
            <Clock className="h-5 w-5 text-zinc-400" />
            <p className="text-sm text-zinc-600 dark:text-zinc-400">
              Nothing is scheduled in this workspace yet.
            </p>
            <p className="text-xs text-zinc-500 dark:text-zinc-400 max-w-md">
              Save a query in the Explorer, then use <span className="font-medium">Schedule</span>{" "}
              on it — either to build a table from its results, or to run a MERGE, UPDATE
              or INSERT as written, on a cadence.
            </p>
            <Button size="sm" variant="outline" asChild>
              <Link href="/explorer">
                <Search className="h-4 w-4 mr-2" />
                Open Explorer
              </Link>
            </Button>
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Query</TableHead>
                <TableHead>Cadence</TableHead>
                <TableHead>Now</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Next run</TableHead>
                <TableHead>Last run</TableHead>
                <TableHead className="text-right">Edit</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((s) => {
                const href = `/explorer/schedules/${s.saved_query_id}`
                const cell = liveCellFor(s.schedule_type, s.saved_query_id, running)
                const breach = openBreachFor(s.saved_query_id, openBreaches)
                const cron = cadenceCron(s)
                return (
                  <TableRow
                    key={s.schedule_id}
                    className="cursor-pointer"
                    // The name below is the link keyboards and new tabs use; the rest of
                    // the row is a larger target for the same page.
                    onClick={() => router.push(href)}
                  >
                    <TableCell className="align-top">
                      <Link
                        href={href}
                        className="font-medium text-sm hover:underline"
                        title={`Open ${s.name}: full run history, details and controls`}
                        // The row navigates too. Without this a click here would route
                        // twice, and a Cmd-click would open the tab and also leave this one.
                        onClick={(e) => e.stopPropagation()}
                      >
                        {s.name}
                      </Link>
                      <div className="text-xs text-zinc-500 dark:text-zinc-400 font-mono truncate max-w-[220px]">
                        {s.materialization === "statement"
                          ? "runs its SQL as written"
                          : s.target_table || "no target table"}
                      </div>
                      {s.connection_name && (
                        <div className="text-xs text-zinc-400">{s.connection_name}</div>
                      )}
                    </TableCell>
                    {/* A cron put into words keeps its expression on hover; one that could
                        not be is shown as typed, in monospace like any other code. */}
                    <TableCell
                      className={`align-top text-xs ${cron && !cron.worded ? "font-mono" : ""}`}
                      title={cron?.worded ? cron.expression : undefined}
                    >
                      {describeCadence(s)}
                    </TableCell>
                    <TableCell className="align-top text-xs">
                      <div className="flex flex-wrap items-center gap-1.5">
                        <LiveStateCell cell={cell} />
                        {breach && <OverdueBadge breach={breach} />}
                      </div>
                    </TableCell>
                    <TableCell className="align-top">{statusBadge(s)}</TableCell>
                    <TableCell className="align-top text-xs">
                      {s.next_run_at ? (
                        <span title={formatAbsoluteTime(s.next_run_at)}>
                          {formatNextRunOrDue(s.next_run_at, formatAbsoluteTime)}
                        </span>
                      ) : s.schedule_type === "after_upstream" && s.status === "active" ? (
                        // Distinct from the bare dash below. This one has no time by
                        // construction, and saying so stops it reading as a fault. Active
                        // only: the fire path skips any other status, so on a paused
                        // schedule this would promise a run that never comes.
                        <span className="text-zinc-400">{describeUpstreamNextRun(s)}</span>
                      ) : (
                        // No next run. Either the schedule is not active (the gateway
                        // sends next_run_at for active ones only, so a paused clock
                        // schedule lands here too) or its time could not be worked out.
                        <span className="text-zinc-400">—</span>
                      )}
                    </TableCell>
                    <TableCell className="align-top text-xs">
                      {s.last_run_at ? (
                        // The same time-and-badge pair as the Details card on the
                        // query's page. A failure's message is on hover; the page has
                        // it in full.
                        <div
                          className="flex flex-wrap items-center gap-1.5"
                          title={s.last_run_status === "failed" ? s.last_run_error || "The last run failed" : undefined}
                        >
                          <span className="text-zinc-500 dark:text-zinc-400">{formatAbsoluteTime(s.last_run_at)}</span>
                          {s.last_run_status && runStatusBadge(s.last_run_status)}
                        </div>
                      ) : (
                        <span className="text-zinc-400">never run</span>
                      )}
                    </TableCell>
                    <TableCell className="align-top">
                      {/* Two different things a row here can be wrong about, so two
                          buttons rather than one ambiguous "Edit": the SQL that runs,
                          and when it runs. Both stop the click from also opening the
                          query's page underneath the dialog. */}
                      <div className="flex items-center justify-end gap-1">
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-7 px-2 text-xs"
                          aria-label={`Edit the SQL of ${s.name}`}
                          disabled={!canEditSavedQuery(workspaceRole, s.created_by, user?.id)}
                          title={
                            canEditSavedQuery(workspaceRole, s.created_by, user?.id)
                              ? "Edit this query's name, SQL or sharing"
                              : `Only the query's creator or a workspace admin can change it. Your role is ${workspaceRole || "unknown"}.`
                          }
                          onClick={(e) => {
                            e.stopPropagation()
                            setEditQuery(s)
                          }}
                        >
                          <SquarePen className="h-3.5 w-3.5 mr-1" />
                          Query
                        </Button>
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-7 px-2 text-xs"
                          aria-label={`Edit the schedule of ${s.name}`}
                          disabled={!canSchedule}
                          title={
                            canSchedule
                              ? "Change the cadence, pause, resume or delete this schedule"
                              : `Changing a schedule needs the admin role or higher. Your role is ${workspaceRole || "unknown"}.`
                          }
                          onClick={(e) => {
                            e.stopPropagation()
                            setEditSchedule(s)
                          }}
                        >
                          <Clock className="h-3.5 w-3.5 mr-1" />
                          Schedule
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        )}
      </Card>

      {editQuery && (
        <SavedQueryEditDialog
          key={`edit-query-${editQuery.saved_query_id}`}
          savedQueryId={editQuery.saved_query_id}
          open
          onOpenChange={(next) => {
            if (!next) setEditQuery(null)
          }}
          // The class and the last-run columns are both derived from the SQL, so an
          // edit can change what this table is showing.
          onSaved={() => void load()}
        />
      )}

      {editSchedule && (
        <SavedQueryModelDialog
          key={`edit-schedule-${editSchedule.saved_query_id}`}
          savedQueryId={editSchedule.saved_query_id}
          savedQueryName={editSchedule.name}
          materialization={editSchedule.materialization}
          targetTable={editSchedule.target_table}
          statementClass={editSchedule.statement_class}
          supportsMaterialization={editSchedule.supports_materialization}
          connectorType={editSchedule.connector_type}
          lastRunStatus={editSchedule.last_run_status}
          lastRunError={editSchedule.last_run_error}
          open
          onOpenChange={(next) => {
            if (!next) setEditSchedule(null)
          }}
          onChanged={() => void load()}
        />
      )}
    </div>
  )
}
