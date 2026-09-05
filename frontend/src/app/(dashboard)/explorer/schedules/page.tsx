"use client"

import { Fragment, useCallback, useEffect, useState } from "react"
import Link from "next/link"
import { PageHeader } from "@/components/layout/PageHeader"
import { authFetch } from "@/lib/api/auth-fetch"
import { Badge } from "@/components/ui/badge"
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
  ChevronDown,
  ChevronRight,
  Clock,
  Loader2,
  RefreshCw,
  Search,
  SquarePen,
} from "lucide-react"
import { formatAbsoluteTime, formatNextRun } from "@/components/explorer/scheduleTime"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { useCurrentUser } from "@/contexts/CurrentUserContext"
import { canEditSavedQuery } from "@/lib/workspace/roles"
import { SavedQueryEditDialog } from "@/components/explorer/SavedQueryEditDialog"
import { SavedQueryModelDialog } from "@/components/explorer/SavedQueryModelDialog"

// Every other schedule route is keyed by a saved-query id, so before this page a
// schedule could only be found by opening the query that owns it — which meant
// "what is running on a schedule in this workspace?" had no answer in the product.
// That is the question this page exists to answer, and the run history below each
// row is the other half of it: a scheduled rebuild that fails at 03:00 leaves no
// trace anyone sees unless the failures are somewhere they can be looked at.

interface ScheduleSpec {
  cron?: string
  every_seconds?: number
  timezone?: string
}

interface ScheduledQuery {
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
  /** Set only for schedule_type "after_pipeline". */
  trigger_pipeline_id?: string
  /** Joined server-side; empty if the upstream pipeline has since been deleted. */
  trigger_pipeline_name?: string
  created_by: string
  created_at: string
  updated_at: string
}

interface SavedQueryRun {
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

function describeCadence(s: ScheduledQuery): string {
  if (s.schedule_type === "after_pipeline") {
    // The id is a poor label, but a truthful one: it is what the trigger still points
    // at when the pipeline behind it has been deleted and the join found no name.
    return `After ${s.trigger_pipeline_name || s.trigger_pipeline_id || "a pipeline"} runs`
  }
  if (s.schedule_type === "interval") {
    const seconds = s.schedule_spec.every_seconds || 0
    if (seconds % 3600 === 0) return `Every ${seconds / 3600} hour${seconds === 3600 ? "" : "s"}`
    if (seconds % 60 === 0) return `Every ${seconds / 60} minute${seconds === 60 ? "" : "s"}`
    return `Every ${seconds} seconds`
  }
  return `${s.schedule_spec.cron} (${s.schedule_spec.timezone || "UTC"})`
}

/**
 * Whether a fire of this schedule would actually do anything. A statement model has
 * no target table by design — it writes wherever its own SQL says — so the absence of
 * one is only a fault in table mode.
 */
function runsDoSomething(s: ScheduledQuery): boolean {
  if (s.materialization === "statement") return true
  return s.materialization === "table" && !!s.target_table
}

function statusBadge(s: ScheduledQuery) {
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
    <Badge variant="outline" className="text-zinc-500">
      {s.status}
    </Badge>
  )
}

function runStatusBadge(status: string) {
  if (status === "succeeded") {
    return <Badge variant="outline" className="border-emerald-500 text-emerald-600">succeeded</Badge>
  }
  if (status === "failed") {
    return <Badge variant="outline" className="border-red-500 text-red-600">failed</Badge>
  }
  return <Badge variant="outline" className="text-zinc-500">{status}</Badge>
}

/** Run history for one query, fetched only when its row is expanded. */
function RunHistory({ savedQueryId }: { savedQueryId: string }) {
  const [runs, setRuns] = useState<SavedQueryRun[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    const load = async () => {
      setLoading(true)
      setError(null)
      try {
        const res = await authFetch(`/api/v1/explorer/saved/${savedQueryId}/runs`, {
          cache: "no-store",
        })
        if (cancelled) return
        // authFetch does not throw on a non-2xx, so this check is the only thing
        // between an error page and a confident, empty "no runs yet".
        if (!res.ok) {
          setError(`Could not load run history (HTTP ${res.status}).`)
          return
        }
        const data = await res.json()
        if (!cancelled) setRuns(Array.isArray(data?.runs) ? data.runs : [])
      } catch {
        if (!cancelled) setError("Could not reach the server to load run history.")
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [savedQueryId])

  if (loading) {
    return (
      <div className="flex items-center gap-2 py-3 text-xs text-zinc-500">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
        Loading run history…
      </div>
    )
  }
  if (error) {
    return <p className="py-3 text-xs text-red-600 dark:text-red-400">{error}</p>
  }
  if (!runs || runs.length === 0) {
    return (
      <p className="py-3 text-xs text-zinc-500">
        No runs recorded yet. History starts from the first run after this feature shipped —
        earlier runs were never written down.
      </p>
    )
  }

  return (
    <div className="py-2 space-y-1">
      {runs.map((r) => (
        <div key={r.run_id} className="flex flex-wrap items-center gap-2 text-xs">
          {runStatusBadge(r.status)}
          <span className="text-zinc-500">{formatAbsoluteTime(r.finished_at)}</span>
          <span className="text-zinc-400">·</span>
          <span className="text-zinc-500">{r.trigger_source}</span>
          <span className="text-zinc-400">·</span>
          <span className="text-zinc-500">{Math.round(r.duration_ms / 100) / 10}s</span>
          {r.error && (
            <span className="text-red-600 dark:text-red-400 break-all">{r.error}</span>
          )}
          {r.auto_pause_reason && (
            <span className="text-amber-600 dark:text-amber-400">
              auto-paused: {r.auto_pause_reason}
            </span>
          )}
        </div>
      ))}
    </div>
  )
}

export default function ScheduledQueriesPage() {
  const [items, setItems] = useState<ScheduledQuery[]>([])
  const [loading, setLoading] = useState(true)
  // Distinct from `items.length === 0`, which asserts the workspace has no
  // schedules. This says the list could not be loaded, so their number is unknown —
  // and "you have nothing scheduled" is the worst possible thing to say wrongly here.
  const [error, setError] = useState<string | null>(null)
  const [expanded, setExpanded] = useState<string | null>(null)
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

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await authFetch("/api/v1/explorer/schedules", { cache: "no-store" })
      if (!res.ok) {
        setError(`Could not load scheduled queries (HTTP ${res.status}).`)
        return
      }
      const data = await res.json()
      setItems(Array.isArray(data?.schedules) ? data.schedules : [])
    } catch {
      setError("Could not reach the server to load scheduled queries.")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="space-y-6">
      <PageHeader
        heading="Scheduled Queries"
        description="Every saved query in this workspace that runs on a schedule, and how each one has been getting on."
      >
        <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
          <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </PageHeader>

      <Card className="p-0 overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center gap-2 py-12 text-sm text-zinc-500">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading scheduled queries…
          </div>
        ) : error ? (
          <div className="flex flex-col items-center gap-2 py-12 text-center">
            <AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400" />
            <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
            <p className="text-xs text-zinc-500">
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
            <p className="text-xs text-zinc-500 max-w-md">
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
                <TableHead className="w-8" />
                <TableHead>Query</TableHead>
                <TableHead>Cadence</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Next run</TableHead>
                <TableHead>Last run</TableHead>
                <TableHead className="text-right">Edit</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((s) => {
                const isOpen = expanded === s.saved_query_id
                return (
                  <Fragment key={s.schedule_id}>
                    <TableRow
                      className="cursor-pointer"
                      onClick={() => setExpanded(isOpen ? null : s.saved_query_id)}
                    >
                      <TableCell className="align-top">
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-6 w-6 p-0"
                          aria-label={`${isOpen ? "Hide" : "Show"} run history for ${s.name}`}
                          aria-expanded={isOpen}
                          onClick={(e) => {
                            e.stopPropagation()
                            setExpanded(isOpen ? null : s.saved_query_id)
                          }}
                        >
                          {isOpen ? (
                            <ChevronDown className="h-3.5 w-3.5" />
                          ) : (
                            <ChevronRight className="h-3.5 w-3.5" />
                          )}
                        </Button>
                      </TableCell>
                      <TableCell className="align-top">
                        <div className="font-medium text-sm">{s.name}</div>
                        <div className="text-xs text-zinc-500 font-mono truncate max-w-[220px]">
                          {s.materialization === "statement"
                            ? "runs its SQL as written"
                            : s.target_table || "no target table"}
                        </div>
                        {s.connection_name && (
                          <div className="text-xs text-zinc-400">{s.connection_name}</div>
                        )}
                      </TableCell>
                      <TableCell className="align-top text-xs font-mono">
                        {describeCadence(s)}
                      </TableCell>
                      <TableCell className="align-top">{statusBadge(s)}</TableCell>
                      <TableCell className="align-top text-xs">
                        {s.next_run_at ? (
                          <span title={formatAbsoluteTime(s.next_run_at)}>
                            {formatNextRun(s.next_run_at)}
                          </span>
                        ) : s.schedule_type === "after_pipeline" ? (
                          // Distinct from the bare dash below, which means "there should
                          // be a time and we do not have one". This one has no time by
                          // construction, and saying so stops it reading as a fault.
                          <span className="text-zinc-400">When the pipeline runs</span>
                        ) : (
                          <span className="text-zinc-400">—</span>
                        )}
                      </TableCell>
                      <TableCell className="align-top text-xs">
                        {s.last_run_at ? (
                          <div className="space-y-0.5">
                            <div className="text-zinc-500">{formatAbsoluteTime(s.last_run_at)}</div>
                            {s.last_run_status === "failed" && (
                              <div
                                className="text-red-600 dark:text-red-400 truncate max-w-[200px]"
                                title={s.last_run_error || "The last run failed"}
                              >
                                failed
                              </div>
                            )}
                          </div>
                        ) : (
                          <span className="text-zinc-400">never run</span>
                        )}
                      </TableCell>
                      <TableCell className="align-top">
                        {/* Two different things a row here can be wrong about, so two
                            buttons rather than one ambiguous "Edit": the SQL that runs,
                            and when it runs. Both stop the click from also toggling the
                            run history open underneath the dialog. */}
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
                    {isOpen && (
                      <TableRow>
                        <TableCell />
                        <TableCell colSpan={6} className="pt-0">
                          <RunHistory savedQueryId={s.saved_query_id} />
                        </TableCell>
                      </TableRow>
                    )}
                  </Fragment>
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
