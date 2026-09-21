"use client"

// The panel a Graph node opens: what the model or pipeline is doing now, its recent runs,
// and for a model the SQL a chosen run executed. It is only opened for a node the caller
// may open (the page's own model, or one whose own route answered 2xx), so nothing here
// has to hide a name; it still asks the SQL's own route, which checks access again.

import { useEffect, useState } from "react"
import Link from "next/link"
import { toast } from "sonner"
import { Check, Copy, ExternalLink, Loader2 } from "lucide-react"

import { cn, formatRelativeTime } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { formatAbsoluteTime } from "@/components/explorer/scheduleTime"
import { getJson } from "@/components/explorer/getJson"
import { formatDuration, runStatusBadge } from "@/components/explorer/scheduledModel"
import { describeRunOrigin, describeSkipReason } from "@/components/explorer/runProvenance"
import { LiveStateCell, LiveStateDetail } from "@/components/explorer/ModelLiveState"
import { AFTER_UPSTREAM, liveCellFor, type Fetched, type LiveCell, type RunningModelsResponse } from "@/components/explorer/liveState"
import {
  isWaitingSkip,
  runDurationMs,
  skipLabel,
  type LineageNode,
  type LineageNodeView,
  type LineageRun,
  type NodeLookup,
} from "@/components/explorer/modelLineage"
import { parseSqlHistory, sqlForRun, type RunSql, type SqlHistory } from "@/components/explorer/modelRunSql"

/** Runs listed in the panel; the Runs tab has the rest. */
export const PANEL_RUNS_LIMIT = 25

const SQL_TIMEOUT_MS = 10_000

/** Which run the panel shows: one from the history, or the rebuild in flight. */
export type RunSelection = { kind: "run"; runId: string } | { kind: "now" } | null

export interface RunPanelTarget {
  node: LineageNode
  view: LineageNodeView
  lookup: NodeLookup | undefined
  /** "Direct upstream", "This model", … */
  relation: string
  /** Null opens on the rebuild running now, or else the newest run. */
  selection: RunSelection
}

type SqlState =
  | { status: "loading" }
  | { status: "denied" }
  | { status: "error"; message: string }
  | { status: "ok"; history: SqlHistory }

const RUN_DOT: Record<string, string> = {
  succeeded: "bg-emerald-500",
  failed: "bg-red-500",
  skipped: "bg-amber-400",
}

export function runDotClass(r: LineageRun): string {
  if (isWaitingSkip(r)) return "border-2 border-amber-400 bg-transparent"
  return RUN_DOT[r.status] ?? "bg-zinc-400"
}

function runHeadline(r: LineageRun): string {
  if (r.status === "skipped") return skipLabel(r)
  const took = runDurationMs(r)
  const status = r.status.charAt(0).toUpperCase() + r.status.slice(1)
  return took === null ? status : `${status} in ${formatDuration(took)}`
}

function runKey(r: LineageRun, i: number): string {
  return r.run_id || `row-${i}`
}

function isLive(cell: LiveCell): boolean {
  return cell.kind === "rebuilding" || cell.kind === "running_no_detail"
}

export function ModelRunPanel({
  target,
  running,
  onClose,
}: {
  target: RunPanelTarget | null
  running: Fetched<RunningModelsResponse>
  onClose: () => void
}) {
  return (
    <Dialog open={!!target} onOpenChange={(open) => !open && onClose()}>
      {target && (
        <DialogContent className="ml-auto flex h-full max-w-xl flex-col gap-4 overflow-y-auto">
          {target.node.kind === "pipeline" ? (
            <PipelineBody target={target} />
          ) : (
            // Keyed so a different model, or a different run clicked in the grid, starts clean.
            <ModelBody
              key={`${target.node.key}|${target.selection?.kind === "run" ? target.selection.runId : target.selection?.kind ?? ""}`}
              target={target}
              running={running}
            />
          )}
        </DialogContent>
      )}
    </Dialog>
  )
}

function PanelHeader({ target, openLabel }: { target: RunPanelTarget; openLabel: string }) {
  const { view, relation } = target
  return (
    <DialogHeader>
      <p className="text-[10px] uppercase tracking-wide text-zinc-400">
        {view.kindLabel} · {relation}
      </p>
      <DialogTitle className="break-words">{view.title}</DialogTitle>
      {view.href && (
        <DialogDescription>
          <Link href={view.href} className="inline-flex items-center gap-1 text-xs underline-offset-2 hover:underline">
            {openLabel}
            <ExternalLink className="h-3 w-3" aria-hidden />
          </Link>
        </DialogDescription>
      )}
    </DialogHeader>
  )
}

function PipelineBody({ target }: { target: RunPanelTarget }) {
  const lookup = target.lookup
  const pipeline = lookup?.status === "pipeline" ? lookup.pipeline : null
  const exec = pipeline?.last_execution ?? null
  const started = exec?.started_at ? Date.parse(exec.started_at) : NaN
  const finished = exec?.completed_at ? Date.parse(exec.completed_at) : NaN
  const took = !Number.isNaN(started) && !Number.isNaN(finished) && finished >= started ? finished - started : null
  return (
    <>
      <PanelHeader target={target} openLabel="Open pipeline" />
      <section className="space-y-1 text-sm">
        <h3 className="text-xs font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">Last execution</h3>
        {!pipeline ? (
          <p className="text-zinc-500 dark:text-zinc-400">{target.view.status}</p>
        ) : !exec ? (
          <p className="text-zinc-500 dark:text-zinc-400">Never run.</p>
        ) : (
          <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs">
            <dt className="text-zinc-500 dark:text-zinc-400">Status</dt>
            <dd>{target.view.status}</dd>
            {exec.started_at && (
              <>
                <dt className="text-zinc-500 dark:text-zinc-400">Started</dt>
                <dd>{formatAbsoluteTime(exec.started_at)}</dd>
              </>
            )}
            {exec.completed_at && (
              <>
                <dt className="text-zinc-500 dark:text-zinc-400">Finished</dt>
                <dd>{formatAbsoluteTime(exec.completed_at)}</dd>
              </>
            )}
            {took !== null && (
              <>
                <dt className="text-zinc-500 dark:text-zinc-400">Took</dt>
                <dd>{formatDuration(took)}</dd>
              </>
            )}
            {exec.error_message && (
              <>
                <dt className="text-zinc-500 dark:text-zinc-400">Error</dt>
                <dd className="break-words text-red-600 dark:text-red-400">{exec.error_message}</dd>
              </>
            )}
          </dl>
        )}
      </section>
      <p className="text-xs text-zinc-500 dark:text-zinc-400">
        A pipeline has no SQL of its own here. Its executions and logs are on the pipeline&apos;s page.
      </p>
    </>
  )
}

function ModelBody({ target, running }: { target: RunPanelTarget; running: Fetched<RunningModelsResponse> }) {
  const { node, lookup } = target
  const runs = lookup?.status === "model" ? lookup.runs : null
  const schedule = node.schedule
  const liveRelevant = schedule?.schedule_type === AFTER_UPSTREAM
  const cell: LiveCell = liveRelevant ? liveCellFor(AFTER_UPSTREAM, node.id, running) : { kind: "not_applicable" }
  const live = isLive(cell)

  const [selection, setSelection] = useState<RunSelection>(() => {
    if (target.selection) return target.selection
    if (live) return { kind: "now" }
    return runs?.[0]?.run_id ? { kind: "run", runId: runs[0].run_id } : null
  })
  const selectedRun =
    selection?.kind === "run" ? (runs?.find((r) => r.run_id === selection.runId) ?? null) : null
  // The rebuild in flight says when it started when the loop gave detail; that is the run
  // whose SQL is wanted.
  const nowStarted = cell.kind === "rebuilding" ? (cell.detail.current?.started_at ?? null) : null
  const showingNow = selection?.kind === "now" && live

  const sql = useSqlHistory(node.id)

  const listed = runs ? runs.slice(0, PANEL_RUNS_LIMIT) : []
  if (selectedRun && !listed.includes(selectedRun)) listed.push(selectedRun)

  return (
    <>
      <PanelHeader target={target} openLabel="Open model" />

      <section className="space-y-1">
        <h3 className="text-xs font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">Now</h3>
        {liveRelevant ? (
          <>
            <div className="flex flex-wrap items-center gap-2 text-xs">
              <LiveStateCell cell={cell} />
              {live && selection?.kind !== "now" && (
                <Button size="sm" variant="outline" className="h-6 px-2 text-xs" onClick={() => setSelection({ kind: "now" })}>
                  Show its SQL
                </Button>
              )}
            </div>
            <LiveStateDetail cell={cell} upstreams={schedule?.upstreams} />
          </>
        ) : (
          <p className="text-xs text-zinc-500 dark:text-zinc-400">
            Whether a run is in progress is checked only for models that run after an upstream. This one runs on its own
            schedule, or has none.
          </p>
        )}
      </section>

      <section className="space-y-2">
        <h3 className="text-xs font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">Recent runs</h3>
        {!runs ? (
          <p className="text-xs text-zinc-500 dark:text-zinc-400">{target.view.status}</p>
        ) : runs.length === 0 ? (
          <p className="text-xs text-zinc-500 dark:text-zinc-400">Never run.</p>
        ) : (
          <ul className="max-h-56 space-y-0.5 overflow-y-auto pr-1" aria-label="Recent runs, newest first">
            {listed.map((r, i) => {
              const selected = !!r.run_id && selection?.kind === "run" && selection.runId === r.run_id
              const when = r.started_at || r.finished_at
              return (
                <li key={runKey(r, i)}>
                  <button
                    type="button"
                    disabled={!r.run_id}
                    aria-pressed={selected}
                    onClick={() => r.run_id && setSelection({ kind: "run", runId: r.run_id })}
                    className={cn(
                      "flex w-full items-center gap-2 rounded px-2 py-1 text-left text-xs hover:bg-zinc-100 disabled:cursor-default dark:hover:bg-zinc-800",
                      selected && "bg-zinc-100 font-medium dark:bg-zinc-800",
                    )}
                  >
                    <span className={cn("h-2.5 w-2.5 shrink-0 rounded-sm", runDotClass(r))} aria-hidden />
                    <span className="min-w-0 flex-1 truncate">{runHeadline(r)}</span>
                    {when && !Number.isNaN(Date.parse(when)) && (
                      <span className="shrink-0 text-zinc-500 dark:text-zinc-400" title={formatAbsoluteTime(when)}>
                        {formatRelativeTime(when)}
                      </span>
                    )}
                  </button>
                </li>
              )
            })}
          </ul>
        )}
      </section>

      {selectedRun && <RunDetail run={selectedRun} />}

      <SqlSection
        // Keyed so "Show current SQL" is not carried over to the next run picked.
        key={showingNow ? "now" : (selectedRun?.run_id ?? "current")}
        sqlState={sql.state}
        onRetry={sql.retry}
        startedAt={showingNow ? nowStarted : (selectedRun?.started_at ?? null)}
        label={showingNow ? "now" : selectedRun ? "run" : "current"}
      />
    </>
  )
}

function RunDetail({ run }: { run: LineageRun }) {
  const took = runDurationMs(run)
  const skip = describeSkipReason(run)
  return (
    <section className="space-y-1">
      <h3 className="text-xs font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">This run</h3>
      <dl className="grid grid-cols-[auto_1fr] items-baseline gap-x-3 gap-y-1 text-xs">
        <dt className="text-zinc-500 dark:text-zinc-400">Status</dt>
        <dd>{runStatusBadge(run.status)}</dd>
        {run.started_at && (
          <>
            <dt className="text-zinc-500 dark:text-zinc-400">Started</dt>
            <dd>{formatAbsoluteTime(run.started_at)}</dd>
          </>
        )}
        {took !== null && (
          <>
            <dt className="text-zinc-500 dark:text-zinc-400">Took</dt>
            <dd>{formatDuration(took)}</dd>
          </>
        )}
        <dt className="text-zinc-500 dark:text-zinc-400">Trigger</dt>
        <dd>{describeRunOrigin({ ...run, trigger_source: run.trigger_source ?? "" }) || "—"}</dd>
        {skip && (
          <>
            <dt className="text-zinc-500 dark:text-zinc-400">Skipped</dt>
            <dd>{skip}</dd>
          </>
        )}
        {typeof run.rows_affected === "number" && (
          <>
            <dt className="text-zinc-500 dark:text-zinc-400">Rows</dt>
            <dd>{run.rows_affected.toLocaleString()}</dd>
          </>
        )}
        {run.error && (
          <>
            <dt className="text-zinc-500 dark:text-zinc-400">Error</dt>
            <dd className="break-words text-red-600 dark:text-red-400">{run.error}</dd>
          </>
        )}
      </dl>
    </section>
  )
}

/** The model's SQL and its edit history, loaded once per panel. */
function useSqlHistory(modelId: string): { state: SqlState; retry: () => void } {
  const [retryTick, setRetryTick] = useState(0)
  const [loaded, setLoaded] = useState<{ key: string; state: SqlState } | null>(null)
  const loadKey = `${modelId}|${retryTick}`

  useEffect(() => {
    const controller = new AbortController()
    ;(async () => {
      let next: SqlState
      try {
        const res = await getJson(`/api/v1/explorer/saved/${encodeURIComponent(modelId)}/versions`, controller.signal, SQL_TIMEOUT_MS)
        // The route's own access check: another member's private model, or one since deleted.
        if (res.status === 403 || res.status === 404) next = { status: "denied" }
        else if (!res.ok) next = { status: "error", message: `Could not load the SQL (HTTP ${res.status}).` }
        else {
          const history = parseSqlHistory(res.body)
          next = history ? { status: "ok", history } : { status: "error", message: "Could not read the SQL." }
        }
      } catch {
        if (controller.signal.aborted) return
        next = { status: "error", message: "Could not reach the server to load the SQL." }
      }
      if (!controller.signal.aborted) setLoaded({ key: loadKey, state: next })
    })()
    return () => controller.abort()
  }, [modelId, loadKey])

  return {
    state: loaded?.key === loadKey ? loaded.state : { status: "loading" },
    retry: () => setRetryTick((t) => t + 1),
  }
}

function SqlSection({
  sqlState: state,
  onRetry,
  startedAt,
  label,
}: {
  sqlState: SqlState
  onRetry: () => void
  startedAt: string | null
  label: "run" | "now" | "current"
}) {
  const [showCurrent, setShowCurrent] = useState(false)
  const [copied, setCopied] = useState(false)

  let heading = "SQL"
  let sql: string | null = null
  let note: string | null = null
  let resolved: RunSql | null = null
  if (state.status === "ok") {
    resolved = label === "current" ? { kind: "current", sql: state.history.current.sql_text } : sqlForRun(state.history, startedAt)
    const which = label === "now" ? "the rebuild running now" : "this run"
    if (showCurrent || resolved.kind === "current") {
      heading = "Current SQL"
      sql = state.history.current.sql_text
      if (label === "now" && !startedAt && !showCurrent) {
        note = "The refresh loop did not say when this rebuild started, so this is the model's SQL as it is now."
      }
    } else if (resolved.kind === "unknown") {
      heading = "Current SQL"
      sql = resolved.current
      note = `The model was edited after ${which} started, and its edit history no longer goes back that far. This is the SQL as it is now, which may not be what ran.`
    } else {
      heading = label === "now" ? "SQL of the rebuild running now" : "SQL when this run started"
      sql = resolved.sql
      const notes: string[] = []
      if (resolved.editedSince) notes.push("The model has been edited since.")
      if (resolved.nearEdit) {
        notes.push(`An edit was saved within a minute of when ${which} started, so it may have run the text from just before or just after that edit.`)
      }
      note = notes.join(" ") || null
    }
  }

  const copy = () => {
    if (sql === null) return
    void navigator.clipboard
      ?.writeText(sql)
      .then(() => {
        setCopied(true)
        toast.success("SQL copied")
        setTimeout(() => setCopied(false), 1500)
      })
      .catch(() => toast.error("Could not copy the SQL"))
  }

  const canToggle = resolved?.kind === "at_run" && resolved.editedSince

  return (
    <section className="flex min-h-0 flex-col gap-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-xs font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">{heading}</h3>
        <div className="flex gap-1">
          {canToggle && (
            <Button size="sm" variant="ghost" className="h-6 px-2 text-xs" onClick={() => setShowCurrent((v) => !v)}>
              {showCurrent ? (label === "now" ? "Show SQL of this rebuild" : "Show SQL of this run") : "Show current SQL"}
            </Button>
          )}
          {sql !== null && (
            <Button size="sm" variant="outline" className="h-6 gap-1 px-2 text-xs" onClick={copy}>
              {copied ? <Check className="h-3 w-3" aria-hidden /> : <Copy className="h-3 w-3" aria-hidden />}
              {copied ? "Copied" : "Copy"}
            </Button>
          )}
        </div>
      </div>
      {state.status === "loading" ? (
        <p className="flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
          <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
          Loading the SQL…
        </p>
      ) : state.status === "denied" ? (
        <p className="text-xs text-zinc-500 dark:text-zinc-400">You can&apos;t open this model&apos;s SQL.</p>
      ) : state.status === "error" ? (
        <div className="flex flex-wrap items-center gap-2">
          <p role="alert" className="text-xs text-red-600 dark:text-red-400">
            {state.message}
          </p>
          <Button size="sm" variant="outline" className="h-6 px-2 text-xs" onClick={onRetry}>
            Retry
          </Button>
        </div>
      ) : (
        <>
          {note && <p className="text-xs text-amber-700 dark:text-amber-400">{note}</p>}
          <pre
            tabIndex={0}
            aria-label={heading}
            className="max-h-96 overflow-auto whitespace-pre rounded-md border bg-zinc-50 p-3 font-mono text-xs text-zinc-800 dark:border-zinc-800 dark:bg-zinc-950 dark:text-zinc-200"
          >
            {sql}
          </pre>
        </>
      )}
    </section>
  )
}
