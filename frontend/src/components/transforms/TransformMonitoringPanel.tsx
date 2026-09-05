"use client"

import { Fragment, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { authGet } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { Badge } from "@/components/ui/badge"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { ChevronDown, ChevronRight, History, Activity } from "lucide-react"
import { formatAbsoluteTime, formatAge, formatCount, formatDuration } from "@/lib/transform-format"

// Read-only consumers of the two new pipeline-scoped endpoints:
//   GET /transforms/pipeline/:id/rollup          → per-run net in→out + honest drop + freshness
//   GET /transforms/pipeline/:id/config-history  → config_snapshot revisions (deduped)

type RollupExecution = {
  execution_id: string
  transform_count: number
  total_input_rows: number
  total_output_rows: number
  rows_dropped: number
  total_duration_ms: number
  failed_count: number
  has_error: boolean
  first_activity_at?: string | null
  latest_activity_at?: string | null
}
type RollupResponse = { pipeline_id: string; executions: RollupExecution[]; truncated?: boolean }

type ConfigRevision = {
  execution_id: string
  first_seen_at: string
  config_snapshot: Record<string, unknown>
  // The endpoint emits `[]` (not null) for the baseline revision; type it honestly
  // so callers null-guard against older payloads too.
  changed_keys: string[] | null
}
type ConfigSlot = {
  table_name: string
  transform_order: number
  transform_id?: string | null
  transform_type: string
  revision_count: number
  revisions: ConfigRevision[]
}
type HistoryResponse = { pipeline_id: string; slots: ConfigSlot[]; truncated: boolean }

const EXEC_DETAIL = (id: string) => `/executions/${id}`

export function TransformMonitoringPanel({ pipelineId }: { pipelineId: string }) {
  const [rollup, setRollup] = useState<RollupExecution[] | null>(null)
  const [rollupTruncated, setRollupTruncated] = useState(false)
  const [history, setHistory] = useState<HistoryResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    const run = async () => {
      setLoading(true)
      setError(null)
      try {
        const [r, h] = await Promise.all([
          authGet<RollupResponse>(`${API_ENDPOINTS.TRANSFORMS.ROLLUP(pipelineId)}?limit=25`),
          authGet<HistoryResponse>(API_ENDPOINTS.TRANSFORMS.CONFIG_HISTORY(pipelineId)),
        ])
        if (cancelled) return
        setRollup(Array.isArray(r?.executions) ? r.executions : [])
        setRollupTruncated(Boolean(r?.truncated))
        setHistory(h ?? { pipeline_id: pipelineId, slots: [], truncated: false })
      } catch (e: any) {
        if (cancelled) return
        setError(String(e?.message || e))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    run()
    return () => {
      cancelled = true
    }
  }, [pipelineId])

  // Show every transform slot, but surface the ones that actually changed across
  // runs first — a slot with a single baseline still lets the user read its config.
  const slots = useMemo(() => {
    const all = history?.slots || []
    return [...all].sort((a, b) => {
      const ac = a.revision_count > 1 ? 0 : 1
      const bc = b.revision_count > 1 ? 0 : 1
      if (ac !== bc) return ac - bc
      const t = a.table_name.localeCompare(b.table_name)
      return t !== 0 ? t : a.transform_order - b.transform_order
    })
  }, [history])
  const hasChanges = slots.some((s) => s.revision_count > 1)

  if (loading) {
    return (
      <div role="status" aria-live="polite" className="text-sm text-zinc-500">
        Loading transform history…
      </div>
    )
  }
  if (error) {
    return (
      <div role="alert" className="text-sm text-red-600 dark:text-red-400">
        Couldn&apos;t load transform history. {error}
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* ── Per-run rollup (P0': net in→out + honest rows dropped + freshness) ── */}
      <div className="space-y-2">
        <div className="flex items-center gap-2 text-sm font-semibold text-zinc-900 dark:text-white">
          <Activity aria-hidden="true" className="h-4 w-4 text-violet-600 dark:text-violet-400" />
          Transform run history
        </div>

        {!rollup || rollup.length === 0 ? (
          <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-4 text-sm text-zinc-500">
            No transform runs recorded yet.
          </div>
        ) : (
          <div className="overflow-x-auto rounded-lg border border-zinc-200 dark:border-zinc-800">
            <table className="w-full text-sm">
              <caption className="sr-only">
                Transform run history — one row per execution, newest first, with net rows in and out,
                rows dropped, duration and data freshness.
              </caption>
              <thead className="bg-zinc-50 dark:bg-zinc-900/60 text-xs text-zinc-500">
                <tr>
                  <th scope="col" className="px-3 py-2 text-left font-medium">Run</th>
                  <th scope="col" className="px-3 py-2 text-right font-medium">Transforms</th>
                  <th scope="col" className="px-3 py-2 text-right font-medium">Rows in → out</th>
                  <th scope="col" className="px-3 py-2 text-right font-medium">Dropped</th>
                  <th scope="col" className="px-3 py-2 text-right font-medium">Duration</th>
                  <th scope="col" className="px-3 py-2 text-right font-medium">Freshness</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-100 dark:divide-zinc-800">
                {rollup.map((r) => (
                  <tr key={r.execution_id} className="text-zinc-800 dark:text-zinc-200">
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-2">
                        <Link
                          href={EXEC_DETAIL(r.execution_id)}
                          className="font-mono text-xs text-blue-600 hover:underline dark:text-blue-400"
                          title={`Open execution ${r.execution_id}`}
                        >
                          {r.execution_id.slice(0, 8)}
                        </Link>
                        {r.has_error && (
                          <Badge variant="destructive" className="text-[10px]">
                            {r.failed_count} failed
                          </Badge>
                        )}
                      </div>
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">{r.transform_count}</td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {formatCount(r.total_input_rows)} <span aria-hidden="true">→</span> {formatCount(r.total_output_rows)}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {r.rows_dropped > 0 ? (
                        <span className="text-amber-700 dark:text-amber-400">−{formatCount(r.rows_dropped)}</span>
                      ) : (
                        <span className="text-zinc-500" title="No rows removed by this run's transforms">—</span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-zinc-500">{formatDuration(r.total_duration_ms)}</td>
                    <td className="px-3 py-2 text-right text-zinc-500">
                      {r.latest_activity_at ? (
                        <div className="flex flex-col items-end leading-tight">
                          <time dateTime={r.latest_activity_at}>{formatAge(r.latest_activity_at)}</time>
                          <span className="text-[10px] text-zinc-400">{formatAbsoluteTime(r.latest_activity_at)}</span>
                        </div>
                      ) : (
                        "—"
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {rollupTruncated && (
          <p className="text-xs text-amber-600 dark:text-amber-400">
            Showing the most recent runs only — older executions are not listed.
          </p>
        )}
        <p className="text-xs text-zinc-500">
          &ldquo;Dropped&rdquo; is the rows a run actually removed (e.g. filter, null-handling, validation);
          transforms that don&apos;t reduce rows show &ldquo;—&rdquo;. &ldquo;Rows in → out&rdquo; is net across
          the whole run, not per transform step.
        </p>
      </div>

      {/* ── Configuration history (P1: config_snapshot revisions, deduped) ── */}
      <div className="space-y-2">
        <div className="flex items-center gap-2 text-sm font-semibold text-zinc-900 dark:text-white">
          <History aria-hidden="true" className="h-4 w-4 text-blue-600 dark:text-blue-400" />
          Configuration history
        </div>

        {slots.length === 0 ? (
          <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-4 text-sm text-zinc-500">
            No transform configuration recorded across the pipeline&apos;s runs yet.
          </div>
        ) : (
          <>
            {!hasChanges && (
              <p className="text-xs text-zinc-500">
                No configuration changes across the recorded runs — showing the current baseline for each transform.
              </p>
            )}
            <div className="space-y-2">
              {slots.map((slot) => (
                <ConfigSlotCard key={`${slot.table_name} ${slot.transform_order}`} slot={slot} />
              ))}
            </div>
          </>
        )}
        {history?.truncated && (
          <p className="text-xs text-amber-600 dark:text-amber-400">
            History was truncated — only the most recent runs are shown.
          </p>
        )}
      </div>
    </div>
  )
}

function ConfigSlotCard({ slot }: { slot: ConfigSlot }) {
  const [open, setOpen] = useState(false)
  const hasChanges = slot.revision_count > 1
  // Show newest revision first so the current config is at the top; keep the older
  // one alongside so a change renders as an inline before → after.
  const revisions = useMemo(() => [...slot.revisions].reverse(), [slot.revisions])

  return (
    <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950">
      <Collapsible open={open} onOpenChange={setOpen}>
        <CollapsibleTrigger asChild>
          <button className="flex w-full items-center justify-between gap-3 p-3 text-left">
            <div className="flex flex-wrap items-center gap-2 min-w-0">
              {open ? (
                <ChevronDown aria-hidden="true" className="h-4 w-4 text-zinc-400" />
              ) : (
                <ChevronRight aria-hidden="true" className="h-4 w-4 text-zinc-400" />
              )}
              <span className="font-medium text-zinc-900 dark:text-white truncate">{slot.table_name}</span>
              <Badge variant="outline" className="font-mono text-[10px]">#{slot.transform_order}</Badge>
              <span className="text-xs text-zinc-500">{slot.transform_type}</span>
            </div>
            {hasChanges ? (
              <Badge variant="secondary" className="shrink-0">{slot.revision_count} versions</Badge>
            ) : (
              <Badge variant="outline" className="shrink-0 text-zinc-500">baseline</Badge>
            )}
          </button>
        </CollapsibleTrigger>
        <CollapsibleContent className="px-3 pb-3">
          <ol className="relative space-y-3 border-l border-zinc-200 dark:border-zinc-800 pl-4">
            {revisions.map((rev, i) => {
              const isCurrent = i === 0
              const prev = revisions[i + 1]
              const changed = rev.changed_keys ?? []
              return (
                <li key={rev.execution_id} className="space-y-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <Link
                      href={EXEC_DETAIL(rev.execution_id)}
                      className="font-mono text-xs text-blue-600 hover:underline dark:text-blue-400"
                      title={`Open execution ${rev.execution_id}`}
                    >
                      {rev.execution_id.slice(0, 8)}
                    </Link>
                    {isCurrent && <Badge variant="success" className="text-[10px]">current</Badge>}
                    <time dateTime={rev.first_seen_at} className="text-xs text-zinc-500">
                      {formatAbsoluteTime(rev.first_seen_at) || formatAge(rev.first_seen_at)}
                    </time>
                    {changed.length > 0 ? (
                      changed.map((k) => (
                        <Badge
                          key={k}
                          variant="outline"
                          className="border-blue-300 text-blue-700 dark:border-blue-800 dark:text-blue-400 text-[10px]"
                        >
                          {k} changed
                        </Badge>
                      ))
                    ) : (
                      <span className="text-[10px] text-zinc-500">baseline</span>
                    )}
                  </div>
                  <RevisionConfig rev={rev} prev={prev} />
                </li>
              )
            })}
          </ol>
        </CollapsibleContent>
      </Collapsible>
    </div>
  )
}

function fmtVal(v: unknown): string {
  if (v === undefined) return "—"
  if (v === null) return "null"
  if (typeof v === "string") return v
  return JSON.stringify(v)
}

// Renders a revision's config as a readable key → value list (not a raw JSON
// dump). Changed keys are highlighted, and when a previous revision exists the
// changed value is shown inline as before → after.
function RevisionConfig({ rev, prev }: { rev: ConfigRevision; prev?: ConfigRevision }) {
  const cur = rev.config_snapshot ?? {}
  const before = (prev?.config_snapshot ?? {}) as Record<string, unknown>
  const changed = new Set(rev.changed_keys ?? [])
  const keys = Object.keys(cur).sort()
  if (keys.length === 0) {
    return <div className="text-[11px] text-zinc-400">(empty config)</div>
  }
  return (
    <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 rounded-md bg-zinc-50 dark:bg-zinc-900 p-2 text-[11px] font-mono">
      {keys.map((k) => {
        const isChanged = changed.has(k)
        const showDiff = isChanged && prev && before[k] !== undefined
        return (
          <Fragment key={k}>
            <dt className={isChanged ? "text-blue-600 dark:text-blue-400 font-semibold" : "text-zinc-500"}>{k}</dt>
            <dd className="text-zinc-800 dark:text-zinc-200 break-all">
              {showDiff ? (
                <>
                  <span className="text-red-500 line-through dark:text-red-400">{fmtVal(before[k])}</span>{" "}
                  <span aria-hidden="true" className="text-zinc-400">→</span>{" "}
                  <span className="text-emerald-600 dark:text-emerald-400">{fmtVal((cur as Record<string, unknown>)[k])}</span>
                </>
              ) : (
                fmtVal((cur as Record<string, unknown>)[k])
              )}
            </dd>
          </Fragment>
        )
      })}
    </dl>
  )
}
