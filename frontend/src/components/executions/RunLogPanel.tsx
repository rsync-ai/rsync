"use client"

/**
 * One run's log — what "View Logs" in a pipeline's ⋯ menu opens.
 *
 * That item used to land on this page's per-table row counts, which the
 * pipeline's own Table statistics tab already shows. This reads the run's
 * events instead: the rows stamped with the run's id, plus the pipeline-level
 * monitoring alerts and self-healing decisions raised while it ran
 * (`scope=run` — those carry no execution id, so a strict filter hid exactly
 * the rows that say why a run failed). Timer ticks (throughput and table-count
 * updates, stage heartbeats) are left out unless asked for
 * (`exclude_routine=true`); a warning or an error is never left out.
 */

import { useCallback, useEffect, useMemo, useState } from "react"
import { AlertCircle, AlertTriangle, Info, RefreshCw, ScrollText, Search } from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { LocalDateTime } from "@/components/ui/local-date-time"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { eventDetail, eventLabel, eventStageLabel } from "@/components/pipeline/eventDisplay"

export const RUN_LOG_PAGE_SIZE = 100

type Cursor = { before_ts: string; before_seq: number; before_event_id: string }

type LogRow = {
  event_id: string
  event_type: string
  execution_id?: string | null
  stage_id?: string | null
  severity?: string | null
  seq?: number | null
  trace_id?: string | null
  occurred_at?: string | null
  received_at?: string | null
  payload: Record<string, unknown>
}

type Level = "all" | "problems"

// What one read of the log produced, tagged with the request it answers so a
// stale response (a toggle or Refresh since) is recognisable as stale.
type Loaded = {
  key: string
  rows: LogRow[]
  next: Cursor | null
  more: boolean
  error: string | null
}

function severityOf(row: LogRow): "error" | "warning" | "info" {
  const s = (row.severity || "").toLowerCase()
  if (s === "error" || s === "critical" || s === "fatal") return "error"
  if (s === "warn" || s === "warning") return "warning"
  return "info"
}

const SEVERITY_BADGE = {
  error: { label: "Error", variant: "destructive", Icon: AlertCircle },
  warning: { label: "Warning", variant: "warning", Icon: AlertTriangle },
  info: { label: "Info", variant: "secondary", Icon: Info },
} as const

function toRows(raw: unknown): LogRow[] {
  if (!Array.isArray(raw)) return []
  return raw.map((item) => {
    const e = (item && typeof item === "object" ? item : {}) as Partial<LogRow>
    const payload = e.payload
    return {
      ...e,
      event_id: String(e.event_id ?? ""),
      event_type: String(e.event_type ?? ""),
      payload: payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {},
    }
  })
}

function rowText(row: LogRow) {
  const ev = {
    event_type: row.event_type,
    stage_id: row.stage_id ?? undefined,
    severity: row.severity ?? undefined,
    payload: row.payload,
  }
  return { label: eventLabel(ev), stage: eventStageLabel(ev), detail: eventDetail(ev) }
}

export function RunLogPanel({ pipelineId, executionId }: { pipelineId: string; executionId: string }) {
  const [showRoutine, setShowRoutine] = useState(false)
  const [refreshes, setRefreshes] = useState(0)
  const [loaded, setLoaded] = useState<Loaded | null>(null)
  const [loadingMore, setLoadingMore] = useState(false)
  const [level, setLevel] = useState<Level>("all")
  const [query, setQuery] = useState("")
  const [open, setOpen] = useState<Set<string>>(() => new Set())

  const fetchPage = useCallback(
    async (cursor: Cursor | null, signal?: AbortSignal) => {
      const qs = new URLSearchParams({
        execution_id: executionId,
        scope: "run",
        limit: String(RUN_LOG_PAGE_SIZE),
      })
      if (!showRoutine) qs.set("exclude_routine", "true")
      // Opaque: echoed back exactly as the server sent it.
      if (cursor) {
        qs.set("before_ts", cursor.before_ts)
        qs.set("before_seq", String(cursor.before_seq))
        qs.set("before_event_id", cursor.before_event_id)
      }
      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/events?${qs.toString()}`, {
        cache: "no-store",
        signal,
      })
      if (!res.ok) throw new Error(res.status === 404 ? "run not found" : `HTTP ${res.status}`)
      const data = (await res.json()) as { events?: unknown; has_more?: boolean; next_cursor?: Cursor | null }
      const page = toRows(data.events)
      return {
        page,
        next: data.next_cursor ?? null,
        more: typeof data.has_more === "boolean" ? data.has_more : page.length === RUN_LOG_PAGE_SIZE,
      }
    },
    [pipelineId, executionId, showRoutine]
  )

  // Every input that changes which rows the first page holds, plus Refresh.
  const requestKey = `${pipelineId}|${executionId}|${showRoutine}|${refreshes}`
  const loading = loaded?.key !== requestKey

  useEffect(() => {
    const ac = new AbortController()
    fetchPage(null, ac.signal).then(
      ({ page, next, more }) => setLoaded({ key: requestKey, rows: page, next, more, error: null }),
      (e: unknown) => {
        if (ac.signal.aborted) return
        setLoaded({ key: requestKey, rows: [], next: null, more: false, error: e instanceof Error ? e.message : "network error" })
      }
    )
    return () => ac.abort()
  }, [fetchPage, requestKey])

  // Rows stay on screen through a reload; only the first read starts empty.
  const rows = useMemo(() => loaded?.rows ?? [], [loaded])
  const error = loading ? null : (loaded?.error ?? null)
  const hasMore = !loading && !!loaded?.more && !!loaded.next

  const reload = () => setRefreshes((n) => n + 1)

  const loadOlder = async () => {
    const cursor = loaded?.next
    if (!cursor || loadingMore || loading) return
    const key = requestKey
    setLoadingMore(true)
    try {
      const { page, next, more } = await fetchPage(cursor)
      setLoaded((prev) => (prev && prev.key === key ? { ...prev, rows: [...prev.rows, ...page], next, more } : prev))
    } catch (e) {
      const message = e instanceof Error ? e.message : "network error"
      setLoaded((prev) => (prev && prev.key === key ? { ...prev, error: message } : prev))
    } finally {
      setLoadingMore(false)
    }
  }

  const texts = useMemo(() => new Map(rows.map((r) => [r.event_id, rowText(r)])), [rows])

  const visible = useMemo(() => {
    const q = query.trim().toLowerCase()
    return rows.filter((r) => {
      if (level === "problems" && severityOf(r) === "info") return false
      if (!q) return true
      const t = texts.get(r.event_id) ?? rowText(r)
      return [t.label, t.stage, t.detail, r.event_type, r.stage_id ?? ""].some((s) => s.toLowerCase().includes(q))
    })
  }, [rows, level, query, texts])

  const problemCount = useMemo(() => rows.filter((r) => severityOf(r) !== "info").length, [rows])

  const toggle = (id: string) =>
    setOpen((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })

  return (
    <Card id="logs" className="scroll-mt-20">
      <CardHeader className="pb-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <CardTitle className="text-lg flex items-center gap-2">
            <ScrollText className="h-5 w-5" />
            Run log
          </CardTitle>
          <Button type="button" variant="outline" size="sm" onClick={reload} disabled={loading}>
            <RefreshCw className={`h-4 w-4 mr-1 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        </div>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          Newest first. Includes monitoring alerts and self-healing decisions raised while this run was active.
        </p>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          <div role="group" aria-label="Log level" className="inline-flex rounded-md border border-zinc-200 dark:border-zinc-700">
            <Button
              type="button"
              size="sm"
              variant={level === "all" ? "secondary" : "ghost"}
              aria-pressed={level === "all"}
              onClick={() => setLevel("all")}
            >
              All
            </Button>
            <Button
              type="button"
              size="sm"
              variant={level === "problems" ? "secondary" : "ghost"}
              aria-pressed={level === "problems"}
              onClick={() => setLevel("problems")}
            >
              Errors &amp; warnings{problemCount > 0 ? ` (${problemCount})` : ""}
            </Button>
          </div>
          <div className="relative flex-1 min-w-[12rem]">
            <Search className="pointer-events-none absolute left-2 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-500 dark:text-zinc-400" />
            <Input
              type="search"
              aria-label="Search the log"
              placeholder="Search the log"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="pl-8 h-9"
            />
          </div>
          <label className="flex items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
            <input
              type="checkbox"
              checked={showRoutine}
              onChange={(e) => setShowRoutine(e.target.checked)}
              className="h-4 w-4"
            />
            Show routine updates
          </label>
        </div>

        {error ? (
          <div role="alert" className="flex items-center justify-between gap-2 rounded border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-800 dark:bg-red-900/20 dark:text-red-300">
            <span>Could not load the log ({error}).</span>
            <Button type="button" size="sm" variant="outline" onClick={reload}>
              Retry
            </Button>
          </div>
        ) : null}

        {loading && rows.length === 0 ? (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading the log…</p>
        ) : !error && rows.length === 0 ? (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">No log entries for this run.</p>
        ) : rows.length > 0 && visible.length === 0 ? (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">No entries match the current filter.</p>
        ) : null}

        {visible.length > 0 ? (
          <ol className="divide-y divide-zinc-100 rounded border border-zinc-200 dark:divide-zinc-800 dark:border-zinc-700">
            {visible.map((row) => {
              const t = texts.get(row.event_id) ?? rowText(row)
              const sev = SEVERITY_BADGE[severityOf(row)]
              const isOpen = open.has(row.event_id)
              const hasPayload = Object.keys(row.payload).length > 0
              return (
                <li key={row.event_id} data-testid="run-log-row" className="p-3 text-sm">
                  <div className="flex items-start gap-3">
                    <span className="w-40 shrink-0 text-xs text-zinc-500 dark:text-zinc-400 tabular-nums">
                      <LocalDateTime value={row.occurred_at || row.received_at} fallback="Time unknown" />
                    </span>
                    <Badge variant={sev.variant} className="shrink-0 gap-1">
                      <sev.Icon className="h-3 w-3" aria-hidden="true" />
                      {sev.label}
                    </Badge>
                    <div className="min-w-0 flex-1 space-y-0.5">
                      <div>
                        <span className="font-medium text-zinc-900 dark:text-zinc-100">{t.label}</span>
                        {t.stage ? <span className="text-xs text-zinc-500 dark:text-zinc-400"> · {t.stage}</span> : null}
                      </div>
                      {t.detail ? (
                        <div className="text-xs break-words text-zinc-700 dark:text-zinc-300">{t.detail}</div>
                      ) : null}
                    </div>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="h-6 px-2 text-xs shrink-0"
                      aria-expanded={isOpen}
                      onClick={() => toggle(row.event_id)}
                    >
                      {isOpen ? "Hide details" : "Details"}
                    </Button>
                  </div>
                  {isOpen ? (
                    <div className="mt-2 space-y-2">
                      <div className="flex flex-wrap items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
                        <span className="font-mono">{row.event_type}</span>
                        {row.stage_id ? <span className="font-mono">stage: {row.stage_id}</span> : null}
                        {typeof row.seq === "number" ? <span className="font-mono">seq: {row.seq}</span> : null}
                        {row.trace_id ? <span className="font-mono">trace: {row.trace_id}</span> : null}
                        {!row.execution_id ? <span>pipeline-level event</span> : null}
                      </div>
                      {hasPayload ? (
                        <pre className="max-h-72 overflow-auto rounded bg-zinc-50 p-2 text-[11px] text-zinc-800 dark:bg-zinc-900 dark:text-zinc-200">
                          {JSON.stringify(row.payload, null, 2)}
                        </pre>
                      ) : null}
                    </div>
                  ) : null}
                </li>
              )
            })}
          </ol>
        ) : null}

        {rows.length > 0 ? (
          <div className="flex items-center justify-between gap-2 text-xs text-zinc-500 dark:text-zinc-400">
            <span>
              {visible.length === rows.length
                ? `${rows.length} entries loaded`
                : `${visible.length} of ${rows.length} loaded entries shown`}
            </span>
            {hasMore ? (
              <Button type="button" variant="outline" size="sm" onClick={() => void loadOlder()} disabled={loadingMore}>
                {loadingMore ? "Loading…" : "Load older"}
              </Button>
            ) : null}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}
