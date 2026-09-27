"use client"

/**
 * Re-snapshot a CDC table that is already streaming.
 *
 * The gap this closes: "Edit tables" backfills only the tables you add, and
 * there was no other control — even though POST /pipelines/:id/cdc/backfill has
 * been live the whole time, with API_ENDPOINTS.PIPELINES.CDC_BACKFILL sitting
 * in api.ts with zero callers.
 *
 * The table list comes from GET /pipelines/:id/table-stats?mode=cdc. That is
 * the pipeline's own record of what it is streaming, and its `qualified_name`
 * is already the SOURCE-side `db.table` name, which is exactly the
 * `data_collections` vocabulary Debezium's signal expects. `mode=cdc` matters:
 * it is the branch that also lists selected tables still waiting for their
 * first event, and the list is paged through to the end because the endpoint
 * defaults to 50 rows — a larger pipeline used to lose every table after the
 * fiftieth, and a waiting table was never offered at all.
 *
 * Whether a re-snapshot can work at all is asked up front (GET on the backfill
 * path). A connector without a signal channel used to get the full mode choice
 * and a live button that could only ever answer cdc_backfill_not_supported.
 * The same answer lists the modes the connector accepts: MongoDB takes blocking
 * only, so the card shows that one mode and what it costs instead of a choice
 * whose other half would be refused.
 *
 * A load is not instant: on the Kafka signal channel the re-snapshot (and the
 * load of tables added via Edit tables) is QUEUED and sent to Debezium once the
 * connector runs with the tables, about a minute later. "Snapshot loads" lists
 * the latest requests from GET .../cdc/snapshot-requests in plain words and
 * polls while any of them is still moving, so the operator sees the load
 * queued, sent, loading and done instead of a toast that claimed it had
 * started (#21).
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { toast } from "sonner"
import { DatabaseBackup, Loader2, RefreshCw } from "lucide-react"

import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { meetsRole } from "@/lib/workspace/roles"
import { emitPipelineRefresh, onPipelineRefresh } from "@/lib/events/pipelineRefresh"
import { formatAbsoluteTime, formatRelativeTime } from "@/lib/utils"
import {
  blockingOnlyDetail,
  describeBackfillAccepted,
  describeSnapshotRequest,
  describeSnapshotRequestSource,
  fetchBackfillCapability,
  fetchSnapshotRequests,
  isBlockingOnly,
  resnapshotDestinationNote,
  snapshotRequestIsActive,
  triggerCdcBackfill,
  type BackfillCapability,
  type BackfillMode,
  type SnapshotRequest,
} from "@/lib/pipeline/cdcBackfill"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

type TableStatRow = {
  qualified_name?: string
  table_name?: string
  mode?: string
  status?: string
}

/**
 * The names a re-snapshot can address: the pipeline's CDC tables, deduped and
 * sorted. Rows in batch mode are excluded — a Debezium signal cannot address
 * them, and offering one would produce a 400 from the orchestrator at best.
 * So are removed tables (`status: "removed"`): their counts stay on the stats
 * card, but the connector no longer captures them, so a re-read cannot reach
 * them — re-add them via Edit tables instead.
 */
export function cdcTableNames(rows: TableStatRow[] | null | undefined): string[] {
  const seen = new Set<string>()
  for (const r of rows || []) {
    if (String(r?.mode || "").toLowerCase() !== "cdc") continue
    if (String(r?.status || "").toLowerCase() === "removed") continue
    const name = String(r?.qualified_name || r?.table_name || "").trim()
    if (name) seen.add(name)
  }
  return Array.from(seen).sort()
}

// The API's own maximum page size (table_stats.go). Paging stops at MAX_PAGES
// so a server that never reports a total cannot keep the card fetching forever.
const PAGE_LIMIT = 1000
const MAX_PAGES = 20

export async function loadAllCdcTableRows(pipelineId: string): Promise<{ rows: TableStatRow[] } | { status: number }> {
  const rows: TableStatRow[] = []
  for (let page = 0; page < MAX_PAGES; page++) {
    const qs = new URLSearchParams({ mode: "cdc", limit: String(PAGE_LIMIT), offset: String(page * PAGE_LIMIT) })
    const res = await authFetch(`${API_ENDPOINTS.PIPELINES.TABLE_STATS(pipelineId)}?${qs}`, { cache: "no-store" })
    if (!res.ok) return { status: res.status }
    const data = (await res.json().catch(() => null)) as { tables?: TableStatRow[]; total?: number } | null
    const batch = Array.isArray(data?.tables) ? data!.tables : []
    rows.push(...batch)
    const total = Number(data?.total)
    if (batch.length < PAGE_LIMIT || (Number.isFinite(total) && rows.length >= total)) break
  }
  return { rows }
}

// Above this many tables the checklist gets a filter box.
const FILTER_THRESHOLD = 10

// What the card offers before the capability check answers (or when it
// cannot): both modes, as it always did, and the POST has the last word.
const LEGACY_MODES: BackfillMode[] = ["incremental", "blocking"]

// How often "Snapshot loads" re-reads while a request is still queued, sent or
// loading. It stops once none is: a finished list has nothing left to change.
const SNAPSHOT_POLL_MS = 5000

const SNAPSHOT_TONE_CLASS: Record<ReturnType<typeof describeSnapshotRequest>["tone"], string> = {
  progress: "text-sky-700 dark:text-sky-300",
  done: "text-emerald-700 dark:text-emerald-400",
  warning: "text-amber-700 dark:text-amber-300",
  error: "text-red-700 dark:text-red-400",
}

function tablesSummary(tables: string[]): string {
  if (tables.length <= 3) return tables.join(", ")
  return `${tables.slice(0, 3).join(", ")} +${tables.length - 3} more`
}

const MODE_BLURB: Record<BackfillMode, string> = {
  incremental:
    "Recommended. Debezium reads the table in chunks while streaming continues, so live changes keep flowing during the re-read.",
  blocking:
    "Streaming pauses for this connector until the whole table has been re-read. Faster to finish, but the lag builds up meanwhile.",
}

export function CdcResnapshotCard({ pipelineId }: { pipelineId: string }) {
  // Mirrors requirePipelineWorkspaceRole(..., security.WSMember) on the gateway
  // route (pipeline_cdc.go:188). `role` is "" while loading; meetsRole fails
  // closed on that.
  const { role } = useWorkspaceRole()
  const canRun = meetsRole(role, "member")

  const [tables, setTables] = useState<string[] | null>(null)
  const [loading, setLoading] = useState(true)
  // A failed read must not render as "this pipeline has no CDC tables" — that
  // reads as a broken pipeline rather than a failed request.
  const [loadError, setLoadError] = useState<string | null>(null)

  const [capability, setCapability] = useState<BackfillCapability>({ state: "unknown" })
  const [filter, setFilter] = useState("")

  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [mode, setMode] = useState<BackfillMode>("incremental")
  const [running, setRunning] = useState(false)
  const [failure, setFailure] = useState<{ title: string; detail: string; tables?: string[] } | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      // The capability check never fails the load: "unknown" leaves the card
      // working exactly as it did before the check existed.
      const [out, cap] = await Promise.all([loadAllCdcTableRows(pipelineId), fetchBackfillCapability(pipelineId)])
      setCapability(cap)
      if ("status" in out) {
        setLoadError(`Could not load this pipeline's tables (HTTP ${out.status})`)
        return
      }
      const names = cdcTableNames(out.rows)
      setTables(names)
      // A table removed since the last read cannot stay selected: the button
      // would count it and the POST would refuse it.
      setSelected((prev) => {
        const kept = Array.from(prev).filter((n) => names.includes(n))
        return kept.length === prev.size ? prev : new Set(kept)
      })
      setLoadError(null)
    } catch {
      setLoadError("Could not load this pipeline's tables — the API is unreachable")
    } finally {
      setLoading(false)
    }
  }, [pipelineId])

  useEffect(() => {
    void load()
  }, [load])

  // Snapshot loads (#21). null = not read yet; a failed read keeps what was
  // shown last. A gateway without the route (404) hides the list for good.
  const [requests, setRequests] = useState<SnapshotRequest[] | null>(null)
  const requestsUnavailable = useRef(false)
  const loadRequests = useCallback(async () => {
    if (requestsUnavailable.current) return
    const out = await fetchSnapshotRequests(pipelineId)
    if (out.state === "unavailable") {
      requestsUnavailable.current = true
      setRequests(null)
    } else if (out.state === "ok") {
      setRequests(out.requests)
    }
  }, [pipelineId])

  useEffect(() => {
    void loadRequests()
  }, [loadRequests])

  const anyRequestActive = (requests || []).some(snapshotRequestIsActive)
  useEffect(() => {
    if (!anyRequestActive) return
    const t = setInterval(() => void loadRequests(), SNAPSHOT_POLL_MS)
    return () => clearInterval(t)
  }, [anyRequestActive, loadRequests])

  // Edit tables (and this card's own re-snapshot) announce themselves on the
  // refresh bus; without this the table list and the loads stayed as they were
  // before the edit until a page reload (#15).
  useEffect(
    () =>
      onPipelineRefresh((pid) => {
        if (pid !== pipelineId) return
        void load()
        void loadRequests()
      }),
    [pipelineId, load, loadRequests]
  )

  const offeredModes = capability.state === "supported" ? capability.modes : LEGACY_MODES
  const blockingOnly = isBlockingOnly(capability)

  // The mode actually used: the operator's pick when the connector accepts it,
  // else the connector's default — never one it would refuse (incremental on
  // MongoDB).
  const effectiveMode: BackfillMode = offeredModes.includes(mode)
    ? mode
    : capability.state === "supported"
      ? capability.defaultMode
      : "incremental"

  const toggle = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  const visible = useMemo(() => {
    const q = filter.trim().toLowerCase()
    return q ? (tables || []).filter((n) => n.toLowerCase().includes(q)) : tables || []
  }, [tables, filter])

  // "Select all" acts on what the filter shows, so a filtered list never
  // silently selects tables the operator cannot see.
  const allVisibleSelected = useMemo(
    () => visible.length > 0 && visible.every((n) => selected.has(n)),
    [visible, selected]
  )

  const toggleVisible = () => {
    setSelected((prev) => {
      const next = new Set(prev)
      for (const n of visible) {
        if (allVisibleSelected) next.delete(n)
        else next.add(n)
      }
      return next
    })
  }

  const unsupported = capability.state === "unsupported"
  const destinationNote = resnapshotDestinationNote(capability)

  const run = async () => {
    setRunning(true)
    setFailure(null)
    try {
      const out = await triggerCdcBackfill({ pipelineId, tables: Array.from(selected), mode: effectiveMode })
      if (out.ok) {
        // A queued answer says so: the load starts once the connector picks it
        // up, and "Snapshot loads" below follows it from there.
        toast.success(describeBackfillAccepted(out.data, { tables: selected.size, mode: effectiveMode }))
        setSelected(new Set())
        // Reaches this card's own listener (the loads list) and the stats card.
        emitPipelineRefresh(pipelineId)
      } else if (out.code === "cdc_backfill_not_supported") {
        // The check could not answer before (older gateway, Connect briefly
        // down) but the POST just did — stop offering what cannot work.
        setCapability({ state: "unsupported", detail: out.detail })
        setSelected(new Set())
        toast.error(out.title)
      } else {
        setFailure({ title: out.title, detail: out.detail, tables: out.tables })
        toast.error(out.title)
      }
    } finally {
      setRunning(false)
    }
  }

  return (
    <Card>
      <CardHeader className="flex flex-row items-start justify-between gap-4 space-y-0">
        <div>
          <CardTitle className="flex items-center gap-2 text-sm font-medium">
            <DatabaseBackup className="h-4 w-4" />
            Re-snapshot tables
          </CardTitle>
          {/* Says what it does NOT do as well: "Edit tables" on the same tab
              also loads rows, and the two were easy to take for each other. */}
          <CardDescription className="text-xs">
            Re-read tables that are already streaming: every existing row is loaded again. This does not change which
            tables stream — to add or remove tables, use Edit tables at the top of this tab (it can also load the
            existing rows of the tables you add).
          </CardDescription>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            void load()
            void loadRequests()
          }}
          disabled={loading || running}
        >
          <RefreshCw className={`h-3 w-3 mr-2 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </CardHeader>

      <CardContent className="space-y-4">
        {loading && tables === null ? (
          <div className="text-sm text-muted-foreground">Loading tables…</div>
        ) : loadError ? (
          <div
            role="alert"
            className="rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300"
          >
            {loadError}
          </div>
        ) : unsupported ? (
          // Stated before any click, and not styled as an error: nothing failed.
          // The table list and mode choice are withheld because no choice made
          // there can succeed.
          <div
            role="status"
            className="rounded border border-zinc-200 bg-zinc-50 p-3 text-sm text-zinc-700 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-300"
          >
            <div className="font-medium">Re-snapshot is not available for this pipeline</div>
            <div className="mt-1 text-xs">{capability.detail}</div>
          </div>
        ) : !tables || tables.length === 0 ? (
          <div className="text-sm text-muted-foreground">No CDC tables found for this pipeline.</div>
        ) : (
          <>
            <div className="space-y-2">
              <div className="flex items-center justify-between gap-3">
                <Label className="text-sm font-medium">
                  Tables to re-read{" "}
                  <span className="font-normal text-muted-foreground">
                    ({selected.size} of {tables.length} selected)
                  </span>
                </Label>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 text-xs"
                  disabled={running || visible.length === 0}
                  onClick={toggleVisible}
                >
                  {allVisibleSelected ? (filter.trim() ? "Clear shown" : "Clear all") : filter.trim() ? "Select shown" : "Select all"}
                </Button>
              </div>
              {tables.length > FILTER_THRESHOLD && (
                <Input
                  value={filter}
                  onChange={(e) => setFilter(e.target.value)}
                  placeholder="Filter tables…"
                  aria-label="Filter tables"
                  className="h-8 text-xs"
                />
              )}
              <div className="max-h-56 overflow-auto rounded border border-zinc-200 dark:border-zinc-800 divide-y divide-zinc-100 dark:divide-zinc-900">
                {visible.length === 0 && (
                  <div className="px-3 py-2 text-xs text-muted-foreground">No tables match “{filter.trim()}”.</div>
                )}
                {visible.map((name) => (
                  <label
                    key={name}
                    className="flex cursor-pointer items-center gap-3 px-3 py-2 text-sm hover:bg-zinc-50 dark:hover:bg-zinc-900/40"
                  >
                    <Checkbox
                      checked={selected.has(name)}
                      disabled={running}
                      onCheckedChange={() => toggle(name)}
                      aria-label={`Re-snapshot ${name}`}
                    />
                    <span className="font-mono text-xs">{name}</span>
                  </label>
                ))}
              </div>
            </div>

            {blockingOnly ? (
              // One mode, so no radio: say which it is and what it costs, before
              // the click — pausing a whole pipeline's streaming is not a detail.
              <div
                role="note"
                data-testid="cdc-resnapshot-blocking-only"
                className="rounded border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900 dark:border-sky-900 dark:bg-sky-950/30 dark:text-sky-200"
              >
                <div className="font-medium">Snapshot mode: blocking — streaming pauses during the re-read</div>
                <div className="mt-1 text-xs">{blockingOnlyDetail(capability)}</div>
              </div>
            ) : (
              <fieldset className="space-y-2">
                <legend className="text-sm font-medium">Snapshot mode</legend>
                {offeredModes.map((m) => (
                  <label key={m} className="flex items-start gap-2 text-sm">
                    <input
                      type="radio"
                      name="snapshot-mode"
                      className="mt-1"
                      checked={effectiveMode === m}
                      disabled={running}
                      onChange={() => setMode(m)}
                    />
                    <span>
                      <span className="capitalize">{m}</span>
                      <span className="block text-xs text-muted-foreground">{MODE_BLURB[m]}</span>
                    </span>
                  </label>
                ))}
              </fieldset>
            )}

            {/* Said before the click, not after: a re-snapshot re-emits every
                row, and what that does at the destination depends on whether
                the destination can upsert — or, for object storage, whether the
                table's folder is emptied first. */}
            <p
              data-testid="cdc-resnapshot-destination-note"
              data-tone={destinationNote.tone}
              className={
                destinationNote.tone === "info"
                  ? "rounded border border-sky-200 bg-sky-50 p-2 text-xs text-sky-900 dark:border-sky-900 dark:bg-sky-950/30 dark:text-sky-200"
                  : "rounded border border-amber-200 bg-amber-50 p-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300"
              }
            >
              {destinationNote.text}
            </p>

            {failure && (
              <div
                role="alert"
                className="rounded border border-red-200 bg-red-50 p-3 text-sm text-red-800 dark:border-red-900/60 dark:bg-red-950/30 dark:text-red-300"
              >
                <div className="font-medium">{failure.title}</div>
                <div className="mt-1 text-xs">{failure.detail}</div>
                {failure.tables && failure.tables.length > 0 && (
                  <div className="mt-2 flex flex-wrap gap-1">
                    {failure.tables.map((t) => (
                      <Badge key={t} variant="outline" className="font-mono text-[11px] font-normal">
                        {t}
                      </Badge>
                    ))}
                  </div>
                )}
              </div>
            )}

            <div className="flex items-center gap-3">
              <Button size="sm" disabled={!canRun || running || selected.size === 0} onClick={() => void run()}>
                {running && <Loader2 className="mr-2 h-3 w-3 animate-spin" />}
                Re-snapshot {selected.size > 0 ? `${selected.size} ${selected.size === 1 ? "table" : "tables"}` : ""}
              </Button>
              {!canRun && (
                <span className="text-xs text-muted-foreground">
                  You need at least Member in this workspace to re-snapshot.
                </span>
              )}
            </div>
          </>
        )}

        {requests && requests.length > 0 && (
          <div className="space-y-2" data-testid="cdc-snapshot-loads">
            <div className="text-sm font-medium">Snapshot loads</div>
            <ul className="max-h-64 overflow-auto rounded border border-zinc-200 dark:border-zinc-800 divide-y divide-zinc-100 dark:divide-zinc-900">
              {requests.map((r) => {
                const s = describeSnapshotRequest(r)
                const when = formatAbsoluteTime(r.requested_at)
                return (
                  <li key={r.id} data-testid="cdc-snapshot-load" data-status={r.status} className="space-y-1 px-3 py-2 text-xs">
                    <div className="flex items-center justify-between gap-2">
                      <span className="font-medium">
                        {describeSnapshotRequestSource(r.source)} <span className="text-muted-foreground">({r.mode})</span>
                      </span>
                      {when && (
                        <span className="text-muted-foreground" title={when}>
                          {formatRelativeTime(r.requested_at)}
                        </span>
                      )}
                    </div>
                    {r.tables.length > 0 && (
                      <div className="break-all font-mono text-muted-foreground">{tablesSummary(r.tables)}</div>
                    )}
                    <div className={`flex items-center gap-1 ${SNAPSHOT_TONE_CLASS[s.tone]}`}>
                      {s.tone === "progress" && <Loader2 className="h-3 w-3 animate-spin" />}
                      <span>{s.label}</span>
                    </div>
                    {s.detail && <div className="text-muted-foreground">{s.detail}</div>}
                  </li>
                )
              })}
            </ul>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
