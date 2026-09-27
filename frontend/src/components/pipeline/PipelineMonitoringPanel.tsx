"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import Link from "next/link"
import { useSearchParams } from "next/navigation"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { ScrollArea } from "@/components/ui/scroll-area"
import { RefreshCw, Pause, Play, StopCircle, Activity, List, BarChart3 } from "lucide-react"
import { toast } from "sonner"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch, authFetchOrThrow } from "@/lib/api/auth-fetch"
import { classifyError } from "@/lib/utils/error-handling"
import { ReasoningTimeline } from "@/components/pipeline/ReasoningTimeline"
import { MonitoringOverviewTab } from "@/components/pipeline/MonitoringOverviewTab"
import { TableStatisticsPanel } from "@/components/pipeline/TableStatisticsPanel"
import { useFeatureFlags } from "@/config/features"
import { PipelineTableSelector } from "@/components/pipeline/PipelineTableSelector"
import { getConnectionMetadata, truncatedTableTotal, type ConnectionTableMetadata } from "@/lib/api/connections"
import { getPipeline, resumePipelineTables, updatePipelineCDCTables, updatePipelineTables } from "@/lib/api/pipelines"
import {
  isLayoutV2Destination,
  isObjectStorageDestination,
  kindMeta,
  objectLayoutSourceFamily,
  objectLayoutTableParts,
  objectStorageTableFolder,
} from "@/lib/pipeline/destinationNamespace"
import {
  describeTableEditResult,
  fetchBackfillCapability,
  isBlockingOnly,
  type BackfillCapability,
  type TableEditOutcome,
} from "@/lib/pipeline/cdcBackfill"
import { makeTableMatcher, parseTableStatsRows } from "@/lib/pipeline/tableStatsRows"
import { extractLatestRowMetrics } from "@/lib/pipeline/dataPlaneRowMetrics"
import {
  isWaitingForFirstData,
  normalizePipelineStatus,
  reconcilePipelineStatus,
  WAITING_FOR_FIRST_DATA_LABEL,
} from "@/lib/pipeline/statusNormalization"
import { usePipelineRuntime } from "@/lib/hooks/usePipelineRuntime"
import { emitPipelineRefresh, onPipelineRefresh } from "@/lib/events/pipelineRefresh"
import { mergeNewestPage } from "@/lib/pipeline/mergeNewestEvents"
import { STATUS_EVENT_TYPES } from "@/lib/pipeline/eventNormalizer"
import { stepInfoFromEvents } from "@/components/pipeline/PipelineLiveStatePanel"
import type { PipelineStateResponse, BlockingReasonDetails } from "@/lib/api/types"

type PipelineRunEvent = {
  pipeline_id: string
  execution_id?: string
  event_id: string
  seq?: number
  event_type: string
  stage_id?: string
  stage_group?: string
  severity?: string
  trace_id?: string
  occurred_at?: string
  received_at: string
  payload: Record<string, unknown>
}

// Opaque paging position, produced by GET /pipelines/:id/events as next_cursor
// and echoed back verbatim. The shape is the server's sort key — clients must
// not build one, because seq is NULL on 21% of rows.
type EventsCursor = {
  before_ts: string
  before_seq: number
  before_event_id: string
}

const EVENTS_PAGE_SIZE = 50

// Stage transitions are a handful per run; the endpoint's cap is 500.
const STATUS_EVENTS_LIMIT = 500

// How often the Activity sub-tab re-reads the newest page while it is open —
// the cadence the Live events card it replaced used.
const ACTIVITY_POLL_MS = 5000

/** Short reason for a failed events read, shown inside "Could not load events (…)". */
function eventsReadError(status: number): string {
  if (status === 404) return "pipeline not found"
  if (status === 403) return "access denied — check pipeline ownership"
  return `HTTP ${status}`
}

function toRunEvents(raw: unknown): PipelineRunEvent[] {
  const rows = Array.isArray(raw) ? raw : []
  return rows.map((e: any) => ({
    ...e,
    payload: e?.payload && typeof e.payload === "object" ? e.payload : {},
    received_at: String(e?.received_at || e?.occurred_at || new Date().toISOString()),
  }))
}

/**
 * The trace of the run the header names. The newest row is often a CDC stream
 * stat, stamped with the pipeline id as its execution and trace, which put the
 * pipeline id beside the run's execution id; the run's own rows come first.
 */
export function runTraceId(
  events: Array<Pick<PipelineRunEvent, "execution_id" | "trace_id">>,
  state: Pick<PipelineStateResponse, "execution_id" | "trace_id"> | null,
  pipelineId: string,
): string | undefined {
  const execId = (state?.execution_id || "").trim()
  const own = execId ? events.find((e) => e.execution_id === execId && e.trace_id)?.trace_id : undefined
  if (own) return own
  if (state?.trace_id) return state.trace_id
  return events.find((e) => e.trace_id && e.execution_id !== pipelineId && e.trace_id !== pipelineId)?.trace_id
}

async function connectionConfig(id: string): Promise<Record<string, unknown> | null> {
  try {
    const r = await authFetch(API_ENDPOINTS.CONNECTIONS.GET(id), { cache: "no-store" })
    if (!r?.ok) return null
    const data = (await r.json()) as Record<string, unknown> | null
    const cfg = data?.["config"]
    return cfg && typeof cfg === "object" && !Array.isArray(cfg) ? (cfg as Record<string, unknown>) : null
  } catch {
    return null
  }
}

function firstConfigString(cfg: Record<string, unknown> | null, ...keys: string[]): string | undefined {
  for (const k of keys) {
    const v = cfg?.[k]
    if (typeof v === "string" && v.trim()) return v.trim()
  }
  return undefined
}

/**
 * The parts of a layout v2 table folder that live in the connections, not the
 * pipeline (#18): the bucket and the destination's own path prefix (the sink's
 * firstStr lookup order), and the source database the <db> folder is named
 * after (orchestrator objectLayoutV2SourceDatabase). Each part is best-effort.
 */
async function loadObjectLayoutInfo(
  destConnectionId: string,
  srcConnectionId: string,
  srcType: string
): Promise<{ bucket?: string; connPrefix?: string; database?: string }> {
  const [dest, src] = await Promise.all([
    connectionConfig(destConnectionId),
    srcConnectionId ? connectionConfig(srcConnectionId) : Promise.resolve(null),
  ])
  const family = objectLayoutSourceFamily(srcType)
  const database =
    family === "mongodb"
      ? firstConfigString(src, "database", "db_name", "db")
      : family === "oracle"
        ? firstConfigString(src, "database", "service_name", "sid")
        : firstConfigString(src, "database")
  return {
    bucket: firstConfigString(dest, "bucket", "bucket_name", "container", "container_name"),
    // undefined = the destination config could not be read, so its prefix is
    // unknown; "" = read, and the connection writes at the bucket root.
    connPrefix: dest
      ? (firstConfigString(dest, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path") ?? "")
      : undefined,
    database,
  }
}

export function PipelineMonitoringPanel(props: { pipelineId: string; variant?: "monitoring" | "table_stats" }) {
  const { pipelineId } = props
  const variant = props.variant ?? "monitoring"
  const searchParams = useSearchParams()
  const lastEditTablesTokenRef = useRef<string>("")

  const [state, setState] = useState<PipelineStateResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [stateError, setStateError] = useState<string | null>(null)
  // The rows, the cursor for the page after them, and whether one exists, in
  // one state: the Activity poll decides all three from the rows it merges
  // into, so they must change together.
  const [feed, setFeed] = useState<{
    events: PipelineRunEvent[]
    nextCursor: EventsCursor | null
    hasMore: boolean
  }>({ events: [], nextCursor: null, hasMore: false })
  const { events, nextCursor, hasMore: hasMoreEvents } = feed
  const [eventsLoading, setEventsLoading] = useState(true)
  const [eventsError, setEventsError] = useState<string | null>(null)
  const [loadingMore, setLoadingMore] = useState(false)
  const loadingMoreRef = useRef(false)
  // Every stage transition, read on its own. The paged feed seldom reaches back
  // to a long-running stage's STAGE_STARTED/STAGE_COMPLETED, so a badge read
  // from the loaded rows said "Pending" until "Load More" found them. Newer
  // transitions also arrive through the feed; groupByStage folds both in.
  const [statusEvents, setStatusEvents] = useState<PipelineRunEvent[]>([])

  const flags = useFeatureFlags()
  // The Monitoring card's sub-tab, controlled: Activity polls only while it is
  // the one on screen, and the Overview's "View in Activity" switches to it.
  const [monitorSubTab, setMonitorSubTab] = useState<string>(() =>
    variant === "table_stats" ? "table-stats" : flags.monitoringOverview ? "monitoring-overview" : "activity"
  )
  const pollInflightRef = useRef(false)

  const [showEditTables, setShowEditTables] = useState(false)
  const [tablesLoading, setTablesLoading] = useState(false)
  const [tablesError, setTablesError] = useState<string | null>(null)
  const [showSelectedTables, setShowSelectedTables] = useState(false)
  const [availableTables, setAvailableTables] = useState<ConnectionTableMetadata[]>([])
  const [tablesTruncatedTotal, setTablesTruncatedTotal] = useState<number | undefined>(undefined)
  const [lastRunRowCounts, setLastRunRowCounts] = useState<Record<string, number>>({})
  // From the same table-stats rows (#5): tables no longer selected whose counts
  // the stats still carry (a re-add defaults to loading its rows).
  const [removedTables, setRemovedTables] = useState<string[]>([])
  // Bumped after a CDC table edit: the edit keeps the execution, so the stats
  // fetch below would otherwise not see a table that just became "removed".
  const [tableStatsRefreshTick, setTableStatsRefreshTick] = useState(0)
  const [pipelineSyncMode, setPipelineSyncMode] = useState<"batch" | "cdc" | null>(null)
  const [pipelineSourceConnectionId, setPipelineSourceConnectionId] = useState<string | null>(null)
  const [pipelineSelectedTables, setPipelineSelectedTables] = useState<string[]>([])
  // Destination namespace (schema/database/dataset) the executor actually writes to.
  // Mirrors pipelines.config.destination_namespace — the authoritative value
  // resolveDestinationNamespace() reads server-side — so users can see where their
  // rows land on the destination without guessing.
  const [pipelineDestNamespace, setPipelineDestNamespace] = useState<string | null>(null)
  const [pipelineDestNamespaceKind, setPipelineDestNamespaceKind] = useState<string | null>(null)
  // Object-storage destinations (#18): a table lands in a folder, not a schema, so
  // the Tables card shows the folder path. The bucket, connection prefix and source
  // database come from the two connections' configs (best-effort; the path is
  // shown without whatever could not be read).
  const [pipelineDestType, setPipelineDestType] = useState<string | null>(null)
  const [pipelineSourceType, setPipelineSourceType] = useState<string | null>(null)
  const [objectLayoutInfo, setObjectLayoutInfo] = useState<Awaited<ReturnType<typeof loadObjectLayoutInfo>> | null>(
    null
  )
  const [cdcBackfillNewTables, setCdcBackfillNewTables] = useState(true)
  // Asked each time the CDC "Edit tables" dialog opens: can this connector
  // backfill the tables you add? Same GET the Re-snapshot card makes.
  const [cdcBackfillCapability, setCdcBackfillCapability] = useState<BackfillCapability>({ state: "unknown" })
  // A saved table edit whose added tables did NOT get their existing rows.
  // Kept on the Tables card until dismissed: a toast alone is gone in seconds,
  // and this is a data-completeness problem, not a notification.
  const [cdcEditWarning, setCdcEditWarning] = useState<TableEditOutcome | null>(null)

  function asObject(v: unknown): Record<string, unknown> | null {
    if (!v) return null
    if (typeof v === "object" && !Array.isArray(v)) return v as Record<string, unknown>
    if (typeof v === "string") {
      const s = v.trim()
      if (!s) return null
      if (s.startsWith("{") || s.startsWith("[")) {
        try {
          const parsed: unknown = JSON.parse(s)
          if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed as Record<string, unknown>
        } catch {
          // ignore
        }
      }
    }
    return null
  }

  function asString(v: unknown): string | undefined {
    if (typeof v === "string") return v
    return undefined
  }

  function inferSourceConnectionId(state: PipelineStateResponse | null, events: PipelineRunEvent[]): string | undefined {
    const details = asObject(state?.blocking_reason?.details) || asObject(state?.blocking_reason) || null
    const direct =
      asString(details?.["source_connection_id"]) ||
      asString(details?.["sourceConnectionId"]) ||
      asString(details?.["source_conn_id"])
    if (direct && direct.trim()) return direct.trim()

    // Scan events for HITL payloads that carried connection IDs (best-effort).
    for (const e of events) {
      const p = asObject(e.payload) || {}
      const meta = asObject(p["metadata"]) || {}
      const d = asObject(p["details"]) || meta
      const v = asString(d?.["source_connection_id"]) || asString(meta?.["source_connection_id"]) || asString(p["source_connection_id"])
      if (v && v.trim()) return v.trim()
    }
    return undefined
  }

  function inferTablesFromEvents(events: PipelineRunEvent[]): string[] {
    const out: string[] = []
    const push = (s: unknown) => {
      const v = typeof s === "string" ? s.trim() : ""
      if (v) out.push(v)
    }
    const pushArray = (arr: unknown) => {
      if (!Array.isArray(arr)) return
      for (const x of arr) push(x)
    }

    for (const e of events) {
      const p = asObject(e.payload) || {}
      const meta = asObject(p["metadata"]) || {}
      const d = asObject(p["details"]) || meta

      pushArray(p["selected_tables"])
      pushArray(meta["selected_tables"])
      pushArray(d["selected_tables"])

      pushArray(p["tables"])
      pushArray(meta["tables"])
      pushArray(d["tables"])

      push(meta["table_name"])
      push(p["table_name"])
      push(p["table"])
      push(meta["table"])
    }

    // uniq + stable order
    const seen = new Set<string>()
    const uniq: string[] = []
    for (const t of out) {
      if (seen.has(t)) continue
      seen.add(t)
      uniq.push(t)
    }
    return uniq
  }

  const fetchState = useCallback(async () => {
    try {
      setStateError(null)
      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, {
        cache: "no-store",
      })
      if (!res.ok) {
        const status = res.status
        if (status === 404) {
          setStateError("Pipeline not found")
        } else if (status === 403) {
          setStateError("Access denied")
        } else {
          setStateError(`Failed to load state (${status})`)
        }
        return
      }
      const data = (await res.json()) as PipelineStateResponse
      // Stabilize stage list to reduce UI flicker when projections are briefly incomplete.
      setState((prev) => {
        if (!prev?.execution_plan?.stages || !Array.isArray(prev.execution_plan.stages)) return data
        const nextStages = data?.execution_plan?.stages
        if (!Array.isArray(nextStages)) return data
        const running = ["processing", "waiting_for_user", "pending"].includes(String(data?.status || ""))
        if (running && nextStages.length > 0 && nextStages.length < prev.execution_plan.stages.length) {
          return {
            ...data,
            execution_plan: {
              ...data.execution_plan,
              stages: prev.execution_plan.stages.map((old) => {
                const upd = nextStages.find((s) => s?.id && s.id === old.id)
                return upd ? { ...old, ...upd } : old
              }),
            },
          }
        }
        return data
      })
    } catch {
      setStateError("Network error")
    } finally {
      setLoading(false)
    }
  }, [pipelineId])

  const fetchStatusEvents = useCallback(async () => {
    try {
      const qs = new URLSearchParams({
        event_types: STATUS_EVENT_TYPES.join(","),
        limit: String(STATUS_EVENTS_LIMIT),
      })
      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.EVENTS(pipelineId)}?${qs.toString()}`, {
        cache: "no-store",
      })
      // A failed read leaves the badges to the loaded rows, which is all they
      // had before this read existed; the feed's own read reports a broken
      // endpoint. Cleared, not kept, so no stale answer outlives it.
      if (!res.ok) {
        setStatusEvents([])
        return
      }
      const data = (await res.json()) as { events?: unknown }
      setStatusEvents(toRunEvents(data.events))
    } catch {
      setStatusEvents([])
    }
  }, [pipelineId])

  const fetchEvents = useCallback(async (cursor?: EventsCursor) => {
    const isLoadingMore = cursor !== undefined
    // A fresh read (mount, Reload, a pipeline refresh) re-reads the transitions
    // too; "Load More" only goes further back, which they already cover.
    if (!isLoadingMore) void fetchStatusEvents()
    if (isLoadingMore) {
      setLoadingMore(true)
      loadingMoreRef.current = true
    } else {
      setEventsLoading(true)
      setEventsError(null)
    }

    try {
      const limit = EVENTS_PAGE_SIZE
      // The cursor is opaque: the server derives it from its own sort key and
      // we echo it back untouched. Reconstructing it here (as the old
      // `before_seq=lastRow.seq` did) is how paging broke for the 21% of
      // events that carry no seq at all.
      const qs = new URLSearchParams({ limit: String(limit) })
      if (cursor) {
        qs.set("before_ts", cursor.before_ts)
        qs.set("before_seq", String(cursor.before_seq))
        qs.set("before_event_id", cursor.before_event_id)
      }
      const url = `${API_ENDPOINTS.PIPELINES.EVENTS(pipelineId)}?${qs.toString()}`

      const res = await authFetch(url, {
        cache: "no-store",
      })
      if (!res.ok) {
        setEventsError(eventsReadError(res.status))
        return
      }
      const data = (await res.json()) as {
        events?: any[]
        has_more?: boolean
        next_cursor?: EventsCursor | null
      }
      const newEvents = toRunEvents(data.events)

      // Trust the server's own end-of-stream signal, falling back to the
      // full-page heuristic only for a response that predates it.
      const hasMore = typeof data.has_more === "boolean" ? data.has_more : newEvents.length === limit
      setFeed(prev => ({
        events: isLoadingMore ? [...prev.events, ...newEvents] : newEvents,
        nextCursor: data.next_cursor ?? null,
        hasMore,
      }))
      setEventsError(null)
    } catch {
      setEventsError("network error")
    } finally {
      setEventsLoading(false)
      setLoadingMore(false)
      loadingMoreRef.current = false
    }
  }, [pipelineId, fetchStatusEvents])

  // Silent re-read of the newest page for the Activity sub-tab: no loading
  // state (the list must not blank every 5 s), merged into what is already
  // shown so pages pulled in with "Load more" survive. A failure keeps the
  // rows and says the list may be stale; it never swaps them for an error.
  const pollNewestEvents = useCallback(async () => {
    // Skipped while "Load more" is in flight: that request's cursor belongs to
    // the list as it was, and a gap-replace landing first would strand it.
    if (pollInflightRef.current || loadingMoreRef.current) return
    pollInflightRef.current = true
    try {
      const url = `${API_ENDPOINTS.PIPELINES.EVENTS(pipelineId)}?limit=${EVENTS_PAGE_SIZE}`
      const res = await authFetch(url, { cache: "no-store" })
      if (!res.ok) {
        setEventsError(eventsReadError(res.status))
        return
      }
      const data = (await res.json()) as {
        events?: unknown
        has_more?: boolean
        next_cursor?: EventsCursor | null
      }
      const fresh = toRunEvents(data.events)
      setFeed(prev => {
        const merged = mergeNewestPage(prev.events, fresh)
        if (!merged.replaced) return { ...prev, events: merged.events }
        return {
          events: merged.events,
          nextCursor: data.next_cursor ?? null,
          hasMore: typeof data.has_more === "boolean" ? data.has_more : fresh.length === EVENTS_PAGE_SIZE,
        }
      })
      setEventsError(null)
    } catch {
      setEventsError("network error")
    } finally {
      pollInflightRef.current = false
    }
  }, [pipelineId])

  useEffect(() => {
    fetchState()
    fetchEvents()
  }, [fetchState, fetchEvents])

  // Activity keeps itself current while it is open (and the page is visible).
  const activityOpen = variant === "monitoring" && monitorSubTab === "activity"
  useEffect(() => {
    if (!activityOpen) return
    const t = window.setInterval(() => {
      if (typeof document !== "undefined" && document.visibilityState === "hidden") return
      void pollNewestEvents()
    }, ACTIVITY_POLL_MS)
    return () => window.clearInterval(t)
  }, [activityOpen, pollNewestEvents])

  // Poll state while active. Status-aware interval: keep a fast cadence while
  // actively processing, back off for the slower waiting/pending states.
  useEffect(() => {
    const status = state?.status
    if (!status) return
    if (!["processing", "waiting_for_user", "pending"].includes(status)) return
    const pollIntervalMs = status === "processing" ? 2500 : 5000
    const t = setInterval(() => {
      // A hidden tab skips its reads (#13); the next visible tick reads.
      if (typeof document !== "undefined" && document.visibilityState === "hidden") return
      void fetchState()
    }, pollIntervalMs)
    return () => clearInterval(t)
  }, [state?.status, fetchState])

  const status = state?.status || (loading ? "loading" : "unknown")

  // CDC-D2: this panel read /state and nothing else, so the Table statistics tab
  // could report "running" for the very execution the Overview tab — one click
  // away, same page load — was already showing as "idle". /state freezes at
  // running when a stream dies; only /runtime can see that. Same reconciliation
  // as the status pill, the CDC action cluster and the header menu, so all five
  // surfaces now answer this question the same way.
  //
  // Only the ESCALATED verdict overrides the badge; in the ordinary case the raw
  // status word is displayed exactly as before.
  const { runtime: pipelineRuntime } = usePipelineRuntime(pipelineId)
  const reconciledStatus = useMemo(
    () => reconcilePipelineStatus(normalizePipelineStatus(state?.status), pipelineRuntime?.phase),
    [state?.status, pipelineRuntime?.phase]
  )
  const statusEscalated = reconciledStatus !== normalizePipelineStatus(state?.status)
  // Issue #20: a CDC stream that set up but never delivered a row is still "running"
  // (with "Streaming pipeline active") to /state. /runtime's waiting_for_data says so.
  const waitingForFirstData = isWaitingForFirstData(reconciledStatus, pipelineRuntime?.phase)

  const pausedByUser = state?.status === "waiting_for_user" && state?.blocking_reason?.type === "paused_by_user"
  // #14: a paused pipeline keeps its last summary ("Streaming pipeline active"),
  // and the stats card below counted its tables as Running. Both read paused now.
  const pipelinePaused = reconciledStatus === "paused" || pausedByUser

  const canStop = ["processing", "waiting_for_user", "pending"].includes(String(state?.status || ""))
  const canPause = String(state?.status || "") === "processing"
  const canResume = pausedByUser

  // Stop / Pause / Resume. The header implements the same three actions and
  // reports failure (PipelineActions.tsx) — these three used to `.catch(() =>
  // null)`, so the SAME button was honest in one place and mute in the other.
  // A 403 from the workspace-role gate looked exactly like a successful stop.
  async function runControlAction(
    action: "stop" | "pause" | "resume",
    endpoint: string,
    body?: string
  ) {
    try {
      await authFetchOrThrow(endpoint, { method: "POST", body })
      toast.success(
        action === "stop" ? "Pipeline stopped" : action === "pause" ? "Pipeline paused" : "Pipeline resumed"
      )
    } catch (e) {
      const err = classifyError(e, `pipeline.${action}` as Parameters<typeof classifyError>[1])
      toast.error(err.title, { description: err.hint ?? err.message })
    } finally {
      // Refresh either way: on failure the server's actual state is exactly
      // what the user needs to see. Through the refresh bus (#13), not only this
      // panel's own fetches: the header badge, the status strip and the Table
      // statistics card each poll on their own and otherwise kept the old status
      // until a full reload. This panel's own listener below runs onRefresh.
      emitPipelineRefresh(pipelineId)
    }
  }

  async function stopPipeline() {
    await runControlAction("stop", API_ENDPOINTS.PIPELINES.STOP(pipelineId))
  }

  async function pausePipeline() {
    await runControlAction(
      "pause",
      API_ENDPOINTS.PIPELINES.PAUSE(pipelineId),
      JSON.stringify({ execution_id: state?.execution_id })
    )
  }

  async function resumePipeline() {
    await runControlAction(
      "resume",
      API_ENDPOINTS.PIPELINES.RESUME(pipelineId),
      JSON.stringify({ execution_id: state?.execution_id })
    )
  }

  const latestTraceID = runTraceId(events, state, pipelineId)

  // The same "Step n/m" as the Overview's timeline; the stage transitions are
  // fetched apart from the paged feed, so a short page does not shorten it.
  const stepInfo = useMemo(
    () => stepInfoFromEvents([...statusEvents, ...events], state),
    [statusEvents, events, state]
  )

  const onRefresh = useCallback(async () => {
    setRefreshing(true)
    try {
      await Promise.all([fetchState(), fetchEvents()])
    } finally {
      setRefreshing(false)
    }
  }, [fetchState, fetchEvents])

  // When header actions (Run/Reload/Pause/Stop etc.) succeed, they emit a client-side refresh event.
  // Without this, fast runs can complete without this panel ever seeing the new execution_id, leaving
  // Table statistics showing the previous execution.
  useEffect(() => {
    return onPipelineRefresh((pid) => {
      if (pid !== pipelineId) return
      void onRefresh()
    })
  }, [pipelineId, onRefresh])

  const uniqStrings = useCallback((arr: string[]) => {
    const seen = new Set<string>()
    const out: string[] = []
    for (const v of arr) {
      const s = String(v || "").trim()
      if (!s || seen.has(s)) continue
      seen.add(s)
      out.push(s)
    }
    return out
  }, [])

  // Best-effort inference for "Edit tables" (kept under Table statistics).
  const inferredTables = useMemo(() => inferTablesFromEvents(events), [events])
  const displaySelectedTables = useMemo(() => {
    const src = pipelineSelectedTables.length > 0 ? pipelineSelectedTables : inferredTables
    return uniqStrings(src)
  }, [pipelineSelectedTables, inferredTables, uniqStrings])
  const inferredSourceConnectionId = useMemo(() => inferSourceConnectionId(state, events), [state, events])
  // #18: on GCS / S3 / Azure Blob (layout v2) a table lands in a folder, so the
  // example is that folder, not "<namespace>.<table>". null = not layout v2, or a
  // part the folder needs is unknown: the example falls back to the old form.
  // The pipeline's recorded layout version is not exposed, so this is inferred
  // the way the orchestrator decides it (destination, source family, prefix).
  const destFolderExample = useMemo(() => {
    if (!pipelineDestNamespace || !isLayoutV2Destination(pipelineDestType ?? undefined)) return null
    const family = objectLayoutSourceFamily(pipelineSourceType ?? undefined)
    const first = displaySelectedTables[0]
    if (!family || !first) return null
    const parts = objectLayoutTableParts(family, objectLayoutInfo?.database, first)
    const folder = objectStorageTableFolder({
      bucket: objectLayoutInfo?.bucket,
      connPrefix: objectLayoutInfo?.connPrefix,
      pipelinePrefix: pipelineDestNamespace,
      sourceFamily: family,
      database: parts.database,
      schema: parts.schema,
      table: parts.table,
    })
    if (!folder) return null
    // The destination's own path prefix could not be read: say so rather than
    // show a path that may be missing its first folders.
    return objectLayoutInfo?.connPrefix === undefined ? `…/${folder}` : folder
  }, [pipelineDestNamespace, pipelineDestType, pipelineSourceType, displaySelectedTables, objectLayoutInfo])

  // Best-effort: fetch pipeline sync_mode so we can enable CDC table editing.
  useEffect(() => {
    let cancelled = false
    getPipeline(pipelineId)
      .then((p) => {
        if (cancelled) return
        const sm = p.sync_mode
        setPipelineSyncMode(sm === "cdc" || sm === "batch" ? sm : null)
        const src = typeof p.source_connection_id === "string" ? String(p.source_connection_id).trim() : ""
        setPipelineSourceConnectionId(src || null)
        // Destination namespace — prefer the structured destination_config, fall back to
        // the legacy top-level string the executor also honors.
        const ns = String(p.destination_config?.namespace ?? p.destination_namespace ?? "").trim()
        setPipelineDestNamespace(ns || null)
        setPipelineDestNamespaceKind(
          typeof p.destination_config?.namespace_kind === "string" ? p.destination_config.namespace_kind : null
        )
        const destType = String(p.destination_connection?.connector_type || "").trim()
        const srcType = String(p.source_connection?.connector_type || "").trim()
        setPipelineDestType(destType || null)
        setPipelineSourceType(srcType || null)
        const destId = String(p.destination_connection_id || "").trim()
        if (isLayoutV2Destination(destType) && destId) {
          void loadObjectLayoutInfo(destId, src, srcType).then((info) => {
            if (!cancelled) setObjectLayoutInfo(info)
          })
        } else {
          setObjectLayoutInfo(null)
        }
        const config = p.config as Record<string, unknown> | undefined
        const raw = config?.["selected_tables"] ?? config?.["selectedTables"] ?? p.selected_tables
        const arr = Array.isArray(raw) ? raw : []
        const cleaned = uniqStrings(
          arr
            .map((t) => String(t || "").trim())
            .filter((t) => Boolean(t))
        )
        setPipelineSelectedTables(cleaned)
      })
      .catch(() => {
        if (cancelled) return
        setPipelineSyncMode(null)
        setPipelineSourceConnectionId(null)
        setPipelineSelectedTables([])
        setPipelineDestNamespace(null)
        setPipelineDestNamespaceKind(null)
        setPipelineDestType(null)
        setPipelineSourceType(null)
        setObjectLayoutInfo(null)
      })
    return () => {
      cancelled = true
    }
  }, [pipelineId])

  const isWaitingForTableSelection =
    String(state?.status || "") === "waiting_for_user" && String(state?.blocking_reason?.type || "") === "table_selection"
  // Some backends report a finished run as "stopped" even when the execution completed successfully.
  // We treat "stopped" as eligible for post-run table editing and table-stats inference.
  const hasCompletedOnce = ["completed", "stopped"].includes(String(state?.status || ""))

  // If the pipeline record doesn't have sync_mode populated (older NL flows), infer from live state.
  const effectiveSyncMode = useMemo(() => {
    if (pipelineSyncMode) return pipelineSyncMode
    const msg = String(state?.message || "").toLowerCase()
    const summary = String(state?.summary || "").toLowerCase()
    if (msg.includes("streaming") || summary.includes("streaming")) return "cdc"
    return null
  }, [pipelineSyncMode, state?.message, state?.summary])
  // Batch: this run's rows only — the events span earlier runs too. CDC runs keep
  // every metrics event (their counters belong to the stream, not one execution).
  const latestRowMetrics = useMemo(
    () => extractLatestRowMetrics(events, effectiveSyncMode === "cdc" ? undefined : state?.execution_id),
    [events, effectiveSyncMode, state?.execution_id],
  )

  const isCdcTableEdit = showEditTables && effectiveSyncMode === "cdc" && !isWaitingForTableSelection
  useEffect(() => {
    if (!isCdcTableEdit) return
    let cancelled = false
    // "unknown" until first answered: the dialog then offers the option as
    // before and the save result reports a refusal, so a slow check never
    // blocks anyone. Re-asked on every open; the answer changes only when the
    // connector is recreated, so showing the previous one meanwhile is safe.
    void fetchBackfillCapability(pipelineId).then((cap) => {
      if (!cancelled) setCdcBackfillCapability(cap)
    })
    return () => {
      cancelled = true
    }
  }, [isCdcTableEdit, pipelineId])
  const cdcBackfillUnavailableDetail =
    cdcBackfillCapability.state === "unsupported" ? cdcBackfillCapability.detail : undefined

  // For table discovery (listing tables), prefer the pipeline's configured source connection ID.
  // Event inference is best-effort and may be unavailable early in the run.
  const effectiveSourceConnectionId = pipelineSourceConnectionId || inferredSourceConnectionId

  // Fetch per-table stats for the latest execution (best-effort) so "Edit tables"
  // can auto-select for older pipelines, show last-run counts, and mark tables no
  // longer selected.
  useEffect(() => {
    let cancelled = false
    async function run() {
      if (!state?.execution_id) return

      try {
        const params = new URLSearchParams()
        params.set("execution_id", state.execution_id)
        params.set("limit", "10000")
        params.set("offset", "0")
        const url = `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/table-stats?${params.toString()}`
        const res = await authFetch(url, { cache: "no-store" })
        if (!res.ok) return
        const data = (await res.json()) as { tables?: Array<Record<string, unknown>> }
        const rawTables = Array.isArray(data?.tables) ? data.tables : []
        // CDC rows carry no run counts (inserted_rows is batch-only):
        // parseTableStatsRows ignores them there even if present (#9).
        const parsed = parseTableStatsRows(rawTables, { cdc: effectiveSyncMode === "cdc" })
        if (cancelled) return
        setLastRunRowCounts(parsed.written)
        setRemovedTables(parsed.removed)
        // Only infer selected tables when not already known/persisted. Removed
        // tables are not in parsed.names: they are no longer selected.
        if (pipelineSelectedTables.length === 0 && parsed.names.length > 0) {
          setPipelineSelectedTables(parsed.names)
        }
      } catch {
        // ignore
      }
    }
    void run()
    return () => {
      cancelled = true
    }
  }, [pipelineId, state?.execution_id, pipelineSelectedTables.length, tableStatsRefreshTick, effectiveSyncMode])

  // UX decision:
  // - On the dedicated "Table statistics" page, ALWAYS show the tables section so users can manage selection.
  // - Inside the Monitoring tab, only show it when it is actionable (HITL table selection or post-run).
  const showTablesControls = variant === "table_stats" || isWaitingForTableSelection || hasCompletedOnce
  const canOpenTablesModal = isWaitingForTableSelection || !!effectiveSourceConnectionId

  async function openEditTables() {
    if (!showTablesControls) return

    // If we're currently in the HITL table_selection step, use the same table list the executor provided.
    // This avoids showing an "edit tables" modal that can't list tables early in the run.
    if (isWaitingForTableSelection) {
      const raw = state?.blocking_reason?.available_tables
      if (Array.isArray(raw) && raw.length > 0) {
        setTablesError(null)
        setTablesTruncatedTotal(truncatedTableTotal(state?.blocking_reason?.details))
        setAvailableTables(
          raw
            .map((t) => ({
              name: String(t?.name || "").trim(),
              schema: typeof t?.schema === "string" ? t.schema : undefined,
              row_count: typeof t?.row_count === "number" ? t.row_count : undefined,
              is_exact_count: typeof t?.is_exact_count === "boolean" ? t.is_exact_count : undefined,
              columns: typeof t?.columns === "number" ? t.columns : undefined,
            }))
            .filter((t) => Boolean(t.name))
        )
        setShowEditTables(true)
        return
      }
    }

    if (!effectiveSourceConnectionId) {
      setTablesError("Source connection is not available yet — configure/validate the source connection first.")
      return
    }
    setTablesError(null)
    setTablesLoading(true)
    // Avoid showing stale table lists while we fetch fresh metadata.
    // (This especially matters when the modal was previously opened during HITL table selection.)
    setAvailableTables([])
    setTablesTruncatedTotal(undefined)
    setShowEditTables(true)
    try {
      // For editing, always load the full list so the user can add more tables.
      const resp = await getConnectionMetadata(effectiveSourceConnectionId, { limit: 5000 })
      setAvailableTables(resp.tables || [])
      setTablesTruncatedTotal(truncatedTableTotal(resp))

    } catch (e: any) {
      setTablesError(String(e?.message || e || "Failed to load tables"))
      setAvailableTables([])
      setTablesTruncatedTotal(undefined)
    } finally {
      setTablesLoading(false)
    }
  }

  // Deep-link: /pipelines/:id?tab=table-stats&editTables=1
  // Used by the header "Edit Pipeline" action to open the tables editor reliably.
  useEffect(() => {
    if (variant !== "table_stats") return
    const token = String(searchParams?.get("editTables") || "").trim()
    if (!token) return
    if (lastEditTablesTokenRef.current === token) return
    if (!canOpenTablesModal) return
    lastEditTablesTokenRef.current = token
    void openEditTables()
  }, [variant, searchParams, canOpenTablesModal])

  return (
    <Card className="border-zinc-200 dark:border-zinc-800">
      <CardHeader className="space-y-1">
        <div className="flex items-start justify-between gap-3">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-5 w-5 text-zinc-500 dark:text-zinc-400" />
              {variant === "table_stats" ? "Table statistics" : "Monitoring"}
            </CardTitle>
            <CardDescription>
              {state?.execution_id ? (
                <>
                  Execution{" "}
                  <Link className="underline" href={`/executions/${state.execution_id}`}>
                    {state.execution_id.slice(0, 8)}
                  </Link>
                  {latestTraceID ? <span className="ml-2 font-mono text-xs">trace {String(latestTraceID).slice(0, 8)}</span> : null}
                </>
              ) : (
                "No active execution yet"
              )}
            </CardDescription>
          </div>

          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={onRefresh} disabled={refreshing}>
              <RefreshCw className={`h-4 w-4 mr-2 ${refreshing ? "animate-spin" : ""}`} />
              Refresh
            </Button>
            {canPause && (
              <Button variant="outline" size="sm" onClick={pausePipeline}>
                <Pause className="h-4 w-4 mr-2" />
                Pause
              </Button>
            )}
            {canResume && (
              <Button variant="outline" size="sm" onClick={resumePipeline}>
                <Play className="h-4 w-4 mr-2" />
                Resume
              </Button>
            )}
            {canStop && (
              <Button variant="outline" size="sm" onClick={stopPipeline}>
                <StopCircle className="h-4 w-4 mr-2" />
                Stop
              </Button>
            )}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {waitingForFirstData ? (
            <Badge
              variant="outline"
              className="border-amber-300 text-amber-800 dark:border-amber-800 dark:text-amber-300"
            >
              {WAITING_FOR_FIRST_DATA_LABEL}
            </Badge>
          ) : (
            <Badge variant={statusEscalated && reconciledStatus === "failed" ? "destructive" : "outline"}>
              {statusEscalated ? reconciledStatus : status}
            </Badge>
          )}
          {stepInfo ? (
            <Badge variant="secondary">
              Step {stepInfo.current_step}/{stepInfo.total_steps}
            </Badge>
          ) : typeof state?.progress?.current_step === "number" && typeof state?.progress?.total_steps === "number" ? (
            <Badge variant="secondary">
              Step {state.progress.current_step}/{state.progress.total_steps}
            </Badge>
          ) : null}
          {state?.blocking_reason?.type ? (
            <Badge variant="secondary">{state.blocking_reason.type}</Badge>
          ) : null}
          {pipelinePaused ? (
            <span className="text-xs text-muted-foreground" data-testid="pipeline-paused-summary">
              Paused — no changes stream until you resume the pipeline.
            </span>
          ) : waitingForFirstData && pipelineRuntime?.message ? (
            <span className="text-xs text-muted-foreground">{pipelineRuntime.message}</span>
          ) : state?.summary ? (
            <span className="text-xs text-muted-foreground">{state.summary}</span>
          ) : null}
        </div>
      </CardHeader>

      <CardContent className="space-y-4">
        {stateError ? (
          <div className="rounded border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/30 dark:text-red-300">
            {stateError}
          </div>
        ) : null}
        <Tabs value={monitorSubTab} onValueChange={setMonitorSubTab}>
          {variant === "monitoring" ? (
            <TabsList className="w-full flex flex-wrap h-auto justify-start gap-1">
              {flags.monitoringOverview && (
                <TabsTrigger value="monitoring-overview">
                  <BarChart3 className="h-4 w-4 mr-2" />
                  Overview
                </TabsTrigger>
              )}
              {/* Was "Trace": the same event stream, then shown as raw event
                  codes. It now reads in words, with the codes behind each
                  row's Details toggle. */}
              <TabsTrigger value="activity">
                <List className="h-4 w-4 mr-2" />
                Activity
              </TabsTrigger>
            </TabsList>
          ) : null}

          {variant === "monitoring" && flags.monitoringOverview && (
            <TabsContent value="monitoring-overview" className="space-y-4">
              <MonitoringOverviewTab pipelineId={pipelineId} onOpenActivity={() => setMonitorSubTab("activity")} />
            </TabsContent>
          )}

          {variant === "monitoring" ? (
            <TabsContent value="activity" className="space-y-3">
            <div className="flex items-center justify-between">
              <div className="text-sm font-medium flex items-center gap-2">
                <List className="h-4 w-4" />
                What the pipeline has been doing
              </div>
              <Button variant="outline" size="sm" onClick={() => fetchEvents()} disabled={eventsLoading}>
                <RefreshCw className={`h-4 w-4 mr-2 ${eventsLoading ? "animate-spin" : ""}`} />
                Reload
              </Button>
            </div>

            {/* A failed read never borrows the empty state (F-284): with nothing
                loaded it says the read failed; with rows already on screen it
                keeps them and says they may be out of date. */}
            {eventsError && events.length === 0 ? (
              <div className="rounded border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/30 dark:text-red-300">
                Could not load events ({eventsError}).
              </div>
            ) : (
              <>
              {eventsError ? (
                <div className="rounded border border-amber-200 bg-amber-50 p-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300">
                  Could not load events ({eventsError}). The list below may be out of date.
                </div>
              ) : null}
              <ScrollArea className="h-[500px]">
                <ReasoningTimeline
                  events={events}
                  statusEvents={statusEvents}
                  loading={eventsLoading}
                  emptyMessage={
                    effectiveSyncMode === "cdc"
                      ? "Waiting for first CDC event… Changes from your source will appear here in real-time."
                      : undefined
                  }
                />
                {hasMoreEvents && !loadingMore && events.length > 0 && nextCursor && (
                  <div className="mt-3">
                    <Button
                      variant="outline"
                      size="sm"
                      className="w-full"
                      onClick={() => fetchEvents(nextCursor)}
                    >
                      Load More Events
                    </Button>
                  </div>
                )}
                {loadingMore && (
                  <div className="text-center text-sm text-muted-foreground mt-3">Loading more events…</div>
                )}
              </ScrollArea>
              </>
            )}
            </TabsContent>
          ) : null}

          {variant === "table_stats" ? (
            <TabsContent value="table-stats" className="space-y-3">
            {/*
              No fixed-height scroll box here. This used to be `h-[500px] overflow-y-auto`
              around the Tables card, the Summary AND the per-table grid, so past ~10
              tables the rows — and the grid's horizontal scrollbar — sat below the fold
              of a box inside the page. The grid now owns its scrolling (sticky header,
              frozen name column, pages), and the card above it stays short.
            */}
            <div className="space-y-3">
              {showTablesControls ? (
                  <Card className="border-zinc-200 dark:border-zinc-800">
                    <CardContent className="space-y-2 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="flex min-w-0 flex-wrap items-center gap-2">
                          <CardTitle className="text-base">Tables</CardTitle>
                          {displaySelectedTables.length > 0 ? (
                            <Badge variant="secondary" className="text-xs tabular-nums">
                              {displaySelectedTables.length} selected
                            </Badge>
                          ) : null}
                        </div>
                        <div className="flex items-center gap-2">
                          {displaySelectedTables.length > 0 ? (
                            <Button
                              size="sm"
                              variant="ghost"
                              aria-expanded={showSelectedTables}
                              onClick={() => setShowSelectedTables((v) => !v)}
                            >
                              {showSelectedTables ? "Hide list" : "Show list"}
                            </Button>
                          ) : null}
                          <Button size="sm" variant="outline" onClick={openEditTables} disabled={!canOpenTablesModal}>
                            {isWaitingForTableSelection ? "Select tables" : "Edit tables"}
                          </Button>
                        </div>
                      </div>
                      <CardDescription className="text-xs">
                        {isWaitingForTableSelection
                          ? "Select what to sync to continue this run."
                          : effectiveSyncMode === "cdc"
                            ? "Choose which tables this pipeline streams; you can also load the existing rows of tables you add. To re-read a table that is already streaming, use Re-snapshot tables below."
                            : "Edit tables for future runs (enabled after a successful run)."}
                        {!canOpenTablesModal
                          ? isWaitingForTableSelection
                            ? " (Waiting for table list…)"
                            : " (Available after the first successful run.)"
                          : null}
                      </CardDescription>

                      {cdcEditWarning ? (
                        <div
                          role="alert"
                          className="rounded-md border border-amber-200 bg-amber-50 p-3 text-amber-900 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-200"
                        >
                          <div className="flex items-start justify-between gap-3">
                            <div className="text-sm font-medium">{cdcEditWarning.title}</div>
                            <Button
                              size="sm"
                              variant="ghost"
                              className="h-6 px-2 text-xs"
                              onClick={() => setCdcEditWarning(null)}
                            >
                              Dismiss
                            </Button>
                          </div>
                          <div className="mt-1 text-xs">{cdcEditWarning.detail}</div>
                          {cdcEditWarning.warnings && cdcEditWarning.warnings.length > 0 ? (
                            <ul className="mt-1 list-disc pl-4 text-xs" data-testid="cdc-edit-warnings">
                              {cdcEditWarning.warnings.map((w) => (
                                <li key={w}>{w}</li>
                              ))}
                            </ul>
                          ) : null}
                          {cdcEditWarning.tables && cdcEditWarning.tables.length > 0 ? (
                            <div className="mt-2 flex flex-wrap gap-1">
                              {cdcEditWarning.tables.map((t) => (
                                <Badge key={t} variant="outline" className="font-mono text-xs">
                                  {t}
                                </Badge>
                              ))}
                            </div>
                          ) : null}
                        </div>
                      ) : null}

                      {pipelineDestNamespace && destFolderExample ? (
                        <div className="text-xs text-muted-foreground" data-testid="destination-folder-example">
                          Destination {kindMeta(pipelineDestNamespaceKind || "path").noun.toLowerCase()}:{" "}
                          <span className="font-mono text-zinc-900 dark:text-zinc-100">{pipelineDestNamespace}</span>
                          {" "}— each table is written to its own folder (e.g.{" "}
                          <span className="font-mono break-all">{destFolderExample}</span>)
                        </div>
                      ) : pipelineDestNamespace ? (
                        <div className="text-xs text-muted-foreground">
                          Destination {kindMeta(pipelineDestNamespaceKind || "schema").noun.toLowerCase()}:{" "}
                          <span className="font-mono text-zinc-900 dark:text-zinc-100">{pipelineDestNamespace}</span>
                          {" "}— tables land here on the destination (e.g. <span className="font-mono">{pipelineDestNamespace}.{displaySelectedTables[0]?.split(".").pop() || "<table>"}</span>)
                        </div>
                      ) : null}

                      {/* Collapsed by default: at 25+ tables the badge cloud pushed the
                          statistics off screen, and the grid below lists every table anyway. */}
                      {showSelectedTables && displaySelectedTables.length > 0 ? (
                        <div className="flex max-h-40 flex-wrap gap-2 overflow-auto rounded-md border border-zinc-200 p-2 dark:border-zinc-800">
                          {displaySelectedTables.map((t) => (
                            <Badge key={t} variant="secondary" className="font-mono text-xs">
                              {t}
                            </Badge>
                          ))}
                        </div>
                      ) : null}

                      {typeof latestRowMetrics?.read === "number" || typeof latestRowMetrics?.written === "number" ? (
                        <div className="text-xs text-muted-foreground">
                          {effectiveSyncMode === "cdc" ? "Latest data-plane rows:" : "Rows this run:"}
                          {typeof latestRowMetrics.read === "number" ? (
                            <>
                              {" "}
                              read <span className="font-mono">{Math.round(latestRowMetrics.read)}</span>
                            </>
                          ) : null}
                          {typeof latestRowMetrics.written === "number" ? (
                            <>
                              {typeof latestRowMetrics.read === "number" ? ", " : " "}
                              written <span className="font-mono">{Math.round(latestRowMetrics.written)}</span>
                            </>
                          ) : null}
                        </div>
                      ) : null}

                      {tablesLoading ? <div className="text-xs text-muted-foreground">Loading tables…</div> : null}
                      {tablesError ? <div className="text-xs text-red-600">{tablesError}</div> : null}
                    </CardContent>
                  </Card>
              ) : null}

              <TableStatisticsPanel
                pipelineId={pipelineId}
                executionId={state?.execution_id}
                pipelineStatus={reconciledStatus}
                blockingReasonType={state?.blocking_reason?.type ?? state?.blocking_reason_type}
                mode={effectiveSyncMode ?? undefined}
              />
            </div>
            </TabsContent>
          ) : null}
        </Tabs>
      </CardContent>

      <PipelineTableSelector
        isOpen={showEditTables}
        autoDismissOnResolve
        onClose={() => setShowEditTables(false)}
        onResolved={() => {
          // best-effort refresh
          fetchState()
          fetchEvents()
        }}
        onTablesSelected={async (tables) => {
          // 1) If workflow is waiting for table selection, resume the active run.
          if (isWaitingForTableSelection) {
            await resumePipelineTables(pipelineId, { execution_id: state?.execution_id, selected_tables: tables })
            // Keep UI selection in sync (useful if user reopens modal quickly).
            setPipelineSelectedTables(tables)
            setShowEditTables(false)
            fetchState()
            fetchEvents()
            return
          }

          // 2) CDC: update Debezium connector (and persist config).
          if (effectiveSyncMode === "cdc") {
            // Don't ask for a backfill the capability check already said will
            // be refused; the outcome below still says the rows were not loaded.
            const result = await updatePipelineCDCTables(pipelineId, tables, {
              backfill_newly_added: cdcBackfillNewTables && !cdcBackfillUnavailableDetail,
            })
            // The gateway answers 200 even when the backfill was refused (the
            // table list DID change), so the result has to be read, not assumed.
            // An older gateway does not name the removed tables (#16) or say the
            // pipeline is paused (#17): both are worked out here as a fallback.
            const stillSelected = makeTableMatcher(tables)
            const removed = pipelineSelectedTables.filter((t) => !stillSelected(t))
            const outcome = describeTableEditResult(result, {
              unavailableDetail: cdcBackfillUnavailableDetail,
              removed,
              paused: pipelinePaused,
            })
            setCdcEditWarning(outcome.tone === "warning" ? outcome : null)
            const notify =
              outcome.tone === "warning" ? toast.warning : outcome.tone === "info" ? toast.info : toast.success
            notify(outcome.title, {
              description: [outcome.detail, ...(outcome.warnings || [])].filter(Boolean).join(" "),
              duration: outcome.tone === "warning" ? 15000 : undefined,
            })
            setPipelineSelectedTables(tables)
            setShowEditTables(false)
            // #15: the Table statistics card and the Re-snapshot card fetch on
            // their own and stayed stale until a reload; the refresh bus reaches
            // them (and this panel's own onRefresh). The tick re-reads the table
            // stats behind "previously loaded".
            emitPipelineRefresh(pipelineId)
            setTableStatsRefreshTick((n) => n + 1)
            return
          }

          // 3) Batch: persist selection for future runs.
          await updatePipelineTables(pipelineId, tables)
          setPipelineSelectedTables(tables)
          setShowEditTables(false)
          fetchState()
          fetchEvents()
        }}
        pipelineId={pipelineId}
        executionId={state?.execution_id}
        sourceType={undefined}
        // Only a table-selection pause names the database; "Edit tables" after a run
        // lets the picker read it from the tables.
        sourceDatabase={isWaitingForTableSelection ? state?.blocking_reason?.details?.source_database : undefined}
        truncatedTotal={tablesTruncatedTotal}
        loading={tablesLoading}
        initialSelectedTables={pipelineSelectedTables}
        showCdcBackfillToggle={effectiveSyncMode === "cdc" && !isWaitingForTableSelection}
        cdcBackfillNewTables={cdcBackfillNewTables}
        onCdcBackfillNewTablesChange={setCdcBackfillNewTables}
        cdcBackfillUnavailableDetail={cdcBackfillUnavailableDetail}
        cdcBackfillBlockingOnly={isBlockingOnly(cdcBackfillCapability)}
        cdcBackfillObjectStorage={
          (cdcBackfillCapability.state === "supported" && cdcBackfillCapability.objectStorage === true) ||
          isObjectStorageDestination(pipelineDestType ?? undefined)
        }
        pipelineStatus={pipelinePaused ? "paused" : reconciledStatus}
        previouslyLoadedTables={removedTables}
        availableTables={(availableTables || []).map((t) => ({
          name: t.name,
          schema: t.schema,
          row_count:
            typeof (lastRunRowCounts[`${t.schema ? `${t.schema}.` : ""}${t.name}`] ?? lastRunRowCounts[String(t.name || "").trim()]) === "number"
              ? (lastRunRowCounts[`${t.schema ? `${t.schema}.` : ""}${t.name}`] ?? lastRunRowCounts[String(t.name || "").trim()])
              : typeof t.row_count === "number" && Number.isFinite(t.row_count) && t.row_count >= 0
                ? t.row_count
                : undefined,
          is_exact_count:
            typeof (lastRunRowCounts[`${t.schema ? `${t.schema}.` : ""}${t.name}`] ?? lastRunRowCounts[String(t.name || "").trim()]) === "number"
              ? true
              : typeof t.is_exact_count === "boolean"
                ? t.is_exact_count
                : undefined,
          columns: typeof t.columns === "number" ? t.columns : undefined,
        }))}
        title={
          isWaitingForTableSelection
            ? "Select Tables to Sync"
            : effectiveSyncMode === "cdc"
              ? "Edit CDC tables"
              : "Edit tables for this pipeline"
        }
      />
    </Card>
  )
}



