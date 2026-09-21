"use client"

// The Graph tab of a model's page: the models and pipelines that wake it, the models it
// wakes, and how each of them last ran. The rules for what a node may say live in
// modelLineage.ts; this file fetches, lays out and draws.

import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react"
import Link from "next/link"
import { useTheme } from "next-themes"
import {
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Edge,
  type Node,
  type NodeProps,
  type ReactFlowInstance,
} from "@xyflow/react"
import { AlertTriangle, Loader2, Maximize, Minus, Plus } from "lucide-react"

import { cn, formatRelativeTime } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { formatDuration, type ScheduledQuery } from "@/components/explorer/scheduledModel"
import { AFTER_UPSTREAM, type Fetched, type RunningModelsResponse } from "@/components/explorer/liveState"
import {
  LINEAGE_NODE_HEIGHT,
  LINEAGE_NODE_WIDTH,
  LINEAGE_OLDER_RUNS_LIMIT,
  LINEAGE_RUNS_LIMIT,
  SCHEDULE_LIST_LIMIT,
  buildModelLineage,
  describeLineageNode,
  initialViewport,
  layoutLineage,
  liveRunningKeys,
  lookupFromResponse,
  needsOlderRuns,
  nextRunsCursor,
  type LineageNode,
  type LineageNodeView,
  type LineageTone,
  type ModelLineage,
  type NodeLookup,
} from "@/components/explorer/modelLineage"
import { getJson } from "@/components/explorer/getJson"
import { buildRunGrid } from "@/components/explorer/modelRunGrid"
import { ModelRunGridTable } from "@/components/explorer/ModelRunGridTable"
import { ModelRunPanel, type RunPanelTarget, type RunSelection } from "@/components/explorer/ModelRunPanel"

/** Lookups in flight at once: a 50-node graph is nine rounds, not fifty requests at once. */
const LOOKUP_CONCURRENCY = 6
const LOOKUP_TIMEOUT_MS = 10_000
const SCHEDULES_TIMEOUT_MS = 15_000

/** A canvas needs a pointer to pan and room to show more than one node; anything else gets the list. */
const CANVAS_MEDIA_QUERY = "(min-width: 640px) and (pointer: fine)"

export interface ModelLineageGraphProps {
  modelId: string
  modelName: string
  /** The page's schedule for the model, or null when it has none. */
  rootSchedule: ScheduledQuery | null
  canSchedule: boolean
  running: Fetched<RunningModelsResponse>
  /** Bumped by the page's Refresh button. */
  reloadTick: number
  onLoadingChange?: (loading: boolean) => void
}

interface LoadedLineage {
  lineage: ModelLineage
  lookups: Map<string, NodeLookup>
  schedulesCapped: boolean
}

type LoadResult = { key: string; data: LoadedLineage } | { key: string; error: string }

async function lookupNode(node: LineageNode, signal: AbortSignal): Promise<NodeLookup> {
  try {
    if (node.kind === "pipeline") {
      const { status, body } = await getJson(`/api/v1/pipelines/${encodeURIComponent(node.id)}`, signal, LOOKUP_TIMEOUT_MS)
      return lookupFromResponse("pipeline", status, body)
    }
    const runsUrl = `/api/v1/explorer/saved/${encodeURIComponent(node.id)}/runs`
    const first = await getJson(`${runsUrl}?limit=${LINEAGE_RUNS_LIMIT}`, signal, LOOKUP_TIMEOUT_MS)
    const lookup = lookupFromResponse("model", first.status, first.body)
    if (lookup.status !== "model") return lookup
    // The route sends a cursor only when it has rows past the page (saved_query_schedules.go).
    const cursor = nextRunsCursor(first.body)
    const firstPage: NodeLookup = { ...lookup, hasMore: !!cursor }
    if (!cursor || !needsOlderRuns(lookup.runs)) return firstPage
    try {
      const older = await getJson(
        `${runsUrl}?limit=${LINEAGE_OLDER_RUNS_LIMIT}&before=${encodeURIComponent(cursor)}`,
        signal,
        LOOKUP_TIMEOUT_MS,
      )
      const more = lookupFromResponse("model", older.status, older.body)
      if (more.status === "model") {
        return { status: "model", runs: [...lookup.runs, ...more.runs], hasMore: !!nextRunsCursor(older.body) }
      }
    } catch (err) {
      if (signal.aborted) throw err
    }
    // The first page is a real answer on its own: an older page that failed leaves it as it was.
    return firstPage
  } catch (err) {
    // The page moved on: nothing is shown for this load, so there is nothing to say.
    if (signal.aborted) throw err
    // A timeout or a network error says nothing about the node.
    return { status: "unavailable" }
  }
}

async function runPool<T>(items: T[], limit: number, work: (item: T) => Promise<void>): Promise<void> {
  let next = 0
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const item = items[next++]
      await work(item)
    }
  })
  await Promise.all(workers)
}

function subscribeCanvasMedia(onChange: () => void): () => void {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") return () => {}
  const mq = window.matchMedia(CANVAS_MEDIA_QUERY)
  mq.addEventListener?.("change", onChange)
  return () => mq.removeEventListener?.("change", onChange)
}

function canvasMediaMatches(): boolean {
  return typeof window !== "undefined" && typeof window.matchMedia === "function" && window.matchMedia(CANVAS_MEDIA_QUERY).matches
}

const TONE_TEXT: Record<LineageTone, string> = {
  success: "text-emerald-700 dark:text-emerald-400",
  failed: "text-red-700 dark:text-red-400",
  warning: "text-amber-700 dark:text-amber-400",
  running: "text-blue-700 dark:text-blue-400",
  neutral: "text-zinc-500 dark:text-zinc-400",
}

const TONE_DOT: Record<LineageTone, string> = {
  success: "bg-emerald-500",
  failed: "bg-red-500",
  warning: "bg-amber-500",
  running: "bg-blue-500 motion-safe:animate-pulse",
  neutral: "bg-zinc-400 dark:bg-zinc-500",
}

const TONE_BORDER: Record<LineageTone, string> = {
  success: "border-l-emerald-500",
  failed: "border-l-red-500",
  warning: "border-l-amber-500",
  running: "border-l-blue-500",
  neutral: "border-l-zinc-300 dark:border-l-zinc-600",
}

function statusLine(v: LineageNodeView): string {
  const status = v.durationMs === null ? v.status : `${v.status} in ${formatDuration(v.durationMs)}`
  if (!v.at || Number.isNaN(Date.parse(v.at))) return status
  return `${status} · ${formatRelativeTime(v.at)}`
}

/**
 * A node opens the run panel when its own route vouched for it: the page's model, or a node
 * with a link. A placeholder for a node the caller can't open, or whose status could not be
 * loaded, has nothing to show there.
 */
function isClickable(n: LineageNode, v: LineageNodeView): boolean {
  return n.role === "root" || !!v.href
}

function relationText(n: LineageNode): string {
  if (n.role === "root") return "This model"
  const side = n.role === "upstream" ? "Upstream" : "Downstream"
  return n.distance === 1 ? `Direct ${side.toLowerCase()}` : `${side}, ${n.distance} hops away`
}

export function ModelLineageGraph(props: ModelLineageGraphProps) {
  const { modelId, modelName, rootSchedule, canSchedule, running, reloadTick, onLoadingChange } = props
  const [retryTick, setRetryTick] = useState(0)
  const [liveTick, setLiveTick] = useState(0)
  const headingRef = useRef<HTMLHeadingElement>(null)
  const [result, setResult] = useState<LoadResult | null>(null)
  const [view, setView] = useState<"graph" | "list">("graph")
  const canvasCapable = useSyncExternalStore(subscribeCanvasMedia, canvasMediaMatches, () => false)

  // Read when a load starts rather than being a dependency of it: the page hands over a new
  // object on every schedule reload. What in it can change the graph is `rootKey`.
  const rootScheduleRef = useRef(rootSchedule)
  useEffect(() => {
    rootScheduleRef.current = rootSchedule
  })
  const rootKey = rootSchedule
    ? JSON.stringify([
        rootSchedule.schedule_id,
        rootSchedule.status,
        rootSchedule.schedule_type,
        rootSchedule.upstream_policy ?? "",
        (rootSchedule.upstreams ?? []).map((u) => `${u.kind}:${u.id}`),
        rootSchedule.last_run_at ?? "",
        rootSchedule.updated_at ?? "",
      ])
    : ""

  const loadKey = `${modelId}|${reloadTick}|${retryTick}|${liveTick}|${rootKey}`
  const loading = result?.key !== loadKey

  useEffect(() => {
    onLoadingChange?.(loading)
    return () => onLoadingChange?.(false)
  }, [loading, onLoadingChange])

  useEffect(() => {
    const controller = new AbortController()
    let cancelled = false
    ;(async () => {
      try {
        const list = await getJson("/api/v1/explorer/schedules", controller.signal, SCHEDULES_TIMEOUT_MS)
        if (cancelled) return
        if (!list.ok) {
          setResult({ key: loadKey, error: `Could not load the models linked to this one (HTTP ${list.status}).` })
          return
        }
        const body = list.body as { schedules?: unknown; count?: unknown } | null
        if (!body || !Array.isArray(body.schedules)) {
          setResult({ key: loadKey, error: "Could not read the models linked to this one." })
          return
        }
        const schedules = body.schedules as ScheduledQuery[]
        const count = typeof body.count === "number" ? body.count : schedules.length
        const lineage = buildModelLineage(modelId, schedules, { rootSchedule: rootScheduleRef.current })
        const lookups = new Map<string, NodeLookup>()
        await runPool(lineage.nodes, LOOKUP_CONCURRENCY, async (node) => {
          lookups.set(node.key, await lookupNode(node, controller.signal))
        })
        if (cancelled) return
        // Set once every lookup is in, never node by node: a name is only shown after the
        // node's own route has answered, and a half-drawn graph would reorder under the reader.
        setResult({
          key: loadKey,
          data: { lineage, lookups, schedulesCapped: Math.max(count, schedules.length) >= SCHEDULE_LIST_LIMIT },
        })
      } catch {
        if (cancelled) return
        setResult({ key: loadKey, error: "Could not reach the server to load the graph." })
      }
    })()
    return () => {
      cancelled = true
      controller.abort()
    }
  }, [modelId, loadKey])

  const data = result && "data" in result ? result.data : null
  const error = !loading && result && "error" in result ? result.error : null

  // A model shown as running shows its last run again once it stops, and that run is the
  // one from before it started: its new run was written after the graph loaded. So when
  // the live check stops listing a drawn model as running, the graph loads again.
  const runningKeys = useMemo(() => (data ? liveRunningKeys(data.lineage.nodes, running) : null), [data, running])
  const [shownRunning, setShownRunning] = useState<Set<string> | null>(null)
  if (runningKeys && runningKeys !== shownRunning) {
    setShownRunning(runningKeys)
    const drawn = new Set(data?.lineage.nodes.map((n) => n.key))
    if (shownRunning && [...shownRunning].some((k) => drawn.has(k) && !runningKeys.has(k))) setLiveTick((t) => t + 1)
  }

  const retry = () => {
    setRetryTick((t) => t + 1)
    // The Retry button is about to go, and focus would fall back to the page.
    headingRef.current?.focus()
  }

  const views = useMemo(() => {
    if (!data) return null
    const map = new Map<string, LineageNodeView>()
    for (const n of data.lineage.nodes) {
      map.set(
        n.key,
        describeLineageNode(n, data.lookups.get(n.key), {
          rootName: modelName,
          running,
          schedulesCapped: data.schedulesCapped,
        }),
      )
    }
    return map
  }, [data, modelName, running])

  const clickableKeys = useMemo(() => {
    const keys = new Set<string>()
    if (!data || !views) return keys
    for (const n of data.lineage.nodes) if (isClickable(n, views.get(n.key)!)) keys.add(n.key)
    return keys
  }, [data, views])

  const grid = useMemo(() => (data ? buildRunGrid(data.lineage, data.lookups, { runningKeys }) : null), [data, runningKeys])

  // Kept as a key, not a node: a reload hands over new lookups, and the open panel shows them.
  const [panel, setPanel] = useState<{ key: string; selection: RunSelection } | null>(null)
  const openPanel = (key: string, selection: RunSelection = null) => setPanel({ key, selection })
  let panelTarget: RunPanelTarget | null = null
  if (panel && data && views && clickableKeys.has(panel.key)) {
    const node = data.lineage.nodes.find((n) => n.key === panel.key)
    if (node) {
      panelTarget = {
        node,
        view: views.get(node.key)!,
        lookup: data.lookups.get(node.key),
        relation: relationText(node),
        selection: panel.selection,
      }
    }
  }

  const effectiveView = canvasCapable ? view : "list"

  // The error text is an alert of its own, so this only says what an error does not.
  let announcement = ""
  if (error) announcement = ""
  else if (loading) announcement = "Loading the graph."
  else if (data && views) {
    announcement = `Graph loaded: ${data.lineage.upstreamsFound} upstream, ${data.lineage.downstreamsFound} downstream.`
    if ([...views.values()].some((v) => v.retryable)) announcement += " Some statuses could not be loaded."
  }

  return (
    <>
    <Card className="overflow-hidden p-0">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b px-4 py-3 dark:border-zinc-800">
        <div className="min-w-0">
          <h2 ref={headingRef} tabIndex={-1} className="text-sm font-semibold text-zinc-900 outline-none dark:text-white">
            Graph
          </h2>
          {data && (
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              {data.lineage.upstreamsFound} upstream · {data.lineage.downstreamsFound} downstream
            </p>
          )}
        </div>
        {canvasCapable && data && data.lineage.nodes.length > 1 && (
          <div role="group" aria-label="Show the chain as" className="flex gap-1">
            {(["graph", "list"] as const).map((v) => (
              <Button
                key={v}
                size="sm"
                variant={view === v ? "secondary" : "ghost"}
                className="h-7 px-2 text-xs"
                aria-pressed={view === v}
                onClick={() => setView(v)}
              >
                {v === "graph" ? "Graph" : "List"}
              </Button>
            ))}
          </div>
        )}
      </div>

      <p role="status" aria-live="polite" className="sr-only">
        {announcement}
      </p>

      {error ? (
        <div className="flex flex-col items-center gap-2 px-4 py-10 text-center">
          <p role="alert" className="text-sm text-red-600 dark:text-red-400">
            {error}
          </p>
          <Button size="sm" variant="outline" onClick={retry}>
            Retry
          </Button>
        </div>
      ) : !data || !views ? (
        <div className="flex items-center justify-center gap-2 py-10 text-sm text-zinc-500 dark:text-zinc-400">
          <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
          Loading the graph…
        </div>
      ) : (
        <div className={loading ? "opacity-60" : ""} aria-busy={loading}>
          <LineageNotices data={data} views={views} onRetry={retry} />
          {data.lineage.nodes.length === 1 ? (
            <EmptyLineage rootSchedule={rootSchedule} canSchedule={canSchedule} capped={data.schedulesCapped} />
          ) : effectiveView === "graph" ? (
            <ReactFlowProvider>
              <LineageCanvas lineage={data.lineage} views={views} clickableKeys={clickableKeys} onOpen={openPanel} />
            </ReactFlowProvider>
          ) : (
            <LineageList lineage={data.lineage} views={views} clickableKeys={clickableKeys} onOpen={openPanel} />
          )}
          {grid && data.lineage.nodes.length > 1 && (
            <ModelRunGridTable grid={grid} views={views} clickableKeys={clickableKeys} selected={panel} onOpen={openPanel} />
          )}
        </div>
      )}
    </Card>
    <ModelRunPanel target={panelTarget} running={running} onClose={() => setPanel(null)} />
    </>
  )
}

function LineageNotices({
  data,
  views,
  onRetry,
}: {
  data: LoadedLineage
  views: Map<string, LineageNodeView>
  onRetry: () => void
}) {
  const { lineage, schedulesCapped } = data
  const found = lineage.upstreamsFound + lineage.downstreamsFound
  const anyRetryable = [...views.values()].some((v) => v.retryable)
  if (!lineage.omitted && !schedulesCapped && !anyRetryable) return null
  return (
    <div className="space-y-2 border-b px-4 py-3 text-xs text-amber-800 dark:border-zinc-800 dark:text-amber-300">
      {lineage.omitted > 0 && (
        <p className="flex items-start gap-1.5">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          Showing {found - lineage.omitted} of {found} linked models and pipelines, nearest first.{" "}
          {lineage.omitted} more {lineage.omitted === 1 ? "is" : "are"} not drawn.
        </p>
      )}
      {schedulesCapped && (
        <p className="flex items-start gap-1.5">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          This workspace has {SCHEDULE_LIST_LIMIT} or more schedules. The graph reads the first {SCHEDULE_LIST_LIMIT}, so
          links from the rest may be missing.
        </p>
      )}
      {anyRetryable && (
        <div className="flex flex-wrap items-center gap-2">
          <p className="flex items-start gap-1.5">
            <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
            Some statuses could not be loaded.
          </p>
          <Button size="sm" variant="outline" className="h-6 px-2 text-xs" onClick={onRetry}>
            Retry
          </Button>
        </div>
      )}
    </div>
  )
}

function EmptyLineage({
  rootSchedule,
  canSchedule,
  capped,
}: {
  rootSchedule: ScheduledQuery | null
  canSchedule: boolean
  capped: boolean
}) {
  // "You can see": another member's private model can wait on this one, and the schedule
  // list leaves it out.
  let text: string
  if (!rootSchedule) text = "This query has no schedule, and no model you can see waits on it."
  else if (rootSchedule.schedule_type === AFTER_UPSTREAM)
    text = "No model or pipeline is chosen to trigger this model, and no model you can see waits on it."
  else text = "No model or pipeline feeds this model, and no model you can see waits on it. It runs on its own schedule."
  if (capped) text = `As far as this page can see, ${text.charAt(0).toLowerCase()}${text.slice(1)}`
  return (
    <div className="px-4 py-10 text-center">
      <p className="text-sm text-zinc-600 dark:text-zinc-300">{text}</p>
      <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
        {canSchedule
          ? "To link it, set a schedule to “After a pipeline or model runs”, on this model or on one that should follow it."
          : "A workspace admin can make it run after another model."}
      </p>
    </div>
  )
}

interface LineageNodeData extends Record<string, unknown> {
  view: LineageNodeView
  isRoot: boolean
  clickable: boolean
}

type LineageFlowNode = Node<LineageNodeData, "lineage">

const HIDDEN_HANDLE_STYLE = { opacity: 0, pointerEvents: "none" as const }

function LineageNodeCard({ data }: NodeProps<LineageFlowNode>) {
  const { view: v, isRoot, clickable } = data
  return (
    <div
      title={clickable ? "Show runs and SQL" : undefined}
      className={cn(
        "relative flex flex-col justify-center rounded-md border border-l-4 bg-white px-3 py-2 shadow-sm dark:border-zinc-700 dark:bg-zinc-900",
        TONE_BORDER[v.tone],
        isRoot && "ring-2 ring-zinc-900/70 dark:ring-white/70",
        clickable ? "cursor-pointer hover:bg-zinc-50 hover:shadow-md dark:hover:bg-zinc-800" : "cursor-default",
      )}
      style={{ width: LINEAGE_NODE_WIDTH, height: LINEAGE_NODE_HEIGHT }}
    >
      <Handle type="target" position={Position.Left} isConnectable={false} style={HIDDEN_HANDLE_STYLE} />
      {isRoot && (
        <span className="absolute -top-2 left-2 rounded bg-zinc-900 px-1 text-[10px] font-medium leading-4 text-white dark:bg-white dark:text-zinc-900">
          This model
        </span>
      )}
      <div className="flex min-w-0 items-baseline gap-1.5">
        <span className="shrink-0 text-[10px] uppercase tracking-wide text-zinc-400">{v.kindLabel}</span>
        {/* No link here: a click opens the run panel, which links to the model. */}
        <span
          className={cn(
            "truncate text-sm font-medium",
            v.named ? "text-zinc-900 dark:text-white" : "italic text-zinc-500 dark:text-zinc-400",
          )}
        >
          {v.title}
        </span>
      </div>
      <div className={cn("flex min-w-0 items-center gap-1.5 text-xs", TONE_TEXT[v.tone])}>
        <span className={cn("h-2 w-2 shrink-0 rounded-full", TONE_DOT[v.tone])} aria-hidden />
        <span className="truncate" title={statusLine(v)}>
          {statusLine(v)}
        </span>
      </div>
      {v.secondary && (
        <div title={v.secondary} className="truncate text-[11px] text-zinc-500 dark:text-zinc-400">
          {v.secondary}
        </div>
      )}
      <Handle type="source" position={Position.Right} isConnectable={false} style={HIDDEN_HANDLE_STYLE} />
    </div>
  )
}

const nodeTypes = { lineage: LineageNodeCard }

const NODE_HANDLES = [
  { type: "target" as const, position: Position.Left, x: 0, y: LINEAGE_NODE_HEIGHT / 2, width: 1, height: 1 },
  { type: "source" as const, position: Position.Right, x: LINEAGE_NODE_WIDTH - 1, y: LINEAGE_NODE_HEIGHT / 2, width: 1, height: 1 },
]

function LineageCanvas({
  lineage,
  views,
  clickableKeys,
  onOpen,
}: {
  lineage: ModelLineage
  views: Map<string, LineageNodeView>
  clickableKeys: Set<string>
  onOpen: (key: string) => void
}) {
  const { resolvedTheme } = useTheme()
  const dark = resolvedTheme === "dark"
  const { zoomIn, zoomOut, fitView } = useReactFlow()
  const wrapperRef = useRef<HTMLDivElement>(null)

  const layout = useMemo(() => {
    // React Flow ids are positions in this list, never the model or pipeline id: a node
    // the caller cannot open must not put its id in the DOM.
    const flowId = new Map(lineage.nodes.map((n, i) => [n.key, `n${i}`]))
    const keyByFlowId = new Map([...flowId].map(([key, id]) => [id, key]))
    const { positions, bounds } = layoutLineage(
      lineage.nodes.map((n) => n.key),
      lineage.edges,
    )
    const rootKey = lineage.nodes[0].key
    const rootPos = positions.get(rootKey) ?? { x: 0, y: 0 }
    return {
      flowId,
      keyByFlowId,
      positions,
      bounds,
      root: { ...rootPos, width: LINEAGE_NODE_WIDTH, height: LINEAGE_NODE_HEIGHT },
      // Changes only when what is drawn changes, so a status refresh keeps the reader's pan.
      signature: `${lineage.nodes.map((n) => n.key).join(",")}|${lineage.edges.map((e) => `${e.from}>${e.to}`).join(",")}`,
    }
  }, [lineage])

  const nodes: LineageFlowNode[] = useMemo(
    () =>
      lineage.nodes.map((n) => ({
        id: layout.flowId.get(n.key)!,
        type: "lineage" as const,
        position: layout.positions.get(n.key) ?? { x: 0, y: 0 },
        data: { view: views.get(n.key)!, isRoot: n.role === "root", clickable: clickableKeys.has(n.key) },
        width: LINEAGE_NODE_WIDTH,
        height: LINEAGE_NODE_HEIGHT,
        draggable: false,
        selectable: false,
        connectable: false,
        focusable: false,
        handles: NODE_HANDLES,
      })),
    [lineage, layout, views, clickableKeys],
  )

  const stroke = dark ? "#a1a1aa" : "#71717a"
  const edges: Edge[] = useMemo(
    () =>
      lineage.edges.map((e, i) => ({
        id: `e${i}`,
        source: layout.flowId.get(e.from)!,
        target: layout.flowId.get(e.to)!,
        type: "smoothstep",
        focusable: false,
        selectable: false,
        // null stops React Flow writing "Edge from n0 to n1" as a label; the type says string.
        ariaLabel: null as unknown as string,
        style: { stroke, strokeWidth: 1.5, ...(e.inactive ? { strokeDasharray: "4 4" } : {}) },
        markerEnd: { type: MarkerType.ArrowClosed, color: stroke },
      })),
    [lineage, layout, stroke],
  )

  const onInit = (instance: ReactFlowInstance<LineageFlowNode, Edge>) => {
    const el = wrapperRef.current
    const size = { width: el?.clientWidth ?? 0, height: el?.clientHeight ?? 0 }
    void instance.setViewport(initialViewport(layout.bounds, layout.root, size))
  }

  const anyInactive = lineage.edges.some((e) => e.inactive)

  return (
    <div>
      <p className="sr-only">
        A drawing of the chain. The List view has the same models and pipelines, with links and a button that shows
        each one&apos;s runs and SQL.
      </p>
      <div className="relative">
        <div ref={wrapperRef} aria-hidden="true" className="h-[420px] w-full bg-zinc-50 dark:bg-zinc-950">
          <ReactFlow<LineageFlowNode, Edge>
            key={layout.signature}
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onInit={onInit}
            onNodeClick={(_, node) => {
              const key = layout.keyByFlowId.get(node.id)
              if (key && clickableKeys.has(key)) onOpen(key)
            }}
            colorMode={dark ? "dark" : "light"}
            nodesDraggable={false}
            nodesConnectable={false}
            nodesFocusable={false}
            edgesFocusable={false}
            elementsSelectable={false}
            disableKeyboardA11y
            zoomOnScroll={false}
            zoomOnDoubleClick={false}
            preventScrolling={false}
            minZoom={0.25}
            maxZoom={1.5}
            proOptions={{ hideAttribution: true }}
          />
        </div>
        <div className="absolute right-2 top-2 flex flex-col gap-1">
          <Button size="icon" variant="outline" className="h-7 w-7 bg-white dark:bg-zinc-900" aria-label="Zoom in" onClick={() => void zoomIn()}>
            <Plus className="h-3.5 w-3.5" aria-hidden />
          </Button>
          <Button size="icon" variant="outline" className="h-7 w-7 bg-white dark:bg-zinc-900" aria-label="Zoom out" onClick={() => void zoomOut()}>
            <Minus className="h-3.5 w-3.5" aria-hidden />
          </Button>
          <Button
            size="icon"
            variant="outline"
            className="h-7 w-7 bg-white dark:bg-zinc-900"
            aria-label="Fit the whole chain"
            onClick={() => void fitView({ padding: 0.1, maxZoom: 1 })}
          >
            <Maximize className="h-3.5 w-3.5" aria-hidden />
          </Button>
        </div>
      </div>
      <LineageLegend showInactive={anyInactive} />
    </div>
  )
}

function LineageLegend({ showInactive }: { showInactive: boolean }) {
  const items: [LineageTone, string][] = [
    ["success", "Succeeded"],
    ["failed", "Failed"],
    ["warning", "Skipped or needs attention"],
    ["running", "Running"],
    ["neutral", "No run, or unknown"],
  ]
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-t px-4 py-2 text-xs text-zinc-500 dark:text-zinc-400 dark:border-zinc-800">
      {items.map(([tone, label]) => (
        <span key={tone} className="inline-flex items-center gap-1.5">
          <span className={cn("h-2 w-2 rounded-full", TONE_DOT[tone].replace("motion-safe:animate-pulse", ""))} aria-hidden />
          {label}
        </span>
      ))}
      <span className="inline-flex items-center gap-1.5">
        <svg width="20" height="6" aria-hidden>
          <line x1="0" y1="3" x2="20" y2="3" stroke="currentColor" strokeWidth="1.5" />
        </svg>
        wakes the next model
      </span>
      {showInactive && (
        <span className="inline-flex items-center gap-1.5">
          <svg width="20" height="6" aria-hidden>
            <line x1="0" y1="3" x2="20" y2="3" stroke="currentColor" strokeWidth="1.5" strokeDasharray="4 4" />
          </svg>
          won&apos;t trigger (paused)
        </span>
      )}
    </div>
  )
}

function LineageList({
  lineage,
  views,
  clickableKeys,
  onOpen,
}: {
  lineage: ModelLineage
  views: Map<string, LineageNodeView>
  clickableKeys: Set<string>
  onOpen: (key: string) => void
}) {
  const byDistance = (a: LineageNode, b: LineageNode) => a.distance - b.distance
  const ups = lineage.nodes.filter((n) => n.role === "upstream").sort(byDistance)
  const downs = lineage.nodes.filter((n) => n.role === "downstream").sort(byDistance)
  const root = lineage.nodes.filter((n) => n.role === "root")
  // The canvas draws these as dashed edges; the list says it in words.
  const paused = new Set(lineage.edges.filter((e) => e.inactive).map((e) => e.to))
  const counted = (label: string, drawn: number, found: number) =>
    found > drawn ? `${label} (${drawn} of ${found})` : `${label} (${drawn})`
  const sections: [string, LineageNode[]][] = [
    [counted("Upstream", ups.length, lineage.upstreamsFound), ups],
    ["This model", root],
    [counted("Downstream", downs.length, lineage.downstreamsFound), downs],
  ]
  return (
    <div className="divide-y dark:divide-zinc-800">
      {sections.map(([heading, rows]) => (
        <section key={heading} className="px-4 py-3">
          <h3 className="mb-2 text-xs font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">{heading}</h3>
          {rows.length === 0 ? (
            <p className="text-xs text-zinc-500 dark:text-zinc-400">None.</p>
          ) : (
            <ul className="space-y-2">
              {rows.map((n) => {
                const v = views.get(n.key)!
                return (
                  <li
                    key={n.key}
                    aria-current={n.role === "root" ? "true" : undefined}
                    className={cn("border-l-4 pl-3", TONE_BORDER[v.tone])}
                  >
                    <div className="flex min-w-0 flex-wrap items-baseline gap-x-2">
                      <span className="text-[10px] uppercase tracking-wide text-zinc-400">{v.kindLabel}</span>
                      {v.href ? (
                        <Link href={v.href} className="text-sm font-medium text-zinc-900 hover:underline dark:text-white">
                          {v.title}
                        </Link>
                      ) : (
                        <span
                          className={cn(
                            "text-sm font-medium",
                            v.named ? "text-zinc-900 dark:text-white" : "italic text-zinc-500 dark:text-zinc-400",
                          )}
                        >
                          {v.title}
                        </span>
                      )}
                      {n.role !== "root" && <span className="text-xs text-zinc-500 dark:text-zinc-400">{relationText(n)}</span>}
                      {paused.has(n.key) && <span className="text-xs text-zinc-500 dark:text-zinc-400">won&apos;t trigger (paused)</span>}
                      {clickableKeys.has(n.key) && (
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-6 px-2 text-xs"
                          aria-label={`Runs and SQL: ${v.title}`}
                          onClick={() => onOpen(n.key)}
                        >
                          Runs &amp; SQL
                        </Button>
                      )}
                    </div>
                    <div className={cn("flex items-center gap-1.5 text-xs", TONE_TEXT[v.tone])}>
                      <span className={cn("h-2 w-2 shrink-0 rounded-full", TONE_DOT[v.tone])} aria-hidden />
                      {statusLine(v)}
                    </div>
                    {v.secondary && <div className="text-[11px] text-zinc-500 dark:text-zinc-400">{v.secondary}</div>}
                  </li>
                )
              })}
            </ul>
          )}
        </section>
      ))}
    </div>
  )
}
