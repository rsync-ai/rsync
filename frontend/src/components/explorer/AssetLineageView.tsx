"use client"

// The workspace Lineage page: which pipelines write which tables, which models read them,
// and which models are refreshed out of step with what they read. The graph comes whole
// from GET /api/v1/explorer/asset-graph; assetLineage.ts holds the logic, this file draws.

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
import { AlertTriangle, Loader2, Maximize, Minus, Plus, Search } from "lucide-react"

import { cn } from "@/lib/utils"
import { authFetch } from "@/lib/api/auth-fetch"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { LINEAGE_NODE_HEIGHT, LINEAGE_NODE_WIDTH, initialViewport, layoutLineage, modelHref } from "@/components/explorer/modelLineage"
import {
  MAX_DRAWN_ASSETS,
  assetHref,
  describeAsset,
  kindLabel,
  needsAttention,
  neighbourhood,
  parseAssetGraph,
  searchAssets,
  triggerLabel,
  wholeGraph,
  type AssetGraph,
  type AssetNode,
  type AssetView,
  type AttentionItem,
  type ModelRefresh,
} from "@/components/explorer/assetLineage"

const ASSET_GRAPH_URL = "/api/v1/explorer/asset-graph"

/** A canvas needs a pointer to pan and room to show more than one node; anything else gets the list. */
const CANVAS_MEDIA_QUERY = "(min-width: 640px) and (pointer: fine)"

/** Attention items shown before "Show all". */
const ATTENTION_PREVIEW = 5

function subscribeCanvasMedia(onChange: () => void): () => void {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") return () => {}
  const mq = window.matchMedia(CANVAS_MEDIA_QUERY)
  mq.addEventListener?.("change", onChange)
  return () => mq.removeEventListener?.("change", onChange)
}

function canvasMediaMatches(): boolean {
  return typeof window !== "undefined" && typeof window.matchMedia === "function" && window.matchMedia(CANVAS_MEDIA_QUERY).matches
}

const KIND_BORDER: Record<string, string> = {
  pipeline: "border-l-violet-500",
  table: "border-l-zinc-300 dark:border-l-zinc-600",
  model: "border-l-sky-500",
}

type LoadResult = { key: string; graph: AssetGraph } | { key: string; error: string }

export function AssetLineageView({ reloadTick }: { reloadTick: number }) {
  const [retryTick, setRetryTick] = useState(0)
  const [result, setResult] = useState<LoadResult | null>(null)
  const [focusId, setFocusId] = useState<string | null>(null)
  const [query, setQuery] = useState("")
  const [view, setView] = useState<"graph" | "list">("graph")
  const graphRef = useRef<HTMLDivElement>(null)
  const canvasCapable = useSyncExternalStore(subscribeCanvasMedia, canvasMediaMatches, () => false)

  const loadKey = `${reloadTick}|${retryTick}`
  const loading = result?.key !== loadKey

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const res = await authFetch(ASSET_GRAPH_URL, { method: "GET" })
        if (cancelled) return
        if (!res.ok) {
          setResult({ key: loadKey, error: `Could not load the lineage graph (HTTP ${res.status}).` })
          return
        }
        const graph = parseAssetGraph(await res.json())
        if (!cancelled) setResult({ key: loadKey, graph })
      } catch {
        if (!cancelled) setResult({ key: loadKey, error: "Could not reach the server to load the lineage graph." })
      }
    })()
    return () => {
      cancelled = true
    }
  }, [loadKey])

  // A failed reload keeps the graph it had: the error is said above it, not instead of it.
  const [lastGraph, setLastGraph] = useState<AssetGraph | null>(null)
  if (result && "graph" in result && result.graph !== lastGraph) setLastGraph(result.graph)
  const graph = result && "graph" in result ? result.graph : lastGraph
  const error = !loading && result && "error" in result ? result.error : null

  const byId = useMemo(() => new Map((graph?.nodes ?? []).map((n) => [n.id, n])), [graph])
  const modelByNode = useMemo(() => new Map((graph?.models ?? []).map((m) => [m.node_id, m])), [graph])
  const attention = useMemo(() => (graph ? needsAttention(graph) : []), [graph])
  const attentionIds = useMemo(() => new Set(attention.map((a) => a.model.node_id)), [attention])

  // A focus the reloaded graph no longer has falls back to the workspace view.
  const focus = focusId && byId.has(focusId) ? focusId : null
  const assetView: AssetView | null = useMemo(() => {
    if (!graph) return null
    if (focus) return neighbourhood(graph, focus)
    return graph.nodes.length <= MAX_DRAWN_ASSETS ? wholeGraph(graph) : null
  }, [graph, focus])

  const showInGraph = (id: string) => {
    setFocusId(id)
    setQuery("")
    graphRef.current?.scrollIntoView?.({ behavior: "smooth", block: "start" })
  }

  const retry = () => setRetryTick((t) => t + 1)

  if (!graph) {
    return (
      <Card className="p-0">
        {error ? (
          <div className="flex flex-col items-center gap-2 px-4 py-10 text-center">
            <p role="alert" className="text-sm text-red-600 dark:text-red-400">
              {error}
            </p>
            <Button size="sm" variant="outline" onClick={retry}>
              Retry
            </Button>
          </div>
        ) : (
          <div className="flex items-center justify-center gap-2 py-10 text-sm text-zinc-500 dark:text-zinc-400">
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            Loading the lineage graph…
          </div>
        )}
      </Card>
    )
  }

  if (graph.nodes.length === 0) {
    return (
      <Card className="px-4 py-10 text-center">
        <p className="text-sm text-zinc-600 dark:text-zinc-300">Nothing to draw yet.</p>
        <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
          Lineage appears once a pipeline has written a table, or a saved query is scheduled as a model.
        </p>
      </Card>
    )
  }

  const effectiveView = canvasCapable ? view : "list"
  const focusNode = focus ? byId.get(focus) : undefined
  const focusHref = focusNode ? assetHref(focusNode) : null

  return (
    <div className="space-y-4">
      {error && (
        <div className="flex flex-wrap items-center gap-2 text-sm text-red-600 dark:text-red-400">
          <p role="alert">{error} Showing the graph from the last load.</p>
          <Button size="sm" variant="outline" className="h-7" onClick={retry}>
            Retry
          </Button>
        </div>
      )}

      <LineageSummary graph={graph} />

      {attention.length > 0 && <AttentionCard items={attention} byId={byId} onShow={showInGraph} />}

      <Card ref={graphRef} className="scroll-mt-4 p-0">
        <div className="flex flex-wrap items-start justify-between gap-3 border-b px-4 py-3 dark:border-zinc-800">
          <div className="min-w-0">
            <h2 className="text-sm font-semibold text-zinc-900 dark:text-white">
              {focusNode ? (
                <>
                  What feeds <span className="font-mono">{focusNode.name}</span>, and what it feeds
                </>
              ) : (
                "Whole workspace"
              )}
            </h2>
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              {assetView && focusNode
                ? `${assetView.upstreamFound} upstream · ${assetView.downstreamFound} downstream`
                : `${graph.nodes.length} ${graph.nodes.length === 1 ? "asset" : "assets"} · ${graph.edges.length} ${graph.edges.length === 1 ? "link" : "links"}`}
            </p>
            {focusNode && (
              <div className="mt-1 flex flex-wrap gap-2">
                {focusHref && (
                  <Link href={focusHref} className="text-xs font-medium text-blue-600 hover:underline dark:text-blue-400">
                    Open {kindLabel(focusNode.kind).toLowerCase()}
                  </Link>
                )}
                <button
                  type="button"
                  className="text-xs font-medium text-zinc-600 hover:underline dark:text-zinc-300"
                  onClick={() => setFocusId(null)}
                >
                  {graph.nodes.length <= MAX_DRAWN_ASSETS ? "Show the whole workspace" : "Clear"}
                </button>
              </div>
            )}
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <AssetSearch graph={graph} query={query} onQuery={setQuery} onPick={showInGraph} />
            {canvasCapable && assetView && assetView.nodeIds.length > 1 && (
              <div role="group" aria-label="Show lineage as" className="flex gap-1">
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
        </div>

        <div className={loading ? "opacity-60" : ""} aria-busy={loading}>
          <GraphNotices graph={graph} view={assetView} />
          {!assetView ? (
            <PickPrompt graph={graph} attention={attention} onPick={showInGraph} />
          ) : assetView.nodeIds.length === 1 ? (
            <p className="px-4 py-10 text-center text-sm text-zinc-600 dark:text-zinc-300">
              Nothing in this workspace feeds it or reads from it.
            </p>
          ) : effectiveView === "graph" ? (
            <ReactFlowProvider>
              <AssetCanvas
                graph={graph}
                view={assetView}
                byId={byId}
                modelByNode={modelByNode}
                attentionIds={attentionIds}
                onFocus={setFocusId}
              />
            </ReactFlowProvider>
          ) : (
            <AssetList
              graph={graph}
              view={assetView}
              byId={byId}
              modelByNode={modelByNode}
              attentionIds={attentionIds}
              onFocus={showInGraph}
            />
          )}
        </div>
      </Card>
    </div>
  )
}

function LineageSummary({ graph }: { graph: AssetGraph }) {
  const { stats } = graph
  const outOfStep = stats.models_with_uncovered_upstreams
  const tiles: { label: string; value: number; note?: string; warn?: boolean }[] = [
    { label: "Pipelines", value: stats.pipelines },
    { label: "Tables written", value: stats.tables },
    { label: "Models", value: stats.models },
    {
      label: "Not refreshed by what they read",
      value: outOfStep,
      note: `of ${stats.models} ${stats.models === 1 ? "model" : "models"}`,
      warn: outOfStep > 0,
    },
  ]
  return (
    <dl className="grid grid-cols-2 gap-3 lg:grid-cols-4">
      {tiles.map((t) => (
        <Card key={t.label} className="px-4 py-3">
          <dt className="text-xs text-zinc-500 dark:text-zinc-400">{t.label}</dt>
          <dd
            className={cn(
              "mt-1 text-2xl font-semibold tabular-nums",
              t.warn ? "text-amber-600 dark:text-amber-400" : "text-zinc-900 dark:text-white",
            )}
          >
            {t.value.toLocaleString()}
            {t.note && <span className="ml-1.5 text-xs font-normal text-zinc-500 dark:text-zinc-400">{t.note}</span>}
          </dd>
        </Card>
      ))}
    </dl>
  )
}

function AttentionCard({
  items,
  byId,
  onShow,
}: {
  items: AttentionItem[]
  byId: Map<string, AssetNode>
  onShow: (nodeId: string) => void
}) {
  const [expanded, setExpanded] = useState(false)
  const shown = expanded ? items : items.slice(0, ATTENTION_PREVIEW)
  return (
    <Card className="p-0">
      <div className="border-b px-4 py-3 dark:border-zinc-800">
        <h2 className="flex items-center gap-1.5 text-sm font-semibold text-zinc-900 dark:text-white">
          <AlertTriangle className="h-4 w-4 text-amber-500" aria-hidden />
          Needs attention
          <span className="font-normal text-zinc-500 dark:text-zinc-400">({items.length})</span>
        </h2>
        <p className="text-xs text-zinc-500 dark:text-zinc-400">
          Models whose refresh does not follow the data they read, or whose SQL reads a table nothing here writes.
        </p>
      </div>
      <ul aria-label="Models that need attention" className="divide-y dark:divide-zinc-800">
        {shown.map(({ model, node, reasons }) => (
          <li key={model.model_id} data-attention={model.model_id} className="flex flex-wrap items-start justify-between gap-2 px-4 py-3">
            <div className="min-w-0 space-y-1">
              <div className="flex flex-wrap items-center gap-2">
                <Link href={modelHref(model.model_id)} className="text-sm font-medium text-zinc-900 hover:underline dark:text-white">
                  {model.name || node?.name || model.model_id}
                </Link>
                <Badge variant="outline" className="text-[11px] font-normal">
                  {triggerLabel(model.trigger)}
                </Badge>
                {model.trigger_paused && (
                  <Badge variant="warning" className="text-[11px] font-normal">
                    Paused
                  </Badge>
                )}
              </div>
              <ul className="list-disc space-y-0.5 pl-4 text-xs text-zinc-600 dark:text-zinc-300">
                {reasons.map((r) => (
                  <li key={r}>{r}</li>
                ))}
              </ul>
            </div>
            {byId.has(model.node_id) && (
              <Button
                size="sm"
                variant="outline"
                className="h-7 text-xs"
                aria-label={`Show ${model.name || model.model_id} in the graph`}
                onClick={() => onShow(model.node_id)}
              >
                Show in graph
              </Button>
            )}
          </li>
        ))}
      </ul>
      {items.length > ATTENTION_PREVIEW && (
        <div className="border-t px-4 py-2 dark:border-zinc-800">
          <Button size="sm" variant="ghost" className="h-7 text-xs" onClick={() => setExpanded((e) => !e)}>
            {expanded ? "Show fewer" : `Show all ${items.length}`}
          </Button>
        </div>
      )}
    </Card>
  )
}

function AssetSearch({
  graph,
  query,
  onQuery,
  onPick,
}: {
  graph: AssetGraph
  query: string
  onQuery: (q: string) => void
  onPick: (id: string) => void
}) {
  const matches = searchAssets(graph, query)
  return (
    <div className="relative w-64 max-w-full">
      <Search className="pointer-events-none absolute left-2 top-2 h-3.5 w-3.5 text-zinc-400" aria-hidden />
      <Input
        type="search"
        value={query}
        onChange={(e) => onQuery(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Escape") onQuery("")
          if (e.key === "Enter" && matches.length > 0) onPick(matches[0].id)
        }}
        placeholder="Find a pipeline, table or model"
        aria-label="Find a pipeline, table or model"
        className="h-7 pl-7 text-xs"
      />
      {query.trim() && (
        <div className="absolute right-0 top-8 z-20 w-72 max-w-[calc(100vw-2rem)] rounded-md border bg-white p-1 shadow-lg dark:border-zinc-700 dark:bg-zinc-900">
          {matches.length === 0 ? (
            <p className="px-2 py-1.5 text-xs text-zinc-500 dark:text-zinc-400">No pipeline, table or model matches.</p>
          ) : (
            <ul aria-label="Matches">
              {matches.map((n) => (
                <li key={n.id}>
                  <button
                    type="button"
                    className="flex w-full items-baseline gap-2 rounded px-2 py-1.5 text-left hover:bg-zinc-100 dark:hover:bg-zinc-800"
                    onClick={() => onPick(n.id)}
                  >
                    <span className="shrink-0 text-[10px] uppercase tracking-wide text-zinc-400">{kindLabel(n.kind)}</span>
                    <span className="truncate font-mono text-xs text-zinc-900 dark:text-white">{n.name}</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  )
}

function GraphNotices({ graph, view }: { graph: AssetGraph; view: AssetView | null }) {
  const t = graph.stats.truncated
  const cut = [
    t.pipelines && "500 pipelines",
    t.models && "500 models",
    t.produced_tables && "5,000 written tables",
  ].filter(Boolean) as string[]
  const omitted = view?.omitted ?? 0
  if (cut.length === 0 && omitted === 0) return null
  const found = view ? view.upstreamFound + view.downstreamFound : 0
  return (
    <div className="space-y-2 border-b px-4 py-3 text-xs text-amber-800 dark:border-zinc-800 dark:text-amber-300">
      {cut.length > 0 && (
        <p className="flex items-start gap-1.5">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          This workspace has more than {cut.join(", ")}. The graph stops there, so some links are missing and a model can
          look as if nothing feeds it.
        </p>
      )}
      {omitted > 0 && (
        <p className="flex items-start gap-1.5">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          Showing {found - omitted} of {found} linked assets, nearest first. {omitted} more{" "}
          {omitted === 1 ? "is" : "are"} not drawn.
        </p>
      )}
    </div>
  )
}

function PickPrompt({
  graph,
  attention,
  onPick,
}: {
  graph: AssetGraph
  attention: AttentionItem[]
  onPick: (id: string) => void
}) {
  // Models first: they are where the lineage questions are. Attention ones lead.
  const flagged = new Set(attention.map((a) => a.model.node_id))
  const picks = graph.nodes
    .filter((n) => n.kind === "model")
    .sort((a, b) => Number(flagged.has(b.id)) - Number(flagged.has(a.id)) || a.name.localeCompare(b.name))
    .slice(0, 6)
  return (
    <div className="px-4 py-8 text-center">
      <p className="text-sm text-zinc-600 dark:text-zinc-300">
        This workspace has {graph.nodes.length.toLocaleString()} assets, too many to draw at once.
      </p>
      <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">Find one above to see what feeds it and what it feeds.</p>
      {picks.length > 0 && (
        <div className="mt-3 flex flex-wrap justify-center gap-2">
          {picks.map((n) => (
            <Button key={n.id} size="sm" variant="outline" className="h-7 font-mono text-xs" onClick={() => onPick(n.id)}>
              {n.name}
            </Button>
          ))}
        </div>
      )}
    </div>
  )
}

interface AssetNodeData extends Record<string, unknown> {
  node: AssetNode
  line: string
  focused: boolean
  flagged: boolean
}

type AssetFlowNode = Node<AssetNodeData, "asset">

const HIDDEN_HANDLE_STYLE = { opacity: 0, pointerEvents: "none" as const }

function AssetNodeCard({ data }: NodeProps<AssetFlowNode>) {
  const { node, line, focused, flagged } = data
  return (
    <div
      title={focused ? undefined : "Show what feeds it and what it feeds"}
      className={cn(
        "relative flex flex-col justify-center rounded-md border border-l-4 bg-white px-3 py-2 shadow-sm dark:border-zinc-700 dark:bg-zinc-900",
        KIND_BORDER[node.kind] ?? KIND_BORDER.table,
        focused ? "cursor-default ring-2 ring-zinc-900/70 dark:ring-white/70" : "cursor-pointer hover:bg-zinc-50 hover:shadow-md dark:hover:bg-zinc-800",
      )}
      style={{ width: LINEAGE_NODE_WIDTH, height: LINEAGE_NODE_HEIGHT }}
    >
      <Handle type="target" position={Position.Left} isConnectable={false} style={HIDDEN_HANDLE_STYLE} />
      {focused && (
        <span className="absolute -top-2 left-2 rounded bg-zinc-900 px-1 text-[10px] font-medium leading-4 text-white dark:bg-white dark:text-zinc-900">
          Selected
        </span>
      )}
      <div className="flex items-center gap-1.5">
        <span className="text-[10px] uppercase tracking-wide text-zinc-400">{kindLabel(node.kind)}</span>
        {flagged && <AlertTriangle className="h-3 w-3 text-amber-500" aria-label="Needs attention" />}
      </div>
      <span className="truncate font-mono text-sm font-medium text-zinc-900 dark:text-white" title={node.name}>
        {node.name}
      </span>
      <span className="truncate text-[11px] text-zinc-500 dark:text-zinc-400" title={line}>
        {line}
      </span>
      <Handle type="source" position={Position.Right} isConnectable={false} style={HIDDEN_HANDLE_STYLE} />
    </div>
  )
}

const nodeTypes = { asset: AssetNodeCard }

const NODE_HANDLES = [
  { type: "target" as const, position: Position.Left, x: 0, y: LINEAGE_NODE_HEIGHT / 2, width: 1, height: 1 },
  { type: "source" as const, position: Position.Right, x: LINEAGE_NODE_WIDTH - 1, y: LINEAGE_NODE_HEIGHT / 2, width: 1, height: 1 },
]

/** How an edge is drawn says how the backend knows it. */
const EVIDENCE_DASH: Record<string, string | undefined> = {
  observed: undefined,
  declared: "6 4",
  inferred: "2 4",
}

function AssetCanvas({
  graph,
  view,
  byId,
  modelByNode,
  attentionIds,
  onFocus,
}: {
  graph: AssetGraph
  view: AssetView
  byId: Map<string, AssetNode>
  modelByNode: Map<string, ModelRefresh>
  attentionIds: Set<string>
  onFocus: (id: string) => void
}) {
  const { resolvedTheme } = useTheme()
  const dark = resolvedTheme === "dark"
  const { zoomIn, zoomOut, fitView } = useReactFlow()
  const wrapperRef = useRef<HTMLDivElement>(null)

  const layout = useMemo(() => {
    // Flow ids are positions in the list, so no asset id is written into the DOM.
    const flowId = new Map(view.nodeIds.map((id, i) => [id, `n${i}`]))
    const idByFlowId = new Map([...flowId].map(([id, f]) => [f, id]))
    const { positions, bounds } = layoutLineage(view.nodeIds, view.edges)
    // The first view centres on the selected asset, or on the start of the flow.
    let anchor = view.focus ? positions.get(view.focus) : undefined
    if (!anchor) {
      for (const p of positions.values()) if (!anchor || p.x < anchor.x || (p.x === anchor.x && p.y < anchor.y)) anchor = p
    }
    return {
      flowId,
      idByFlowId,
      positions,
      bounds,
      root: { ...(anchor ?? { x: 0, y: 0 }), width: LINEAGE_NODE_WIDTH, height: LINEAGE_NODE_HEIGHT },
      signature: `${view.nodeIds.join(",")}|${view.edges.map((e) => `${e.from}>${e.to}`).join(",")}`,
    }
  }, [view])

  const nodes: AssetFlowNode[] = useMemo(
    () =>
      view.nodeIds.flatMap((id) => {
        const node = byId.get(id)
        if (!node) return []
        return [
          {
            id: layout.flowId.get(id)!,
            type: "asset" as const,
            position: layout.positions.get(id) ?? { x: 0, y: 0 },
            data: {
              node,
              line: describeAsset(node, graph, modelByNode.get(id)),
              focused: id === view.focus,
              flagged: attentionIds.has(id),
            },
            width: LINEAGE_NODE_WIDTH,
            height: LINEAGE_NODE_HEIGHT,
            draggable: false,
            selectable: false,
            connectable: false,
            focusable: false,
            handles: NODE_HANDLES,
          },
        ]
      }),
    [view, layout, byId, graph, modelByNode, attentionIds],
  )

  const stroke = dark ? "#a1a1aa" : "#71717a"
  const wake = dark ? "#60a5fa" : "#2563eb"
  const edges: Edge[] = useMemo(
    () =>
      view.edges.map((e, i) => {
        const color = e.kind === "triggers" ? wake : stroke
        const dash = EVIDENCE_DASH[e.evidence]
        return {
          id: `e${i}`,
          source: layout.flowId.get(e.from)!,
          target: layout.flowId.get(e.to)!,
          type: "smoothstep",
          focusable: false,
          selectable: false,
          // null stops React Flow writing "Edge from n0 to n1" as a label; the type says string.
          ariaLabel: null as unknown as string,
          style: { stroke: color, strokeWidth: 1.5, ...(dash ? { strokeDasharray: dash } : {}) },
          markerEnd: { type: MarkerType.ArrowClosed, color },
        }
      }),
    [view, layout, stroke, wake],
  )

  const onInit = (instance: ReactFlowInstance<AssetFlowNode, Edge>) => {
    const el = wrapperRef.current
    const size = { width: el?.clientWidth ?? 0, height: el?.clientHeight ?? 0 }
    void instance.setViewport(initialViewport(layout.bounds, layout.root, size))
  }

  const evidence = new Set(view.edges.map((e) => e.evidence))
  const anyWake = view.edges.some((e) => e.kind === "triggers")

  return (
    <div>
      <p className="sr-only">
        A drawing of the lineage. The List view has the same pipelines, tables and models, with links and a button that
        centres the graph on each one.
      </p>
      <div className="relative">
        <div ref={wrapperRef} aria-hidden="true" className="h-[480px] w-full bg-zinc-50 dark:bg-zinc-950">
          <ReactFlow<AssetFlowNode, Edge>
            key={layout.signature}
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onInit={onInit}
            onNodeClick={(_, n) => {
              const id = layout.idByFlowId.get(n.id)
              if (id && id !== view.focus) onFocus(id)
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
            minZoom={0.2}
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
            aria-label="Fit everything drawn"
            onClick={() => void fitView({ padding: 0.1, maxZoom: 1 })}
          >
            <Maximize className="h-3.5 w-3.5" aria-hidden />
          </Button>
        </div>
      </div>
      <AssetLegend evidence={evidence} wakeColor={anyWake ? wake : null} />
    </div>
  )
}

function LegendLine({ dash, color = "currentColor", label }: { dash?: string; color?: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <svg width="20" height="6" aria-hidden>
        <line x1="0" y1="3" x2="20" y2="3" stroke={color} strokeWidth="1.5" strokeDasharray={dash} />
      </svg>
      {label}
    </span>
  )
}

function AssetLegend({ evidence, wakeColor }: { evidence: Set<string>; wakeColor: string | null }) {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-t px-4 py-2 text-xs text-zinc-500 dark:text-zinc-400 dark:border-zinc-800">
      {(["pipeline", "table", "model"] as const).map((k) => (
        <span key={k} className="inline-flex items-center gap-1.5">
          <span className={cn("h-3 w-1 rounded-sm border-l-4", KIND_BORDER[k])} aria-hidden />
          {kindLabel(k)}
        </span>
      ))}
      {evidence.has("observed") && <LegendLine label="seen in a run" />}
      {evidence.has("declared") && <LegendLine dash={EVIDENCE_DASH.declared} label="configured" />}
      {evidence.has("inferred") && <LegendLine dash={EVIDENCE_DASH.inferred} label="read from a model's SQL" />}
      {wakeColor && <LegendLine color={wakeColor} label="wakes the model when it finishes" />}
    </div>
  )
}

function relationText(view: AssetView, id: string): string {
  const role = view.role.get(id)
  const d = view.distance.get(id) ?? 0
  if (role === "focus") return "Selected"
  const side = role === "upstream" ? "upstream" : "downstream"
  return d === 1 ? `Direct ${side}` : `${side.charAt(0).toUpperCase()}${side.slice(1)}, ${d} hops away`
}

function AssetList({
  graph,
  view,
  byId,
  modelByNode,
  attentionIds,
  onFocus,
}: {
  graph: AssetGraph
  view: AssetView
  byId: Map<string, AssetNode>
  modelByNode: Map<string, ModelRefresh>
  attentionIds: Set<string>
  onFocus: (id: string) => void
}) {
  const drawn = view.nodeIds.map((id) => byId.get(id)).filter((n): n is AssetNode => !!n)
  let sections: [string, AssetNode[]][]
  if (view.focus) {
    const byDistance = (a: AssetNode, b: AssetNode) => (view.distance.get(a.id) ?? 0) - (view.distance.get(b.id) ?? 0)
    const ups = drawn.filter((n) => view.role.get(n.id) === "upstream").sort(byDistance)
    const downs = drawn.filter((n) => view.role.get(n.id) === "downstream").sort(byDistance)
    const counted = (label: string, shown: number, found: number) =>
      found > shown ? `${label} (${shown} of ${found})` : `${label} (${shown})`
    sections = [
      [counted("Upstream", ups.length, view.upstreamFound), ups],
      ["Selected", drawn.filter((n) => n.id === view.focus)],
      [counted("Downstream", downs.length, view.downstreamFound), downs],
    ]
  } else {
    const ofKind = (k: string) => drawn.filter((n) => n.kind === k).sort((a, b) => a.name.localeCompare(b.name))
    sections = [
      [`Pipelines (${ofKind("pipeline").length})`, ofKind("pipeline")],
      [`Tables (${ofKind("table").length})`, ofKind("table")],
      [`Models (${ofKind("model").length})`, ofKind("model")],
    ]
  }
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
                const href = assetHref(n)
                return (
                  <li
                    key={n.id}
                    data-asset-row
                    aria-current={n.id === view.focus ? "true" : undefined}
                    className={cn("border-l-4 pl-3", KIND_BORDER[n.kind] ?? KIND_BORDER.table)}
                  >
                    <div className="flex min-w-0 flex-wrap items-baseline gap-x-2">
                      <span className="text-[10px] uppercase tracking-wide text-zinc-400">{kindLabel(n.kind)}</span>
                      {href ? (
                        <Link href={href} className="font-mono text-sm font-medium text-zinc-900 hover:underline dark:text-white">
                          {n.name}
                        </Link>
                      ) : (
                        <span className="font-mono text-sm font-medium text-zinc-900 dark:text-white">{n.name}</span>
                      )}
                      {view.focus && n.id !== view.focus && <span className="text-xs text-zinc-500 dark:text-zinc-400">{relationText(view, n.id)}</span>}
                      {attentionIds.has(n.id) && (
                        <span className="inline-flex items-center gap-1 text-xs text-amber-700 dark:text-amber-400">
                          <AlertTriangle className="h-3 w-3" aria-hidden />
                          Needs attention
                        </span>
                      )}
                      {n.id !== view.focus && (
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-6 px-2 text-xs"
                          aria-label={`Show what feeds ${n.name} and what it feeds`}
                          onClick={() => onFocus(n.id)}
                        >
                          Focus
                        </Button>
                      )}
                    </div>
                    <div className="text-[11px] text-zinc-500 dark:text-zinc-400">
                      {describeAsset(n, graph, modelByNode.get(n.id))}
                    </div>
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
