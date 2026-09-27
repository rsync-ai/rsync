"use client"

// The workspace Lineage page: which pipelines write which tables, which models read them,
// and which models are refreshed out of step with what they read. The graph comes whole
// from GET /api/v1/explorer/asset-graph; assetLineage.ts holds the logic, this file draws.

import { createContext, useCallback, useContext, useEffect, useId, useMemo, useRef, useState } from "react"
import Link from "next/link"
import {
  BaseEdge,
  EdgeLabelRenderer,
  MarkerType,
  getBezierPath,
  type Edge,
  type EdgeProps,
  type Node,
  type NodeProps,
} from "@xyflow/react"
import { AlertTriangle, Columns3, Loader2, Search } from "lucide-react"

import { cn } from "@/lib/utils"
import { authFetch } from "@/lib/api/auth-fetch"
import { listConnections } from "@/lib/api/connections"
import { useGraphLayout } from "@/lib/hooks/useGraphLayout"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { modelHref } from "@/components/explorer/modelLineage"
import {
  ColumnsPanel,
  columnsPanelHeight,
  tablesFor,
  useWarehouseTables,
  type WarehouseTablesCache,
} from "@/components/explorer/AssetColumns"
import {
  CanvasStatus,
  CardHandles,
  GraphListToggle,
  LegendLine,
  LineageFlowCanvas,
  nodeHandles,
  useCanvasCapable,
  useEdgeColors,
} from "@/components/explorer/LineageFlowCanvas"
import {
  ASSET_NODE_HEIGHT,
  ASSET_NODE_WIDTH,
  MAX_DRAWN_ASSETS,
  assetHref,
  describeAsset,
  foldUnreadTables,
  kindLabel,
  layoutAssets,
  needsAttention,
  neighbourhood,
  parseAssetGraph,
  searchAssets,
  triggerLabel,
  warehouseIds,
  warehouseLabel,
  wholeGraph,
  type AssetEdge,
  type AssetGraph,
  type AssetNode,
  type AssetView,
  type AttentionItem,
  type FoldedView,
  type ModelRefresh,
} from "@/components/explorer/assetLineage"

const ASSET_GRAPH_URL = "/api/v1/explorer/asset-graph"

/** Attention items shown before "Show all". */
const ATTENTION_PREVIEW = 5

const KIND_BORDER: Record<string, string> = {
  pipeline: "border-l-violet-500",
  table: "border-l-zinc-300 dark:border-l-zinc-600",
  model: "border-l-sky-500",
}

type LoadResult = { key: string; warehouse: string | null; graph: AssetGraph } | { key: string; error: string }

const NO_NAMES: ReadonlyMap<string, string> = new Map()

const NOTHING_OPEN: ReadonlySet<string> = new Set()

/** Hops the reader can draw on each side of a focused asset. */
const DEPTHS = [1, 2, 3, Infinity]

export function AssetLineageView({ reloadTick }: { reloadTick: number }) {
  const [retryTick, setRetryTick] = useState(0)
  const [result, setResult] = useState<LoadResult | null>(null)
  const [focusId, setFocusId] = useState<string | null>(null)
  const [query, setQuery] = useState("")
  const [view, setView] = useState<"graph" | "list">("graph")
  // The warehouse (connection id) the backend narrows the graph to; null is all of them.
  const [warehouse, setWarehouse] = useState<string | null>(null)
  const [depth, setDepth] = useState(1)
  // The assets whose next hop the reader opened, for one focus at one depth.
  const [expansion, setExpansion] = useState<{ key: string; ids: ReadonlySet<string> }>({ key: "", ids: NOTHING_OPEN })
  const tables = useWarehouseTables()
  const graphRef = useRef<HTMLDivElement>(null)
  const canvasCapable = useCanvasCapable()

  const loadKey = `${reloadTick}|${retryTick}|${warehouse ?? ""}`
  const loading = result?.key !== loadKey

  useEffect(() => {
    let cancelled = false
    const url = warehouse ? `${ASSET_GRAPH_URL}?connection_id=${encodeURIComponent(warehouse)}` : ASSET_GRAPH_URL
    ;(async () => {
      try {
        const res = await authFetch(url, { method: "GET" })
        if (cancelled) return
        if (!res.ok) {
          setResult({ key: loadKey, error: `Could not load the lineage graph (HTTP ${res.status}).` })
          return
        }
        const graph = parseAssetGraph(await res.json())
        if (!cancelled) setResult({ key: loadKey, warehouse, graph })
      } catch {
        if (!cancelled) setResult({ key: loadKey, error: "Could not reach the server to load the lineage graph." })
      }
    })()
    return () => {
      cancelled = true
    }
  }, [loadKey, warehouse])

  // A failed reload keeps the graph it had: the error is said above it, not instead of it.
  const [lastGraph, setLastGraph] = useState<AssetGraph | null>(null)
  if (result && "graph" in result && result.graph !== lastGraph) setLastGraph(result.graph)
  const graph = result && "graph" in result ? result.graph : lastGraph
  const error = !loading && result && "error" in result ? result.error : null

  // The filter offers every warehouse the last unfiltered load had, so picking one does not
  // take the others off the list.
  const [unfiltered, setUnfiltered] = useState<AssetGraph | null>(null)
  if (result && "graph" in result && result.warehouse === null && result.graph !== unfiltered) setUnfiltered(result.graph)
  const warehouses = useMemo(() => (unfiltered ? warehouseIds(unfiltered) : []), [unfiltered])
  const multiWarehouse = warehouses.length >= 2

  // Warehouse names come from the connection list, asked for only when there is a choice to
  // make. If it fails, each warehouse is shown by a short id instead: the filter still works.
  const [names, setNames] = useState<ReadonlyMap<string, string>>(NO_NAMES)
  useEffect(() => {
    if (!multiWarehouse) return
    let cancelled = false
    listConnections()
      .then(({ connections }) => {
        if (!cancelled) setNames(new Map((connections ?? []).map((c) => [c.id, c.name])))
      })
      .catch(() => {})
    return () => {
      cancelled = true
    }
  }, [multiWarehouse, reloadTick])

  // Each asset is labelled with its warehouse only while more than one is drawn.
  const warehouseNames = useMemo(
    () => (multiWarehouse && warehouse === null ? new Map(warehouses.map((id) => [id, warehouseLabel(id, names)])) : null),
    [multiWarehouse, warehouse, warehouses, names],
  )
  const scopeName = warehouse ? `Everything in ${warehouseLabel(warehouse, names)}` : "Whole workspace"
  const warehouseFilter = (multiWarehouse || warehouse !== null) && (
    <WarehouseFilter warehouses={warehouses} names={names} value={warehouse} onChange={setWarehouse} />
  )

  const byId = useMemo(() => new Map((graph?.nodes ?? []).map((n) => [n.id, n])), [graph])
  const modelByNode = useMemo(() => new Map((graph?.models ?? []).map((m) => [m.node_id, m])), [graph])
  const attention = useMemo(() => (graph ? needsAttention(graph) : []), [graph])
  const attentionIds = useMemo(() => new Set(attention.map((a) => a.model.node_id)), [attention])

  // A focus the reloaded graph no longer has falls back to the workspace view.
  const focus = focusId && byId.has(focusId) ? focusId : null
  // What is drawn: a new one starts from its own first view.
  const signature = `${warehouse ?? ""}|${focus ?? ""}|${focus ? depth : ""}`
  const expanded = expansion.key === signature ? expansion.ids : NOTHING_OPEN
  const expand = useCallback(
    (id: string) =>
      setExpansion((prev) => ({ key: signature, ids: new Set([...(prev.key === signature ? prev.ids : NOTHING_OPEN), id]) })),
    [signature],
  )
  const assetView: AssetView | null = useMemo(() => {
    if (!graph) return null
    if (focus) return neighbourhood(graph, focus, { depth, expanded })
    return graph.nodes.length <= MAX_DRAWN_ASSETS ? wholeGraph(graph) : null
  }, [graph, focus, depth, expanded])

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
      <div className="space-y-4">
        {warehouseFilter}
        <Card className="px-4 py-10 text-center">
          <p className="text-sm text-zinc-600 dark:text-zinc-300">
            {warehouse ? "Nothing in this warehouse to draw." : "Nothing to draw yet."}
          </p>
          <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
            Lineage appears once a pipeline has written a table, or a saved query is scheduled as a model.
          </p>
        </Card>
      </div>
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

      {warehouseFilter}

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
                scopeName
              )}
            </h2>
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              {assetView && focusNode
                ? `${shownOf(assetView.upstreamShown, assetView.upstreamFound)} upstream · ${shownOf(assetView.downstreamShown, assetView.downstreamFound)} downstream`
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
                  {graph.nodes.length <= MAX_DRAWN_ASSETS ? (warehouse ? "Show the whole warehouse" : "Show the whole workspace") : "Clear"}
                </button>
              </div>
            )}
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <AssetSearch graph={graph} warehouseNames={warehouseNames} query={query} onQuery={setQuery} onPick={showInGraph} />
            {focusNode && assetView && assetView.upstreamFound + assetView.downstreamFound > 0 && (
              <DepthPicker value={depth} onChange={setDepth} />
            )}
            {canvasCapable && assetView && assetView.nodeIds.length > 1 && (
              <GraphListToggle label="Show lineage as" value={view} onChange={setView} />
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
            <AssetCanvas
              graph={graph}
              view={assetView}
              byId={byId}
              modelByNode={modelByNode}
              attentionIds={attentionIds}
              warehouseNames={warehouseNames}
              scopeName={scopeName}
              signature={signature}
              tables={tables}
              onFocus={setFocusId}
              onExpand={expand}
            />
          ) : (
            <AssetList
              graph={graph}
              view={assetView}
              byId={byId}
              modelByNode={modelByNode}
              attentionIds={attentionIds}
              warehouseNames={warehouseNames}
              tables={tables}
              onFocus={showInGraph}
              onExpand={expand}
            />
          )}
        </div>
      </Card>
    </div>
  )
}

/** "3" when every one is drawn, "2 of 3" when some are not. */
function shownOf(shown: number, found: number): string {
  return shown === found ? `${found}` : `${shown} of ${found}`
}

function DepthPicker({ value, onChange }: { value: number; onChange: (depth: number) => void }) {
  return (
    <div role="group" aria-label="Hops drawn on each side" className="flex items-center gap-1">
      <span className="text-xs text-zinc-500 dark:text-zinc-400" aria-hidden>
        Hops
      </span>
      {DEPTHS.map((d) => (
        <Button
          key={d}
          size="sm"
          variant={value === d ? "secondary" : "ghost"}
          className="h-7 min-w-7 px-2 text-xs"
          aria-pressed={value === d}
          aria-label={d === Infinity ? "Every hop" : `${d} ${d === 1 ? "hop" : "hops"}`}
          onClick={() => onChange(d)}
        >
          {d === Infinity ? "All" : d}
        </Button>
      ))}
    </div>
  )
}

function WarehouseFilter({
  warehouses,
  names,
  value,
  onChange,
}: {
  warehouses: string[]
  names: ReadonlyMap<string, string>
  value: string | null
  onChange: (id: string | null) => void
}) {
  const id = useId()
  const options = warehouses
    .map((w) => ({ id: w, label: warehouseLabel(w, names) }))
    .sort((a, b) => a.label.localeCompare(b.label))
  return (
    <div className="flex items-center gap-2">
      <label htmlFor={id} className="text-xs font-medium text-zinc-600 dark:text-zinc-300">
        Warehouse
      </label>
      <select
        id={id}
        className="h-7 max-w-64 rounded-md border border-zinc-300 bg-white px-2 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500 dark:border-zinc-700 dark:bg-zinc-900"
        value={value ?? ""}
        onChange={(e) => onChange(e.target.value || null)}
      >
        <option value="">All warehouses</option>
        {options.map((o) => (
          <option key={o.id} value={o.id}>
            {o.label}
          </option>
        ))}
      </select>
    </div>
  )
}

/** The warehouse an asset is in, when more than one is drawn; null otherwise. */
function warehouseOf(node: AssetNode, names: ReadonlyMap<string, string> | null): string | null {
  return (names && node.connection_id && names.get(node.connection_id)) || null
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
  warehouseNames,
  query,
  onQuery,
  onPick,
}: {
  graph: AssetGraph
  warehouseNames: ReadonlyMap<string, string> | null
  query: string
  onQuery: (q: string) => void
  onPick: (id: string) => void
}) {
  const matches = useMemo(() => searchAssets(graph, query), [graph, query])
  const listId = useId()
  // The highlighted match belongs to the query it was chosen for: typing starts at the top.
  const [active, setActive] = useState({ query, index: 0 })
  const index = active.query === query ? Math.min(active.index, matches.length - 1) : 0
  const [open, setOpen] = useState(true)
  const shown = open && query.trim() !== ""
  const optionId = (i: number) => `${listId}-${i}`

  const move = (by: number) => {
    if (matches.length === 0) return
    setOpen(true)
    setActive({ query, index: (index + by + matches.length) % matches.length })
  }

  return (
    <div className="relative w-64 max-w-full">
      <Search className="pointer-events-none absolute left-2 top-2 h-3.5 w-3.5 text-zinc-400" aria-hidden />
      <Input
        type="search"
        role="combobox"
        aria-expanded={shown}
        aria-controls={listId}
        aria-autocomplete="list"
        aria-activedescendant={shown && matches.length > 0 ? optionId(index) : undefined}
        value={query}
        onChange={(e) => {
          setOpen(true)
          onQuery(e.target.value)
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        onKeyDown={(e) => {
          if (e.key === "ArrowDown" || e.key === "ArrowUp") {
            e.preventDefault()
            move(e.key === "ArrowDown" ? 1 : -1)
          } else if (e.key === "Escape") {
            onQuery("")
          } else if (e.key === "Enter" && shown && matches.length > 0) {
            e.preventDefault()
            onPick(matches[index].id)
          }
        }}
        placeholder="Find a pipeline, table or model"
        aria-label="Find a pipeline, table or model"
        className="h-7 pl-7 text-xs"
      />
      <p role="status" className="sr-only">
        {shown ? `${matches.length} ${matches.length === 1 ? "match" : "matches"}` : ""}
      </p>
      {shown && (
        <div className="absolute right-0 top-8 z-20 w-72 max-w-[calc(100vw-2rem)] rounded-md border bg-white p-1 shadow-lg dark:border-zinc-700 dark:bg-zinc-900">
          {matches.length === 0 ? (
            <p className="px-2 py-1.5 text-xs text-zinc-500 dark:text-zinc-400">No pipeline, table or model matches.</p>
          ) : (
            <ul id={listId} role="listbox" aria-label="Matches">
              {matches.map((n, i) => (
                <li
                  key={n.id}
                  id={optionId(i)}
                  role="option"
                  aria-selected={i === index}
                  className={cn(
                    "flex cursor-pointer items-baseline gap-2 rounded px-2 py-1.5",
                    i === index ? "bg-zinc-100 dark:bg-zinc-800" : "hover:bg-zinc-50 dark:hover:bg-zinc-800/60",
                  )}
                  // Keeps focus in the box, so the list is not closed by the blur before the click lands.
                  onMouseDown={(e) => e.preventDefault()}
                  onMouseMove={() => i !== index && setActive({ query, index: i })}
                  onClick={() => onPick(n.id)}
                >
                  {/* The spaces are for screen readers, which read the option as one string. */}
                  <span className="shrink-0 text-[10px] uppercase tracking-wide text-zinc-400">{kindLabel(n.kind)}</span>{" "}
                  <span className="truncate font-mono text-xs text-zinc-900 dark:text-white">{n.name}</span>
                  {warehouseOf(n, warehouseNames) && (
                    <>
                      {" "}
                      <span className="ml-auto shrink-0 truncate text-[11px] text-zinc-500 dark:text-zinc-400">
                        {warehouseOf(n, warehouseNames)}
                      </span>
                    </>
                  )}
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
  const shown = view ? view.upstreamShown + view.downstreamShown : 0
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
          Drawing the nearest {shown} of the {shown + omitted} assets in reach, to keep it readable. {omitted} more{" "}
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

/** What a card on the canvas can ask of the page: the drawing is out of reach of its props. */
interface CardActions {
  /** Draws the next hop beyond an asset. `flowId` is its card, which the view keeps still. */
  expand: (id: string, flowId: string) => void
  toggleColumns: (node: AssetNode, flowId: string) => void
  tables: WarehouseTablesCache
}

const CardActionsContext = createContext<CardActions | null>(null)

interface AssetNodeData extends Record<string, unknown> {
  node: AssetNode
  line: string
  warehouse: string | null
  focused: boolean
  flagged: boolean
  /** The side of the focus it is on; null for the focus, or with no focus. */
  side: "upstream" | "downstream" | null
  /** Its next hop away from the focus that is not drawn. */
  hidden: number
  /** The height of its open column list; 0 when the list is shut. */
  panel: number
}

type AssetFlowNode = Node<AssetNodeData, "asset">

/** The border-t and py-2 around a card's column list. */
const COLUMNS_FRAME_HEIGHT = 17

/** Card controls: out of the tab order (the canvas is hidden from screen readers; the List has them) and not a drag. */
const IN_CARD = { tabIndex: -1 }

function AssetNodeCard({ id, data }: NodeProps<AssetFlowNode>) {
  const { node, line, warehouse, focused, flagged, side, hidden, panel } = data
  const columnsOpen = panel > 0
  const actions = useContext(CardActionsContext)
  const stop = (act: () => void) => (e: { stopPropagation: () => void }) => {
    e.stopPropagation()
    act()
  }
  return (
    <div
      title={focused ? undefined : "Show what feeds it and what it feeds"}
      className={cn(
        "relative flex flex-col rounded-md border border-l-4 bg-white shadow-sm dark:border-zinc-700 dark:bg-zinc-900",
        KIND_BORDER[node.kind] ?? KIND_BORDER.table,
        focused ? "cursor-default ring-2 ring-zinc-900/70 dark:ring-white/70" : "cursor-pointer hover:shadow-md",
      )}
      style={{ width: ASSET_NODE_WIDTH, height: ASSET_NODE_HEIGHT + panel }}
    >
      <CardHandles top={ASSET_NODE_HEIGHT / 2} />
      {focused && (
        <span className="absolute -top-2 left-2 rounded bg-zinc-900 px-1 text-[10px] font-medium leading-4 text-white dark:bg-white dark:text-zinc-900">
          Selected
        </span>
      )}
      <div
        className={cn("flex shrink-0 flex-col justify-center px-3 py-2", !focused && "hover:bg-zinc-50 dark:hover:bg-zinc-800")}
        style={{ height: ASSET_NODE_HEIGHT - 2 }}
      >
        <div className="flex min-w-0 items-center gap-1.5">
          <span className="shrink-0 text-[10px] uppercase tracking-wide text-zinc-400">{kindLabel(node.kind)}</span>
          {warehouse && (
            <span className="truncate text-[10px] text-zinc-400" title={warehouse}>
              · {warehouse}
            </span>
          )}
          {flagged && <AlertTriangle className="h-3 w-3 shrink-0 text-amber-500" aria-label="Needs attention" />}
          {node.kind === "table" && actions && (
            <button
              type="button"
              {...IN_CARD}
              aria-expanded={columnsOpen}
              title={columnsOpen ? "Hide its columns" : "Show its columns"}
              className="nodrag nopan ml-auto inline-flex shrink-0 items-center gap-1 rounded px-1 text-[10px] text-zinc-500 hover:bg-zinc-100 hover:text-zinc-900 dark:text-zinc-400 dark:hover:bg-zinc-800 dark:hover:text-white"
              onClick={stop(() => actions.toggleColumns(node, id))}
            >
              <Columns3 className="h-3 w-3" aria-hidden />
              {columnsOpen ? "Hide columns" : "Columns"}
            </button>
          )}
        </div>
        <span className="truncate font-mono text-sm font-medium text-zinc-900 dark:text-white" title={node.name}>
          {node.name}
        </span>
        <span className="truncate text-[11px] text-zinc-500 dark:text-zinc-400" title={line}>
          {line}
        </span>
      </div>
      {columnsOpen && actions && (
        // Reading columns is not asking to focus the table.
        <div
          className="nodrag nopan min-h-0 flex-1 cursor-default overflow-hidden border-t px-3 py-2 dark:border-zinc-800"
          onClick={(e) => e.stopPropagation()}
        >
          <ColumnsPanel
            node={node}
            tables={tablesFor(node, actions.tables)}
            onRetry={() => node.connection_id && actions.tables.load(node.connection_id)}
            inCanvas
            className="h-full"
          />
        </div>
      )}
      {hidden > 0 && side && actions && (
        <button
          type="button"
          {...IN_CARD}
          title={`Show ${hidden} more ${side}`}
          className={cn(
            "nodrag nopan absolute z-10 -translate-y-1/2 rounded-full border bg-white px-1.5 text-[10px] font-medium leading-4 text-zinc-700 shadow-sm hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-200 dark:hover:bg-zinc-800",
            side === "upstream" ? "-left-3 -translate-x-1/2" : "-right-3 translate-x-1/2",
          )}
          style={{ top: ASSET_NODE_HEIGHT / 2 }}
          onClick={stop(() => actions.expand(node.id, id))}
        >
          +{hidden}
        </button>
      )}
    </div>
  )
}

interface FoldNodeData extends Record<string, unknown> {
  count: number
}

type FoldFlowNode = Node<FoldNodeData, "fold">

type DrawnNode = AssetFlowNode | FoldFlowNode

/** A pipeline's tables that nothing drawn reads, as one card that opens in place. */
function FoldCard({ data }: NodeProps<FoldFlowNode>) {
  return (
    <div
      title="Show these tables"
      className={cn(
        "relative flex cursor-pointer flex-col justify-center rounded-md border border-l-4 border-dashed bg-white px-3 py-2 shadow-sm hover:bg-zinc-50 hover:shadow-md dark:border-zinc-700 dark:bg-zinc-900 dark:hover:bg-zinc-800",
        KIND_BORDER.table,
      )}
      style={{ width: ASSET_NODE_WIDTH, height: ASSET_NODE_HEIGHT }}
    >
      <CardHandles top={ASSET_NODE_HEIGHT / 2} />
      <span className="text-[10px] uppercase tracking-wide text-zinc-400">Tables</span>
      <span className="truncate text-sm font-medium text-zinc-900 dark:text-white">{data.count} tables no model reads</span>
      <span className="truncate text-[11px] text-zinc-500 dark:text-zinc-400">Click to show them</span>
    </div>
  )
}

const nodeTypes = { asset: AssetNodeCard, fold: FoldCard }

const ASSET_HANDLES = nodeHandles(ASSET_NODE_WIDTH, ASSET_NODE_HEIGHT / 2)

const KIND_MAP_FILL: Record<string, string> = { pipeline: "#8b5cf6", table: "#a1a1aa", model: "#0ea5e9" }

/** How an edge is drawn says how the backend knows it. */
const EVIDENCE_DASH: Record<string, string | undefined> = {
  observed: undefined,
  declared: "6 4",
  inferred: "2 4",
}

const EDGE_KIND_TEXT: Record<string, string> = {
  writes: "Writes",
  materializes: "Builds",
  reads: "Read by",
  triggers: "Wakes when it finishes",
}

const EVIDENCE_TEXT: Record<string, string> = {
  observed: "seen in a run",
  declared: "configured",
  inferred: "read from a model's SQL",
}

/** What an edge says when the pointer rests on it. */
export function edgeLabel(e: Pick<AssetEdge, "kind" | "evidence">): string {
  const kind = EDGE_KIND_TEXT[e.kind] ?? e.kind
  const evidence = EVIDENCE_TEXT[e.evidence]
  return evidence ? `${kind} · ${evidence}` : kind
}

interface EvidenceEdgeData extends Record<string, unknown> {
  label: string
  color: string
}

type EvidenceFlowEdge = Edge<EvidenceEdgeData, "evidence">

/** A curved edge with a dot at its middle that names the link and how it is known. */
function EvidenceEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style,
  markerEnd,
  data,
}: EdgeProps<EvidenceFlowEdge>) {
  const [path, labelX, labelY] = getBezierPath({ sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition })
  return (
    <>
      <BaseEdge id={id} path={path} style={style} markerEnd={markerEnd} />
      {data && (
        <EdgeLabelRenderer>
          <div
            className="nodrag nopan group absolute flex h-4 w-4 items-center justify-center"
            style={{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`, pointerEvents: "all" }}
          >
            <span
              className="block h-2 w-2 rounded-full border-[1.5px] bg-zinc-50 dark:bg-zinc-950"
              style={{ borderColor: data.color }}
            />
            <span className="pointer-events-none absolute left-1/2 top-4 z-10 hidden -translate-x-1/2 whitespace-nowrap rounded border bg-white px-1.5 py-0.5 text-[10px] text-zinc-700 shadow-sm group-hover:block dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-200">
              {data.label}
            </span>
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  )
}

const edgeTypes = { evidence: EvidenceEdge }

/** What one layout is of; the drawing is made from it, so cards and positions always agree. */
interface AssetDrawingInput {
  /** The canvas this drawing belongs to (see AssetLineageView's signature). */
  signature: string
  view: AssetView
  folded: FoldedView
  /** Tables whose columns are open → the height their list adds under the card. */
  panels: ReadonlyMap<string, number>
  /** Asset id → card id. Card ids are graph positions, so no asset id is written into the DOM. */
  flowId: Map<string, string>
}

function layoutDrawing(input: AssetDrawingInput) {
  return layoutAssets(input.folded.nodeIds, input.folded.edges, (id) => ({
    width: ASSET_NODE_WIDTH,
    height: ASSET_NODE_HEIGHT + (input.panels.get(id) ?? 0),
  }))
}

function AssetCanvas({
  graph,
  view,
  byId,
  modelByNode,
  attentionIds,
  warehouseNames,
  scopeName,
  signature,
  tables,
  onFocus,
  onExpand,
}: {
  graph: AssetGraph
  view: AssetView
  byId: Map<string, AssetNode>
  modelByNode: Map<string, ModelRefresh>
  attentionIds: Set<string>
  warehouseNames: ReadonlyMap<string, string> | null
  /** What the unfocused drawing is of: the workspace, or one warehouse of it. */
  scopeName: string
  /** Names what is drawn; the reader's opened boxes and columns last as long as it does. */
  signature: string
  tables: WarehouseTablesCache
  onFocus: (id: string) => void
  onExpand: (id: string) => void
}) {
  const { stroke, accent } = useEdgeColors()
  // The boxes of unread tables the reader opened, or all of them.
  const [opened, setOpened] = useState<{ signature: string; open: ReadonlySet<string> | "all" }>({
    signature,
    open: NOTHING_OPEN,
  })
  const open = opened.signature === signature ? opened.open : NOTHING_OPEN
  const setOpen = (next: ReadonlySet<string> | "all") => setOpened({ signature, open: next })
  // The tables whose columns are shown on their cards.
  const [columns, setColumns] = useState<{ signature: string; open: ReadonlySet<string> }>({ signature, open: NOTHING_OPEN })
  const columnsOpen = columns.signature === signature ? columns.open : NOTHING_OPEN
  // The card the reader last acted on, kept still while the drawing moves around it.
  const [anchor, setAnchor] = useState<{ signature: string; flowId: string } | null>(null)

  const folded = useMemo(() => foldUnreadTables(view, graph, open), [view, graph, open])
  // Each open column list as tall as what it shows, so one column does not get ten rows' room.
  const panels = useMemo(() => {
    const heights = new Map<string, number>()
    for (const id of columnsOpen) {
      const node = byId.get(id)
      if (node) heights.set(id, COLUMNS_FRAME_HEIGHT + columnsPanelHeight(node, tablesFor(node, tables)))
    }
    return heights
  }, [columnsOpen, byId, tables])
  const indexOf = useMemo(() => new Map(graph.nodes.map((n, i) => [n.id, i])), [graph])

  const input: AssetDrawingInput = useMemo(() => {
    const flowId = new Map<string, string>()
    for (const id of folded.nodeIds) {
      const group = folded.groups.get(id)
      const at = indexOf.get(group ? group.pipeline : id)
      if (at !== undefined) flowId.set(id, `${group ? "f" : "n"}${at}`)
    }
    return { signature, view, folded, panels, flowId }
  }, [signature, view, folded, panels, indexOf])

  const layoutKey = [
    signature,
    folded.nodeIds.join(","),
    folded.edges.map((e) => `${e.from}>${e.to}:${e.kind}:${e.evidence}`).join(","),
    [...panels].map(([id, height]) => `${id}:${height}`).join(","),
  ].join("|")
  const { settled, pending, error } = useGraphLayout(layoutKey, input, layoutDrawing)

  const drawing = useMemo(() => {
    if (!settled) return null
    const { input: laidOut, layout } = settled
    const idByFlowId = new Map([...laidOut.flowId].map(([id, f]) => [f, id]))
    const focusAt = laidOut.view.focus ? layout.positions.get(laidOut.view.focus) : undefined
    const root = focusAt
      ? {
          ...focusAt,
          width: ASSET_NODE_WIDTH,
          height: ASSET_NODE_HEIGHT + (laidOut.panels.get(laidOut.view.focus!) ?? 0),
        }
      : null
    return { ...laidOut, layoutKey: settled.key, idByFlowId, positions: layout.positions, bounds: layout.bounds, root }
  }, [settled])

  const nodes: DrawnNode[] = useMemo(
    () =>
      (drawing?.folded.nodeIds ?? []).flatMap((id): DrawnNode[] => {
        const flowId = drawing!.flowId.get(id)
        const position = drawing!.positions.get(id)
        if (!flowId || !position) return []
        const panel = drawing!.panels.get(id) ?? 0
        const card = {
          id: flowId,
          position,
          width: ASSET_NODE_WIDTH,
          height: ASSET_NODE_HEIGHT + panel,
          selectable: false,
          connectable: false,
          focusable: false,
          handles: ASSET_HANDLES,
        }
        const group = drawing!.folded.groups.get(id)
        if (group) return [{ ...card, type: "fold", data: { count: group.tables.length } }]
        const node = byId.get(id)
        if (!node) return []
        const role = drawing!.view.role.get(id)
        return [
          {
            ...card,
            type: "asset",
            data: {
              node,
              line: describeAsset(node, graph, modelByNode.get(id)),
              warehouse: warehouseOf(node, warehouseNames),
              focused: id === drawing!.view.focus,
              flagged: attentionIds.has(id),
              side: role === "upstream" || role === "downstream" ? role : null,
              hidden: drawing!.view.hiddenBeyond.get(id) ?? 0,
              panel,
            },
          },
        ]
      }),
    [drawing, byId, graph, modelByNode, attentionIds, warehouseNames],
  )

  const edges: EvidenceFlowEdge[] = useMemo(
    () =>
      (drawing?.folded.edges ?? []).flatMap((e, i): EvidenceFlowEdge[] => {
        const source = drawing!.flowId.get(e.from)
        const target = drawing!.flowId.get(e.to)
        if (!source || !target) return []
        const color = e.kind === "triggers" ? accent : stroke
        const dash = EVIDENCE_DASH[e.evidence]
        return [
          {
            id: `e${i}`,
            source,
            target,
            type: "evidence",
            focusable: false,
            selectable: false,
            // null stops React Flow writing "Edge from n0 to n1" as a label; the type says string.
            ariaLabel: null as unknown as string,
            style: { stroke: color, strokeWidth: 1.5, ...(dash ? { strokeDasharray: dash } : {}) },
            markerEnd: { type: MarkerType.ArrowClosed, color },
            data: { label: edgeLabel(e), color },
          },
        ]
      }),
    [drawing, stroke, accent],
  )

  const actions: CardActions = useMemo(
    () => ({
      expand: (id, flowId) => {
        setAnchor({ signature, flowId })
        onExpand(id)
      },
      toggleColumns: (node, flowId) => {
        const next = new Set(columnsOpen)
        if (next.has(node.id)) next.delete(node.id)
        else {
          next.add(node.id)
          if (node.connection_id) tables.load(node.connection_id)
        }
        setAnchor({ signature, flowId })
        setColumns({ signature, open: next })
      },
      tables,
    }),
    [signature, columnsOpen, tables, onExpand],
  )

  if (!drawing) {
    return (
      <CanvasStatus>
        {error ? (
          <span role="alert">Could not lay out the graph. The List view has the same pipelines, tables and models.</span>
        ) : (
          <>
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            Laying out the graph…
          </>
        )}
      </CanvasStatus>
    )
  }

  const onNodeClick = (flowId: string) => {
    const id = drawing.idByFlowId.get(flowId)
    if (!id) return
    if (drawing.folded.groups.has(id)) {
      setAnchor({ signature, flowId })
      setOpen(open === "all" ? open : new Set([...open, id]))
    } else if (id !== drawing.view.focus) onFocus(id)
  }

  const evidence = new Set(drawing.folded.edges.map((e) => e.evidence))
  const anyWake = drawing.folded.edges.some((e) => e.kind === "triggers")
  const foldedTables = [...folded.groups.values()].reduce((n, g) => n + g.tables.length, 0)
  const focusName = view.focus ? byId.get(view.focus)?.name : undefined
  const focusFlowId = view.focus ? input.flowId.get(view.focus) : undefined
  const keepFocusStill = () => setAnchor(focusFlowId ? { signature, flowId: focusFlowId } : null)

  return (
    <>
      {(folded.groups.size > 0 || folded.opened > 0) && (
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b px-4 py-2 text-xs text-zinc-600 dark:border-zinc-800 dark:text-zinc-300">
          <p>
            {folded.groups.size > 0
              ? `${foldedTables} tables no model reads are folded into one box per pipeline. Click a box to open it.`
              : "Every table is drawn."}
          </p>
          {folded.groups.size > 0 && (
            <Button
              size="sm"
              variant="outline"
              className="h-6 px-2 text-xs"
              onClick={() => {
                keepFocusStill()
                setOpen("all")
              }}
            >
              Show every table
            </Button>
          )}
          {folded.opened > 0 && (
            <Button
              size="sm"
              variant="outline"
              className="h-6 px-2 text-xs"
              onClick={() => {
                keepFocusStill()
                setOpen(NOTHING_OPEN)
              }}
            >
              Fold unread tables again
            </Button>
          )}
        </div>
      )}
      <CardActionsContext.Provider value={actions}>
        <LineageFlowCanvas<DrawnNode>
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          bounds={drawing.bounds}
          root={drawing.root}
          signature={drawing.signature}
          layoutKey={drawing.layoutKey}
          onNodeClick={onNodeClick}
          description="A drawing of the lineage. The List view has the same pipelines, tables and models, with links, their columns, and a button that centres the graph on each one."
          title={focusName ? `What feeds ${focusName}, and what it feeds` : scopeName}
          legend={<AssetLegend evidence={evidence} wakeColor={anyWake ? accent : null} expandable={view.hiddenBeyond.size > 0} />}
          minimapColor={(n) => (n.type === "asset" ? KIND_MAP_FILL[n.data.node.kind] : undefined) ?? KIND_MAP_FILL.table}
          pending={pending}
          anchor={anchor?.signature === signature ? anchor.flowId : null}
        />
      </CardActionsContext.Provider>
    </>
  )
}

function AssetLegend({
  evidence,
  wakeColor,
  expandable,
}: {
  evidence: Set<string>
  wakeColor: string | null
  expandable: boolean
}) {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-t px-4 py-2 text-xs text-zinc-500 dark:text-zinc-400 dark:border-zinc-800">
      {(["pipeline", "table", "model"] as const).map((k) => (
        <span key={k} className="inline-flex items-center gap-1.5">
          <span className={cn("h-3 w-1 rounded-sm border-l-4", KIND_BORDER[k])} aria-hidden />
          {kindLabel(k)}
        </span>
      ))}
      {evidence.has("observed") && <LegendLine label={EVIDENCE_TEXT.observed} />}
      {evidence.has("declared") && <LegendLine dash={EVIDENCE_DASH.declared} label={EVIDENCE_TEXT.declared} />}
      {evidence.has("inferred") && <LegendLine dash={EVIDENCE_DASH.inferred} label={EVIDENCE_TEXT.inferred} />}
      {wakeColor && <LegendLine color={wakeColor} label="wakes the model when it finishes" />}
      {expandable && <span>+N on a card draws its next hop</span>}
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
  warehouseNames,
  tables,
  onFocus,
  onExpand,
}: {
  graph: AssetGraph
  view: AssetView
  byId: Map<string, AssetNode>
  modelByNode: Map<string, ModelRefresh>
  attentionIds: Set<string>
  warehouseNames: ReadonlyMap<string, string> | null
  tables: WarehouseTablesCache
  onFocus: (id: string) => void
  onExpand: (id: string) => void
}) {
  const [columnsOpen, setColumnsOpen] = useState<ReadonlySet<string>>(NOTHING_OPEN)
  const panelIds = useId()
  const toggleColumns = (n: AssetNode) => {
    const next = new Set(columnsOpen)
    if (next.has(n.id)) next.delete(n.id)
    else {
      next.add(n.id)
      if (n.connection_id) tables.load(n.connection_id)
    }
    setColumnsOpen(next)
  }

  const drawn = view.nodeIds.map((id) => byId.get(id)).filter((n): n is AssetNode => !!n)
  let sections: [string, AssetNode[]][]
  if (view.focus) {
    const byDistance = (a: AssetNode, b: AssetNode) => (view.distance.get(a.id) ?? 0) - (view.distance.get(b.id) ?? 0)
    const ups = drawn.filter((n) => view.role.get(n.id) === "upstream").sort(byDistance)
    const downs = drawn.filter((n) => view.role.get(n.id) === "downstream").sort(byDistance)
    sections = [
      [`Upstream (${shownOf(ups.length, view.upstreamFound)})`, ups],
      ["Selected", drawn.filter((n) => n.id === view.focus)],
      [`Downstream (${shownOf(downs.length, view.downstreamFound)})`, downs],
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
                const warehouse = warehouseOf(n, warehouseNames)
                const hidden = view.hiddenBeyond.get(n.id) ?? 0
                const side = view.role.get(n.id)
                const showsColumns = columnsOpen.has(n.id)
                const panelId = `${panelIds}-${view.nodeIds.indexOf(n.id)}`
                return (
                  <li
                    key={n.id}
                    data-asset-row
                    aria-current={n.id === view.focus ? "true" : undefined}
                    className={cn("border-l-4 pl-3", KIND_BORDER[n.kind] ?? KIND_BORDER.table)}
                  >
                    <div className="flex min-w-0 flex-wrap items-baseline gap-x-2">
                      <span className="text-[10px] uppercase tracking-wide text-zinc-400">{kindLabel(n.kind)}</span>
                      {warehouse && <span className="text-[11px] text-zinc-500 dark:text-zinc-400">{warehouse}</span>}
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
                      {hidden > 0 && (side === "upstream" || side === "downstream") && (
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-6 px-2 text-xs"
                          aria-label={`Show ${hidden} more ${side} of ${n.name}`}
                          onClick={() => onExpand(n.id)}
                        >
                          +{hidden} {side}
                        </Button>
                      )}
                      {n.kind === "table" && (
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-6 px-2 text-xs"
                          aria-expanded={showsColumns}
                          aria-controls={showsColumns ? panelId : undefined}
                          aria-label={`Columns of ${n.name}`}
                          onClick={() => toggleColumns(n)}
                        >
                          <Columns3 className="mr-1 h-3 w-3" aria-hidden />
                          Columns
                        </Button>
                      )}
                    </div>
                    <div className="text-[11px] text-zinc-500 dark:text-zinc-400">
                      {describeAsset(n, graph, modelByNode.get(n.id))}
                    </div>
                    {showsColumns && (
                      <div id={panelId} className="mt-2 max-w-md rounded-md border p-2 dark:border-zinc-800">
                        <ColumnsPanel
                          node={n}
                          tables={tablesFor(n, tables)}
                          onRetry={() => n.connection_id && tables.load(n.connection_id)}
                        />
                      </div>
                    )}
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
