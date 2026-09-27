/**
 * The workspace asset graph (GET /api/v1/explorer/asset-graph, api-gateway
 * asset_graph.go) and the pure logic the Lineage page draws it with.
 *
 * The graph is derived, not configured. Every edge carries how the backend knows it:
 * `observed` (a run recorded it), `declared` (someone configured it) or `inferred`
 * (parsed out of a model's SQL). The page keeps those apart, because an inferred edge is
 * a suggestion and drawing it like an observed one would overstate what is known.
 */

import { modelHref } from "@/components/explorer/modelLineage"
import { layoutGraph, type GraphLayout } from "@/lib/elk-layout"
import type { SchemaTableLike } from "@/lib/explorer/schemaTree"

export interface AssetNode {
  id: string
  kind: string
  name: string
  ref_id?: string
  connection_id?: string
}

export interface AssetEdge {
  from: string
  to: string
  kind: string
  evidence: string
}

export interface ModelRefresh {
  model_id: string
  node_id: string
  name: string
  trigger: string
  trigger_upstreams: string[]
  trigger_paused: boolean
  upstreams: string[]
  uncovered_upstreams: string[]
  unresolved: string[]
  ambiguous: boolean
}

export interface AssetGraphStats {
  pipelines: number
  tables: number
  models: number
  edges: Record<string, number>
  models_with_uncovered_upstreams: number
  models_with_unresolved_references: number
  models_with_multiple_upstreams: number
  truncated: { pipelines: boolean; produced_tables: boolean; models: boolean }
}

export interface AssetGraph {
  nodes: AssetNode[]
  edges: AssetEdge[]
  models: ModelRefresh[]
  stats: AssetGraphStats
}

/** More than this and a drawing stops being readable, so the page draws one asset's neighbourhood. */
export const MAX_DRAWN_ASSETS = 80

function arr<T>(v: unknown): T[] {
  return Array.isArray(v) ? (v as T[]) : []
}

function num(v: unknown): number {
  return typeof v === "number" && Number.isFinite(v) ? v : 0
}

/**
 * The response, with every list defaulted: a Go nil slice arrives as null, and a
 * missing list must read as empty rather than crash the page.
 */
export function parseAssetGraph(body: unknown): AssetGraph {
  const b = (body ?? {}) as Record<string, unknown>
  const s = (b.stats ?? {}) as Record<string, unknown>
  const t = (s.truncated ?? {}) as Record<string, unknown>
  return {
    nodes: arr<AssetNode>(b.nodes),
    edges: arr<AssetEdge>(b.edges),
    models: arr<Record<string, unknown>>(b.models).map((m) => ({
      model_id: String(m.model_id ?? ""),
      node_id: String(m.node_id ?? ""),
      name: String(m.name ?? ""),
      trigger: String(m.trigger ?? "manual"),
      trigger_upstreams: arr<string>(m.trigger_upstreams),
      trigger_paused: m.trigger_paused === true,
      upstreams: arr<string>(m.upstreams),
      uncovered_upstreams: arr<string>(m.uncovered_upstreams),
      unresolved: arr<string>(m.unresolved),
      ambiguous: m.ambiguous === true,
    })),
    stats: {
      pipelines: num(s.pipelines),
      tables: num(s.tables),
      models: num(s.models),
      edges: (s.edges && typeof s.edges === "object" ? s.edges : {}) as Record<string, number>,
      models_with_uncovered_upstreams: num(s.models_with_uncovered_upstreams),
      models_with_unresolved_references: num(s.models_with_unresolved_references),
      models_with_multiple_upstreams: num(s.models_with_multiple_upstreams),
      truncated: {
        pipelines: t.pipelines === true,
        produced_tables: t.produced_tables === true,
        models: t.models === true,
      },
    },
  }
}

const KIND_LABELS: Record<string, string> = { pipeline: "Pipeline", table: "Table", model: "Model" }

export function kindLabel(kind: string): string {
  return KIND_LABELS[kind] ?? kind
}

const TRIGGER_LABELS: Record<string, string> = {
  manual: "Refreshed by hand",
  cron: "Runs on a cron schedule",
  interval: "Runs on an interval",
  after_upstream: "Runs after its upstreams",
}

export function triggerLabel(trigger: string): string {
  return TRIGGER_LABELS[trigger] ?? trigger
}

/**
 * The warehouses the graph's assets live in, as connection ids. A pipeline counts under
 * the warehouse it writes to, which is also what `?connection_id=` filters it by.
 */
export function warehouseIds(graph: AssetGraph): string[] {
  return [...new Set(graph.nodes.flatMap((n) => (n.connection_id ? [n.connection_id] : [])))].sort()
}

/** A warehouse's connection name, or a short id when the connection list has no name for it. */
export function warehouseLabel(id: string, names: ReadonlyMap<string, string>): string {
  return names.get(id) ?? `Warehouse ${id.slice(0, 8)}`
}

/** Where a node's own page is. Tables have none: a table is not a row the product owns. */
export function assetHref(node: AssetNode): string | null {
  if (!node.ref_id) return null
  if (node.kind === "model") return modelHref(node.ref_id)
  if (node.kind === "pipeline") return `/pipelines/${encodeURIComponent(node.ref_id)}`
  return null
}

export type AssetRole = "focus" | "upstream" | "downstream"

export interface AssetView {
  /** Drawn nodes: the focus first, then nearest first. */
  nodeIds: string[]
  /** Edges whose ends are both drawn. */
  edges: AssetEdge[]
  focus: string | null
  role: Map<string, AssetRole>
  distance: Map<string, number>
  /** Everything upstream of the focus, however far. */
  upstreamFound: number
  /** Everything downstream of the focus, however far. */
  downstreamFound: number
  upstreamShown: number
  downstreamShown: number
  /** Per drawn node, its next hop away from the focus that is not drawn. */
  hiddenBeyond: Map<string, number>
  /** Within the depth but not drawn, because of the cap. */
  omitted: number
}

export interface NeighbourhoodOptions {
  /** Hops drawn on each side of the focus; Infinity draws them all. */
  depth?: number
  /** Nodes whose next hop is drawn whatever the depth. */
  expanded?: ReadonlySet<string>
  cap?: number
}

/** Hops from `start`, following a node's `next` only while it is within `depth` or expanded. */
function walk(
  start: string,
  next: Map<string, string[]>,
  depth = Infinity,
  expanded: ReadonlySet<string> = new Set(),
): Map<string, number> {
  const dist = new Map<string, number>([[start, 0]])
  let frontier = [start]
  for (let d = 1; frontier.length > 0; d++) {
    const following: string[] = []
    for (const id of frontier) {
      if (d > depth && !expanded.has(id)) continue
      for (const n of next.get(id) ?? []) {
        if (dist.has(n)) continue
        dist.set(n, d)
        following.push(n)
      }
    }
    frontier = following
  }
  dist.delete(start)
  return dist
}

/**
 * What feeds the focus and what it feeds, `depth` hops out on each side plus the next
 * hop of every expanded node, capped at `cap` nodes with the nearest kept. The totals
 * count every hop, drawn or not. A node that is both (a cycle) counts as upstream.
 */
export function neighbourhood(
  graph: AssetGraph,
  focus: string,
  { depth = Infinity, expanded, cap = MAX_DRAWN_ASSETS }: NeighbourhoodOptions = {},
): AssetView {
  const out = new Map<string, string[]>()
  const into = new Map<string, string[]>()
  for (const e of graph.edges) {
    out.set(e.from, [...(out.get(e.from) ?? []), e.to])
    into.set(e.to, [...(into.get(e.to) ?? []), e.from])
  }
  const allUps = walk(focus, into)
  const allDowns = walk(focus, out)
  for (const id of allUps.keys()) allDowns.delete(id)

  const ups = walk(focus, into, depth, expanded)
  const downs = walk(focus, out, depth, expanded)
  for (const id of ups.keys()) downs.delete(id)

  const found = [
    ...[...ups].map(([id, d]) => ({ id, d, role: "upstream" as const })),
    ...[...downs].map(([id, d]) => ({ id, d, role: "downstream" as const })),
  ].sort((a, b) => a.d - b.d || a.id.localeCompare(b.id))
  const kept = found.slice(0, Math.max(0, cap - 1))

  const role = new Map<string, AssetRole>([[focus, "focus"]])
  const distance = new Map<string, number>([[focus, 0]])
  for (const k of kept) {
    role.set(k.id, k.role)
    distance.set(k.id, k.d)
  }
  const nodeIds = [focus, ...kept.map((k) => k.id)]
  const drawn = new Set(nodeIds)
  const hiddenBeyond = new Map<string, number>()
  for (const k of kept) {
    const onward = (k.role === "upstream" ? into : out).get(k.id) ?? []
    const hidden = new Set(onward.filter((id) => !drawn.has(id))).size
    if (hidden > 0) hiddenBeyond.set(k.id, hidden)
  }
  return {
    nodeIds,
    edges: graph.edges.filter((e) => drawn.has(e.from) && drawn.has(e.to)),
    focus,
    role,
    distance,
    upstreamFound: allUps.size,
    downstreamFound: allDowns.size,
    upstreamShown: kept.filter((k) => k.role === "upstream").length,
    downstreamShown: kept.filter((k) => k.role === "downstream").length,
    hiddenBeyond,
    omitted: found.length - kept.length,
  }
}

export function wholeGraph(graph: AssetGraph): AssetView {
  return {
    nodeIds: graph.nodes.map((n) => n.id),
    edges: graph.edges,
    focus: null,
    role: new Map(),
    distance: new Map(),
    upstreamFound: 0,
    downstreamFound: 0,
    upstreamShown: 0,
    downstreamShown: 0,
    hiddenBeyond: new Map(),
    omitted: 0,
  }
}

/** Fewer unread tables than this stay drawn: a box would hide almost nothing. */
export const FOLD_MIN = 3

/** A pipeline's tables that nothing drawn reads, drawn as one box. */
export interface FoldGroup {
  id: string
  pipeline: string
  tables: string[]
}

export interface FoldedView {
  nodeIds: string[]
  edges: AssetEdge[]
  /** Boxes still folded, keyed by their node id. */
  groups: Map<string, FoldGroup>
  /** Boxes that would fold but are drawn open. */
  opened: number
}

/** The node id of a pipeline's box of unread tables. */
export function foldId(pipeline: string): string {
  return `fold:${pipeline}`
}

const EVIDENCE_STRENGTH = ["inferred", "declared", "observed"]

/**
 * A pipeline that writes many tables no model reads stacks them in one column as tall as
 * the page, and the drawing can then only be read by zooming until the words vanish. So
 * the tables only one pipeline writes, and nothing drawn reads, fold into one box per
 * pipeline. The focused asset is never folded away, nor is the focused pipeline's output:
 * that is what was asked to see. `open` names the boxes drawn open, or "all".
 */
export function foldUnreadTables(
  view: AssetView,
  graph: AssetGraph,
  open: ReadonlySet<string> | "all" = new Set(),
): FoldedView {
  const kinds = new Map(graph.nodes.map((n) => [n.id, n.kind]))
  const into = new Map<string, AssetEdge[]>()
  const readers = new Set<string>()
  for (const e of view.edges) {
    into.set(e.to, [...(into.get(e.to) ?? []), e])
    readers.add(e.from)
  }
  const unread = new Map<string, string[]>()
  for (const id of view.nodeIds) {
    if (id === view.focus || kinds.get(id) !== "table" || readers.has(id)) continue
    const writes = into.get(id) ?? []
    const writer = writes[0]?.from
    if (!writer || writer === view.focus || kinds.get(writer) !== "pipeline") continue
    if (!writes.every((e) => e.from === writer && e.kind === "writes")) continue
    unread.set(writer, [...(unread.get(writer) ?? []), id])
  }

  const groups = new Map<string, FoldGroup>()
  const folded = new Map<string, string>()
  let opened = 0
  for (const [pipeline, tables] of unread) {
    if (tables.length < FOLD_MIN) continue
    const id = foldId(pipeline)
    if (open === "all" || open.has(id)) {
      opened++
      continue
    }
    groups.set(id, { id, pipeline, tables })
    for (const t of tables) folded.set(t, id)
  }
  if (groups.size === 0) return { nodeIds: view.nodeIds, edges: view.edges, groups, opened }

  const nodeIds: string[] = []
  for (const id of view.nodeIds) {
    const group = folded.get(id)
    if (!group) nodeIds.push(id)
    else if (groups.get(group)!.tables[0] === id) nodeIds.push(group)
  }
  const edges = view.edges.filter((e) => !folded.has(e.to))
  for (const g of groups.values()) {
    // The box's link is only as sure as the least sure write in it.
    const evidence = EVIDENCE_STRENGTH.find((grade) =>
      g.tables.some((t) => into.get(t)!.some((e) => e.evidence === grade)),
    )
    edges.push({ from: g.pipeline, to: g.id, kind: "writes", evidence: evidence ?? "observed" })
  }
  return { nodeIds, edges, groups, opened }
}

/** An asset's card on the canvas. */
export const ASSET_NODE_WIDTH = 256
export const ASSET_NODE_HEIGHT = 88

/** Space between assets linked to nothing, and between them and the drawing above. */
const LOOSE_GAP = 20
const LOOSE_TOP_GAP = 56

/**
 * Where each asset goes: linked ones in columns, upstreams on the left. Assets linked to
 * nothing are set in rows under that drawing, as wide as it is; left to the layout they
 * would share its first column and make it as tall as there are of them.
 */
export async function layoutAssets(
  nodeIds: string[],
  edges: { from: string; to: string }[],
  sizeOf: (id: string) => { width: number; height: number } = () => ({
    width: ASSET_NODE_WIDTH,
    height: ASSET_NODE_HEIGHT,
  }),
): Promise<GraphLayout> {
  const linked = new Set(edges.flatMap((e) => [e.from, e.to]))
  const { positions, bounds } = await layoutGraph(
    nodeIds.filter((id) => linked.has(id)).map((id) => ({ id, ...sizeOf(id) })),
    edges,
  )
  const loose = nodeIds.filter((id) => !linked.has(id))
  if (loose.length === 0) return { positions, bounds }

  const sizes = loose.map(sizeOf)
  const step = Math.max(...sizes.map((s) => s.width)) + LOOSE_GAP
  const columns = Math.max(Math.floor((bounds.width + LOOSE_GAP) / step), Math.min(loose.length, 4))
  const drewLinked = positions.size > 0
  let top = drewLinked ? bounds.y + bounds.height + LOOSE_TOP_GAP : 0
  const left = drewLinked ? bounds.x : 0
  for (let row = 0; row * columns < loose.length; row++) {
    const inRow = loose.slice(row * columns, (row + 1) * columns)
    inRow.forEach((id, i) => positions.set(id, { x: left + i * step, y: top }))
    top += Math.max(...sizes.slice(row * columns, (row + 1) * columns).map((s) => s.height)) + LOOSE_GAP
  }
  const looseWidth = Math.min(loose.length, columns) * step - LOOSE_GAP
  const y = drewLinked ? bounds.y : 0
  return {
    positions,
    bounds: {
      x: left,
      y,
      width: Math.max(drewLinked ? bounds.width : 0, looseWidth),
      height: top - LOOSE_GAP - y,
    },
  }
}

export type ColumnsLookup =
  | { found: "one"; table: SchemaTableLike }
  | { found: "none" }
  | { found: "many"; count: number }

/**
 * The warehouse table an asset table node names. The graph names a table
 * `schema.table`, or bare when its producer gave no schema (asset_graph.go
 * splitModelTarget), so a bare name matches in any schema, and can match more than one.
 */
export function findWarehouseTable(assetName: string, tables: SchemaTableLike[]): ColumnsLookup {
  const dot = assetName.lastIndexOf(".")
  const table = assetName.slice(dot + 1).toLowerCase()
  const schema = dot > 0 ? assetName.slice(0, dot).toLowerCase() : null
  const hits = tables.filter(
    (t) => t.name.toLowerCase() === table && (schema === null || (t.schema ?? "").toLowerCase() === schema),
  )
  if (hits.length === 1) return { found: "one", table: hits[0] }
  return hits.length === 0 ? { found: "none" } : { found: "many", count: hits.length }
}

function plural(n: number, one: string, many: string): string {
  return `${n} ${n === 1 ? one : many}`
}

function nameList(ids: string[], byId: Map<string, AssetNode>): string {
  const names = ids.map((id) => byId.get(id)?.name || id)
  if (names.length <= 3) return names.join(", ")
  return `${names.slice(0, 3).join(", ")} and ${names.length - 3} more`
}

export interface AttentionItem {
  model: ModelRefresh
  node: AssetNode | undefined
  reasons: string[]
}

/**
 * Models whose freshness depends on someone remembering: an upstream that does not
 * refresh them, a table reference nothing produces, or a reference that could be more
 * than one table. The reasons are in words, one per problem.
 */
export function needsAttention(graph: AssetGraph): AttentionItem[] {
  const byId = new Map(graph.nodes.map((n) => [n.id, n]))
  const items: AttentionItem[] = []
  for (const m of graph.models) {
    const reasons: string[] = []
    const uncovered = m.uncovered_upstreams
    if (uncovered.length > 0) {
      const names = nameList(uncovered, byId)
      const verb = uncovered.length === 1 ? "runs" : "run"
      if (m.trigger === "manual") reasons.push(`Reads from ${names}, but is only refreshed by hand.`)
      else if (m.trigger === "after_upstream")
        reasons.push(`Reads from ${names}, but does not refresh when ${uncovered.length === 1 ? "it" : "they"} ${verb}.`)
      else reasons.push(`Reads from ${names}, but runs on a clock rather than when ${uncovered.length === 1 ? "it" : "they"} ${verb}.`)
    }
    if (m.trigger_paused && (uncovered.length > 0 || m.trigger_upstreams.length > 0))
      reasons.push("Its schedule is paused.")
    if (m.unresolved.length > 0)
      reasons.push(
        `Its SQL reads ${m.unresolved.join(", ")}, which no pipeline or model here writes, so where ${m.unresolved.length === 1 ? "that table gets" : "those tables get"} data is unknown.`,
      )
    if (m.ambiguous) reasons.push("A table name in its SQL could be more than one table, so an upstream shown may be the wrong one.")
    if (reasons.length > 0) items.push({ model: m, node: byId.get(m.node_id), reasons })
  }
  return items.sort(
    (a, b) =>
      b.model.uncovered_upstreams.length - a.model.uncovered_upstreams.length ||
      b.model.unresolved.length - a.model.unresolved.length ||
      a.model.name.localeCompare(b.model.name),
  )
}

/** A short line under a node's name. */
export function describeAsset(node: AssetNode, graph: AssetGraph, model: ModelRefresh | undefined): string {
  if (node.kind === "model") {
    const trigger = model ? triggerLabel(model.trigger) : "Model"
    return model?.trigger_paused ? `${trigger} · paused` : trigger
  }
  const outgoing = graph.edges.filter((e) => e.from === node.id)
  if (node.kind === "pipeline") {
    const writes = outgoing.filter((e) => e.kind === "writes").length
    const triggers = outgoing.filter((e) => e.kind === "triggers").length
    const parts = [`Writes ${plural(writes, "table", "tables")}`]
    if (triggers > 0) parts.push(`wakes ${plural(triggers, "model", "models")}`)
    return parts.join(", ")
  }
  const readers = outgoing.filter((e) => e.kind === "reads").length
  return readers === 0 ? "No model reads it" : `Read by ${plural(readers, "model", "models")}`
}

/** Names that start with the query first; then models, pipelines and tables; then by name. */
export function searchAssets(graph: AssetGraph, query: string, limit = 8): AssetNode[] {
  const q = query.trim().toLowerCase()
  if (!q) return []
  const rank: Record<string, number> = { model: 0, pipeline: 1, table: 2 }
  return graph.nodes
    .filter((n) => n.name.toLowerCase().includes(q))
    .sort(
      (a, b) =>
        Number(!a.name.toLowerCase().startsWith(q)) - Number(!b.name.toLowerCase().startsWith(q)) ||
        (rank[a.kind] ?? 3) - (rank[b.kind] ?? 3) ||
        a.name.localeCompare(b.name),
    )
    .slice(0, limit)
}
