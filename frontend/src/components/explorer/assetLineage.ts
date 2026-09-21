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
  upstreamFound: number
  downstreamFound: number
  /** Found but not drawn, because of the cap. */
  omitted: number
}

function walk(start: string, next: Map<string, string[]>): Map<string, number> {
  const dist = new Map<string, number>([[start, 0]])
  let frontier = [start]
  for (let d = 1; frontier.length > 0; d++) {
    const following: string[] = []
    for (const id of frontier) {
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
 * Everything upstream of the focus and everything downstream of it, transitively, capped
 * at `cap` nodes with the nearest kept. A node that is both (a cycle) counts as upstream.
 */
export function neighbourhood(graph: AssetGraph, focus: string, cap = MAX_DRAWN_ASSETS): AssetView {
  const out = new Map<string, string[]>()
  const into = new Map<string, string[]>()
  for (const e of graph.edges) {
    out.set(e.from, [...(out.get(e.from) ?? []), e.to])
    into.set(e.to, [...(into.get(e.to) ?? []), e.from])
  }
  const ups = walk(focus, into)
  const downs = walk(focus, out)
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
  return {
    nodeIds,
    edges: graph.edges.filter((e) => drawn.has(e.from) && drawn.has(e.to)),
    focus,
    role,
    distance,
    upstreamFound: ups.size,
    downstreamFound: downs.size,
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
    omitted: 0,
  }
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
