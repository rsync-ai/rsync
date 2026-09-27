import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import { AssetLineageView, edgeLabel } from "@/components/explorer/AssetLineageView"
import {
  ASSET_NODE_HEIGHT,
  ASSET_NODE_WIDTH,
  assetHref,
  findWarehouseTable,
  foldUnreadTables,
  layoutAssets,
  needsAttention,
  neighbourhood,
  parseAssetGraph,
  searchAssets,
  warehouseIds,
  warehouseLabel,
  wholeGraph,
  type AssetGraph,
} from "@/components/explorer/assetLineage"
import { revealShift } from "@/components/explorer/LineageFlowCanvas"
import { MIN_READABLE_FIT_ZOOM } from "@/components/explorer/modelLineage"
import { authFetch } from "@/lib/api/auth-fetch"
import { listConnections } from "@/lib/api/connections"

// GET /api/v1/explorer/asset-graph (asset_graph.go) had no reader. The Lineage page draws it,
// says which models are refreshed out of step with what they read, and keeps the backend's
// three grades of evidence apart: an edge read out of SQL is not drawn like one seen in a run.

const media = vi.hoisted(() => ({ canvas: false }))

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("@/lib/api/connections", () => ({ listConnections: vi.fn() }))
vi.mock("next-themes", () => ({ useTheme: () => ({ resolvedTheme: "light" }) }))

const mockFetch = authFetch as unknown as Mock
const mockConnections = listConnections as unknown as Mock
/** The lineage canvas on a laptop: 65% of a 920px-tall window. */
const CANVAS = { width: 1250, height: 600 }
const URL = "/api/v1/explorer/asset-graph"

beforeAll(() => {
  // What React Flow needs from a browser that jsdom does not have (as in model-lineage-graph.test.tsx).
  class ResizeObserverStub {
    constructor(private cb: ResizeObserverCallback) {}
    observe(target: Element) {
      const contentRect = { width: 800, height: 480 } as DOMRectReadOnly
      this.cb([{ target, contentRect } as ResizeObserverEntry], this as unknown as ResizeObserver)
    }
    unobserve() {}
    disconnect() {}
  }
  class DOMMatrixReadOnlyStub {
    m22: number
    constructor(transform?: string) {
      const scale = transform?.match(/scale\(([\d.]+)\)/)?.[1]
      this.m22 = scale !== undefined ? Number(scale) : 1
    }
  }
  Object.assign(globalThis, { ResizeObserver: ResizeObserverStub, DOMMatrixReadOnly: DOMMatrixReadOnlyStub })
  Object.defineProperties(HTMLElement.prototype, {
    offsetHeight: { configurable: true, get() { return parseFloat(this.style.height) || 1 } },
    offsetWidth: { configurable: true, get() { return parseFloat(this.style.width) || 1 } },
    // The canvas reads its own size to pick the first view: 65% of a laptop window's height.
    clientWidth: { configurable: true, get() { return this.hasAttribute("data-lineage-canvas") ? CANVAS.width : 0 } },
    clientHeight: { configurable: true, get() { return this.hasAttribute("data-lineage-canvas") ? CANVAS.height : 0 } },
  })
  ;(SVGElement.prototype as unknown as { getBBox: () => DOMRect }).getBBox = () =>
    ({ x: 0, y: 0, width: 0, height: 0 }) as DOMRect
  window.matchMedia = ((query: string) => ({
    matches: media.canvas,
    media: query,
    onchange: null,
    addEventListener: () => {},
    removeEventListener: () => {},
    addListener: () => {},
    removeListener: () => {},
    dispatchEvent: () => false,
  })) as unknown as typeof window.matchMedia
})

beforeEach(() => {
  mockFetch.mockReset()
  mockConnections.mockReset()
  mockConnections.mockResolvedValue({ connections: [], total: 0 })
  media.canvas = false
})

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

const translateOf = (transform: string) =>
  transform.match(/translate\((-?[\d.]+)px, ?(-?[\d.]+)px\)/)!.slice(1, 3).map(Number) as [number, number]

/** Where a card is on the canvas's screen, in pixels from the canvas's top left. */
function screenBox(container: HTMLElement, card: HTMLElement) {
  const view = container.querySelector<HTMLElement>(".react-flow__viewport")!.style.transform
  const [tx, ty] = translateOf(view)
  const zoom = Number(view.match(/scale\(([\d.]+)\)/)![1])
  const [x, y] = translateOf(card.style.transform)
  const [width, height] = [parseFloat(card.style.width), parseFloat(card.style.height)]
  return { left: tx + x * zoom, top: ty + y * zoom, right: tx + (x + width) * zoom, bottom: ty + (y + height) * zoom }
}

// orders_sync writes orders and customers and wakes weekly_rollup. daily_revenue reads both
// tables on a cron and materializes analytics.daily_revenue, which weekly_rollup reads. So
// weekly_rollup is woken by the pipeline but not by the model it actually reads.
const P1 = "pipeline:p-1"
const P2 = "pipeline:p-2"
const T_ORDERS = "table:c-1:public.orders"
const T_CUST = "table:c-1:public.customers"
const T_DAILY = "table:c-1:analytics.daily_revenue"
const T_EVENTS = "table:c-2:public.events"
const M1 = "model:m-1"
const M2 = "model:m-2"
const M3 = "model:m-3"

const BODY = {
  nodes: [
    { id: P1, kind: "pipeline", name: "orders_sync", ref_id: "p-1" },
    { id: P2, kind: "pipeline", name: "events_sync", ref_id: "p-2" },
    { id: T_ORDERS, kind: "table", name: "public.orders", connection_id: "c-1" },
    { id: T_CUST, kind: "table", name: "public.customers", connection_id: "c-1" },
    { id: T_DAILY, kind: "table", name: "analytics.daily_revenue", connection_id: "c-1" },
    { id: T_EVENTS, kind: "table", name: "public.events", connection_id: "c-2" },
    { id: M1, kind: "model", name: "daily_revenue", ref_id: "m-1" },
    { id: M2, kind: "model", name: "weekly_rollup", ref_id: "m-2" },
    { id: M3, kind: "model", name: "scratch_report", ref_id: "m-3" },
  ],
  edges: [
    { from: P1, to: T_ORDERS, kind: "writes", evidence: "observed" },
    { from: P1, to: T_CUST, kind: "writes", evidence: "observed" },
    { from: P1, to: M2, kind: "triggers", evidence: "declared" },
    { from: T_ORDERS, to: M1, kind: "reads", evidence: "inferred" },
    { from: T_CUST, to: M1, kind: "reads", evidence: "inferred" },
    { from: M1, to: T_DAILY, kind: "materializes", evidence: "declared" },
    { from: T_DAILY, to: M2, kind: "reads", evidence: "inferred" },
    { from: P2, to: T_EVENTS, kind: "writes", evidence: "observed" },
  ],
  models: [
    {
      model_id: "m-1",
      node_id: M1,
      name: "daily_revenue",
      trigger: "cron",
      trigger_upstreams: [],
      trigger_paused: false,
      upstreams: [P1],
      uncovered_upstreams: [P1],
      unresolved: ["public.refunds"],
      ambiguous: false,
    },
    {
      model_id: "m-2",
      node_id: M2,
      name: "weekly_rollup",
      trigger: "after_upstream",
      trigger_upstreams: [P1],
      trigger_paused: false,
      upstreams: [M1],
      uncovered_upstreams: [M1],
      unresolved: null,
      ambiguous: true,
    },
    {
      model_id: "m-3",
      node_id: M3,
      name: "scratch_report",
      trigger: "manual",
      trigger_upstreams: null,
      trigger_paused: false,
      upstreams: null,
      uncovered_upstreams: null,
      unresolved: null,
      ambiguous: false,
    },
  ],
  stats: {
    pipelines: 2,
    tables: 4,
    models: 3,
    edges: { writes: 3, triggers: 1, reads: 3, materializes: 1 },
    models_with_uncovered_upstreams: 2,
    models_with_unresolved_references: 1,
    models_with_multiple_upstreams: 0,
    truncated: { pipelines: false, produced_tables: false, models: false },
  },
}

const graph: AssetGraph = parseAssetGraph(BODY)

// The workspace in the bug report (25 assets, 19 links): four pipelines writing 9, 4, 1 and 2
// tables, two models whose SQL names a table nothing writes, and one chain
// pipeline → table → model → table → model. Drawn as-is it is a 1304 x 1756 column of tables.
const PA = "pipeline:p-a"
const PB = "pipeline:p-b"
const WIDE = (() => {
  const nodes: { id: string; kind: string; name: string; ref_id?: string; connection_id?: string }[] = []
  const edges: { from: string; to: string; kind: string; evidence: string }[] = []
  const pipeline = (key: string, tables: string[]) => {
    nodes.push({ id: `pipeline:p-${key}`, kind: "pipeline", name: `${key}_sync`, ref_id: `p-${key}`, connection_id: "wh-1" })
    for (const t of tables) {
      nodes.push({ id: `table:wh-1:${t}`, kind: "table", name: t, connection_id: "wh-1" })
      edges.push({ from: `pipeline:p-${key}`, to: `table:wh-1:${t}`, kind: "writes", evidence: "observed" })
    }
  }
  pipeline("a", Array.from({ length: 9 }, (_, i) => `raw.a${i}`))
  pipeline("b", Array.from({ length: 4 }, (_, i) => `raw.b${i}`))
  pipeline("c", ["raw.c0"])
  pipeline("d", ["raw.d0", "raw.d1"])
  for (const m of ["m-1", "m-2", "m-3", "m-4"]) nodes.push({ id: `model:${m}`, kind: "model", name: `model_${m}`, ref_id: m })
  nodes.push({ id: "table:wh-1:gold.g", kind: "table", name: "gold.g", connection_id: "wh-1" })
  edges.push(
    { from: "table:wh-1:raw.d1", to: "model:m-3", kind: "reads", evidence: "inferred" },
    { from: "model:m-3", to: "table:wh-1:gold.g", kind: "materializes", evidence: "declared" },
    { from: "table:wh-1:gold.g", to: "model:m-4", kind: "reads", evidence: "inferred" },
  )
  return { ...BODY, nodes, edges, models: [] }
})()

describe("assetLineage helpers", () => {
  it("lists each warehouse once, and names one the connection list does not know by a short id", () => {
    expect(warehouseIds(graph)).toEqual(["c-1", "c-2"])
    expect(warehouseIds(parseAssetGraph(null))).toEqual([])
    expect(warehouseLabel("c-1", new Map([["c-1", "analytics_wh"]]))).toBe("analytics_wh")
    expect(warehouseLabel("0f3c9a1e-77aa-4b1d-9c1e-2f7d6e5a4b3c", new Map())).toBe("Warehouse 0f3c9a1e")
  })

  it("reads a Go nil slice as an empty list, and a missing body as an empty graph", () => {
    const m3 = graph.models.find((m) => m.model_id === "m-3")!
    expect(m3.upstreams).toEqual([])
    expect(m3.trigger_upstreams).toEqual([])
    const empty = parseAssetGraph(null)
    expect(empty.nodes).toEqual([])
    expect(empty.stats.truncated).toEqual({ pipelines: false, produced_tables: false, models: false })
  })

  it("walks everything upstream and downstream of the focus, nearest first", () => {
    const v = neighbourhood(graph, M1)
    expect(v.nodeIds[0]).toBe(M1)
    expect(new Set(v.nodeIds)).toEqual(new Set([M1, T_ORDERS, T_CUST, P1, T_DAILY, M2]))
    expect(v.role.get(P1)).toBe("upstream")
    expect(v.distance.get(P1)).toBe(2)
    expect(v.role.get(M2)).toBe("downstream")
    expect(v.distance.get(M2)).toBe(2)
    expect([v.upstreamFound, v.downstreamFound, v.omitted]).toEqual([3, 2, 0])
    // An edge is drawn when both ends are: the pipeline waking weekly_rollup is, the other pipeline is not.
    expect(v.edges).toHaveLength(7)
    expect(v.edges.some((e) => e.from === P2)).toBe(false)
  })

  it("keeps the nearest when capped and counts the rest", () => {
    const v = neighbourhood(graph, M1, { cap: 3 })
    expect(v.nodeIds).toEqual([M1, T_DAILY, T_CUST])
    expect(v.omitted).toBe(3)
    expect([v.upstreamShown, v.downstreamShown]).toEqual([1, 1])
  })

  it("draws as many hops as asked, and says what waits beyond each edge card", () => {
    const one = neighbourhood(graph, M1, { depth: 1 })
    expect(new Set(one.nodeIds)).toEqual(new Set([M1, T_ORDERS, T_CUST, T_DAILY]))
    // The totals still count every hop: the header says "2 of 3 upstream".
    expect([one.upstreamShown, one.upstreamFound, one.downstreamShown, one.downstreamFound]).toEqual([2, 3, 1, 2])
    expect(one.omitted).toBe(0)
    // Both tables are written by orders_sync; weekly_rollup reads the model's table.
    expect(Object.fromEntries(one.hiddenBeyond)).toEqual({ [T_ORDERS]: 1, [T_CUST]: 1, [T_DAILY]: 1 })
    expect(one.edges).toHaveLength(3)

    const two = neighbourhood(graph, M1, { depth: 2 })
    expect(two.nodeIds).toHaveLength(6)
    expect(two.hiddenBeyond.size).toBe(0)
  })

  it("draws the next hop of an expanded asset only, whatever the depth", () => {
    const v = neighbourhood(graph, M1, { depth: 1, expanded: new Set([T_ORDERS]) })
    expect(new Set(v.nodeIds)).toEqual(new Set([M1, T_ORDERS, T_CUST, T_DAILY, P1]))
    expect(v.distance.get(P1)).toBe(2)
    // orders_sync is now drawn, so nothing waits behind customers either.
    expect(Object.fromEntries(v.hiddenBeyond)).toEqual({ [T_DAILY]: 1 })
    // An expanded asset that is not drawn opens nothing.
    expect(neighbourhood(graph, M1, { depth: 1, expanded: new Set([P1]) }).nodeIds).toHaveLength(4)
  })

  it("counts a node on a cycle once, as upstream", () => {
    const loop = parseAssetGraph({
      nodes: [
        { id: "a", kind: "model", name: "a" },
        { id: "b", kind: "table", name: "b" },
      ],
      edges: [
        { from: "a", to: "b", kind: "materializes", evidence: "declared" },
        { from: "b", to: "a", kind: "reads", evidence: "inferred" },
      ],
    })
    const v = neighbourhood(loop, "a")
    expect([v.upstreamFound, v.downstreamFound]).toEqual([1, 0])
    expect(v.nodeIds).toEqual(["a", "b"])
  })

  it("says in words why a model needs attention, and leaves a clean one out", () => {
    const items = needsAttention(graph)
    expect(items.map((i) => i.model.model_id)).toEqual(["m-1", "m-2"])
    expect(items[0].reasons).toEqual([
      "Reads from orders_sync, but runs on a clock rather than when it runs.",
      "Its SQL reads public.refunds, which no pipeline or model here writes, so where that table gets data is unknown.",
    ])
    expect(items[1].reasons).toEqual([
      "Reads from daily_revenue, but does not refresh when it runs.",
      "A table name in its SQL could be more than one table, so an upstream shown may be the wrong one.",
    ])
  })

  it("links pipelines and models to their pages, and tables nowhere", () => {
    expect(assetHref(graph.nodes.find((n) => n.id === P1)!)).toBe("/pipelines/p-1")
    expect(assetHref(graph.nodes.find((n) => n.id === M1)!)).toBe("/explorer/schedules/m-1?tab=graph")
    expect(assetHref(graph.nodes.find((n) => n.id === T_ORDERS)!)).toBeNull()
  })

  it("finds by name, prefix matches and models first", () => {
    expect(searchAssets(graph, "daily").map((n) => n.id)).toEqual([M1, T_DAILY])
    expect(searchAssets(graph, "  ")).toEqual([])
  })
})

describe("folding tables no model reads", () => {
  const wide = parseAssetGraph(WIDE)

  it("starts from a graph the size of the reported one: 25 assets, 19 links", () => {
    expect(wide.nodes).toHaveLength(25)
    expect(wide.edges).toHaveLength(19)
  })

  it("folds each pipeline's unread tables into one box, and leaves read ones and a lone one drawn", () => {
    const f = foldUnreadTables(wholeGraph(wide), wide)
    expect([...f.groups.values()].map((g) => [g.pipeline, g.tables.length])).toEqual([
      [PA, 9],
      [PB, 4],
    ])
    expect(f.nodeIds).toHaveLength(25 - 13 + 2)
    // raw.d1 is read by a model; raw.d0 is its pipeline's only unread table, so a box would hide nothing.
    expect(f.nodeIds).toEqual(expect.arrayContaining(["table:wh-1:raw.d0", "table:wh-1:raw.d1", "table:wh-1:raw.c0"]))
    expect(f.nodeIds.some((id) => id.startsWith("table:wh-1:raw.a"))).toBe(false)
    // One write per box, and no edge left pointing at a folded table.
    const drawn = new Set(f.nodeIds)
    expect(f.edges.every((e) => drawn.has(e.from) && drawn.has(e.to))).toBe(true)
    const intoA = f.edges.filter((e) => e.from === PA)
    expect(intoA).toEqual([{ from: PA, to: f.groups.keys().next().value, kind: "writes", evidence: "observed" }])
  })

  it("draws a box open when asked, and never folds the tables of the focused pipeline", () => {
    const [groupA] = [...foldUnreadTables(wholeGraph(wide), wide).groups.keys()]
    const opened = foldUnreadTables(wholeGraph(wide), wide, new Set([groupA]))
    expect([...opened.groups.values()].map((g) => g.pipeline)).toEqual([PB])
    expect(opened.nodeIds).toHaveLength(25 - 4 + 1)
    expect(foldUnreadTables(wholeGraph(wide), wide, "all").nodeIds).toHaveLength(25)
    expect(foldUnreadTables(neighbourhood(wide, PA), wide).groups.size).toBe(0)
  })

  it("lays the folded workspace out so it can be read whole, with unlinked assets in rows, not a column", async () => {
    const f = foldUnreadTables(wholeGraph(wide), wide)
    const { positions, bounds } = await layoutAssets(f.nodeIds, f.edges)
    expect(positions.size).toBe(f.nodeIds.length)
    const fit = Math.min((CANVAS.width - 48) / bounds.width, (CANVAS.height - 48) / bounds.height)
    expect(fit).toBeGreaterThanOrEqual(MIN_READABLE_FIT_ZOOM)
    // The two models linked to nothing sit side by side, under everything else.
    const m1 = positions.get("model:m-1")!
    const m2 = positions.get("model:m-2")!
    expect(m1.y).toBe(m2.y)
    for (const [id, p] of positions) if (!id.startsWith("model:m-1") && !id.startsWith("model:m-2")) expect(p.y).toBeLessThan(m1.y)
  })

  it("makes room under a table whose columns are open", async () => {
    const ids = ["p", "a", "b"]
    const links = [
      { from: "p", to: "a" },
      { from: "p", to: "b" },
    ]
    // A column list as tall as one with five rows.
    const panel = 167
    const tall = (open: string) => (id: string) => ({
      width: ASSET_NODE_WIDTH,
      height: ASSET_NODE_HEIGHT + (id === open ? panel : 0),
    })
    const shut = await layoutAssets(ids, links)
    const opened = await layoutAssets(ids, links, tall("a"))
    expect(opened.bounds.height - shut.bounds.height).toBe(panel)
    const [top, below] = [opened.positions.get("a")!, opened.positions.get("b")!].sort((x, y) => x.y - y.y)
    expect(below.y - top.y).toBeGreaterThanOrEqual(ASSET_NODE_HEIGHT + (top === opened.positions.get("a") ? panel : 0))
  })
})

describe("revealShift", () => {
  // A 1000px canvas; the card the reader clicked spans 400..656, the added one is given.
  const shift = (lo: number, hi: number, keep: [number, number] = [400, 656]) => revealShift(lo, hi, keep[0], keep[1], 1000)

  it("leaves the view alone when the added cards are on the canvas", () => {
    expect(shift(700, 956)).toBe(0)
    expect(shift(24, 280)).toBe(0)
  })

  it("pans only as far as brings them in, with a margin", () => {
    expect(shift(900, 1156)).toBe(-180)
    expect(shift(-300, -44, [400, 600])).toBe(324)
  })

  it("shows where a span wider than the canvas starts", () => {
    expect(shift(700, 2000, [700, 956])).toBe(-676)
  })

  it("stops where the clicked card would leave the canvas", () => {
    expect(shift(900, 1500)).toBe(-376)
    expect(shift(-600, -44, [400, 900])).toBe(76)
  })

  it("does not push a clicked card that is already at the edge any further", () => {
    expect(shift(900, 1156, [10, 266])).toBe(0)
    expect(shift(-300, -44, [800, 990])).toBe(0)
  })
})

describe("findWarehouseTable", () => {
  const tables = [
    { name: "orders", schema: "public", columns: [] },
    { name: "orders", schema: "staging", columns: [] },
    { name: "Customers", schema: "public", columns: [] },
  ]

  it("finds a schema-qualified name in its schema, whatever the case", () => {
    expect(findWarehouseTable("public.orders", tables)).toEqual({ found: "one", table: tables[0] })
    expect(findWarehouseTable("PUBLIC.customers", tables)).toEqual({ found: "one", table: tables[2] })
  })

  it("matches a bare name in any schema, and says when that is more than one", () => {
    expect(findWarehouseTable("customers", tables)).toEqual({ found: "one", table: tables[2] })
    expect(findWarehouseTable("orders", tables)).toEqual({ found: "many", count: 2 })
  })

  it("says when the warehouse does not list it", () => {
    expect(findWarehouseTable("analytics.orders", tables)).toEqual({ found: "none" })
    expect(findWarehouseTable("refunds", [])).toEqual({ found: "none" })
  })
})

describe("AssetLineageView", () => {
  it("summarises the workspace and lists the models that need attention", async () => {
    mockFetch.mockResolvedValue(res(200, BODY))
    render(<AssetLineageView reloadTick={0} />)

    const list = await screen.findByRole("list", { name: "Models that need attention" })
    expect(mockFetch).toHaveBeenCalledWith(URL, { method: "GET" })
    const rows = [...list.querySelectorAll("[data-attention]")].map((r) => r.getAttribute("data-attention"))
    expect(rows).toEqual(["m-1", "m-2"])
    expect(within(list).getByRole("link", { name: "daily_revenue" })).toHaveAttribute("href", "/explorer/schedules/m-1?tab=graph")
    expect(within(list).getByText("Runs on a cron schedule")).toBeInTheDocument()

    const tile = screen.getByText("Not refreshed by what they read").parentElement!
    expect(tile).toHaveTextContent("2of 3 models")
  })

  it("focuses an asset from the attention list and lists what feeds it and what it feeds", async () => {
    mockFetch.mockResolvedValue(res(200, BODY))
    render(<AssetLineageView reloadTick={0} />)

    // The whole workspace fits, so it is listed by kind before anything is picked.
    expect(await screen.findByText("Whole workspace")).toBeInTheDocument()
    expect(screen.getByText("Pipelines (2)")).toBeInTheDocument()

    await userEvent.click(screen.getByRole("button", { name: "Show daily_revenue in the graph" }))
    expect(screen.getByRole("heading", { name: /What feeds daily_revenue, and what it feeds/ })).toBeInTheDocument()

    // One hop each side to start, and the header says how much more there is.
    expect(screen.getByText("2 of 3 upstream · 1 of 2 downstream")).toBeInTheDocument()
    const section = (heading: RegExp) => screen.getByRole("heading", { name: heading }).closest("section")!
    const names = (s: HTMLElement) => [...s.querySelectorAll("[data-asset-row] .font-mono")].map((n) => n.textContent)
    expect(names(section(/^Upstream \(2 of 3\)/))).toEqual(["public.customers", "public.orders"])
    expect(names(section(/^Downstream \(1 of 2\)/))).toEqual(["analytics.daily_revenue"])

    // One asset's next hop opens on its own.
    await userEvent.click(screen.getByRole("button", { name: "Show 1 more upstream of public.orders" }))
    expect(screen.getByText("3 upstream · 1 of 2 downstream")).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /more upstream of public\.customers/ })).toBeNull()

    // Every hop.
    const hops = screen.getByRole("group", { name: "Hops drawn on each side" })
    await userEvent.click(within(hops).getByRole("button", { name: "Every hop" }))
    expect(within(hops).getByRole("button", { name: "Every hop" })).toHaveAttribute("aria-pressed", "true")
    expect(screen.getByText("3 upstream · 2 downstream")).toBeInTheDocument()
    expect(names(section(/^Upstream \(3\)/))).toEqual(["public.customers", "public.orders", "orders_sync"])
    expect(names(section(/^Downstream \(2\)/))).toEqual(["analytics.daily_revenue", "weekly_rollup"])
    expect(within(section(/^Upstream/)).getByText("Upstream, 2 hops away")).toBeInTheDocument()
    expect(within(section(/^Upstream/)).getByRole("link", { name: "orders_sync" })).toHaveAttribute("href", "/pipelines/p-1")

    // Focus moves along the chain, and back out to the whole workspace.
    await userEvent.click(screen.getByRole("button", { name: "Show what feeds weekly_rollup and what it feeds" }))
    expect(screen.getByRole("heading", { name: /What feeds weekly_rollup/ })).toBeInTheDocument()
    await userEvent.click(screen.getByRole("button", { name: "Show the whole workspace" }))
    expect(screen.getByText("Whole workspace")).toBeInTheDocument()
  })

  it("opens a table's columns under its row in the list, one request per warehouse", async () => {
    const orders = [{ name: "order_id", type: "uuid", is_primary_key: true }]
    mockFetch.mockImplementation(async (url: string) =>
      url.includes("/schema-index")
        ? res(200, {
            tables: [
              { name: "orders", schema: "public", columns: orders },
              { name: "customers", schema: "public", columns: [{ name: "email", type: "text" }] },
            ],
          })
        : res(200, BODY),
    )
    render(<AssetLineageView reloadTick={0} />)
    await screen.findByText("Whole workspace")
    // Pipelines and models have no columns to show.
    expect(screen.getAllByRole("button", { name: /^Columns of / })).toHaveLength(4)

    const button = screen.getByRole("button", { name: "Columns of public.orders" })
    expect(button).toHaveAttribute("aria-expanded", "false")
    expect(button).not.toHaveAttribute("aria-controls")
    await userEvent.click(button)
    expect(button).toHaveAttribute("aria-expanded", "true")
    const panel = document.getElementById(button.getAttribute("aria-controls")!)!
    expect(await within(panel).findByRole("list", { name: "Columns of public.orders" })).toHaveTextContent("order_id")
    expect(panel.closest("[data-asset-row]")).toHaveTextContent("public.orders")

    await userEvent.click(screen.getByRole("button", { name: "Columns of public.customers" }))
    expect(await screen.findByRole("list", { name: "Columns of public.customers" })).toHaveTextContent("email")
    await userEvent.click(screen.getByRole("button", { name: "Columns of public.events" }))
    const schemaCalls = () => mockFetch.mock.calls.map(([u]) => String(u)).filter((u) => u.includes("/schema-index"))
    expect(schemaCalls()).toEqual([
      "/api/v1/explorer/connections/c-1/schema-index",
      "/api/v1/explorer/connections/c-2/schema-index",
    ])

    await userEvent.click(button)
    expect(button).toHaveAttribute("aria-expanded", "false")
    expect(screen.queryByRole("list", { name: "Columns of public.orders" })).toBeNull()
  })

  it("names an edge by what it does and how the backend knows it", () => {
    expect(edgeLabel({ kind: "writes", evidence: "observed" })).toBe("Writes · seen in a run")
    expect(edgeLabel({ kind: "materializes", evidence: "declared" })).toBe("Builds · configured")
    expect(edgeLabel({ kind: "reads", evidence: "inferred" })).toBe("Read by · read from a model's SQL")
    expect(edgeLabel({ kind: "triggers", evidence: "declared" })).toBe("Wakes when it finishes · configured")
  })

  it("draws edges by how the backend knows them", async () => {
    media.canvas = true
    mockFetch.mockResolvedValue(res(200, BODY))
    const { container } = render(<AssetLineageView reloadTick={0} />)

    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(9))
    expect(container.querySelectorAll(".react-flow__edge")).toHaveLength(8)
    const dashes = [...container.querySelectorAll<SVGPathElement>(".react-flow__edge-path")].map(
      (p) => p.style.strokeDasharray || "solid",
    )
    // 3 writes seen in a run; the trigger and the materialization configured; 3 reads parsed out of SQL.
    expect(dashes.filter((d) => d === "solid")).toHaveLength(3)
    expect(dashes.filter((d) => d === "6 4")).toHaveLength(2)
    expect(dashes.filter((d) => d === "2 4")).toHaveLength(3)
    expect(screen.getByText("read from a model's SQL")).toBeInTheDocument()

    // Each edge names itself when the pointer rests on its middle.
    const canvas = container.querySelector(".react-flow")!.parentElement!
    expect(within(canvas).getAllByText("Read by · read from a model's SQL")).toHaveLength(3)
    expect(within(canvas).getByText("Wakes when it finishes · configured")).toBeInTheDocument()

    // A click on a card focuses it: one hop each side stays drawn.
    fireEvent.click(within(canvas).getByText("daily_revenue"))
    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(4))
    expect(container.querySelectorAll(".react-flow__edge")).toHaveLength(3)
    expect(within(canvas).getByText("Selected")).toBeInTheDocument()

    // +1 on the model's table draws the model that reads it, and nothing else.
    const more = within(canvas).getAllByTitle(/^Show 1 more (upstream|downstream)$/)
    expect(more.map((b) => b.title).sort()).toEqual(["Show 1 more downstream", "Show 1 more upstream", "Show 1 more upstream"])
    fireEvent.click(within(canvas).getByTitle("Show 1 more downstream"))
    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(5))
    expect(within(canvas).getByText("weekly_rollup")).toBeInTheDocument()
    // Asking for a hop is not asking to focus the card it is on.
    expect(within(canvas).getByText("Selected").closest(".react-flow__node")).toHaveTextContent("daily_revenue")
  })

  it("puts a moved card back when a new layout lands, and keeps the card the reader acted on where they left it", async () => {
    media.canvas = true
    const tables = [{ name: "daily_revenue", schema: "analytics", columns: [{ name: "day", type: "date", is_primary_key: true }] }]

    // Focus the model, and open its table's columns: the same drawing twice, once untouched.
    async function open(moveFirst: boolean) {
      mockFetch.mockImplementation(async (url: string) =>
        url.includes("/schema-index") ? res(200, { tables, foreign_keys: [] }) : res(200, BODY),
      )
      const { container, unmount } = render(<AssetLineageView reloadTick={0} />)
      await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(9))
      const canvas = container.querySelector(".react-flow")!.parentElement!
      fireEvent.click(within(canvas).getByText("daily_revenue"))
      await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(4))

      const card = (text: string) => within(canvas).getByText(text).closest<HTMLElement>(".react-flow__node")!
      const flowAt = (text: string) => translateOf(card(text).style.transform)
      const screenAt = (text: string) => {
        const { left, top } = screenBox(container, card(text))
        return [left, top]
      }
      // A drag as d3-drag reads one: pressed on the card, moved and released on the window.
      // d3-drag follows the drag on event.view, which jsdom will not take in the init.
      const mouse = (target: Element | Window, type: string, x: number, y: number) => {
        const event = new MouseEvent(type, { bubbles: true, cancelable: true, button: 0, clientX: x, clientY: y })
        Object.defineProperty(event, "view", { value: window })
        fireEvent(target, event)
      }
      const drag = (text: string, dx: number, dy: number) => {
        mouse(card(text), "mousedown", 600, 300)
        mouse(window, "mousemove", 600 + dx / 2, 300 + dy / 2)
        mouse(window, "mousemove", 600 + dx, 300 + dy)
        mouse(window, "mouseup", 600 + dx, 300 + dy)
      }

      let tableBefore: number[] | null = null
      if (moveFirst) {
        await userEvent.click(screen.getByRole("button", { name: "Unlock to move cards" }))
        const ordersFrom = flowAt("public.orders")
        drag("public.orders", 240, 160)
        await waitFor(() => expect(flowAt("public.orders")).not.toEqual(ordersFrom))
        const tableFrom = flowAt("analytics.daily_revenue")
        drag("analytics.daily_revenue", -480, 200)
        await waitFor(() => expect(flowAt("analytics.daily_revenue")).not.toEqual(tableFrom))
        tableBefore = screenAt("analytics.daily_revenue")
      }
      // A new layout, with the card as tall as its column list, that adds no card.
      fireEvent.click(within(card("analytics.daily_revenue")).getByTitle("Show its columns"))
      await waitFor(() => expect(within(card("analytics.daily_revenue")).getByText("day")).toBeInTheDocument())
      await waitFor(() => expect(parseFloat(card("analytics.daily_revenue").style.height)).toBeGreaterThan(ASSET_NODE_HEIGHT))
      expect(container.querySelectorAll(".react-flow__node")).toHaveLength(4)
      const after = { orders: flowAt("public.orders"), table: flowAt("analytics.daily_revenue"), tableOnScreen: screenAt("analytics.daily_revenue") }
      unmount()
      return { ...after, tableBefore }
    }

    // jsdom gives every element an empty box, so each point of a drag reads as the canvas's
    // edge and React Flow pans the view under it. Give the canvas its size.
    const box = vi.spyOn(Element.prototype, "getBoundingClientRect").mockImplementation(function (this: Element) {
      const { width, height } = this.classList.contains("react-flow") ? CANVAS : { width: 0, height: 0 }
      return { x: 0, y: 0, left: 0, top: 0, right: width, bottom: height, width, height, toJSON: () => ({}) }
    })
    try {
      const untouched = await open(false)
      const moved = await open(true)
      // A card moved in the old drawing takes its place in the new one, like every other card.
      expect(moved.orders).toEqual(untouched.orders)
      expect(moved.table).toEqual(untouched.table)
      // The card whose columns were opened stays where the reader had dragged it on screen.
      expect(moved.tableOnScreen.map(Math.round)).toEqual(moved.tableBefore!.map(Math.round))
    } finally {
      box.mockRestore()
    }
  })

  // A model whose table `n` models read. Focused on the model, the table's +n draws a column of n cards.
  function fanOut(n: number) {
    const [src, hub] = ["table:c-1:raw.src", "table:c-1:gold.hub"]
    return {
      ...BODY,
      nodes: [
        { id: "pipeline:p-1", kind: "pipeline", name: "src_sync", ref_id: "p-1" },
        { id: src, kind: "table", name: "raw.src", connection_id: "c-1" },
        { id: "model:m-0", kind: "model", name: "hub_model", ref_id: "m-0" },
        { id: hub, kind: "table", name: "gold.hub", connection_id: "c-1" },
        ...Array.from({ length: n }, (_, i) => ({ id: `model:r-${i}`, kind: "model", name: `reader_${i}`, ref_id: `r-${i}` })),
      ],
      edges: [
        { from: "pipeline:p-1", to: src, kind: "writes", evidence: "observed" },
        { from: src, to: "model:m-0", kind: "reads", evidence: "inferred" },
        { from: "model:m-0", to: hub, kind: "materializes", evidence: "declared" },
        ...Array.from({ length: n }, (_, i) => ({ from: hub, to: `model:r-${i}`, kind: "reads", evidence: "inferred" })),
      ],
      models: [],
    }
  }

  /** Focuses the fan-out's model, clicks its table's +n, and returns the canvas once the n cards are drawn. */
  async function openFan(n: number) {
    media.canvas = true
    mockFetch.mockResolvedValue(res(200, fanOut(n)))
    const { container } = render(<AssetLineageView reloadTick={0} />)
    const canvas = await waitFor(() => {
      const el = container.querySelector(".react-flow")!.parentElement!
      expect(within(el).getByText("hub_model")).toBeInTheDocument()
      return el
    })
    fireEvent.click(within(canvas).getByText("hub_model"))
    const hub = () => within(canvas).getByText("gold.hub").closest<HTMLElement>(".react-flow__node")!
    await waitFor(() => expect(within(hub()).getByTitle(`Show ${n} more downstream`)).toBeInTheDocument())
    const before = container.querySelectorAll(".react-flow__node").length
    fireEvent.click(within(hub()).getByTitle(`Show ${n} more downstream`))
    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(before + n))
    const cards = [...container.querySelectorAll<HTMLElement>(".react-flow__node")]
    const zoom = Number(container.querySelector<HTMLElement>(".react-flow__viewport")!.style.transform.match(/scale\(([\d.]+)\)/)![1])
    return { container, cards, zoom, hub: screenBox(container, hub()) }
  }

  const inCanvas = (box: ReturnType<typeof screenBox>) =>
    box.left >= 0 && box.top >= 0 && box.right <= CANVAS.width && box.bottom <= CANVAS.height

  it("zooms out after a +N to fit every card, the one clicked included", async () => {
    // Six cards stacked are too tall for the canvas at full size, but readable fitted.
    const { container, cards, zoom } = await openFan(6)
    const boxes = cards.map((c) => screenBox(container, c))
    expect(boxes.filter((b) => !inCanvas(b))).toEqual([])

    // Fitted, not panned: the zoom that fits the whole drawing, and the drawing centred.
    const [left, top] = [Math.min(...boxes.map((b) => b.left)), Math.min(...boxes.map((b) => b.top))]
    const [right, bottom] = [Math.max(...boxes.map((b) => b.right)), Math.max(...boxes.map((b) => b.bottom))]
    const fit = Math.min((CANVAS.width - 48) / ((right - left) / zoom), (CANVAS.height - 48) / ((bottom - top) / zoom), 1)
    expect(zoom).toBeLessThan(1)
    expect(zoom).toBeGreaterThanOrEqual(MIN_READABLE_FIT_ZOOM)
    expect(zoom).toBeCloseTo(fit, 3)
    expect(left).toBeCloseTo(CANVAS.width - right, 0)
    expect(top).toBeCloseTo(CANVAS.height - bottom, 0)
  })

  it("after a +N too big to read whole, zooms out only to the readable size and keeps the card clicked in view", async () => {
    const { container, cards, zoom, hub } = await openFan(14)
    expect(zoom).toBe(MIN_READABLE_FIT_ZOOM)
    expect(inCanvas(hub)).toBe(true)
    // The column it added sits beside the card, half of it above. The view moves toward the
    // column's start as far as keeps the card on the canvas; the rest is a pan away.
    expect(hub.bottom).toBeCloseTo(CANVAS.height - 24, 0)
    const added = cards.filter((c) => /reader_/.test(c.textContent ?? "")).map((c) => screenBox(container, c))
    expect(added.filter(inCanvas).length).toBeGreaterThan(0)
    expect(added.some((b) => !inCanvas(b))).toBe(true)
    // Across, the card clicked stays put through the zoom and moves only as far as the column
    // needs to fit: the column's edge lands 24px inside the canvas.
    expect(Math.max(...added.map((b) => b.right))).toBeCloseTo(CANVAS.width - 24, 0)
  })

  it("shows a table's columns on its card, from the warehouse's schema", async () => {
    media.canvas = true
    const columns = Array.from({ length: 12 }, (_, i) => ({ name: `col_${i}`, type: i === 0 ? "uuid" : "text", is_primary_key: i === 0 }))
    const tables = [
      { name: "orders", schema: "public", columns },
      { name: "customers", schema: "public", columns: [{ name: "customer_id", type: "bigint", is_primary_key: true }] },
    ]
    mockFetch.mockImplementation(async (url: string) =>
      url.includes("/schema-index") ? res(200, { tables, foreign_keys: [] }) : res(200, BODY),
    )
    const { container } = render(<AssetLineageView reloadTick={0} />)
    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(9))
    const canvas = container.querySelector(".react-flow")!.parentElement!
    const card = () => within(canvas).getByText("public.orders").closest<HTMLElement>(".react-flow__node")!

    fireEvent.click(within(card()).getByTitle("Show its columns"))
    expect(mockFetch).toHaveBeenCalledWith("/api/v1/explorer/connections/c-1/schema-index")
    await waitFor(() => expect(within(card()).getByText("col_0")).toBeInTheDocument())
    expect(within(card()).getByText("key")).toBeInTheDocument()
    // Ten at a time; the pager reaches the rest.
    expect(within(card()).getByText("1–10 of 12")).toBeInTheDocument()
    expect(within(card()).queryByText("col_11")).toBeNull()
    // The card is laid out as tall as its ten rows, search and pager, and reading it does not focus the table.
    const height = (el: HTMLElement) => parseFloat(el.style.height)
    await waitFor(() => expect(height(card())).toBeGreaterThanOrEqual(ASSET_NODE_HEIGHT + 10 * 20))
    fireEvent.click(within(card()).getByText("col_0"))
    expect(within(canvas).queryByText("Selected")).toBeNull()

    // The next table in the same warehouse needs no second request, and one column gets a short card.
    const customers = () => within(canvas).getByText("public.customers").closest<HTMLElement>(".react-flow__node")!
    fireEvent.click(within(customers()).getByTitle("Show its columns"))
    await waitFor(() => expect(within(customers()).getByText("customer_id")).toBeInTheDocument())
    expect(mockFetch.mock.calls.filter(([u]) => String(u).includes("/schema-index"))).toHaveLength(1)
    await waitFor(() => expect(height(customers())).toBeGreaterThan(ASSET_NODE_HEIGHT))
    expect(height(customers())).toBeLessThanOrEqual(ASSET_NODE_HEIGHT + 80)
    expect(height(card())).toBeGreaterThan(height(customers()))
  })

  it("draws a workspace whose pipelines write many tables so every card is readable in the first view", async () => {
    media.canvas = true
    mockFetch.mockResolvedValue(res(200, WIDE))
    const { container } = render(<AssetLineageView reloadTick={0} />)

    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(14))
    const inView = () => {
      const vp = container.querySelector<HTMLElement>(".react-flow__viewport")!.style.transform
      const [, tx, ty, z] = vp.match(/translate\((-?[\d.]+)px, ?(-?[\d.]+)px\) scale\(([\d.]+)\)/)!.map(Number)
      const cards = [...container.querySelectorAll<HTMLElement>(".react-flow__node")].map((n) => {
        const [, x, y] = n.style.transform.match(/translate\((-?[\d.]+)px, ?(-?[\d.]+)px\)/)!.map(Number)
        return { x: tx + x * z, y: ty + y * z, right: tx + (x + ASSET_NODE_WIDTH) * z, bottom: ty + (y + ASSET_NODE_HEIGHT) * z }
      })
      return { zoom: z, outside: cards.filter((c) => c.x < 0 || c.y < 0 || c.right > CANVAS.width || c.bottom > CANVAS.height).length }
    }
    await waitFor(() => expect(inView().zoom).not.toBe(1))
    expect(inView().zoom).toBeGreaterThanOrEqual(MIN_READABLE_FIT_ZOOM)
    expect(inView().outside).toBe(0)

    // The nine tables no model reads are one box, which opens in place.
    const canvas = container.querySelector(".react-flow")!.parentElement!
    fireEvent.click(within(canvas).getByText("9 tables no model reads"))
    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(14 - 1 + 9))
    expect(within(canvas).getByText("raw.a0")).toBeInTheDocument()
    await userEvent.click(screen.getByRole("button", { name: "Fold unread tables again" }))
    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(14))

    // Resetting the view never lands below the readable size, as "fit everything" used to.
    await userEvent.click(screen.getByRole("button", { name: "Reset view" }))
    expect(inView().zoom).toBeGreaterThanOrEqual(MIN_READABLE_FIT_ZOOM)
  })

  it("opens a drawing too big to read at its start, with an overview map, and in full screen", async () => {
    media.canvas = true
    mockFetch.mockResolvedValue(res(200, WIDE))
    const { container } = render(<AssetLineageView reloadTick={0} />)

    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(14))
    expect(container.querySelector(".react-flow__minimap")).toBeNull()

    // Every table drawn is too tall to fit at a readable size. Opening them leaves the view
    // where the reader had it; a map shows where the rest is, and Reset view starts from
    // the cards at a readable size.
    const zoom = () =>
      Number(container.querySelector<HTMLElement>(".react-flow__viewport")!.style.transform.match(/scale\(([\d.]+)\)/)![1])
    await waitFor(() => expect(zoom()).not.toBe(1))
    const before = zoom()
    await userEvent.click(screen.getByRole("button", { name: "Show every table" }))
    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(25))
    await waitFor(() => expect(container.querySelector(".react-flow__minimap")).not.toBeNull())
    expect(zoom()).toBe(before)
    expect(screen.queryByRole("button", { name: "Show every table" })).toBeNull()
    await userEvent.click(screen.getByRole("button", { name: "Reset view" }))
    expect(zoom()).toBe(MIN_READABLE_FIT_ZOOM)

    const opener = screen.getByRole("button", { name: "Full screen" })
    await userEvent.click(opener)
    const dialog = screen.getByRole("dialog", { name: "Whole workspace" })
    await waitFor(() => expect(dialog.querySelectorAll(".react-flow__node")).toHaveLength(25))
    expect(within(dialog).queryByRole("button", { name: "Full screen" })).toBeNull()
    await userEvent.keyboard("{Escape}")
    expect(screen.queryByRole("dialog")).toBeNull()
    expect(opener).toHaveFocus()
  })

  it("asks for a pick when the workspace is too big to draw, and finds by search", async () => {
    const many = {
      ...BODY,
      nodes: [
        ...BODY.nodes,
        ...Array.from({ length: 80 }, (_, i) => ({ id: `table:c-9:t${i}`, kind: "table", name: `filler_${i}` })),
      ],
    }
    mockFetch.mockResolvedValue(res(200, many))
    render(<AssetLineageView reloadTick={0} />)

    expect(await screen.findByText(/has 89 assets, too many to draw at once/)).toBeInTheDocument()
    expect(document.querySelectorAll("[data-asset-row]")).toHaveLength(0)

    const search = screen.getByRole("combobox", { name: "Find a pipeline, table or model" })
    await userEvent.type(search, "weekly")
    const matches = screen.getByRole("listbox", { name: "Matches" })
    expect(within(matches).getAllByRole("option")).toHaveLength(1)
    expect(screen.getByRole("status")).toHaveTextContent("1 match")
    await userEvent.type(search, "{Enter}")
    expect(screen.getByRole("heading", { name: /What feeds weekly_rollup/ })).toBeInTheDocument()
    expect(search).toHaveValue("")

    // The arrow keys move through the matches; Enter picks the highlighted one.
    await userEvent.type(search, "daily")
    const options = within(screen.getByRole("listbox", { name: "Matches" })).getAllByRole("option")
    expect(options.map((o) => o.textContent)).toEqual(["Model daily_revenue", "Table analytics.daily_revenue Warehouse c-1"])
    expect(search).toHaveAttribute("aria-activedescendant", options[0].id)
    await userEvent.keyboard("{ArrowDown}")
    expect(search).toHaveAttribute("aria-activedescendant", options[1].id)
    expect(options[1]).toHaveAttribute("aria-selected", "true")
    await userEvent.keyboard("{ArrowDown}")
    expect(search).toHaveAttribute("aria-activedescendant", options[0].id)
    await userEvent.keyboard("{ArrowUp}{Enter}")
    expect(screen.getByRole("heading", { name: /What feeds analytics\.daily_revenue/ })).toBeInTheDocument()

    // Leaving the box closes the list; a click on a match still picks it.
    await userEvent.type(search, "orders")
    expect(screen.getByRole("listbox", { name: "Matches" })).toBeInTheDocument()
    await userEvent.tab()
    expect(screen.queryByRole("listbox")).toBeNull()
    expect(search).toHaveAttribute("aria-expanded", "false")
    await userEvent.click(search)
    await userEvent.click(within(screen.getByRole("listbox", { name: "Matches" })).getByText("orders_sync"))
    expect(screen.getByRole("heading", { name: /What feeds orders_sync/ })).toBeInTheDocument()
  })


  it("filters the graph to one warehouse, and names each warehouse from its connection", async () => {
    mockConnections.mockResolvedValue({
      connections: [
        { id: "c-1", name: "analytics_wh" },
        { id: "c-2", name: "events_wh" },
      ],
      total: 2,
    })
    const EVENTS_ONLY = {
      ...BODY,
      nodes: BODY.nodes.filter((n) => n.id === P2 || n.id === T_EVENTS),
      edges: BODY.edges.filter((e) => e.from === P2),
      models: [],
    }
    mockFetch.mockImplementation(async (url: string) => res(200, url.includes("connection_id=") ? EVENTS_ONLY : BODY))
    render(<AssetLineageView reloadTick={0} />)

    const filter = await screen.findByRole("combobox", { name: "Warehouse" })
    await waitFor(() =>
      expect(within(filter).getAllByRole("option").map((o) => o.textContent)).toEqual(["All warehouses", "analytics_wh", "events_wh"]),
    )
    // With more than one warehouse drawn, each table says which one it is in.
    const row = (name: string) => screen.getByText(name, { selector: ".font-mono" }).closest("[data-asset-row]") as HTMLElement
    expect(within(row("public.events")).getByText("events_wh")).toBeInTheDocument()
    expect(within(row("public.orders")).getByText("analytics_wh")).toBeInTheDocument()

    // The backend filters: pipelines by where they write, models by where they run.
    await userEvent.selectOptions(filter, "events_wh")
    expect(mockFetch).toHaveBeenLastCalledWith(`${URL}?connection_id=c-2`, { method: "GET" })
    expect(await screen.findByText("Everything in events_wh")).toBeInTheDocument()
    await waitFor(() => expect(screen.getByText("Pipelines (1)")).toBeInTheDocument())
    // One warehouse drawn: labelling every card with it says nothing.
    expect(within(row("public.events")).queryByText("events_wh")).toBeNull()
    // The other warehouses stay on offer.
    expect(within(filter).getAllByRole("option")).toHaveLength(3)

    await userEvent.selectOptions(filter, "All warehouses")
    expect(mockFetch).toHaveBeenLastCalledWith(URL, { method: "GET" })
    expect(await screen.findByText("Whole workspace")).toBeInTheDocument()
  })

  it("keeps the warehouse filter out of a one-warehouse workspace, and names a warehouse it cannot look up by id", async () => {
    const ONE = { ...BODY, nodes: BODY.nodes.filter((n) => n.id !== T_EVENTS), edges: BODY.edges.filter((e) => e.to !== T_EVENTS) }
    mockFetch.mockResolvedValue(res(200, ONE))
    const { unmount } = render(<AssetLineageView reloadTick={0} />)
    await screen.findByText("Whole workspace")
    expect(screen.queryByRole("combobox", { name: "Warehouse" })).toBeNull()
    expect(mockConnections).not.toHaveBeenCalled()
    unmount()

    // The connection list did not load: the filter still works, by a short id.
    mockConnections.mockRejectedValue(new Error("network"))
    mockFetch.mockResolvedValue(res(200, BODY))
    render(<AssetLineageView reloadTick={0} />)
    const filter = await screen.findByRole("combobox", { name: "Warehouse" })
    await waitFor(() => expect(mockConnections).toHaveBeenCalled())
    expect(within(filter).getAllByRole("option").map((o) => o.textContent)).toEqual(["All warehouses", "Warehouse c-1", "Warehouse c-2"])
  })

  it("says when the graph was cut short", async () => {
    mockFetch.mockResolvedValue(
      res(200, { ...BODY, stats: { ...BODY.stats, truncated: { pipelines: true, produced_tables: false, models: false } } }),
    )
    render(<AssetLineageView reloadTick={0} />)
    expect(await screen.findByText(/more than 500 pipelines\. The graph stops there/)).toBeInTheDocument()
  })

  it("says so when there is nothing to draw", async () => {
    mockFetch.mockResolvedValue(res(200, { nodes: null, edges: null, models: null, stats: {} }))
    render(<AssetLineageView reloadTick={0} />)
    expect(await screen.findByText("Nothing to draw yet.")).toBeInTheDocument()
  })

  it("offers a retry after a failed load, and keeps the graph when a reload fails", async () => {
    mockFetch.mockResolvedValueOnce(res(500, { error: "boom" }))
    mockFetch.mockResolvedValueOnce(res(200, BODY))
    mockFetch.mockRejectedValueOnce(new Error("network"))
    const { rerender } = render(<AssetLineageView reloadTick={0} />)

    expect(await screen.findByRole("alert")).toHaveTextContent("HTTP 500")
    await userEvent.click(screen.getByRole("button", { name: "Retry" }))
    await screen.findByText("Whole workspace")

    rerender(<AssetLineageView reloadTick={1} />)
    expect(await screen.findByRole("alert")).toHaveTextContent("Showing the graph from the last load.")
    expect(screen.getByText("Whole workspace")).toBeInTheDocument()
  })
})
