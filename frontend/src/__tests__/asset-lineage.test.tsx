import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import { AssetLineageView } from "@/components/explorer/AssetLineageView"
import {
  assetHref,
  needsAttention,
  neighbourhood,
  parseAssetGraph,
  searchAssets,
  type AssetGraph,
} from "@/components/explorer/assetLineage"
import { authFetch } from "@/lib/api/auth-fetch"

// GET /api/v1/explorer/asset-graph (asset_graph.go) had no reader. The Lineage page draws it,
// says which models are refreshed out of step with what they read, and keeps the backend's
// three grades of evidence apart: an edge read out of SQL is not drawn like one seen in a run.

const media = vi.hoisted(() => ({ canvas: false }))

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("next-themes", () => ({ useTheme: () => ({ resolvedTheme: "light" }) }))

const mockFetch = authFetch as unknown as Mock
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

describe("assetLineage helpers", () => {
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
    const v = neighbourhood(graph, M1, 3)
    expect(v.nodeIds).toEqual([M1, T_DAILY, T_CUST])
    expect(v.omitted).toBe(3)
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
    expect(screen.getByText("3 upstream · 2 downstream")).toBeInTheDocument()

    const section = (heading: RegExp) => screen.getByRole("heading", { name: heading }).closest("section")!
    const names = (s: HTMLElement) => [...s.querySelectorAll("[data-asset-row] .font-mono")].map((n) => n.textContent)
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

    // A click on a card focuses it: only its neighbourhood stays drawn.
    const canvas = container.querySelector(".react-flow")!.parentElement!
    fireEvent.click(within(canvas).getByText("daily_revenue"))
    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(6))
    expect(container.querySelectorAll(".react-flow__edge")).toHaveLength(7)
    expect(within(canvas).getByText("Selected")).toBeInTheDocument()
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

    const search = screen.getByRole("searchbox", { name: "Find a pipeline, table or model" })
    await userEvent.type(search, "weekly")
    const matches = screen.getByRole("list", { name: "Matches" })
    expect(within(matches).getAllByRole("button")).toHaveLength(1)
    await userEvent.type(search, "{Enter}")
    expect(screen.getByRole("heading", { name: /What feeds weekly_rollup/ })).toBeInTheDocument()
    expect(search).toHaveValue("")
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
