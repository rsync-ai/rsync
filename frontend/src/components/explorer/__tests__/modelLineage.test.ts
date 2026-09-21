import { describe, expect, it } from "vitest"
import {
  LINEAGE_NODE_HEIGHT,
  LINEAGE_NODE_WIDTH,
  MAX_LINEAGE_NODES,
  buildModelLineage,
  describeLineageNode,
  initialViewport,
  layoutLineage,
  lineageKey,
  liveRunningKeys,
  lookupFromResponse,
  needsOlderRuns,
  nextRunsCursor,
  type DescribeLineageContext,
  type LineageNode,
  type LineagePipeline,
  type NodeLookup,
} from "@/components/explorer/modelLineage"
import type { ScheduledQuery } from "@/components/explorer/scheduledModel"
import type { RunningModelsResponse } from "@/components/explorer/liveState"

type Upstream = { kind: string; id: string; name?: string }

function sched(id: string, overrides: Partial<ScheduledQuery> = {}): ScheduledQuery {
  return {
    schedule_id: `s-${id}`,
    saved_query_id: id,
    name: id,
    connection_id: "c-1",
    schedule_type: "cron",
    schedule_spec: { cron: "0 3 * * *" },
    status: "active",
    materialization: "table",
    created_by: "u-1",
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-01T00:00:00Z",
    ...overrides,
  }
}

/** A model that runs after `upstreams`. A bare string is a model upstream. */
function after(id: string, upstreams: (string | Upstream)[], overrides: Partial<ScheduledQuery> = {}): ScheduledQuery {
  return sched(id, {
    schedule_type: "after_upstream",
    schedule_spec: {},
    upstreams: upstreams.map((u) => (typeof u === "string" ? { kind: "model", id: u, name: u } : u)),
    upstream_policy: "any",
    ...overrides,
  })
}

const keys = (nodes: LineageNode[]) => nodes.map((n) => n.key)
const m = (id: string) => lineageKey("model", id)
const p = (id: string) => lineageKey("pipeline", id)

describe("buildModelLineage", () => {
  it("walks up through the upstreams and down through the models that wait on it", () => {
    // a → b → root → c → d
    const schedules = [sched("a"), after("b", ["a"]), after("root", ["b"]), after("c", ["root"]), after("d", ["c"])]
    const g = buildModelLineage("root", schedules)

    expect(keys(g.nodes)).toEqual([m("root"), m("b"), m("c"), m("a"), m("d")])
    expect(g.nodes.map((n) => [n.role, n.distance])).toEqual([
      ["root", 0],
      ["upstream", 1],
      ["downstream", 1],
      ["upstream", 2],
      ["downstream", 2],
    ])
    expect(g.edges.map((e) => `${e.from}>${e.to}`).sort()).toEqual(
      [`${m("a")}>${m("b")}`, `${m("b")}>${m("root")}`, `${m("root")}>${m("c")}`, `${m("c")}>${m("d")}`].sort(),
    )
    expect(g).toMatchObject({ upstreamsFound: 2, downstreamsFound: 2, omitted: 0 })
  })

  it("points every edge from the upstream to the model it wakes", () => {
    const g = buildModelLineage("root", [sched("up"), after("root", ["up"]), after("down", ["root"])])
    expect(g.edges).toContainEqual({ from: m("up"), to: m("root"), inactive: false })
    expect(g.edges).toContainEqual({ from: m("root"), to: m("down"), inactive: false })
    expect(g.edges).not.toContainEqual(expect.objectContaining({ from: m("root"), to: m("up") }))
  })

  it("leaves out a sibling: another model woken by the same upstream is not in this model's chain", () => {
    const g = buildModelLineage("root", [sched("up"), after("root", ["up"]), after("sibling", ["up"])])
    expect(keys(g.nodes)).toEqual([m("root"), m("up")])
  })

  it("leaves out an upstream's other upstreams' downstreams and a downstream's other upstreams", () => {
    // other → down ← root: `other` feeds the downstream but is not upstream of root.
    const g = buildModelLineage("root", [sched("root"), sched("other"), after("down", ["root", "other"])])
    expect(keys(g.nodes)).toEqual([m("root"), m("down")])
    const down = g.nodes.find((n) => n.key === m("down"))!
    expect(down.upstreamsNotDrawn).toBe(1)
    expect(g.edges).toEqual([{ from: m("root"), to: m("down"), inactive: false }])
  })

  it("draws a pipeline as a leaf and does not walk past it", () => {
    const g = buildModelLineage("root", [
      after("root", [{ kind: "pipeline", id: "pl-1", name: "orders sync" }]),
      // A model id that happens to equal the pipeline id is a different node.
      after("pl-1", ["ghost"]),
      sched("ghost"),
    ])
    expect(keys(g.nodes)).toEqual([m("root"), p("pl-1")])
    expect(g.nodes[1]).toMatchObject({ kind: "pipeline", role: "upstream", claimedName: "orders sync" })
    expect(g.nodes[1].schedule).toBeUndefined()
  })

  it("ends a ring instead of walking it forever", () => {
    // root → x → y → root, stored before the gateway refused rings.
    const g = buildModelLineage("root", [after("root", ["y"]), after("x", ["root"]), after("y", ["x"])])
    expect(g.nodes.length).toBe(3)
    expect(new Set(keys(g.nodes)).size).toBe(3)
    // Each model appears once, on whichever side reached it first.
    expect(g.nodes.filter((n) => n.role === "root")).toHaveLength(1)
  })

  it("never draws a node twice when it is both an upstream and a downstream", () => {
    const g = buildModelLineage("root", [after("root", ["both"]), after("both", ["root"])])
    expect(keys(g.nodes)).toEqual([m("root"), m("both")])
    expect(g.edges).toHaveLength(2)
  })

  it("counts upstreams of a fan-in model that the walk never reached", () => {
    const g = buildModelLineage("root", [
      sched("root"),
      sched("x"),
      sched("y"),
      after("fan", ["root", "x", { kind: "pipeline", id: "pl" }], { upstream_policy: "all" }),
    ])
    const fan = g.nodes.find((n) => n.key === m("fan"))!
    expect(fan.upstreamsNotDrawn).toBe(2)
  })

  it("keeps the nearest nodes under the cap, both sides together, and counts the rest", () => {
    // Up: a chain of 10. Down: a chain of 10. Cap of 7 = root + 6 nearest.
    const schedules: ScheduledQuery[] = [sched("u10")]
    for (let i = 9; i >= 1; i--) schedules.push(after(`u${i}`, [`u${i + 1}`]))
    schedules.push(after("root", ["u1"]))
    schedules.push(after("d1", ["root"]))
    for (let i = 2; i <= 10; i++) schedules.push(after(`d${i}`, [`d${i - 1}`]))

    const g = buildModelLineage("root", schedules, { maxNodes: 7 })
    expect(g.nodes).toHaveLength(7)
    expect(g.nodes.every((n) => n.distance <= 3)).toBe(true)
    expect(keys(g.nodes).filter((k) => k.includes(":u"))).toHaveLength(3)
    expect(keys(g.nodes).filter((k) => k.includes(":d"))).toHaveLength(3)
    expect(g).toMatchObject({ upstreamsFound: 10, downstreamsFound: 10, omitted: 14 })
    // The farthest drawn upstream's own upstream is off the page, and says so.
    expect(g.nodes.find((n) => n.key === m("u3"))!.upstreamsNotDrawn).toBe(1)
    // No edge points at a node that is not drawn.
    const drawn = new Set(keys(g.nodes))
    expect(g.edges.every((e) => drawn.has(e.from) && drawn.has(e.to))).toBe(true)
  })

  it("takes upstreams and downstreams in turn at the same distance, so neither side fills the cap", () => {
    const ups = Array.from({ length: 60 }, (_, i) => sched(`u${i}`))
    const upKeys = ups.map((u) => ({ kind: "model", id: u.saved_query_id }))
    // 60 direct upstreams listed before the one direct downstream: the downstream still gets a place.
    const lopsided = buildModelLineage("root", [...ups, after("root", upKeys), after("d0", ["root"])], { maxNodes: 50 })
    expect(keys(lopsided.nodes)).toContain(m("d0"))
    expect(lopsided).toMatchObject({ upstreamsFound: 60, downstreamsFound: 1, omitted: 12 })

    const downs = Array.from({ length: 30 }, (_, i) => after(`d${i}`, ["root"]))
    const even = buildModelLineage("root", [...ups.slice(0, 30), after("root", upKeys.slice(0, 30)), ...downs], {
      maxNodes: 21,
    })
    expect(keys(even.nodes).filter((k) => k.includes(":u"))).toHaveLength(10)
    expect(keys(even.nodes).filter((k) => k.includes(":d"))).toHaveLength(10)
    // Nearer always comes first: a turn never lets a farther node in ahead of a nearer one.
    const chain = [sched("u1"), after("root", ["u1"]), after("d1", ["root"]), after("d2", ["d1"]), after("d3", ["d2"])]
    expect(keys(buildModelLineage("root", chain, { maxNodes: 3 }).nodes).sort()).toEqual([m("d1"), m("root"), m("u1")].sort())
  })

  it("defaults the cap to MAX_LINEAGE_NODES", () => {
    const schedules = [sched("root"), ...Array.from({ length: 80 }, (_, i) => after(`d${i}`, ["root"]))]
    const g = buildModelLineage("root", schedules)
    expect(g.nodes).toHaveLength(MAX_LINEAGE_NODES)
    expect(g.omitted).toBe(80 - (MAX_LINEAGE_NODES - 1))
  })

  it("uses the page's own schedule when the capped list does not contain the model", () => {
    const rootSchedule = after("root", ["up"])
    const g = buildModelLineage("root", [sched("up")], { rootSchedule })
    expect(keys(g.nodes)).toEqual([m("root"), m("up")])
    expect(g.nodes[0].schedule).toBe(rootSchedule)
  })

  it("prefers the list's row over the page's copy, and ignores a page schedule for another model", () => {
    const listed = after("root", ["up"])
    const g = buildModelLineage("root", [listed, sched("up")], { rootSchedule: after("root", ["stale"]) })
    expect(g.nodes[0].schedule).toBe(listed)
    expect(keys(g.nodes)).toEqual([m("root"), m("up")])

    const other = buildModelLineage("root", [sched("up")], { rootSchedule: after("not-root", ["up"]) })
    expect(keys(other.nodes)).toEqual([m("root")])
  })

  it("ignores upstreams left on a schedule that no longer runs after them", () => {
    const g = buildModelLineage("root", [
      sched("root", { upstreams: [{ kind: "model", id: "old" }] }),
      sched("old"),
      sched("cron-down", { upstreams: [{ kind: "model", id: "root" }] }),
    ])
    expect(keys(g.nodes)).toEqual([m("root")])
    expect(g.edges).toEqual([])
  })

  it("ignores upstream entries with an unknown kind or no id", () => {
    const g = buildModelLineage("root", [
      after("root", [
        { kind: "table", id: "t" },
        { kind: "model", id: "" },
        { kind: "model", id: "ok" },
      ]),
      sched("ok"),
    ])
    expect(keys(g.nodes)).toEqual([m("root"), m("ok")])
  })

  it("marks an edge inactive when the model it wakes is paused", () => {
    const g = buildModelLineage("root", [sched("root"), after("down", ["root"], { status: "paused" })])
    expect(g.edges).toEqual([{ from: m("root"), to: m("down"), inactive: true }])
  })

  it("takes the first non-empty name any downstream gave an upstream", () => {
    const g = buildModelLineage("root", [
      after("root", [{ kind: "model", id: "priv", name: "  " }]),
      after("other", [{ kind: "model", id: "priv", name: " secret_model " }]),
    ])
    expect(g.nodes.find((n) => n.key === m("priv"))!.claimedName).toBe("secret_model")
  })

  it("draws only the model when nothing links to it", () => {
    const g = buildModelLineage("root", [sched("root"), sched("unrelated")])
    expect(g).toEqual({
      nodes: [expect.objectContaining({ key: m("root"), role: "root", distance: 0 })],
      edges: [],
      upstreamsFound: 0,
      downstreamsFound: 0,
      omitted: 0,
    })
  })

  it("keeps the model itself even with a cap below one", () => {
    const g = buildModelLineage("root", [sched("root"), after("down", ["root"])], { maxNodes: 0 })
    expect(keys(g.nodes)).toEqual([m("root")])
    expect(g.omitted).toBe(1)
  })
})

describe("lookupFromResponse", () => {
  it("reads 403 and 404 as a node the caller may not open", () => {
    expect(lookupFromResponse("model", 403, null)).toEqual({ status: "denied" })
    expect(lookupFromResponse("pipeline", 404, { name: "x" })).toEqual({ status: "denied" })
    expect(lookupFromResponse("pipeline", 404, { error: "not found" })).toEqual({ status: "denied" })
    expect(lookupFromResponse("pipeline", 403, { error: "insufficient workspace role" })).toEqual({ status: "denied" })
  })

  it("does not read the pipeline route's own 404 as a refusal: the caller was already let in", () => {
    expect(lookupFromResponse("pipeline", 404, { error: "pipeline_not_found", message: "Pipeline not found" })).toEqual({
      status: "unavailable",
    })
    // Only the pipeline route words it that way.
    expect(lookupFromResponse("model", 404, { error: "pipeline_not_found" })).toEqual({ status: "denied" })
  })

  it("reads any other failure as unknown, not denied", () => {
    for (const code of [0, 401, 429, 500, 502, 302]) {
      expect(lookupFromResponse("model", code, { runs: [] })).toEqual({ status: "unavailable" })
    }
    // GetPipeline's failed read after the gate (pipelines.go): a 500, not a 404.
    expect(lookupFromResponse("pipeline", 500, { error: "pipeline_fetch_failed", message: "Failed to load pipeline" })).toEqual({
      status: "unavailable",
    })
  })

  it("reads a 2xx without the expected body as unknown", () => {
    expect(lookupFromResponse("model", 200, null)).toEqual({ status: "unavailable" })
    expect(lookupFromResponse("model", 200, "ok")).toEqual({ status: "unavailable" })
    expect(lookupFromResponse("model", 200, { runs: "nope" })).toEqual({ status: "unavailable" })
    expect(lookupFromResponse("model", 200, {})).toEqual({ status: "unavailable" })
    expect(lookupFromResponse("pipeline", 200, null)).toEqual({ status: "unavailable" })
  })

  it("keeps only run rows that carry a status", () => {
    const runs = [{ status: "succeeded" }, null, { status: 3 }, "x", { skip_reason: "a" }, { status: "failed" }]
    expect(lookupFromResponse("model", 200, { runs })).toEqual({
      status: "model",
      runs: [{ status: "succeeded" }, { status: "failed" }],
    })
  })

  it("keeps a run's error text, which says why a skip with no reason was skipped", () => {
    const run = { status: "skipped", error: "a run of this model is already in progress" }
    expect(lookupFromResponse("model", 200, { runs: [run] })).toEqual({ status: "model", runs: [run] })
  })

  it("passes a pipeline body through", () => {
    const body = { name: "orders", status: "active" }
    expect(lookupFromResponse("pipeline", 200, body)).toEqual({ status: "pipeline", pipeline: body })
  })
})

describe("older runs", () => {
  it("reads the next page's cursor, and none when the runs ended", () => {
    expect(nextRunsCursor({ runs: [], next_cursor: "2026-09-16T03:00:00Z_r9" })).toBe("2026-09-16T03:00:00Z_r9")
    expect(nextRunsCursor({ runs: [] })).toBeNull()
    expect(nextRunsCursor({ runs: [], next_cursor: "" })).toBeNull()
    expect(nextRunsCursor({ runs: [], next_cursor: 7 })).toBeNull()
    expect(nextRunsCursor(null)).toBeNull()
  })

  it("asks for older runs only when every run in hand is a skip that says nothing", () => {
    const waiting = { status: "skipped", skip_reason: "waiting_on_upstreams" }
    const locked = { status: "skipped", error: "a run of this model is already in progress" }
    expect(needsOlderRuns([waiting, locked, waiting])).toBe(true)
    expect(needsOlderRuns([waiting, { status: "failed" }])).toBe(false)
    expect(needsOlderRuns([{ status: "skipped" }])).toBe(false)
    expect(needsOlderRuns([])).toBe(false)
  })
})

const RUNNING_EMPTY: RunningModelsResponse = { models: [], count: 0, limit: 200, temporal_available: true }
const CTX: DescribeLineageContext = {
  rootName: "revenue_rollup",
  running: { status: "ok", data: RUNNING_EMPTY },
  schedulesCapped: false,
}

function node(id: string, overrides: Partial<LineageNode> = {}): LineageNode {
  const kind = overrides.kind ?? "model"
  return {
    key: lineageKey(kind, id),
    kind,
    id,
    role: "upstream",
    distance: 1,
    upstreamsNotDrawn: 0,
    ...overrides,
  }
}

const runs = (
  ...rows: { status: string; skip_reason?: string; error?: string; finished_at?: string; started_at?: string }[]
): NodeLookup => ({
  status: "model",
  runs: rows,
})

describe("describeLineageNode: what a model may say", () => {
  it("shows a placeholder, not the claimed name, when the model's own route said no", () => {
    const v = describeLineageNode(node("priv", { claimedName: "secret_model" }), { status: "denied" }, CTX)
    expect(v).toEqual({
      title: "A model you can't open",
      named: false,
      href: null,
      kindLabel: "Model",
      tone: "neutral",
      status: "Status hidden",
      at: null,
      durationMs: null,
      secondary: null,
      retryable: false,
    })
    expect(JSON.stringify(v)).not.toContain("secret_model")
    expect(JSON.stringify(v)).not.toContain("priv")
  })

  it("shows a retryable placeholder, not the claimed name, when the route failed or never answered", () => {
    for (const lookup of [{ status: "unavailable" } as const, undefined]) {
      const v = describeLineageNode(node("priv", { claimedName: "secret_model" }), lookup, CTX)
      expect(v).toMatchObject({ title: "A model", named: false, href: null, status: "Status unavailable", retryable: true })
      expect(JSON.stringify(v)).not.toContain("secret_model")
    }
  })

  it("shows the claimed name and a link once the model's runs answered 2xx", () => {
    const v = describeLineageNode(node("q 2", { claimedName: "orders_daily" }), runs({ status: "succeeded" }), CTX)
    expect(v).toMatchObject({ title: "orders_daily", named: true, href: "/explorer/schedules/q%202?tab=graph" })
  })

  it("says Unnamed model when a visible model has no name to show", () => {
    expect(describeLineageNode(node("q"), runs(), CTX)).toMatchObject({ title: "Unnamed model", named: false })
    expect(describeLineageNode(node("q", { schedule: sched("q", { name: "  " }) }), runs(), CTX)).toMatchObject({
      title: "Unnamed model",
      named: false,
    })
  })

  it("names a model from its own schedule row even when its runs could not be loaded", () => {
    const v = describeLineageNode(node("q", { schedule: sched("q", { name: "orders_kpi" }) }), { status: "unavailable" }, CTX)
    expect(v).toMatchObject({ title: "orders_kpi", named: true, href: "/explorer/schedules/q?tab=graph" })
    expect(v).toMatchObject({ tone: "neutral", status: "Status unavailable", retryable: true })
  })

  it("names the root from the page and never links it", () => {
    const v = describeLineageNode(node("root", { role: "root", distance: 0 }), { status: "denied" }, CTX)
    expect(v).toMatchObject({ title: "revenue_rollup", named: true, href: null })
  })
})

describe("describeLineageNode: a model's status", () => {
  const visible = (overrides: Partial<ScheduledQuery> = {}) => node("q", { schedule: sched("q", overrides) })

  it("reads the newest run", () => {
    expect(describeLineageNode(visible(), runs({ status: "succeeded", finished_at: "2026-09-16T03:00:00Z" }), CTX)).toMatchObject({
      tone: "success",
      status: "Succeeded",
      at: "2026-09-16T03:00:00Z",
    })
    expect(describeLineageNode(visible(), runs({ status: "failed", started_at: "2026-09-16T02:00:00Z" }), CTX)).toMatchObject({
      tone: "failed",
      status: "Failed",
      at: "2026-09-16T02:00:00Z",
    })
    expect(describeLineageNode(visible(), runs({ status: "queued" }), CTX)).toMatchObject({ tone: "neutral", status: "queued", at: null })
    expect(describeLineageNode(visible(), runs(), CTX)).toMatchObject({ tone: "neutral", status: "Never run", at: null })
  })

  it("names each skip reason", () => {
    const cases: [string | undefined, string][] = [
      ["upstream_failed", "Skipped: upstream failed"],
      ["upstream_skipped", "Skipped: upstream skipped"],
      ["chain_depth_exceeded", "Skipped: hop limit"],
      ["something_new", "Skipped: something_new"],
      [undefined, "Skipped"],
    ]
    for (const [skip_reason, status] of cases) {
      expect(describeLineageNode(visible(), runs({ status: "skipped", skip_reason }), CTX)).toMatchObject({ tone: "warning", status })
    }
  })

  it("looks past 'waiting for other upstreams' skips to the last real outcome", () => {
    const v = describeLineageNode(
      node("q", { schedule: sched("q", { schedule_type: "after_upstream", upstream_policy: "all", upstreams: [{ kind: "model", id: "a" }, { kind: "model", id: "b" }] }) }),
      runs(
        { status: "skipped", skip_reason: "waiting_on_upstreams", finished_at: "2026-09-16T04:00:00Z" },
        { status: "skipped", skip_reason: "waiting_on_upstreams" },
        { status: "succeeded", finished_at: "2026-09-15T03:00:00Z" },
      ),
      CTX,
    )
    expect(v).toMatchObject({ tone: "success", status: "Succeeded", at: "2026-09-15T03:00:00Z", secondary: "Waiting for other upstreams" })
  })

  it("does not say it is waiting when the newest run is a real outcome", () => {
    const v = describeLineageNode(
      node("q", { schedule: sched("q", { schedule_type: "after_upstream", upstream_policy: "all", upstreams: [{ kind: "model", id: "a" }, { kind: "model", id: "b" }] }) }),
      runs({ status: "succeeded" }, { status: "skipped", skip_reason: "waiting_on_upstreams" }),
      CTX,
    )
    expect(v).toMatchObject({ status: "Succeeded", secondary: "Waits on all 2" })
  })

  it("stays neutral when every recent run is a waiting skip", () => {
    const v = describeLineageNode(
      visible(),
      runs({ status: "skipped", skip_reason: "waiting_on_upstreams", started_at: "2026-09-16T04:00:00Z" }),
      CTX,
    )
    expect(v).toMatchObject({ tone: "neutral", status: "Waiting for other upstreams", at: "2026-09-16T04:00:00Z" })
  })

  it("looks past a skip written because another run of the model was still going", () => {
    const locked = { status: "skipped", error: "a run of this model is already in progress", finished_at: "2026-09-16T05:00:00Z" }
    expect(
      describeLineageNode(visible(), runs(locked, { status: "succeeded", finished_at: "2026-09-16T04:00:00Z" }), CTX),
    ).toMatchObject({ tone: "success", status: "Succeeded", at: "2026-09-16T04:00:00Z" })
    // With nothing behind it, it says what it was rather than a bare "Skipped".
    expect(describeLineageNode(visible(), runs(locked), CTX)).toMatchObject({
      tone: "neutral",
      status: "Skipped: a run was already in progress",
      at: "2026-09-16T05:00:00Z",
    })
    // Any other skip with no reason is still an outcome.
    expect(describeLineageNode(visible(), runs({ status: "skipped", error: "something else" }, { status: "succeeded" }), CTX)).toMatchObject({
      tone: "warning",
      status: "Skipped",
    })
  })

  it("shows a rebuild in progress over the last outcome", () => {
    const row = sched("q", { schedule_type: "after_upstream", upstreams: [{ kind: "model", id: "a" }] })
    const detail = { saved_query_id: "q", queued_completions: 0, current: null, refreshes_this_run: 1, refreshes_per_run: 100 }
    const running = (state: string, phase?: string): DescribeLineageContext => ({
      ...CTX,
      running: {
        status: "ok",
        data: {
          ...RUNNING_EMPTY,
          models: [
            {
              saved_query_id: "q",
              name: "q",
              workflow_id: "w",
              state,
              detail_available: !!phase,
              detail: phase ? { ...detail, phase } : undefined,
            },
          ],
          count: 1,
        },
      },
    })
    const last = runs({ status: "failed" })
    expect(describeLineageNode(node("q", { schedule: row }), last, running("running", "rebuilding"))).toMatchObject({
      tone: "running",
      status: "Rebuilding now",
      at: null,
    })
    expect(describeLineageNode(node("q", { schedule: row }), last, running("running"))).toMatchObject({ tone: "running", status: "Running" })
    expect(describeLineageNode(node("q", { schedule: row }), last, running("running", "waiting"))).toMatchObject({
      tone: "failed",
      status: "Failed",
      secondary: "Waiting on upstreams",
    })
    // A clock schedule is never asked about, so a stale row for it changes nothing.
    expect(describeLineageNode(node("q", { schedule: sched("q") }), last, running("running", "rebuilding"))).toMatchObject({
      status: "Failed",
    })
  })
})

describe("describeLineageNode: the line under a model's status", () => {
  it("says how many upstreams are not drawn, after the schedule's own state rather than instead of it", () => {
    const n = node("q", { schedule: sched("q", { status: "paused" }), upstreamsNotDrawn: 1 })
    expect(describeLineageNode(n, runs(), CTX).secondary).toBe("Paused · +1 more upstream not shown")
    const active = node("q", { schedule: sched("q"), upstreamsNotDrawn: 3 })
    expect(describeLineageNode(active, runs(), CTX).secondary).toBe("+3 more upstreams not shown")
  })

  it("says a model with no row is not scheduled, unless the list was cut off", () => {
    expect(describeLineageNode(node("q"), runs(), CTX).secondary).toBe("Not scheduled")
    expect(describeLineageNode(node("q"), runs(), { ...CTX, schedulesCapped: true }).secondary).toBe(
      "Schedule not loaded (500+ schedules)",
    )
    // The page asked for the root's row by id, so its absence is real.
    expect(describeLineageNode(node("q", { role: "root", distance: 0 }), runs(), { ...CTX, schedulesCapped: true }).secondary).toBe(
      "Not scheduled",
    )
  })

  it("names a paused or otherwise inactive schedule", () => {
    expect(describeLineageNode(node("q", { schedule: sched("q", { status: "paused" }) }), runs(), CTX).secondary).toBe("Paused")
    expect(
      describeLineageNode(node("q", { schedule: sched("q", { status: "paused", auto_paused_reason: "3 failures" }) }), runs(), CTX)
        .secondary,
    ).toBe("Paused automatically")
    expect(describeLineageNode(node("q", { schedule: sched("q", { status: "disabled" }) }), runs(), CTX).secondary).toBe("disabled")
  })

  it("says how many upstreams an all-policy model waits on, and nothing for one or for any", () => {
    const up = [
      { kind: "model", id: "a" },
      { kind: "pipeline", id: "b" },
    ]
    const withPolicy = (policy: "any" | "all", upstreams = up) =>
      node("q", { schedule: sched("q", { schedule_type: "after_upstream", upstream_policy: policy, upstreams }) })
    expect(describeLineageNode(withPolicy("all"), runs(), CTX).secondary).toBe("Waits on all 2")
    expect(describeLineageNode(withPolicy("any"), runs(), CTX).secondary).toBeNull()
    expect(describeLineageNode(withPolicy("all", up.slice(0, 1)), runs(), CTX).secondary).toBeNull()
  })
})

describe("describeLineageNode: pipelines", () => {
  const pl = node("pl 1", { kind: "pipeline" })
  const pipe = (pipeline: object): NodeLookup => ({ status: "pipeline", pipeline: pipeline as LineagePipeline })

  it("hides a pipeline the caller cannot open", () => {
    expect(describeLineageNode(node("pl", { kind: "pipeline", claimedName: "billing_sync" }), { status: "denied" }, CTX)).toEqual({
      title: "A pipeline you can't open",
      named: false,
      href: null,
      kindLabel: "Pipeline",
      tone: "neutral",
      status: "Status hidden",
      at: null,
      durationMs: null,
      secondary: null,
      retryable: false,
    })
  })

  it("shows a retryable placeholder when the pipeline could not be loaded", () => {
    for (const lookup of [{ status: "unavailable" } as const, undefined, runs({ status: "succeeded" })]) {
      expect(describeLineageNode(node("pl", { kind: "pipeline", claimedName: "billing_sync" }), lookup, CTX)).toMatchObject({
        title: "A pipeline",
        named: false,
        href: null,
        status: "Status unavailable",
        retryable: true,
      })
    }
  })

  it("names and links a pipeline its route returned, with its last execution", () => {
    const v = describeLineageNode(
      pl,
      pipe({ name: " orders sync ", status: "paused", last_execution: { status: "completed", started_at: "s", completed_at: "c" } }),
      CTX,
    )
    expect(v).toEqual({
      title: "orders sync",
      named: true,
      href: "/pipelines/pl%201",
      kindLabel: "Pipeline",
      tone: "success",
      status: "Succeeded",
      at: "c",
      durationMs: null,
      secondary: "Paused",
      retryable: false,
    })
  })

  it("reads each execution status", () => {
    const cases: [object | null, string, string][] = [
      [null, "neutral", "Never run"],
      [{ status: "failed", error_message: "boom" }, "failed", "Failed"],
      [{ status: "running" }, "running", "Running"],
      [{ status: "canceled" }, "neutral", "Cancelled"],
      [{ status: "failed", error_message: "silent_drop_detected: a.b: read=5 landed=0" }, "warning", "Silent Drop"],
      [{ status: "failed", error_message: "waiting_for_credential_reauth" }, "warning", "Awaiting Re-Auth"],
      [{ status: "pending" }, "neutral", "Pending"],
      [{ status: "" }, "neutral", "Pending"],
      [{ status: "provisioning" }, "neutral", "provisioning"],
    ]
    for (const [last_execution, tone, status] of cases) {
      expect(describeLineageNode(pl, pipe({ name: "x", last_execution }), CTX)).toMatchObject({ tone, status })
    }
  })

  it("does not call a CDC pipeline healthy from its setup run, but does repeat a failure", () => {
    expect(describeLineageNode(pl, pipe({ name: "x", sync_mode: "cdc", last_execution: { status: "completed" } }), CTX)).toMatchObject({
      tone: "neutral",
      status: "CDC pipeline",
      at: null,
    })
    expect(
      describeLineageNode(pl, pipe({ name: "x", data_loading_strategy: { mode: "cdc" }, last_execution: { status: "failed" } }), CTX),
    ).toMatchObject({ tone: "failed", status: "Failed" })
  })

  it("says Unnamed pipeline and Draft for a draft with no name or runs", () => {
    expect(describeLineageNode(pl, pipe({ status: "draft", last_execution: null }), CTX)).toMatchObject({
      title: "Unnamed pipeline",
      named: false,
      status: "Never run",
      secondary: "Draft",
    })
  })
})

describe("layoutLineage", () => {
  it("places upstreams left of the model and downstreams right of it", () => {
    const { positions, bounds } = layoutLineage(
      ["up", "root", "down"],
      [
        { from: "up", to: "root" },
        { from: "root", to: "down" },
      ],
    )
    const x = (k: string) => positions.get(k)!.x
    expect(x("up")).toBeLessThan(x("root"))
    expect(x("root")).toBeLessThan(x("down"))
    expect(x("root") - x("up")).toBeGreaterThanOrEqual(LINEAGE_NODE_WIDTH)
    expect(bounds.height).toBe(LINEAGE_NODE_HEIGHT)
    expect(bounds.width).toBeGreaterThanOrEqual(3 * LINEAGE_NODE_WIDTH)
  })

  it("gives zero bounds for no nodes", () => {
    expect(layoutLineage([], []).bounds).toEqual({ x: 0, y: 0, width: 0, height: 0 })
  })
})

describe("initialViewport", () => {
  const root = { x: 300, y: 0, width: LINEAGE_NODE_WIDTH, height: LINEAGE_NODE_HEIGHT }

  it("fits and centres the whole chain when it is readable", () => {
    const v = initialViewport({ x: 0, y: 0, width: 500, height: 100 }, root, { width: 1000, height: 420 })
    expect(v).toEqual({ x: 250, y: 160, zoom: 1 })
  })

  it("shrinks to fit when the chain is a little too wide", () => {
    const v = initialViewport({ x: 10, y: 0, width: 1200, height: 76 }, root, { width: 1000, height: 420 })
    expect(v.zoom).toBeCloseTo((1000 - 48) / 1200)
    expect(v.x).toBeCloseTo((1000 - 1200 * v.zoom) / 2 - 10 * v.zoom)
  })

  it("centres the model at full size when fitting would make the words too small", () => {
    const v = initialViewport({ x: 0, y: 0, width: 4000, height: 76 }, root, { width: 1000, height: 420 })
    expect(v).toEqual({ x: 500 - (300 + LINEAGE_NODE_WIDTH / 2), y: 210 - LINEAGE_NODE_HEIGHT / 2, zoom: 1 })
  })

  it("falls back to the origin before the canvas has a size", () => {
    const b = { x: 0, y: 0, width: 500, height: 100 }
    expect(initialViewport(b, root, { width: 0, height: 420 })).toEqual({ x: 0, y: 0, zoom: 1 })
    expect(initialViewport(b, root, { width: 1000, height: Number.NaN })).toEqual({ x: 0, y: 0, zoom: 1 })
    expect(initialViewport({ ...b, width: 0 }, root, { width: 1000, height: 420 })).toEqual({ x: 0, y: 0, zoom: 1 })
  })
})

describe("liveRunningKeys", () => {
  const live = (models: { id: string; state: string; phase?: string }[]): DescribeLineageContext["running"] => ({
    status: "ok",
    data: {
      ...RUNNING_EMPTY,
      models: models.map(({ id, state, phase }) => ({
        saved_query_id: id,
        name: id,
        workflow_id: `w-${id}`,
        state,
        detail_available: !!phase,
        detail: phase
          ? { saved_query_id: id, queued_completions: 0, current: null, refreshes_this_run: 1, refreshes_per_run: 100, phase }
          : undefined,
      })),
      count: models.length,
    },
  })
  const chained = (id: string) => node(id, { schedule: after(id, ["a"]) })

  it("lists the drawn after-upstream models that are rebuilding or running with no detail", () => {
    const nodes = [chained("reb"), chained("run"), chained("wait"), chained("idle"), node("clock", { schedule: sched("clock") })]
    const running = live([
      { id: "reb", state: "running", phase: "rebuilding" },
      { id: "run", state: "running" },
      { id: "wait", state: "running", phase: "waiting" },
      { id: "clock", state: "running", phase: "rebuilding" },
    ])
    expect(liveRunningKeys(nodes, running)).toEqual(new Set([m("reb"), m("run")]))
  })

  it("has no answer when the live check has none, which is not the same as nothing running", () => {
    expect(liveRunningKeys([chained("q")], { status: "loading" })).toBeNull()
    expect(liveRunningKeys([chained("q")], { status: "error", message: "x" })).toBeNull()
    expect(liveRunningKeys([chained("q")], live([]))).toEqual(new Set())
  })
})
