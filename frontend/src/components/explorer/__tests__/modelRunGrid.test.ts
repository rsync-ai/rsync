import { describe, expect, it } from "vitest"
import { buildModelLineage, lineageKey, type LineageRun, type NodeLookup } from "@/components/explorer/modelLineage"
import { buildRunGrid, cellRun, runRounds, type RunGrid, type RunGridCell } from "@/components/explorer/modelRunGrid"
import type { ScheduledQuery } from "@/components/explorer/scheduledModel"

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

function after(id: string, upstreams: { kind: string; id: string }[], policy: "any" | "all" = "any"): ScheduledQuery {
  return sched(id, { schedule_type: "after_upstream", schedule_spec: {}, upstreams, upstream_policy: policy })
}

const m = (id: string) => lineageKey("model", id)
const up = (id: string) => ({ kind: "model", id })

/** A run finishing `minute` minutes past 03:00, woken by `from` when given. */
function run(runId: string, minute: number, status = "succeeded", from?: { id: string; runId?: string; kind?: string }): LineageRun {
  const at = (min: number) => new Date(Date.UTC(2026, 8, 16, 3, min)).toISOString()
  return {
    run_id: runId,
    status,
    started_at: at(minute),
    finished_at: at(minute),
    duration_ms: 1000,
    ...(from ? { upstream_kind: from.kind ?? "model", upstream_id: from.id, upstream_run_id: from.runId } : {}),
  }
}

function waiting(runId: string, minute: number, from: { id: string; runId?: string }): LineageRun {
  return { ...run(runId, minute, "skipped", from), skip_reason: "waiting_on_upstreams" }
}

const model = (runs: LineageRun[], hasMore = false): NodeLookup => ({ status: "model", runs, hasMore })

/** Each row's cells as short words: the run id for a run, the kind otherwise. */
function show(grid: RunGrid | null): Record<string, string[]> {
  const out: Record<string, string[]> = {}
  for (const row of grid!.rows) {
    out[row.node.id] = row.cells.map((c: RunGridCell) => {
      const r = cellRun(c)
      if (!r) return c.kind
      const extra = c.kind === "run" && c.alsoLinked ? `+${c.alsoLinked}` : ""
      return `${c.kind === "waiting" ? "waiting " : ""}${r.run_id}${extra}`
    })
  }
  return out
}

describe("runRounds", () => {
  it("puts each outcome with the waiting rows written before it, and a round still waiting on its own", () => {
    const rows = [
      waiting("w3", 9, { id: "u1" }),
      run("m2", 8),
      waiting("w2b", 7, { id: "u2" }),
      waiting("w2a", 6, { id: "u1" }),
      run("m1", 5),
      waiting("w1", 4, { id: "u1" }),
    ]
    const rounds = runRounds(rows, true)
    expect(rounds.map((r) => [r.main?.run_id ?? null, r.waiting.map((w) => w.run_id), r.incomplete])).toEqual([
      [null, ["w3"], false],
      ["m2", ["w2b", "w2a"], false],
      // Its earlier waiting rows can be on the next page.
      ["m1", ["w1"], true],
    ])
    expect(runRounds(rows, false).at(-1)!.incomplete).toBe(false)
    expect(runRounds([], true)).toEqual([])
  })
})

describe("buildRunGrid", () => {
  it("lines up each run of the model with the upstream run it names and the downstream runs that name it", () => {
    // a → root → c
    const lineage = buildModelLineage("root", [sched("a"), after("root", [up("a")]), after("c", [up("root")])])
    const lookups = new Map<string, NodeLookup>([
      [m("a"), model([run("a2", 20), run("a1", 10)])],
      [m("root"), model([run("r2", 21, "failed", { id: "a", runId: "a2" }), run("r1", 11, "succeeded", { id: "a", runId: "a1" })])],
      [m("c"), model([run("c1", 12, "succeeded", { id: "root", runId: "r1" })])],
    ])
    const grid = buildRunGrid(lineage, lookups)!
    // Oldest on the left; upstreams, then the model, then downstreams.
    expect(grid.rows.map((r) => r.node.id)).toEqual(["a", "root", "c"])
    expect(grid.columns.map((c) => c.now)).toEqual([false, false])
    expect(show(grid)).toEqual({
      a: ["a1", "a2"],
      root: ["r1", "r2"],
      // A failed run wakes nothing, so nothing names r2.
      c: ["c1", "none"],
    })
    expect(grid.olderRuns).toBe(false)
  })

  it("does not match runs by time: a run that names no upstream run is not linked", () => {
    const lineage = buildModelLineage("root", [sched("a"), after("root", [up("a")])])
    const lookups = new Map<string, NodeLookup>([
      [m("a"), model([run("a1", 10)])],
      // Woken by a, but written before upstream_run_id was recorded.
      [m("root"), model([run("r1", 11, "succeeded", { id: "a" })])],
    ])
    expect(show(buildRunGrid(lineage, lookups))).toEqual({ a: ["woke"], root: ["r1"] })
  })

  it("finds a fan-in model's upstreams on the waiting rows of its round, and leaves one it never recorded blank", () => {
    const lineage = buildModelLineage("root", [sched("u1"), sched("u2"), sched("u3"), after("root", [up("u1"), up("u2"), up("u3")], "all")])
    const lookups = new Map<string, NodeLookup>([
      [m("u1"), model([run("u1-1", 10)])],
      [m("u2"), model([run("u2-1", 12)])],
      // u3 ran, but the loop folded its completion in without writing a row that names it.
      [m("u3"), model([run("u3-1", 11)])],
      [m("root"), model([run("r1", 13, "succeeded", { id: "u2", runId: "u2-1" }), waiting("w1", 10, { id: "u1", runId: "u1-1" })])],
    ])
    const cells = show(buildRunGrid(lineage, lookups))
    expect(cells).toMatchObject({ u1: ["u1-1"], u2: ["u2-1"], u3: ["none"], root: ["r1"] })
  })

  it("shows a round still waiting as a waiting cell for the model", () => {
    const lineage = buildModelLineage("root", [sched("u1"), sched("u2"), after("root", [up("u1"), up("u2")], "all")])
    const lookups = new Map<string, NodeLookup>([
      [m("u1"), model([run("u1-1", 10)])],
      [m("u2"), model([])],
      [m("root"), model([waiting("w1", 10, { id: "u1", runId: "u1-1" })])],
    ])
    expect(show(buildRunGrid(lineage, lookups))).toMatchObject({ u1: ["u1-1"], u2: ["none"], root: ["waiting w1"] })
  })

  it("says nothing about a node the caller can't open, and marks what is only linked through it as unknown", () => {
    // hidden → mid → root → down
    const lineage = buildModelLineage("root", [after("mid", [up("hidden")]), after("root", [up("mid")]), after("down", [up("root")])], {})
    const lookups = new Map<string, NodeLookup>([
      [m("hidden"), { status: "denied" }],
      [m("mid"), { status: "unavailable" }],
      [m("root"), model([run("r1", 11, "succeeded", { id: "mid", runId: "mid-1" })])],
      [m("down"), model([run("d1", 12, "succeeded", { id: "root", runId: "r1" })])],
    ])
    expect(show(buildRunGrid(lineage, lookups))).toEqual({
      hidden: ["hidden"],
      mid: ["unknown"],
      root: ["r1"],
      down: ["d1"],
    })
  })

  it("marks a link that may be in runs older than the ones loaded", () => {
    const lineage = buildModelLineage("root", [sched("a"), after("root", [up("a")])])
    const lookups = new Map<string, NodeLookup>([
      // a's loaded runs all finished after root's run started: a1 is on a later page.
      [m("a"), model([run("a9", 50)], true)],
      [m("root"), model([run("r1", 11, "succeeded", { id: "a", runId: "a1" })])],
    ])
    expect(show(buildRunGrid(lineage, lookups))).toEqual({ a: ["not_loaded"], root: ["r1"] })

    // Loaded back past it, the run is simply not recorded any more.
    lookups.set(m("a"), model([run("a9", 50), run("a0", 1)], true))
    expect(show(buildRunGrid(lineage, lookups))).toEqual({ a: ["woke"], root: ["r1"] })
  })

  it("shows a pipeline that woke a run without a run of its own", () => {
    const lineage = buildModelLineage("root", [after("root", [{ kind: "pipeline", id: "pl" }])])
    const lookups = new Map<string, NodeLookup>([
      [lineageKey("pipeline", "pl"), { status: "pipeline", pipeline: { name: "orders" } }],
      [m("root"), model([run("r2", 20), run("r1", 11, "succeeded", { id: "pl", kind: "pipeline" })])],
    ])
    expect(show(buildRunGrid(lineage, lookups))).toEqual({ pl: ["woke", "none"], root: ["r1", "r2"] })
  })

  it("counts other rounds of a downstream woken by the same run", () => {
    const lineage = buildModelLineage("root", [after("root", []), after("c", [up("root")])])
    const lookups = new Map<string, NodeLookup>([
      [m("root"), model([run("r1", 10)])],
      [m("c"), model([run("c2", 13, "succeeded", { id: "root", runId: "r1" }), run("c1", 11, "failed", { id: "root", runId: "r1" })])],
    ])
    expect(show(buildRunGrid(lineage, lookups))).toEqual({ root: ["r1"], c: ["c2+1"] })
  })

  it("adds a Now column only when the live check names a drawn model", () => {
    const lineage = buildModelLineage("root", [sched("a"), after("root", [up("a")]), after("hidden", [up("root")])])
    const lookups = new Map<string, NodeLookup>([
      [m("a"), model([])],
      [m("root"), model([run("r1", 10)])],
      [m("hidden"), { status: "denied" }],
    ])
    expect(buildRunGrid(lineage, lookups, { runningKeys: null })!.columns).toHaveLength(1)
    expect(buildRunGrid(lineage, lookups, { runningKeys: new Set() })!.columns).toHaveLength(1)

    const grid = buildRunGrid(lineage, lookups, { runningKeys: new Set([m("root")]) })!
    expect(grid.columns.map((c) => c.now)).toEqual([false, true])
    expect(show(grid)).toEqual({ a: ["none", "none"], root: ["r1", "running"], hidden: ["hidden", "hidden"] })
  })

  it("draws the newest runs and says there are older ones", () => {
    const lineage = buildModelLineage("root", [after("root", [])])
    const rows = Array.from({ length: 5 }, (_, i) => run(`r${5 - i}`, 50 - i))
    const lookups = new Map<string, NodeLookup>([[m("root"), model(rows)]])
    const grid = buildRunGrid(lineage, lookups, { maxColumns: 3 })!
    expect(show(grid)).toEqual({ root: ["r3", "r4", "r5"] })
    expect(grid.olderRuns).toBe(true)

    lookups.set(m("root"), model(rows.slice(0, 2), true))
    expect(buildRunGrid(lineage, lookups)!.olderRuns).toBe(true)
  })

  it("draws no grid when the model's own runs did not load", () => {
    const lineage = buildModelLineage("root", [after("root", [])])
    for (const lookup of [{ status: "unavailable" } as const, { status: "denied" } as const, undefined]) {
      const lookups = new Map<string, NodeLookup>(lookup ? [[m("root"), lookup]] : [])
      expect(buildRunGrid(lineage, lookups)).toBeNull()
    }
  })
})
