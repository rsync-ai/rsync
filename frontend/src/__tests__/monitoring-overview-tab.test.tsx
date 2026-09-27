/**
 * Monitoring → Overview: tiles, the tables-needing-attention list, the one-line
 * agent summary, and the 30 s refresh that pauses while the browser tab is hidden.
 *
 * Three independent reads (runtime, monitoring overview, table stats); a failed
 * one keeps its last good numbers and says so, it never blanks the tile.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
}))

import {
  MonitoringOverviewTab,
  OVERVIEW_POLL_MS,
  agentDecisionsLine,
  deliveryTile,
  tablesNeedingAttention,
  type AttentionTable,
} from "@/components/pipeline/MonitoringOverviewTab"

type Reply = { status?: number; body?: unknown } | "network"

const ago = (ms: number) => new Date(Date.now() - ms).toISOString()

let replies: { runtime: Reply; overview: Reply; tables: Reply }

function route(url: string): Reply {
  if (url.includes("/table-stats")) return replies.tables
  if (url.includes("/monitoring/overview")) return replies.overview
  if (url.includes("/runtime")) return replies.runtime
  throw new Error(`unexpected url ${url}`)
}

function calls(fragment: string): string[] {
  return authFetch.mock.calls.map((c) => String(c[0])).filter((u) => u.includes(fragment))
}

const cdcRuntime = (pending = 0) => ({
  body: { pipeline_id: "p1", execution_id: "p1", mode: "cdc", phase: "streaming", health: "healthy", dependencies: [], updated_at: ago(0), liveness: { pending_events: pending } },
})

// null = the pipeline has never run (the runtime omits execution_id).
const batchRuntime = (phase = "completed", executionId: string | null = "exec-1") => ({
  body: { pipeline_id: "p1", execution_id: executionId ?? undefined, mode: "batch", phase, health: "healthy", dependencies: [], updated_at: ago(0) },
})

const overview = (extra: Record<string, unknown> = {}) => ({
  body: {
    pipeline_id: "p1",
    agent_reasoning: { total_decisions: 0, recent_decisions: [] },
    data_plane: { sink_lag_messages: 0, lag_measured_at: ago(60_000) },
    links: {},
    ...extra,
  },
})

function cdcTable(name: string, extra: Partial<AttentionTable> = {}): AttentionTable {
  return { qualified_name: name, mode: "cdc", status: "running", total_events: 10, applied_total_events: 10, last_applied_ts: ago(60_000), dlq_rows: 0, ...extra }
}

/**
 * The summary the gateway would compute for these tables.
 *
 * computeCDCSummary reports a total only when some table MEASURED it, and omits it
 * otherwise — so a fixture whose tables carry counters must carry the matching
 * totals, or it describes a shape the server cannot produce. That distinction is now
 * load-bearing in the UI: an absent total means "nothing counted this", which the
 * Backlog tile must not render as a green "Caught up" (it is exactly the state a
 * pipeline with no stats rows is in). `unmeasuredCdc` below is how a test asks for
 * the genuinely-unmeasured shape.
 */
const tableStats = (tables: AttentionTable[], summary: Record<string, unknown> = {}, total = tables.length) => {
  const measured = tables.filter((t) => t.mode === "cdc" && typeof t.total_events === "number")
  const cdcTotals = measured.length
    ? {
        total_cdc_events: measured.reduce((n, t) => n + (t.total_events ?? 0), 0),
        total_applied_cdc_events: measured.reduce((n, t) => n + (t.applied_total_events ?? 0), 0),
      }
    : {}
  return {
    body: {
      summary: { total_tables: total, tables_failed: 0, total_dlq_rows: 0, ...cdcTotals, ...summary },
      tables,
      total,
    },
  }
}

/** A CDC table the stats projector has never written a row for. */
const unmeasuredCdcTable = (name: string): AttentionTable => ({
  qualified_name: name,
  mode: "cdc",
  status: "waiting_for_data",
  dlq_rows: 0,
})

beforeEach(() => {
  authFetch.mockReset()
  authFetch.mockImplementation(async (url: string) => {
    const r = route(url)
    if (r === "network") throw new TypeError("Failed to fetch")
    const status = r.status ?? 200
    return { ok: status < 400, status, json: async () => r.body }
  })
  replies = { runtime: cdcRuntime(), overview: overview(), tables: tableStats([cdcTable("public.a"), cdcTable("public.b"), cdcTable("public.c")]) }
})

afterEach(() => {
  cleanup()
  vi.useRealTimers()
  Reflect.deleteProperty(document, "visibilityState")
})

function tile(id: string) {
  return screen.getByTestId(`overview-tile-${id}`)
}

async function flush() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(0)
  })
}

describe("tablesNeedingAttention", () => {
  it("puts failed first, then failed rows, then degraded, then the furthest behind", () => {
    const items = tablesNeedingAttention(
      [
        cdcTable("behind.small", { total_events: 12, applied_total_events: 10 }),
        cdcTable("fine"),
        cdcTable("dlq.few", { dlq_rows: 2 }),
        cdcTable("behind.big", { total_events: 500, applied_total_events: 10 }),
        cdcTable("failed.one", { status: "failed", dlq_rows: 3 }),
        cdcTable("degraded.one", { status: "degraded" }),
        cdcTable("dlq.many", { dlq_rows: 40 }),
      ],
      true
    )
    expect(items.map((i) => i.name)).toEqual(["failed.one", "dlq.many", "dlq.few", "degraded.one", "behind.big", "behind.small"])
    expect(items[0]).toMatchObject({ reason: "Failed · 3 rows failed to load", tone: "bad" })
    expect(items[1].reason).toBe("40 rows failed to load")
    expect(items[4].reason).toMatch(/^490 changes waiting · last write /)
  })

  // A table's total_events counts changes only, never its full-load rows, so a
  // table whose load still sat in Kafka was left off this list and the card read
  // "All 4 tables caught up" beside thousands of rows waiting.
  it("counts full-load rows the destination has not written as behind", () => {
    const items = tablesNeedingAttention(
      [
        cdcTable("loaded", { snapshot_rows: 2400, applied_snapshot_rows: 2400 }),
        cdcTable("loading", { snapshot_rows: 4000, applied_snapshot_rows: 1000 }),
        cdcTable("both", { snapshot_rows: 10, applied_snapshot_rows: null, total_events: 5, applied_total_events: 3 }),
        cdcTable("never.counted", { snapshot_rows: null, applied_snapshot_rows: null }),
        cdcTable("over.applied", { snapshot_rows: 3, applied_snapshot_rows: 5 }),
      ],
      true
    )
    expect(items.map((i) => [i.name, i.reason.split(" · ")[0]])).toEqual([
      ["loading", "3,000 full-load rows waiting"],
      ["both", "10 full-load rows and 2 changes waiting"],
    ])
  })

  it("breaks a tie in backlog toward the table whose last write is oldest", () => {
    const items = tablesNeedingAttention(
      [
        cdcTable("recent", { total_events: 20, applied_total_events: 10, last_applied_ts: ago(60_000) }),
        cdcTable("old", { total_events: 20, applied_total_events: 10, last_applied_ts: ago(3_600_000) }),
      ],
      true
    )
    expect(items.map((i) => i.name)).toEqual(["old", "recent"])
  })

  it("ignores backlog for batch and words a degraded batch table like Table statistics", () => {
    const items = tablesNeedingAttention(
      [
        { qualified_name: "b.lagging", mode: "batch", status: "completed", total_events: 9, applied_total_events: 0 },
        { qualified_name: "b.none", mode: "batch", status: "degraded", read_rows: 50, inserted_rows: 0 },
        { qualified_name: "b.some", mode: "batch", status: "degraded", read_rows: 50, inserted_rows: 20 },
      ],
      false
    )
    expect(items.map((i) => [i.name, i.reason])).toEqual([
      ["b.none", "Degraded · destination wrote 0 rows"],
      ["b.some", "Degraded · 20 of 50 rows written"],
    ])
  })
})

describe("agentDecisionsLine", () => {
  it("is null with no decisions", () => {
    expect(agentDecisionsLine({ total_decisions: 0 })).toBeNull()
    expect(agentDecisionsLine(undefined)).toBeNull()
  })

  it("names the lowest confidence and says 'at least' at the query's 20-row cap", () => {
    expect(agentDecisionsLine({ total_decisions: 3, recent_decisions: [{ confidence: 0.9 }, { confidence: 0.62 }, {}] })).toBe(
      "3 automated decisions in the last 24h, lowest confidence 62%"
    )
    expect(agentDecisionsLine({ total_decisions: 20 })).toBe("At least 20 automated decisions in the last 24h")
  })
})

describe("deliveryTile", () => {
  const caughtUp = { value: "Caught up", detail: "nothing waiting in Kafka", tone: "ok" } as const
  const noReading = { value: "No reading", detail: "no lag measurement recorded yet", tone: "neutral" } as const
  const stalled = { value: "9 changes", detail: "captured but not arriving", tone: "bad" } as const
  const draining = { value: "9 changes", detail: "in Kafka — the sink is draining them now", tone: "warn" } as const
  const pending = { value: "42 changes", detail: "read by the sink, not yet written", tone: "warn" } as const
  const countersCaughtUp = { value: "Caught up", detail: "nothing waiting to be written", tone: "ok" } as const
  const notMeasured = { value: "Not measured", detail: "no change counts recorded yet", tone: "neutral" } as const

  it("takes the broker's reading when the counters have nothing to add", () => {
    expect(deliveryTile(caughtUp, countersCaughtUp)).toBe(caughtUp)
    expect(deliveryTile(caughtUp, notMeasured)).toBe(caughtUp)
    expect(deliveryTile(draining, countersCaughtUp)).toBe(draining)
  })
  it("shows changes read but not written over a broker that says nothing is waiting", () => {
    expect(deliveryTile(caughtUp, pending)).toBe(pending)
    expect(deliveryTile(noReading, pending)).toBe(pending)
    expect(deliveryTile(null, pending)).toBe(pending)
  })
  it("keeps a broker fault over counter pending: the stall is the bigger problem", () => {
    expect(deliveryTile(stalled, pending)).toBe(stalled)
    expect(deliveryTile(draining, pending)).toBe(draining)
  })
  it("never reads green from the counters alone", () => {
    expect(deliveryTile(noReading, countersCaughtUp)).toBe(noReading)
    expect(deliveryTile(null, countersCaughtUp)).toBeNull()
  })
})

describe("MonitoringOverviewTab (CDC)", () => {
  it("shows four tiles and reads the stable CDC table stats with status sort", async () => {
    replies.overview = overview({ data_plane: { sink_lag_messages: 1280, lag_measured_at: ago(30_000) } })
    render(<MonitoringOverviewTab pipelineId="p1" />)

    expect(await screen.findByText("1,280 changes")).toBeInTheDocument()
    expect(within(tile("delivery")).getByText("in Kafka, not yet read by the sink")).toBeInTheDocument()
    expect(tile("delivery")).toHaveAttribute("data-tone", "warn")
    expect(within(tile("errors")).getByText("None")).toBeInTheDocument()
    expect(within(tile("errors")).getByText("no failed tables or rows since the stream started")).toBeInTheDocument()
    expect(within(tile("freshness")).getByText("since the last change was written")).toBeInTheDocument()
    // Backlog and Waiting in Kafka were two tiles answering one question; one
    // said "Caught up" beside the other's 1,280 changes.
    expect(screen.getAllByTestId(/^overview-tile-/).map((t) => t.dataset.testid)).toEqual([
      "overview-tile-freshness",
      "overview-tile-capture",
      "overview-tile-delivery",
      "overview-tile-errors",
    ])

    const [url] = calls("/table-stats")
    expect(url).toContain("mode=cdc")
    expect(url).toContain("sort=status")
    expect(url).not.toContain("execution_id")
    expect(calls("/monitoring/overview")[0]).toContain("range=last_24h")
  })

  it("says all tables are caught up and links to Table statistics", async () => {
    render(<MonitoringOverviewTab pipelineId="p1" />)
    const link = await screen.findByRole("link", { name: /Table statistics/ })
    expect(link).toHaveAttribute("href", "/pipelines/p1?tab=table-stats")
    expect(screen.getByText(/All 3 tables caught up/)).toBeInTheDocument()
  })

  it("lists at most five tables needing attention, then '+N more'", async () => {
    replies.tables = tableStats(
      Array.from({ length: 7 }, (_, i) => cdcTable(`t${i}`, { dlq_rows: 10 + i })),
      { total_dlq_rows: 91 }
    )
    render(<MonitoringOverviewTab pipelineId="p1" />)
    const list = await screen.findByTestId("overview-attention")
    expect(within(list).getAllByRole("listitem").map((li) => li.textContent)).toEqual([
      "t616 rows failed to load",
      "t515 rows failed to load",
      "t414 rows failed to load",
      "t313 rows failed to load",
      "t212 rows failed to load",
    ])
    expect(screen.getByText("+2 more")).toBeInTheDocument()
    expect(tile("errors")).toHaveAttribute("data-tone", "warn")
    expect(within(tile("errors")).getByText("91 failed rows")).toBeInTheDocument()
  })

  it("marks freshness amber only when changes are waiting and the last write is old", async () => {
    replies.runtime = cdcRuntime(42)
    replies.tables = tableStats([cdcTable("public.a", { last_applied_ts: ago(20 * 60_000) })])
    render(<MonitoringOverviewTab pipelineId="p1" />)
    expect(await screen.findByText("42 changes")).toBeInTheDocument()
    expect(tile("freshness")).toHaveAttribute("data-tone", "warn")
    // The broker reads 0 (the default overview), but the sink has read 42 changes
    // it has not written: the later stage wins.
    expect(tile("delivery")).toHaveAttribute("data-tone", "warn")
    expect(within(tile("delivery")).getByText("read by the sink, not yet written")).toBeInTheDocument()
  })

  // Live on 2026-09-25 the tile read "34,233 changes" while those were the
  // initial load's rows.
  it("calls a full load's waiting rows rows, not changes", async () => {
    const load = { status: "completed", mode: "blocking", tables_total: 6, tables_done: 6, reloading_tables: 0, snapshot_rows_waiting: 67_200 }
    const liveness = { pending_events: 67_200, sink_lag_messages: 34_233, sink_committed_moving: true }
    replies.runtime = { body: { ...cdcRuntime().body, load, liveness } }
    render(<MonitoringOverviewTab pipelineId="p1" />)
    expect(await within(await screen.findByTestId("overview-tile-delivery")).findByText("34,233 rows")).toBeInTheDocument()
    expect(within(tile("delivery")).getByText("in Kafka — the sink is draining them now")).toBeInTheDocument()
  })

  it("calls rows the sink has read but not written rows too, while the load's rows are being written", async () => {
    const load = { status: "started", mode: "blocking", tables_total: 6, tables_done: 2, reloading_tables: 0 }
    replies.runtime = { body: { ...cdcRuntime().body, load, liveness: { pending_events: 500 } } }
    render(<MonitoringOverviewTab pipelineId="p1" />)
    expect(await within(await screen.findByTestId("overview-tile-delivery")).findByText("500 rows")).toBeInTheDocument()
    expect(within(tile("delivery")).getByText("read by the sink, not yet written")).toBeInTheDocument()
  })

  it("treats a quiet stream with nothing waiting as fresh, however old the last write", async () => {
    replies.tables = tableStats([cdcTable("public.a", { last_applied_ts: ago(5 * 3_600_000) })])
    render(<MonitoringOverviewTab pipelineId="p1" />)
    expect(await screen.findByText("5h ago")).toBeInTheDocument()
    expect(tile("freshness")).toHaveAttribute("data-tone", "ok")
  })

  it("says there is no Kafka reading instead of guessing one", async () => {
    replies.overview = overview({ data_plane: {} })
    render(<MonitoringOverviewTab pipelineId="p1" />)
    expect(await within(await screen.findByTestId("overview-tile-delivery")).findByText("No reading")).toBeInTheDocument()
  })
})

describe("MonitoringOverviewTab (batch)", () => {
  it("shows two tiles and reads the latest run's table stats", async () => {
    replies.runtime = batchRuntime()
    replies.tables = tableStats([{ qualified_name: "x", mode: "batch", status: "failed", completed_at: ago(3 * 3_600_000) }], { tables_failed: 1 })
    render(<MonitoringOverviewTab pipelineId="p1" />)

    expect(await screen.findByText("1 table failed")).toBeInTheDocument()
    expect(screen.getAllByTestId(/^overview-tile-/)).toHaveLength(2)
    expect(within(tile("freshness")).getByText("3h ago")).toBeInTheDocument()
    expect(within(tile("freshness")).getByText("since the last run finished")).toBeInTheDocument()
    expect(within(tile("errors")).getByText("in the last run")).toBeInTheDocument()
    const [url] = calls("/table-stats")
    expect(url).toContain("execution_id=exec-1")
    expect(url).not.toContain("mode=cdc")
  })

  it("says 'No run yet' and asks for no table stats before the first run", async () => {
    replies.runtime = batchRuntime("idle", null)
    render(<MonitoringOverviewTab pipelineId="p1" />)
    expect(await within(await screen.findByTestId("overview-tile-freshness")).findByText("No run yet")).toBeInTheDocument()
    expect(calls("/table-stats")).toHaveLength(0)
  })
})

describe("MonitoringOverviewTab reads", () => {
  it("says a first read failed instead of showing an empty, healthy pipeline", async () => {
    replies.runtime = { status: 503 }
    render(<MonitoringOverviewTab pipelineId="p1" />)
    expect(await screen.findByText(/Couldn't load this pipeline's status \(HTTP 503\)/)).toBeInTheDocument()
    expect(screen.queryByTestId("overview-tile-freshness")).toBeNull()
  })

  it("keeps the last Kafka reading when a later poll fails, and says so", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "setTimeout", "clearTimeout", "Date"] })
    replies.overview = overview({ data_plane: { sink_lag_messages: 7, lag_measured_at: ago(1000) } })
    render(<MonitoringOverviewTab pipelineId="p1" />)
    await flush()
    expect(within(tile("delivery")).getByText("7 changes")).toBeInTheDocument()

    replies.overview = "network"
    await act(async () => {
      await vi.advanceTimersByTimeAsync(OVERVIEW_POLL_MS)
    })
    expect(within(tile("delivery")).getByText("7 changes")).toBeInTheDocument()
    expect(within(tile("delivery")).getByText("Couldn't refresh (network error); showing the last reading")).toBeInTheDocument()
  })

  it("refreshes every 30 s, skips while the tab is hidden and catches up when it is shown", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "setTimeout", "clearTimeout", "Date"] })
    let visibility: DocumentVisibilityState = "visible"
    Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility })

    render(<MonitoringOverviewTab pipelineId="p1" />)
    await flush()
    expect(calls("/runtime")).toHaveLength(1)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(OVERVIEW_POLL_MS)
    })
    expect(calls("/runtime")).toHaveLength(2)
    expect(calls("/table-stats")).toHaveLength(2)

    visibility = "hidden"
    await act(async () => {
      await vi.advanceTimersByTimeAsync(OVERVIEW_POLL_MS * 3)
    })
    expect(calls("/runtime")).toHaveLength(2)

    visibility = "visible"
    await act(async () => {
      document.dispatchEvent(new Event("visibilitychange"))
      await vi.advanceTimersByTimeAsync(0)
    })
    expect(calls("/runtime")).toHaveLength(3)
  })
})

describe("MonitoringOverviewTab on another pipeline", () => {
  it("drops the previous pipeline's numbers instead of showing them under the new one", async () => {
    replies.overview = overview({ data_plane: { sink_lag_messages: 7, lag_measured_at: ago(1000) } })
    const { rerender } = render(<MonitoringOverviewTab pipelineId="p1" />)
    expect(await screen.findByText("7 changes")).toBeInTheDocument()

    // p2's reads are still in flight: nothing of p1's may show meanwhile.
    let answerP2: (value: unknown) => void = () => {}
    authFetch.mockImplementation(() => new Promise((resolve) => (answerP2 = resolve)))
    rerender(<MonitoringOverviewTab pipelineId="p2" />)
    expect(screen.queryByText("7 changes")).toBeNull()
    expect(screen.getByText("Loading…")).toBeInTheDocument()
    expect(calls("/pipelines/p2/runtime")).toHaveLength(1)
    answerP2({ ok: false, status: 404, json: async () => ({}) })
  })
})

describe("MonitoringOverviewTab agent line", () => {
  it("is hidden when there were no automated decisions", async () => {
    render(<MonitoringOverviewTab pipelineId="p1" onOpenActivity={() => {}} />)
    await screen.findByText(/All 3 tables caught up/)
    expect(screen.queryByTestId("overview-decisions")).toBeNull()
    expect(screen.queryByRole("button", { name: "View in Activity" })).toBeNull()
  })

  it("summarises decisions in one line and opens Activity", async () => {
    replies.overview = overview({ agent_reasoning: { total_decisions: 2, recent_decisions: [{ confidence: 0.8 }, { confidence: 0.55 }] } })
    const onOpenActivity = vi.fn()
    render(<MonitoringOverviewTab pipelineId="p1" onOpenActivity={onOpenActivity} />)
    const line = await screen.findByTestId("overview-decisions")
    expect(line).toHaveTextContent("2 automated decisions in the last 24h, lowest confidence 55%")
    fireEvent.click(within(line).getByRole("button", { name: "View in Activity" }))
    expect(onOpenActivity).toHaveBeenCalledTimes(1)
  })
})

/**
 * "Caught up" used to be printed for three different situations, only one of which
 * is health:
 *
 *  1. the sink really has nothing waiting                       -> caught up
 *  2. nothing has ever counted a change for this pipeline       -> not measured
 *  3. Debezium is dead, so no change is being produced at all   -> capture stopped
 *
 * (3) is KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED: with the only producer gone every
 * topic drains to lag 0, so lag, the badge and the ack ledger all read healthy over
 * an 877k-row capture hole. (2) is the same missing data the Throughput card renders
 * as a wall of zeros. These tests hold the three apart.
 */
describe("MonitoringOverviewTab: zero is not automatically health", () => {
  const debezium = (status: string, last_error?: string) => ({
    kind: "debezium_task",
    identifier: "cdc-aa4c1a3c",
    status,
    ...(last_error ? { last_error } : {}),
  })

  const cdcRuntimeWith = (liveness: Record<string, unknown>, dependencies: unknown[] = []) => ({
    body: {
      pipeline_id: "p1",
      execution_id: "p1",
      mode: "cdc",
      phase: "streaming",
      health: "healthy",
      dependencies,
      updated_at: ago(0),
      liveness: { pending_events: 0, ...liveness },
    },
  })

  it("says 'No reading', not 'Caught up', when nothing counted a change and the broker has no lag", async () => {
    replies.tables = tableStats([unmeasuredCdcTable("public.orders"), unmeasuredCdcTable("public.users")])
    replies.overview = overview({ data_plane: {} })
    render(<MonitoringOverviewTab pipelineId="p1" />)

    const delivery = await screen.findByTestId("overview-tile-delivery")
    expect(await within(delivery).findByText("No reading")).toBeInTheDocument()
    expect(delivery).toHaveAttribute("data-tone", "neutral")
    expect(within(delivery).queryByText("Caught up")).toBeNull()
  })

  it("calls a lag of 0 'Capture stopped' when the source stream is down", async () => {
    replies.runtime = cdcRuntimeWith(
      { sink_lag_messages: 0, sink_committed_moving: false, sink_lag_measured_at: ago(20_000) },
      [debezium("unhealthy", "debezium connector not found in kafka connect")]
    )
    render(<MonitoringOverviewTab pipelineId="p1" />)

    const kafka = await screen.findByTestId("overview-tile-delivery")
    expect(within(kafka).getByText("Capture stopped")).toBeInTheDocument()
    expect(within(kafka).getByText(/nothing is being captured — not caught up/)).toBeInTheDocument()
    expect(kafka).toHaveAttribute("data-tone", "bad")

    const capture = screen.getByTestId("overview-tile-capture")
    expect(within(capture).getByText("Stopped")).toBeInTheDocument()
    expect(within(capture).getByText("debezium connector not found in kafka connect")).toBeInTheDocument()
    expect(capture).toHaveAttribute("data-tone", "bad")
  })

  it("still says 'Caught up' when capture is healthy and nothing is waiting", async () => {
    replies.runtime = cdcRuntimeWith(
      { sink_lag_messages: 0, sink_committed_moving: false, sink_lag_measured_at: ago(20_000) },
      [debezium("healthy")]
    )
    render(<MonitoringOverviewTab pipelineId="p1" />)

    const kafka = await screen.findByTestId("overview-tile-delivery")
    expect(within(kafka).getByText("Caught up")).toBeInTheDocument()
    expect(kafka).toHaveAttribute("data-tone", "ok")
    expect(within(screen.getByTestId("overview-tile-capture")).getByText("Capturing")).toBeInTheDocument()
  })

  it("separates a sink draining a backlog from one that has stopped committing", async () => {
    replies.runtime = cdcRuntimeWith(
      { sink_lag_messages: 1280, sink_committed_moving: true, sink_lag_measured_at: ago(10_000) },
      [debezium("healthy")]
    )
    const { unmount } = render(<MonitoringOverviewTab pipelineId="p1" />)
    let kafka = await screen.findByTestId("overview-tile-delivery")
    expect(within(kafka).getByText("1,280 changes")).toBeInTheDocument()
    expect(within(kafka).getByText("in Kafka — the sink is draining them now")).toBeInTheDocument()
    expect(kafka).toHaveAttribute("data-tone", "warn")
    unmount()

    replies.runtime = cdcRuntimeWith(
      {
        sink_lag_messages: 1280,
        sink_committed_moving: false,
        sink_stalled: true,
        sink_stalled_seconds: 420,
        sink_lag_measured_at: ago(10_000),
      },
      [debezium("healthy")]
    )
    render(<MonitoringOverviewTab pipelineId="p1" />)
    kafka = await screen.findByTestId("overview-tile-delivery")
    expect(within(kafka).getByText("1,280 changes")).toBeInTheDocument()
    expect(within(kafka).getByText(/has not committed any progress for 7m/)).toBeInTheDocument()
    expect(kafka).toHaveAttribute("data-tone", "bad")
  })

  it("shows the runtime's lag even when the monitoring overview is unavailable", async () => {
    // FEATURE_MONITORING_OVERVIEW off => /monitoring/overview 404s. The lag tile used
    // to render nothing at all; the safety signal must not depend on that flag.
    replies.overview = { status: 404, body: {} }
    replies.runtime = cdcRuntimeWith(
      { sink_lag_messages: 512, sink_committed_moving: true, sink_lag_measured_at: ago(15_000) },
      [debezium("healthy")]
    )
    render(<MonitoringOverviewTab pipelineId="p1" />)

    const kafka = await screen.findByTestId("overview-tile-delivery")
    expect(within(kafka).getByText("512 changes")).toBeInTheDocument()
    expect(kafka).toHaveAttribute("data-tone", "warn")
  })

  it("reports no reading rather than zero when nothing has measured the lag", async () => {
    // The overview answers, but carries no lag value, and the runtime has no
    // reading either: nobody has measured it. "No reading" — never a green zero.
    replies.overview = overview({ data_plane: {} })
    replies.runtime = cdcRuntimeWith({}, [debezium("healthy")])
    render(<MonitoringOverviewTab pipelineId="p1" />)

    const kafka = await screen.findByTestId("overview-tile-delivery")
    expect(within(kafka).getByText("No reading")).toBeInTheDocument()
    expect(kafka).toHaveAttribute("data-tone", "neutral")
  })

  it("surfaces a failed lag read instead of pretending there is no backlog", async () => {
    // Both sources gone (overview 404 and no runtime reading). The F-242 rule at
    // the top of this file: a read error must never render as an empty, healthy
    // pipeline — so the tile says it could not load rather than "Caught up".
    replies.overview = { status: 404, body: {} }
    replies.runtime = cdcRuntimeWith({}, [debezium("healthy")])
    render(<MonitoringOverviewTab pipelineId="p1" />)

    const kafka = await screen.findByTestId("overview-tile-delivery")
    expect(within(kafka).getByText(/Couldn't load/)).toBeInTheDocument()
    expect(within(kafka).queryByText("Caught up")).toBeNull()
  })

  it("does not call a batch pipeline's capture stopped (it has no source stream)", async () => {
    replies.runtime = batchRuntime()
    render(<MonitoringOverviewTab pipelineId="p1" />)
    await screen.findByTestId("overview-tile-freshness")
    // Capture and Delivery are CDC-only.
    expect(screen.queryByTestId("overview-tile-capture")).toBeNull()
    expect(screen.getAllByTestId(/^overview-tile-/)).toHaveLength(2)
  })
})
