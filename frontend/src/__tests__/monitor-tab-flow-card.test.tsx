/**
 * Data flow tab: the CDC Flow card and the Consumers list.
 *
 * Prod, 2026-09-25, pipeline 600b012e: the Throughput card read 0 in every cell
 * and "measuring…" forever while 16,909 rows sat in the destination. Two bugs,
 * both pinned here:
 *
 *  1. Every one of those rows was a snapshot read, and the card only drew the
 *     insert/update/delete counters. `total_events` excludes snapshot rows by
 *     design (table_stats.go capturedInsertsSQL), so a pipeline whose whole life
 *     so far is its initial load showed a wall of zeros. The card now shows the
 *     full-load rows as their own line, and the change counters under "since the
 *     load".
 *
 *  2. useCounterRate took a sample only when the counter CHANGED (an effect keyed
 *     on the value). A flat counter therefore never got its second sample, and the
 *     rate said "measuring…" for as long as the page was open — the one reading
 *     that should say "0 / min". It now samples on every successful poll.
 *
 * Consumers: the card printed "measured 20s ago ago" (formatAge already says
 * "ago") under every group, and spent a bordered box per group on a list that is
 * mostly zeros. It now names the role first, measures once in the header, and
 * folds the per-table lag away unless something is behind.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render, screen, within } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))
vi.mock("@/components/pipeline/DiagnosePanel", () => ({ DiagnosePanel: () => null }))

import {
  MonitorTab,
  advanceOffsetBaselines,
  appendRateSample,
  computeRatePerMin,
  offsetMovedSince,
} from "@/components/pipeline/MonitorTab"

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

// runtime.load: the initial load the orchestrator recorded. Undefined = none.
let runtimeLoad: Record<string, unknown> | undefined
let runtimeDeps: Record<string, unknown>[]

const RUNTIME = (mode: "cdc" | "batch") => ({
  pipeline_id: "p1",
  execution_id: mode === "cdc" ? "p1" : "e1",
  mode,
  phase: "streaming",
  health: "healthy",
  dependencies: runtimeDeps,
  updated_at: "2026-09-25T10:00:00Z",
  ...(runtimeLoad ? { load: runtimeLoad } : {}),
})

// The prod shape: 16,909 snapshot rows, nothing else ever captured.
function snapshotOnly(extra: Record<string, unknown> = {}) {
  return {
    summary: {
      mode: "cdc",
      total_tables: 3,
      tables_completed: 0,
      tables_running: 3,
      tables_failed: 0,
      tables_degraded: 0,
      tables_waiting_for_data: 0,
      total_inserts: 0,
      total_updates: 0,
      total_deletes: 0,
      total_cdc_events: 0,
      total_applied_inserts: 0,
      total_applied_updates: 0,
      total_applied_deletes: 0,
      total_applied_cdc_events: 0,
      total_snapshot_rows: 16909,
      total_applied_snapshot_rows: 16909,
      ...extra,
    },
    tables: [],
  }
}

let stats: () => unknown
let consumers: () => unknown
let mode: "cdc" | "batch"

beforeEach(() => {
  vi.clearAllMocks()
  mode = "cdc"
  runtimeLoad = undefined
  runtimeDeps = []
  stats = () => snapshotOnly()
  consumers = () => ({ consumers: [], measured: true })
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/runtime")) return res(200, RUNTIME(mode))
    if (u.includes("/table-stats")) return res(200, stats())
    if (u.includes("/consumers")) return res(200, consumers())
    return res(200, {})
  })
})
afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

describe("CDC Flow card", () => {
  it("shows the full-load rows, not a wall of zeros", async () => {
    render(<MonitorTab pipelineId="p1" />)
    const card = await screen.findByTestId("flow-card")
    const load = within(card).getByTestId("flow-row-load")
    expect(within(load).getAllByText("16,909")).toHaveLength(2) // captured, written
    expect(within(card).getByText(/Changes since the load/i)).toBeInTheDocument()
    expect(screen.queryByText("Throughput")).toBeNull()
  })

  // A stream that has only loaded read Inserts, Updates, Deletes and All changes
  // as four rows of 0 / 0 (prod, 600b012e): one line says it.
  it("says 'none yet' once, not four rows of zeros, when no change has been captured", async () => {
    render(<MonitorTab pipelineId="p1" />)
    const card = await screen.findByTestId("flow-card")
    expect(within(card).getByTestId("flow-row-changes-none")).toHaveTextContent("Changes since the loadNone yet")
    for (const label of ["Inserts", "Updates", "Deletes", "All changes"]) {
      expect(within(card).queryByText(label)).toBeNull()
    }
  })

  it("control: keeps the breakdown once any change has been captured", async () => {
    stats = () =>
      snapshotOnly({ total_inserts: 12, total_cdc_events: 12, total_applied_inserts: 10, total_applied_cdc_events: 10 })
    render(<MonitorTab pipelineId="p1" />)
    const card = await screen.findByTestId("flow-card")
    expect(within(card).queryByTestId("flow-row-changes-none")).toBeNull()
    for (const label of ["Inserts", "Updates", "Deletes", "All changes"]) {
      expect(within(card).getByText(label)).toBeInTheDocument()
    }
  })

  it("control: an unmeasured change count stays '–', never 'none yet'", async () => {
    stats = () => {
      const s = snapshotOnly()
      for (const k of Object.keys(s.summary)) {
        if (/^total_(applied_)?(inserts|updates|deletes|cdc_events)$/.test(k)) delete (s.summary as Record<string, unknown>)[k]
      }
      return s
    }
    render(<MonitorTab pipelineId="p1" />)
    const card = await screen.findByTestId("flow-card")
    expect(within(card).queryByTestId("flow-row-changes-none")).toBeNull()
    expect(within(card).getByText("All changes")).toBeInTheDocument()
  })

  it("links to Table statistics for the per-table view", async () => {
    render(<MonitorTab pipelineId="p1" />)
    const card = await screen.findByTestId("flow-card")
    const link = within(card).getByRole("link", { name: /table statistics/i })
    expect(link.getAttribute("href")).toContain("tab=table-stats")
  })

  it("shows a missing snapshot count as unmeasured, not as 0", async () => {
    stats = () => {
      const s = snapshotOnly()
      delete (s.summary as Record<string, unknown>).total_snapshot_rows
      delete (s.summary as Record<string, unknown>).total_applied_snapshot_rows
      return s
    }
    render(<MonitorTab pipelineId="p1" />)
    const load = await screen.findByTestId("flow-row-load")
    expect(within(load).getAllByText("–")).toHaveLength(2)
  })

  it("lists only tables that need attention, and no 'Tables completed' for a stream", async () => {
    stats = () => snapshotOnly({ total_tables: 5, tables_running: 2, tables_degraded: 1, tables_waiting_for_data: 2 })
    render(<MonitorTab pipelineId="p1" />)
    const card = await screen.findByTestId("flow-card")
    expect(within(card).getByText("Tables degraded")).toBeInTheDocument()
    expect(within(card).getByText("Tables with no data yet")).toBeInTheDocument()
    expect(within(card).queryByText("Tables completed")).toBeNull()
    expect(within(card).queryByText("Tables running")).toBeNull()
  })

  // DMS-style: the card says where the pipeline is before it gives the counts.
  it("says the load completed and replication is ongoing, with when it finished", async () => {
    runtimeLoad = {
      status: "completed", mode: "blocking", tables_total: 3, tables_done: 3, reloading_tables: 0,
      started_at: "2026-09-25T09:00:00Z", completed_at: "2026-09-25T09:30:00Z",
    }
    render(<MonitorTab pipelineId="p1" />)
    const line = await screen.findByTestId("flow-load-status")
    expect(line).toHaveTextContent("Load completed, replication ongoing")
    expect(line).toHaveTextContent(/finished/i)
  })

  it("shows why a load failed, in words on the card", async () => {
    runtimeLoad = {
      status: "failed", mode: "blocking", tables_total: 3, tables_done: 1, reloading_tables: 0,
      started_at: "2026-09-25T09:00:00Z", last_error: "the batch historical load failed and CDC was not started",
    }
    render(<MonitorTab pipelineId="p1" />)
    const line = await screen.findByTestId("flow-load-status")
    expect(line).toHaveTextContent("Full load failed")
    expect(line).toHaveTextContent("the batch historical load failed and CDC was not started")
  })

  it("claims nothing about a load that was never recorded", async () => {
    render(<MonitorTab pipelineId="p1" />)
    await screen.findByTestId("flow-row-load")
    expect(screen.queryByTestId("flow-load-status")).toBeNull()
  })

  it("control: a batch pipeline keeps the Throughput card", async () => {
    mode = "batch"
    stats = () => ({ summary: { mode: "batch", total_tables: 1, tables_completed: 1, total_read_rows: 10, total_inserted_rows: 10 } })
    render(<MonitorTab pipelineId="p1" />)
    expect(await screen.findByText("Throughput")).toBeInTheDocument()
    expect(screen.queryByTestId("flow-card")).toBeNull()
  })
})

describe("Rate now", () => {
  // Two rows of "0 / min" said one thing twice; a measured standstill on both
  // sides is one line. It still has to reach a reading: "measuring…" forever was
  // the #1203 bug.
  it("says nothing is moving, once, for counters that are polled and do not move", async () => {
    vi.useFakeTimers()
    render(<MonitorTab pipelineId="p1" />)
    await tick(0)
    expect(screen.getByTestId("throughput-rate")).toHaveTextContent("measuring…")

    await tick(5000)
    await tick(5000)
    const rate = screen.getByTestId("throughput-rate")
    expect(rate).toHaveTextContent("Rate nowNothing moving")
    expect(rate).not.toHaveTextContent("0 / min")
    expect(rate).not.toHaveTextContent("measuring…")
    expect(within(rate).queryByText("Captured")).toBeNull()
  })

  // A 0 / min rate cannot tell a stream that went quiet a minute ago from one idle
  // since yesterday; the tables' last_event_ts / last_applied_ts can.
  it("says when each side last moved, under Nothing moving", async () => {
    vi.useFakeTimers()
    const hoursAgo = (h: number) => new Date(Date.now() - h * 3_600_000).toISOString()
    stats = () => ({
      ...snapshotOnly(),
      tables: [
        { qualified_name: "public.orders", last_event_ts: hoursAgo(2), last_applied_ts: hoursAgo(3) },
        { qualified_name: "public.users", last_event_ts: hoursAgo(5), last_applied_ts: hoursAgo(5) },
      ],
      total: 2,
    })
    render(<MonitorTab pipelineId="p1" />)
    await tick(0)
    await tick(5000)
    await tick(5000)
    expect(screen.getByTestId("throughput-rate")).toHaveTextContent("Nothing moving")
    expect(screen.getByTestId("throughput-idle-detail")).toHaveTextContent("last captured 2h ago, last written 3h ago")
  })

  it("control: no idle detail from one page of a paginated table list", async () => {
    vi.useFakeTimers()
    stats = () => ({
      ...snapshotOnly(),
      tables: [{ qualified_name: "public.orders", last_event_ts: "2026-09-25T08:00:00Z", last_applied_ts: "2026-09-25T08:00:00Z" }],
      total: 120,
    })
    render(<MonitorTab pipelineId="p1" />)
    await tick(0)
    await tick(5000)
    await tick(5000)
    expect(screen.getByTestId("throughput-rate")).toHaveTextContent("Nothing moving")
    expect(screen.queryByTestId("throughput-idle-detail")).toBeNull()
  })

  // No endpoint serves a windowed count, so the longer window is the page's own:
  // from its first reading. It waits for a whole minute rather than say "(0 min)".
  it("says how much moved since the page opened, once a minute has passed", async () => {
    vi.useFakeTimers()
    let n = 1000
    stats = () => {
      n += 500
      return snapshotOnly({ total_snapshot_rows: n, total_applied_snapshot_rows: n - 100 })
    }
    render(<MonitorTab pipelineId="p1" />)
    await tick(0)
    for (let i = 0; i < 11; i++) await tick(5000)
    expect(screen.queryByTestId("throughput-since-open")).toBeNull()
    await tick(5000)
    await tick(5000)
    expect(screen.getByTestId("throughput-since-open")).toHaveTextContent(
      "Since this page opened (1 min): +6,500 captured · +6,500 written",
    )
  })

  it("control: one side moving keeps both rows, with the other's 0 / min", async () => {
    vi.useFakeTimers()
    let n = 1000
    stats = () => {
      n += 500
      return snapshotOnly({ total_snapshot_rows: n, total_applied_snapshot_rows: 1000 })
    }
    render(<MonitorTab pipelineId="p1" />)
    await tick(0)
    await tick(5000)
    await tick(5000)
    const rate = screen.getByTestId("throughput-rate")
    expect(within(rate).getByText("6,000 / min")).toBeInTheDocument()
    expect(within(rate).getByText("0 / min")).toBeInTheDocument()
    expect(rate).not.toHaveTextContent("Nothing moving")
  })

  it("counts full-load rows as movement", async () => {
    vi.useFakeTimers()
    let n = 1000
    stats = () => {
      n += 500
      return snapshotOnly({ total_snapshot_rows: n, total_applied_snapshot_rows: n })
    }
    render(<MonitorTab pipelineId="p1" />)
    await tick(0)
    await tick(5000)
    await tick(5000)
    const rate = screen.getByTestId("throughput-rate")
    // 500 rows every 5 s on both sides.
    expect(rate).not.toHaveTextContent("measuring…")
    expect(within(rate).getAllByText("6,000 / min")).toHaveLength(2)
  })
})

describe("appendRateSample", () => {
  const t0 = 1_700_000_000_000
  it("adds a sample on every poll, even when the value is unchanged", () => {
    let s = appendRateSample([], 5, t0)
    s = appendRateSample(s, 5, t0 + 5000)
    s = appendRateSample(s, 5, t0 + 10_000)
    expect(s).toHaveLength(3)
    expect(computeRatePerMin(s)).toBe(0)
  })
  it("drops the history when nothing measured the counter", () => {
    expect(appendRateSample([{ t: t0, v: 5 }], undefined, t0 + 5000)).toEqual([])
    expect(appendRateSample([{ t: t0, v: 5 }], null, t0 + 5000)).toEqual([])
  })
  it("starts again on a counter that went backwards", () => {
    expect(appendRateSample([{ t: t0, v: 50 }], 10, t0 + 5000)).toEqual([{ t: t0 + 5000, v: 10 }])
  })
  it("keeps one sample older than the window, and no more", () => {
    let s: { t: number; v: number }[] = []
    for (let i = 0; i <= 30; i++) s = appendRateSample(s, i, t0 + i * 5000)
    expect(s[s.length - 1].t - s[1].t).toBeLessThanOrEqual(60_000)
    expect(s[s.length - 1].t - s[0].t).toBeGreaterThan(60_000)
  })
})

describe("Consumers list", () => {
  const ago = (ms: number) => new Date(Date.now() - ms).toISOString()
  const group = (extra: Record<string, unknown> = {}) => ({
    group: "rsync.sink-aa4c1a3c",
    role: "sink",
    state: "Stable",
    members: 1,
    total_lag: 0,
    measured_at: ago(120_000),
    topics: [
      { topic: "rsync.cdc-aa4c1a3c.public.orders", table: "public.orders", lag: 0, committed: 98000 },
      { topic: "rsync.cdc-aa4c1a3c.public.users", table: "public.users", lag: 0, committed: 4100 },
    ],
    ...extra,
  })

  it("never says 'ago ago', and says when it was measured once", async () => {
    consumers = () => ({ measured: true, consumers: [group(), group({ group: "rsync.sink-aa4c1a3c-stream", role: "sink_stream" })] })
    render(<MonitorTab pipelineId="p1" />)
    const card = await screen.findByTestId("consumers-card")
    await within(card).findByText("rsync.sink-aa4c1a3c")
    expect(card.textContent).not.toMatch(/ago ago/)
    expect(within(card).getAllByText(/measured 2m ago/)).toHaveLength(1)
  })

  it("folds the per-table lag away when nothing is behind", async () => {
    consumers = () => ({ measured: true, consumers: [group()] })
    render(<MonitorTab pipelineId="p1" />)
    const row = await screen.findByTestId("consumer-rsync.sink-aa4c1a3c")
    const details = row.querySelector("details")
    expect(details).not.toBeNull()
    expect(details!.open).toBe(false)
    expect(within(row).getByText(/2 tables/)).toBeInTheDocument()
  })

  // `committed` was served per topic and never shown. It is a position, not a row
  // count (the load's rows and tombstones are in it), so it is labelled as one.
  it("shows each table's committed offset, and how far it moved since the page opened", async () => {
    vi.useFakeTimers()
    let orders = 98000
    consumers = () => {
      const g = group({
        topics: [
          { topic: "rsync.cdc-aa4c1a3c.public.orders", table: "public.orders", lag: 0, committed: orders },
          { topic: "rsync.cdc-aa4c1a3c.public.users", table: "public.users", lag: 0, committed: 4100 },
        ],
      })
      orders += 250
      return { measured: true, consumers: [g] }
    }
    render(<MonitorTab pipelineId="p1" />)
    await tick(0)
    const row = screen.getByTestId("consumer-rsync.sink-aa4c1a3c")
    expect(within(row).getByRole("table", { name: "Committed offsets for rsync.sink-aa4c1a3c" })).toBeInTheDocument()
    expect(within(row).getByRole("columnheader", { name: "Committed offset" })).toBeInTheDocument()
    const ordersRow = within(row).getByTestId("consumer-topic-rsync.cdc-aa4c1a3c.public.orders")
    expect(ordersRow).toHaveTextContent("public.orders98,000")
    expect(row).not.toHaveTextContent("since this page opened")

    await tick(5000)
    await tick(5000)
    expect(within(row).getByTestId("consumer-topic-rsync.cdc-aa4c1a3c.public.orders")).toHaveTextContent("98,500+500")
    expect(within(row).getByTestId("consumer-topic-rsync.cdc-aa4c1a3c.public.users")).toHaveTextContent("4,100")
    expect(within(row).getByTestId("consumer-topic-rsync.cdc-aa4c1a3c.public.users")).not.toHaveTextContent("+")
    expect(row).toHaveTextContent("offsets +500 since this page opened")
  })

  it("opens the per-table lag when a table is behind", async () => {
    consumers = () => ({
      measured: true,
      consumers: [
        group({
          total_lag: 120,
          topics: [
            { topic: "rsync.cdc-aa4c1a3c.public.orders", table: "public.orders", lag: 120, committed: 98000 },
            { topic: "rsync.cdc-aa4c1a3c.public.users", table: "public.users", lag: 0, committed: 4100 },
          ],
        }),
      ],
    })
    render(<MonitorTab pipelineId="p1" />)
    const row = await screen.findByTestId("consumer-rsync.sink-aa4c1a3c")
    expect(row.querySelector("details")!.open).toBe(true)
    expect(within(row).getByText("120 behind")).toBeInTheDocument()
  })
})

describe("committed-offset baselines", () => {
  const topic = (committed?: number) => ({ topic: "t1", table: "public.t1", lag: 0, committed })
  const c = (committed?: number) => [{ group: "g", role: "sink", state: "Stable", members: 1, total_lag: 0, measured_at: "", topics: [topic(committed)] }]

  it("baselines a topic the first time it is seen, then reports the move from there", () => {
    const b = advanceOffsetBaselines({}, c(100) as never)!
    expect(b).not.toBeNull()
    expect(offsetMovedSince(b, "g", topic(100) as never)).toBe(0)
    expect(advanceOffsetBaselines(b, c(160) as never)).toBeNull()
    expect(offsetMovedSince(b, "g", topic(160) as never)).toBe(60)
  })

  it("re-baselines an offset that went backwards, never showing a negative move", () => {
    const b = advanceOffsetBaselines({}, c(100) as never)!
    const reset = advanceOffsetBaselines(b, c(10) as never)!
    expect(offsetMovedSince(reset, "g", topic(10) as never)).toBe(0)
    expect(offsetMovedSince(reset, "g", topic(40) as never)).toBe(30)
  })

  it("says nothing for a topic with no committed offset", () => {
    expect(advanceOffsetBaselines({}, c(undefined) as never)).toBeNull()
    expect(offsetMovedSince({}, "g", topic(undefined) as never)).toBe(0)
  })
})

describe("Dependencies card", () => {
  const iso = (msAgo: number) => new Date(Date.now() - msAgo).toISOString()

  // consecutive_failures and last_healthy_at are served on every dependency row
  // (dependency_probe.go writeHealth) and were never shown.
  it("says how long an unhealthy dependency has been failing, and when it was last healthy", async () => {
    runtimeDeps = [
      {
        kind: "kafka_sink_worker",
        identifier: "sink-aa4c1a3c",
        status: "unhealthy",
        last_checked_at: iso(5_000),
        last_healthy_at: iso(3 * 3_600_000),
        consecutive_failures: 12,
        last_error: "dial tcp: i/o timeout",
      },
    ]
    render(<MonitorTab pipelineId="p1" />)
    expect(await screen.findByTestId("dependency-streak-kafka_sink_worker")).toHaveTextContent(
      "not healthy for 12 checks in a row · last healthy 3h ago",
    )
  })

  it("control: a healthy dependency gets no streak line", async () => {
    runtimeDeps = [
      {
        kind: "kafka_sink_worker",
        identifier: "sink-aa4c1a3c",
        status: "healthy",
        last_checked_at: iso(5_000),
        last_healthy_at: iso(5_000),
        consecutive_failures: 0,
      },
    ]
    render(<MonitorTab pipelineId="p1" />)
    await screen.findByText("sink-aa4c1a3c")
    expect(screen.queryByTestId("dependency-streak-kafka_sink_worker")).toBeNull()
  })
})
