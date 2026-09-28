/**
 * Table statistics must show the run the page names, and keep showing it while it runs.
 *
 * U-TS-RACE — bug class: a run-scoped read issued before the run is known. The
 * Monitoring panel mounts the grid before its /state answers, so the first read
 * carried no execution_id and came back pipeline-wide (prod P3: 54 tables /
 * 376,150 rows for a 6-table pipeline). When that /state failed, nothing re-read
 * it, so the pipeline-wide grid stayed until a manual refresh. Pinned: the grid
 * waits for the run (executionPending), and the Monitoring panel retries a
 * failed first /state the way the Live State panel already did.
 *
 * U-TS-STALE — bug class: a live view that reads once. The grid fetched only on
 * a parameter change or the refresh bus, so a Reload in progress froze at its
 * first numbers (prod P2: "Completed 680,000", 2/5 tables, while the run was at
 * 1.06M). Pinned: a running pipeline re-reads on a tick; controls: a finished
 * one does not, and a hidden tab skips its ticks.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  useSearchParams: () => new URLSearchParams(),
  usePathname: () => "/pipelines/p1",
}))
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: null, loading: false, error: null }),
}))
vi.mock("@/components/pipeline/MonitoringOverviewTab", () => ({ MonitoringOverviewTab: () => null }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
  authFetchOrThrow: (...a: unknown[]) => authFetch(...a),
}))

import { TABLE_STATS_POLL_MS, TableStatisticsPanel } from "@/components/pipeline/TableStatisticsPanel"
import { PipelineMonitoringPanel } from "@/components/pipeline/PipelineMonitoringPanel"

const RUN = "0321ff04-0000-0000-0000-000000000000"

function ok(body: unknown) {
  return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) } as unknown as Response
}

let read = 680_000
function statsBody() {
  return {
    summary: {
      mode: "batch",
      total_tables: 1,
      tables_completed: 0,
      tables_failed: 0,
      tables_running: 1,
      tables_degraded: 0,
      total_read_rows: read,
      total_inserted_rows: read,
      total_dlq_rows: 0,
    },
    tables: [
      {
        qualified_name: "datingapp.messages",
        table_name: "messages",
        schema_name: "datingapp",
        mode: "batch",
        status: "running",
        read_rows: read,
        inserted_rows: read,
        dlq_rows: 0,
        started_at: "2026-09-27T16:00:00Z",
        updated_at: "2026-09-27T16:01:00Z",
      },
    ],
    total: 1,
  }
}

const statsCalls = () => authFetch.mock.calls.map(([u]) => String(u)).filter((u) => u.includes("/table-stats"))
const statsExecutions = () =>
  statsCalls().map((u) => new URL(u, "http://localhost").searchParams.get("execution_id"))

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

let hidden = false
beforeEach(() => {
  authFetch.mockReset()
  localStorage.clear()
  read = 680_000
  hidden = false
  vi.useFakeTimers()
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => (hidden ? "hidden" : "visible") })
  authFetch.mockImplementation(async (url: string) => (String(url).includes("/table-stats") ? ok(statsBody()) : ok({})))
})
afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

describe("Table statistics — U-TS-RACE: no run-less read while the run is unknown", () => {
  it("waits for the run, then reads that run only", async () => {
    const { rerender } = render(
      <TableStatisticsPanel pipelineId="p1" executionPending mode="batch" pipelineStatus="completed" />,
    )
    await tick(0)
    expect(statsCalls()).toHaveLength(0)

    rerender(<TableStatisticsPanel pipelineId="p1" executionId={RUN} mode="batch" pipelineStatus="completed" />)
    await tick(0)
    expect(statsExecutions()).toEqual([RUN])
  })

  it("control: a pipeline that has never run still reads pipeline-wide", async () => {
    render(<TableStatisticsPanel pipelineId="p1" mode="batch" pipelineStatus="completed" />)
    await tick(0)
    expect(statsExecutions()).toEqual([null])
  })

  it("the Monitoring panel retries a failed first /state, then scopes the grid to its run", async () => {
    let stateReads = 0
    authFetch.mockImplementation(async (url: string) => {
      const u = String(url)
      if (u.endsWith("/state")) {
        stateReads += 1
        if (stateReads === 1) return { ok: false, status: 502, json: async () => ({}), text: async () => "" }
        return ok({ schema_version: 1, pipeline_id: "p1", execution_id: RUN, status: "completed" })
      }
      if (u.includes("/table-stats")) return ok(statsBody())
      if (u.includes("/events")) return ok({ events: [] })
      return ok({})
    })
    render(<PipelineMonitoringPanel pipelineId="p1" variant="table_stats" />)
    await tick(0)
    expect(stateReads).toBe(1)
    await tick(10_000)
    expect(stateReads).toBeGreaterThan(1)
    expect(statsExecutions()).not.toContain(null)
    expect(statsExecutions()).toContain(RUN)
  })
})

describe("Table statistics — U-TS-STALE: a running pipeline's grid keeps reading", () => {
  it("re-reads on a tick and shows the new count", async () => {
    render(<TableStatisticsPanel pipelineId="p1" executionId={RUN} mode="batch" pipelineStatus="running" />)
    await tick(0)
    expect(statsCalls()).toHaveLength(1)
    expect(screen.getAllByText("680,000").length).toBeGreaterThan(0)

    read = 1_060_000
    await tick(TABLE_STATS_POLL_MS)
    expect(statsCalls()).toHaveLength(2)
    expect(statsExecutions()).toEqual([RUN, RUN])
    expect(screen.getAllByText("1,060,000").length).toBeGreaterThan(0)
  })

  it("reads once more when the run finishes, so the grid shows its last tables", async () => {
    // prod P2 337bf717: polling stopped at 2 tables / 1,135,253 while the run
    // finished with 5 / 1,183,074; only a manual Refresh showed the end.
    const { rerender } = render(
      <TableStatisticsPanel pipelineId="p1" executionId={RUN} mode="batch" pipelineStatus="running" />,
    )
    await tick(0)
    expect(statsCalls()).toHaveLength(1)

    read = 1_183_074
    rerender(<TableStatisticsPanel pipelineId="p1" executionId={RUN} mode="batch" pipelineStatus="completed" />)
    await tick(0)
    expect(statsCalls()).toHaveLength(2)
    expect(screen.getAllByText("1,183,074").length).toBeGreaterThan(0)

    await tick(TABLE_STATS_POLL_MS * 3)
    expect(statsCalls()).toHaveLength(2)
  })

  it("control: a finished run does not poll", async () => {
    render(<TableStatisticsPanel pipelineId="p1" executionId={RUN} mode="batch" pipelineStatus="completed" />)
    await tick(0)
    await tick(TABLE_STATS_POLL_MS * 5)
    expect(statsCalls()).toHaveLength(1)
  })

  it("control: a hidden tab skips its ticks and reads on return", async () => {
    render(<TableStatisticsPanel pipelineId="p1" executionId={RUN} mode="batch" pipelineStatus="running" />)
    await tick(0)
    hidden = true
    await tick(TABLE_STATS_POLL_MS * 3)
    expect(statsCalls()).toHaveLength(1)
    hidden = false
    await act(async () => {
      document.dispatchEvent(new Event("visibilitychange"))
    })
    await tick(0)
    expect(statsCalls()).toHaveLength(2)
  })
})

describe("Table statistics — a degraded API never blanks or hammers", () => {
  it("a failed background poll keeps the last good rows", async () => {
    render(<TableStatisticsPanel pipelineId="p1" executionId={RUN} mode="batch" pipelineStatus="running" />)
    await tick(0)
    expect(screen.getAllByText("680,000").length).toBeGreaterThan(0)

    authFetch.mockImplementation(async () => ({ ok: false, status: 502, json: async () => ({}), text: async () => "" }))
    await tick(TABLE_STATS_POLL_MS)
    expect(statsCalls()).toHaveLength(2)
    expect(screen.queryByText(/Failed to load table stats/)).toBeNull()
    expect(screen.getAllByText("680,000").length).toBeGreaterThan(0)
  })

  it("control: a failed foreground read still shows its error", async () => {
    authFetch.mockImplementation(async () => ({ ok: false, status: 502, json: async () => ({}), text: async () => "" }))
    render(<TableStatisticsPanel pipelineId="p1" executionId={RUN} mode="batch" pipelineStatus="completed" />)
    await tick(0)
    expect(screen.getByText(/Failed to load table stats \(502\)/)).toBeInTheDocument()
  })

  it("while /state keeps failing the grid says it is waiting, and the retry backs off", async () => {
    let stateReads = 0
    authFetch.mockImplementation(async (url: string) => {
      const u = String(url)
      if (u.endsWith("/state")) {
        stateReads += 1
        return { ok: false, status: 502, json: async () => ({}), text: async () => "" }
      }
      if (u.includes("/events")) return ok({ events: [] })
      return ok({})
    })
    render(<PipelineMonitoringPanel pipelineId="p1" variant="table_stats" />)
    await tick(0)
    expect(screen.getByText(/Waiting for the pipeline's run/)).toBeInTheDocument()
    // 5 + 10 + 20 + 40 + 60 + 60 s: six retries in 195 s, where a fixed 5 s retry made 39.
    await tick(195_000)
    expect(stateReads).toBe(7)
    expect(statsCalls()).toHaveLength(0)
  })
})
