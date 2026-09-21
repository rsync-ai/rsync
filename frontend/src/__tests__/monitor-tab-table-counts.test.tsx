/**
 * Monitor tab, Throughput card: the table counts must add up to the total.
 *
 * api-gateway now reports a selected CDC table that has sent nothing yet as
 * `waiting_for_data` and counts it in `tables_waiting_for_data` — no longer in
 * `tables_running` (table_stats.go computeCDCSummary). The card's footer listed
 * completed / failed / running only, so a pipeline that had just been set up read
 * "Tables completed 1 / 5" plus "Tables running 1": three tables missing from the
 * screen. Degraded tables were missing the same way.
 */

import { beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, waitFor, within } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))
vi.mock("@/components/pipeline/DiagnosePanel", () => ({ DiagnosePanel: () => null }))

import { MonitorTab } from "@/components/pipeline/MonitorTab"

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

const RUNTIME_CDC = {
  pipeline_id: "p1",
  execution_id: "e1",
  mode: "cdc",
  phase: "waiting_for_data",
  health: "healthy",
  dependencies: [],
  updated_at: "2026-09-16T10:00:00Z",
}

// Every field computeCDCSummary always sends (no omitempty on the counts).
function cdcSummary(counts: {
  completed: number
  failed: number
  running: number
  degraded: number
  waiting: number
}) {
  const total = counts.completed + counts.failed + counts.running + counts.degraded + counts.waiting
  return {
    summary: {
      mode: "cdc",
      total_tables: total,
      tables_completed: counts.completed,
      tables_failed: counts.failed,
      tables_running: counts.running,
      tables_degraded: counts.degraded,
      tables_waiting_for_data: counts.waiting,
      total_inserts: 3,
      total_applied_inserts: 3,
      total_dlq_rows: 0,
      tables_with_dlq: 0,
    },
    tables: [],
    total,
  }
}

function serve(stats: unknown) {
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/runtime")) return res(200, RUNTIME_CDC)
    if (u.includes("/table-stats")) return res(200, stats)
    return res(200, { events: [] })
  })
}

type FooterRow = { label: string; value: number }

/**
 * Every row of the Throughput card's table-count footer, in screen order.
 * An array, not a map: a row printed twice must count twice, or a duplicate
 * line would pass the "adds up" check while the screen shows more than the total.
 */
async function footerRows(): Promise<FooterRow[]> {
  const completed = await screen.findByText("Tables completed")
  const footer = completed.closest("div.border-t") as HTMLElement
  expect(footer).not.toBeNull()
  const out: FooterRow[] = []
  for (const row of Array.from(footer.children)) {
    const [label, value] = Array.from(row.children).map((c) => (c.textContent || "").trim())
    out.push({ label, value: Number(value.split("/")[0].replace(/,/g, "").trim()) })
  }
  expect(out.length).toBeGreaterThan(0)
  return out
}

function expectReconciles(rows: FooterRow[], total: number) {
  expect(total).toBeGreaterThan(0)
  const labels = rows.map((r) => r.label)
  expect(new Set(labels).size).toBe(labels.length) // no status printed twice
  expect(rows.reduce((sum, r) => sum + r.value, 0)).toBe(total)
}

const valueOf = (rows: FooterRow[], label: string) => rows.find((r) => r.label === label)?.value

beforeEach(() => {
  vi.clearAllMocks()
})

describe("Monitor tab — table counts reconcile with the total", () => {
  it("shows tables with no data yet, and every row adds up to the total", async () => {
    serve(cdcSummary({ completed: 1, failed: 0, running: 1, degraded: 1, waiting: 2 }))
    render(<MonitorTab pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText("Tables with no data yet")).toBeInTheDocument())
    const rows = await footerRows()
    const totalText = within(screen.getByText("Tables completed").parentElement as HTMLElement).getByText("1 / 5")
    expect(totalText).toBeInTheDocument()

    expect(rows.map((r) => r.label)).toEqual([
      "Tables completed",
      "Tables degraded",
      "Tables running",
      "Tables with no data yet",
    ]) // zero rows (failed) stay hidden
    expect(valueOf(rows, "Tables with no data yet")).toBe(2)
    expect(valueOf(rows, "Tables degraded")).toBe(1)
    expect(valueOf(rows, "Tables running")).toBe(1)
    expectReconciles(rows, 5)
  })

  it("control: with nothing waiting or degraded there is no extra row and it still adds up", async () => {
    serve(cdcSummary({ completed: 2, failed: 1, running: 1, degraded: 0, waiting: 0 }))
    render(<MonitorTab pipelineId="p1" />)

    const rows = await footerRows()
    expect(rows.map((r) => r.label)).toEqual(["Tables completed", "Tables failed", "Tables running"])
    expectReconciles(rows, 4)
    expect(screen.queryByText("Tables with no data yet")).toBeNull()
  })

  it("shows tables a gateway did not put in a named count as 'in another state'", async () => {
    // A gateway older than tables_waiting_for_data / tables_degraded still sends the total.
    serve({
      summary: { mode: "cdc", total_tables: 4, tables_completed: 1, tables_failed: 0, tables_running: 1 },
      tables: [],
      total: 4,
    })
    render(<MonitorTab pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText("Tables in another state")).toBeInTheDocument())
    const rows = await footerRows()
    expect(rows.map((r) => r.label)).toEqual(["Tables completed", "Tables running", "Tables in another state"])
    expect(valueOf(rows, "Tables in another state")).toBe(2)
    expectReconciles(rows, 4)
  })
})
