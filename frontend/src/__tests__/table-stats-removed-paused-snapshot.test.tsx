/**
 * The table-statistics card on a CDC pipeline after the CDC bug sweep.
 *
 *  #19  A table taken out of the selection keeps its stats row (`status:
 *       "removed"`): its rows are still at the destination and its counts are
 *       still in the totals. It must read "Removed", never "Running", and the
 *       summary says how many there are (`tables_removed`).
 *  #14  A paused pipeline's tables are not moving: the row badge and the summary
 *       tile read "Paused", not "Running".
 *  #7/#11  Snapshot rows (`snapshot_rows` / `applied_snapshot_rows`) get their
 *       own columns next to I/U/D, and a legend says what each counter measures
 *       — none of them is the destination's current row count.
 *  #15  The card re-reads when this pipeline's refresh event fires.
 *  A table added without loading its existing rows (`load_mode:
 *       "streaming_only"`) is marked "Changes only": the destination holds only
 *       what changed since. This replaced the Tables card's "Expected rows" note.
 *
 * Every "shows X" test has the control that does not.
 */

import { beforeEach, describe, expect, it, vi } from "vitest"
import { act, render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

import { TableStatisticsPanel } from "@/components/pipeline/TableStatisticsPanel"
import { emitPipelineRefresh } from "@/lib/events/pipelineRefresh"

function ok(body: unknown) {
  return { ok: true, status: 200, json: async () => body } as unknown as Response
}

const cdcRow = (name: string, extra: Record<string, unknown> = {}) => ({
  qualified_name: `public.${name}`,
  table_name: name,
  schema_name: "public",
  mode: "cdc",
  status: "running",
  inserts: 4,
  updates: 2,
  deletes: 1,
  total_events: 7,
  applied_inserts: 4,
  applied_updates: 2,
  applied_deletes: 1,
  applied_total_events: 7,
  updated_at: "2026-09-24T10:00:00Z",
  ...extra,
})

function serve(body: { summary: Record<string, unknown>; tables: unknown[] }) {
  authFetch.mockResolvedValue(ok({ ...body, total: body.tables.length }))
}

const headings = () =>
  Array.from(document.querySelectorAll("table thead th")).map((h) => (h.textContent || "").trim())

/** A body cell by its column heading (row text concatenates with no separator). */
function cellUnder(heading: string, rowIndex = 0): string {
  const col = headings().indexOf(heading)
  if (col < 0) throw new Error(`no "${heading}" column; headings are ${JSON.stringify(headings())}`)
  const row = document.querySelectorAll("table tbody tr")[rowIndex]
  return (row.querySelectorAll("td")[col]?.textContent || "").trim()
}

/** The value of a summary tile, found by its label. */
function tile(label: string): string | null {
  const el = screen.queryAllByText(label).find((e) => e.nextElementSibling?.className.includes("text-lg"))
  return el ? (el.nextElementSibling?.textContent || "").trim() : null
}

const statusCell = (rowIndex: number) => cellUnder("Status", rowIndex)

beforeEach(() => {
  vi.clearAllMocks()
})

describe("#19 removed tables", () => {
  it("renders a removed row as Removed, not Running, and counts it in the summary", async () => {
    serve({
      summary: { mode: "cdc", total_tables: 1, tables_running: 1, tables_completed: 0, tables_failed: 0, tables_removed: 1 },
      tables: [cdcRow("orders"), cdcRow("old_orders", { status: "removed" })],
    })
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await waitFor(() => expect(screen.getByText("old_orders")).toBeInTheDocument())

    expect(statusCell(0)).toBe("Running")
    expect(statusCell(1)).toBe("Removed")
    // Its counts are still drawn: the data is still at the destination.
    expect(cellUnder("Captured I", 1)).toBe("4")
    expect(tile("Removed")).toBe("1")
  })

  it("control: no removed table, no Removed tile", async () => {
    serve({
      summary: { mode: "cdc", total_tables: 1, tables_running: 1, tables_completed: 0, tables_failed: 0 },
      tables: [cdcRow("orders")],
    })
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await waitFor(() => expect(screen.getByText("orders")).toBeInTheDocument())

    expect(tile("Removed")).toBeNull()
    expect(screen.queryByText("Removed")).not.toBeInTheDocument()
  })
})

describe("Changes only", () => {
  it("marks a table added without loading its existing rows, and says why", async () => {
    serve({
      summary: { mode: "cdc", total_tables: 2, tables_running: 2, tables_completed: 0, tables_failed: 0 },
      tables: [cdcRow("orders"), cdcRow("items", { load_mode: "streaming_only" })],
    })
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await waitFor(() => expect(screen.getByText("items")).toBeInTheDocument())

    // Control: row 0's rows were loaded (no load_mode), so it carries no mark.
    expect(statusCell(0)).toBe("Running")
    expect(statusCell(1)).toBe("RunningChanges only")
    expect(screen.getByText("Changes only")).toHaveAttribute(
      "title",
      "Added without loading its existing rows: the destination holds only the changes made since it was added."
    )
  })

  it("control: a removed streaming-only table reads Removed alone", async () => {
    serve({
      summary: { mode: "cdc", total_tables: 1, tables_running: 0, tables_completed: 0, tables_failed: 0, tables_removed: 1 },
      tables: [cdcRow("items", { status: "removed", load_mode: "streaming_only" })],
    })
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await waitFor(() => expect(screen.getByText("items")).toBeInTheDocument())
    expect(statusCell(0)).toBe("Removed")
    expect(screen.queryByText("Changes only")).not.toBeInTheDocument()
  })
})

describe("#14 paused pipeline", () => {
  const body = {
    summary: { mode: "cdc", total_tables: 2, tables_running: 2, tables_completed: 0, tables_failed: 0 },
    tables: [cdcRow("orders"), cdcRow("items")],
  }

  it("reads Paused on the rows and in the summary while the pipeline is paused", async () => {
    serve(body)
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" pipelineStatus="paused" />)
    await waitFor(() => expect(screen.getByText("orders")).toBeInTheDocument())

    expect(statusCell(0)).toBe("Paused")
    expect(statusCell(1)).toBe("Paused")
    expect(tile("Paused")).toBe("2")
    expect(tile("Running")).toBeNull()
    expect(screen.queryByText("Running")).not.toBeInTheDocument()
  })

  it("a batch run paused by the user reads Paused too", async () => {
    serve(body)
    render(
      <TableStatisticsPanel pipelineId="p1" mode="cdc" pipelineStatus="waiting_for_user" blockingReasonType="paused_by_user" />
    )
    await waitFor(() => expect(screen.getByText("orders")).toBeInTheDocument())
    expect(statusCell(0)).toBe("Paused")
  })

  it("control: a running pipeline reads Running", async () => {
    serve(body)
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" pipelineStatus="running" />)
    await waitFor(() => expect(screen.getByText("orders")).toBeInTheDocument())

    expect(statusCell(0)).toBe("Running")
    expect(tile("Running")).toBe("2")
    expect(tile("Paused")).toBeNull()
  })

  it("a removed row stays Removed while paused", async () => {
    serve({ ...body, tables: [cdcRow("orders"), cdcRow("old", { status: "removed" })] })
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" pipelineStatus="paused" />)
    await waitFor(() => expect(screen.getByText("old")).toBeInTheDocument())
    expect(statusCell(1)).toBe("Removed")
  })
})

describe("#7/#11 snapshot rows and what the counters mean", () => {
  it("draws snapshot columns next to I/U/D when the backend reports them, with every row as wide as the header", async () => {
    serve({
      summary: {
        mode: "cdc",
        total_tables: 1,
        tables_running: 1,
        tables_completed: 0,
        tables_failed: 0,
        total_snapshot_rows: 1500,
        total_applied_snapshot_rows: 1490,
      },
      tables: [cdcRow("orders", { snapshot_rows: 1500, applied_snapshot_rows: 1490 })],
    })
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await waitFor(() => expect(screen.getByText("orders")).toBeInTheDocument())

    const h = headings()
    expect(h.indexOf("Captured Snapshot")).toBeGreaterThan(-1)
    expect(h.indexOf("Captured Snapshot")).toBeLessThan(h.indexOf("Captured I"))
    expect(h.indexOf("Applied Snapshot")).toBeLessThan(h.indexOf("Applied I"))
    expect(cellUnder("Captured Snapshot")).toBe("1,500")
    expect(cellUnder("Applied Snapshot")).toBe("1,490")
    expect(tile("Captured Snapshot Rows")).toBe("1,500")
    expect(tile("Applied Snapshot Rows")).toBe("1,490")

    const bodyCells = Array.from(document.querySelectorAll("table tbody tr")).map((r) => r.querySelectorAll("td").length)
    for (const n of bodyCells) expect(n).toBe(h.length)
  })

  it("control: an older backend without snapshot counts gets no snapshot columns", async () => {
    serve({
      summary: { mode: "cdc", total_tables: 1, tables_running: 1, tables_completed: 0, tables_failed: 0 },
      tables: [cdcRow("orders")],
    })
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await waitFor(() => expect(screen.getByText("orders")).toBeInTheDocument())

    expect(headings()).not.toContain("Captured Snapshot")
    expect(headings()).not.toContain("Applied Snapshot")
  })

  it("explains the counters on a CDC card, and says none is the destination's row count", async () => {
    serve({
      summary: { mode: "cdc", total_tables: 1, tables_running: 1, tables_completed: 0, tables_failed: 0 },
      tables: [cdcRow("orders")],
    })
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)

    const legend = await screen.findByTestId("cdc-counter-legend")
    expect(legend).toHaveTextContent(/Captured = change events Debezium wrote to Kafka/)
    expect(legend).toHaveTextContent(/Applied = events the sink wrote to the destination/)
    expect(legend).toHaveTextContent(/a re-snapshot reads every row again/)
    expect(legend).toHaveTextContent(/None of these is the destination's current row count/)
  })

  it("control: a batch card has no CDC legend", async () => {
    serve({
      summary: { mode: "batch", total_tables: 1, tables_running: 0, tables_completed: 1, tables_failed: 0 },
      tables: [{ qualified_name: "public.orders", table_name: "orders", mode: "batch", status: "completed", read_rows: 1, inserted_rows: 1 }],
    })
    render(<TableStatisticsPanel pipelineId="p1" mode="batch" />)
    await waitFor(() => expect(screen.getByText("orders")).toBeInTheDocument())
    expect(screen.queryByTestId("cdc-counter-legend")).not.toBeInTheDocument()
  })
})

describe("#15 refresh bus", () => {
  it("re-reads on this pipeline's refresh event, and not on another pipeline's", async () => {
    serve({
      summary: { mode: "cdc", total_tables: 1, tables_running: 1, tables_completed: 0, tables_failed: 0 },
      tables: [cdcRow("orders")],
    })
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await waitFor(() => expect(screen.getByText("orders")).toBeInTheDocument())
    const reads = () => authFetch.mock.calls.filter(([u]) => String(u).includes("/pipelines/p1/table-stats")).length
    const before = reads()
    expect(before).toBeGreaterThan(0)

    act(() => emitPipelineRefresh("p2"))
    expect(reads()).toBe(before)

    act(() => emitPipelineRefresh("p1"))
    await waitFor(() => expect(reads()).toBe(before + 1))
  })
})
