/**
 * Regression tests for the table-statistics table — F-281 and F-285.
 *
 *  F-281  MIXED MODE SHIFTS EVERY ROW UNDER THE WRONG HEADING. The header cells
 *         gate on `isBatch` / `isCDC` (`TableStatisticsPanel.tsx:268-269`), and
 *         BOTH are true when `resolvedMode === "mixed"` — so the header row is
 *         batch columns + CDC columns. The body cells gate on the row's own
 *         mode instead (`:585` `table.mode === "batch"`, `:595`
 *         `table.mode === "cdc"`), so a batch row emits the 2 batch cells and
 *         none of the 10 CDC ones. Its "Dropped" and "Updated" values then land
 *         under "Captured I" and "Captured U". A CDC row shifts the other way.
 *         The numbers on screen are real; the column they sit in is a lie.
 *
 *         `"mixed"` is genuinely produced — `table_stats.go:872` sets it
 *         whenever a pipeline has both batch and CDC tables — so this is not a
 *         theoretical branch.
 *
 *         The invariant a table cannot violate: every body row has exactly as
 *         many cells as the header has columns. That is what these tests
 *         assert, because it is the property that makes a column mean anything.
 *
 *  F-285  "NO DATA" AND "ZERO ROWS" RENDER IDENTICALLY. `formatNumber` opens
 *         with `if (!num) return "0"` (`:103`), which catches `undefined` and
 *         `null` alongside a real 0. An operator reading "0 written" cannot
 *         tell a destination that rejected everything from a column the
 *         backend never populated — and those call for opposite responses.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

import { TableStatisticsPanel } from "@/components/pipeline/TableStatisticsPanel"

function ok(body: unknown) {
  return {
    ok: true,
    status: 200,
    json: async () => body,
  } as unknown as Response
}

const BATCH_ROW = {
  qualified_name: "public.orders",
  table_name: "orders",
  schema_name: "public",
  mode: "batch",
  status: "completed",
  read_rows: 1200,
  inserted_rows: 1200,
  dlq_rows: 3,
  updated_at: "2026-08-05T10:00:00Z",
}

const CDC_ROW = {
  qualified_name: "public.events",
  table_name: "events",
  schema_name: "public",
  mode: "cdc",
  status: "running",
  inserts: 40,
  updates: 5,
  deletes: 1,
  total_events: 46,
  applied_inserts: 40,
  applied_updates: 5,
  applied_deletes: 1,
  applied_total_events: 46,
  dlq_rows: 0,
  updated_at: "2026-08-05T10:00:00Z",
}

/** Column count of the single header row, and of each body row. */
function columnCounts() {
  const table = document.querySelector("table")!
  const headerCells = table.querySelectorAll("thead th").length
  const bodyRows = Array.from(table.querySelectorAll("tbody tr"))
  return {
    headerCells,
    bodyRowCells: bodyRows.map((r) => r.querySelectorAll("td").length),
  }
}

/**
 * Read one body cell BY ITS COLUMN HEADING. Asserting on `row.textContent`
 * would be worthless here: the cells concatenate with no separator, so
 * "0 read, 0 written, 10:00:00" arrives as "00010:00:00" and a `/0/` match
 * proves nothing about which column it came from. Positional lookup is also
 * what makes these assertions sensitive to F-281 rather than blind to it.
 */
function cellUnder(heading: string, rowIndex = 0): string {
  const table = document.querySelector("table")!
  const heads = Array.from(table.querySelectorAll("thead th")).map((h) =>
    (h.textContent || "").trim(),
  )
  const col = heads.indexOf(heading)
  if (col < 0) throw new Error(`no "${heading}" column; headings are ${JSON.stringify(heads)}`)
  const row = table.querySelectorAll("tbody tr")[rowIndex]
  return (row.querySelectorAll("td")[col]?.textContent || "").trim()
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe("F-281 — mixed mode keeps every row under its own headings", () => {
  it("gives a batch row and a CDC row the same cell count as the header", async () => {
    authFetch.mockResolvedValue(
      ok({
        summary: { mode: "mixed", total_tables: 2 },
        tables: [BATCH_ROW, CDC_ROW],
        total: 2,
      }),
    )

    render(<TableStatisticsPanel pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(/orders/)).toBeInTheDocument())

    const { headerCells, bodyRowCells } = columnCounts()
    expect(headerCells).toBeGreaterThan(0)
    for (const n of bodyRowCells) {
      expect(n).toBe(headerCells)
    }
  })

  // THE BOUND, twice over: a single-mode pipeline must not grow the other
  // mode's columns just because mixed mode now pads.
  it("leaves a pure batch table at its own width", async () => {
    authFetch.mockResolvedValue(
      ok({ summary: { mode: "batch", total_tables: 1 }, tables: [BATCH_ROW], total: 1 }),
    )

    render(<TableStatisticsPanel pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(/orders/)).toBeInTheDocument())

    expect(screen.queryByText(/captured i/i)).toBeNull()
    const { headerCells, bodyRowCells } = columnCounts()
    expect(bodyRowCells[0]).toBe(headerCells)
  })

  it("leaves a pure CDC table at its own width", async () => {
    authFetch.mockResolvedValue(
      ok({ summary: { mode: "cdc", total_tables: 1 }, tables: [CDC_ROW], total: 1 }),
    )

    render(<TableStatisticsPanel pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(/events/)).toBeInTheDocument())

    expect(screen.queryByText(/^Read$/)).toBeNull()
    const { headerCells, bodyRowCells } = columnCounts()
    expect(bodyRowCells[0]).toBe(headerCells)
  })
})

describe("F-285 — an unpopulated metric is not reported as a measured zero", () => {
  it("does not print 0 for a column the backend never sent", async () => {
    authFetch.mockResolvedValue(
      ok({
        summary: { mode: "batch", total_tables: 1 },
        tables: [
          {
            qualified_name: "public.orders",
            table_name: "orders",
            schema_name: "public",
            mode: "batch",
            status: "running",
            // read_rows and inserted_rows deliberately absent: this run has not
            // reported them yet.
            updated_at: "2026-08-05T10:00:00Z",
          },
        ],
        total: 1,
      }),
    )

    render(<TableStatisticsPanel pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(/orders/)).toBeInTheDocument())

    expect(cellUnder("Read")).toBe("—")
    expect(cellUnder("Written")).toBe("—")
  })

  // THE BOUND. A real zero is a measurement and must survive as "0".
  it("still prints 0 when the backend reports zero", async () => {
    authFetch.mockResolvedValue(
      ok({
        summary: { mode: "batch", total_tables: 1 },
        tables: [
          {
            qualified_name: "public.orders",
            table_name: "orders",
            schema_name: "public",
            mode: "batch",
            status: "completed",
            read_rows: 0,
            inserted_rows: 0,
            updated_at: "2026-08-05T10:00:00Z",
          },
        ],
        total: 1,
      }),
    )

    render(<TableStatisticsPanel pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(/orders/)).toBeInTheDocument())

    expect(cellUnder("Read")).toBe("0")
    expect(cellUnder("Written")).toBe("0")
  })
})

/**
 * Issue #21 — a selected CDC table with no stats row read "Running" with every
 * counter at 0. The API now marks it `waiting_for_data` and omits its counters;
 * the panel must say "No data yet" in the status badge and the count cells, and
 * must not print a measured 0 for any of them.
 */
const WAITING_ROW = {
  qualified_name: "public.audit",
  table_name: "audit",
  schema_name: "public",
  mode: "cdc",
  status: "waiting_for_data",
  // No counters: the backend never measured any. dlq_rows is always sent.
  dlq_rows: 0,
  updated_at: "2026-08-05T10:00:00Z",
}

const CDC_COUNT_HEADINGS = [
  "Captured I",
  "Captured U",
  "Captured D",
  "Captured Total",
  "Applied I",
  "Applied U",
  "Applied D",
  "Applied Total",
]

/** Value shown under a summary-card label (the grid of summary figures). */
function summaryValue(label: string): string {
  const grid = document.querySelector("div.grid")
  if (!grid) throw new Error("no summary grid")
  const labels = Array.from(grid.querySelectorAll(":scope > div > div:first-child")).filter(
    (el) => (el.textContent || "").trim() === label,
  )
  if (labels.length !== 1) throw new Error(`want one summary label "${label}", found ${labels.length}`)
  return (labels[0].nextElementSibling?.textContent || "").trim()
}

describe("#21 — a CDC table with nothing captured reads 'No data yet', not Running and 0", () => {
  it("labels the waiting row and its count cells 'No data yet', beside a live row that keeps its numbers", async () => {
    authFetch.mockResolvedValue(
      ok({
        summary: {
          mode: "cdc",
          total_tables: 2,
          tables_completed: 0,
          tables_failed: 0,
          tables_running: 1,
          tables_waiting_for_data: 1,
          total_inserts: 40,
          total_updates: 5,
          total_deletes: 1,
          total_applied_inserts: 40,
          total_applied_updates: 5,
          total_applied_deletes: 1,
        },
        tables: [CDC_ROW, WAITING_ROW],
        total: 2,
      }),
    )

    render(<TableStatisticsPanel pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(/audit/)).toBeInTheDocument())

    // The waiting row (index 1).
    expect(cellUnder("Status", 1)).toBe("No data yet")
    for (const heading of CDC_COUNT_HEADINGS) {
      expect(cellUnder(heading, 1)).toBe("No data yet")
    }
    expect(cellUnder("Dropped", 1)).toBe("No data yet")

    // Control: the live row (index 0) still reports what it measured.
    expect(cellUnder("Status", 0)).toBe("Running")
    expect(cellUnder("Captured I", 0)).toBe("40")
    expect(cellUnder("Applied Total", 0)).toBe("46")
    expect(cellUnder("Dropped", 0)).toBe("0")

    // Summary: the waiting table is counted as such, and the totals are real.
    expect(summaryValue("Running")).toBe("1")
    expect(summaryValue("No data yet")).toBe("1")
    expect(summaryValue("Captured Inserts")).toBe("40")

    // F-281 still holds with the new cells.
    const { headerCells, bodyRowCells } = columnCounts()
    for (const n of bodyRowCells) expect(n).toBe(headerCells)
  })

  it("does not show 0 for the CDC totals when no table has reported anything", async () => {
    authFetch.mockResolvedValue(
      ok({
        summary: {
          mode: "cdc",
          total_tables: 1,
          tables_completed: 0,
          tables_failed: 0,
          tables_running: 0,
          tables_waiting_for_data: 1,
          total_inserts: 0,
          total_updates: 0,
          total_deletes: 0,
          total_applied_inserts: 0,
          total_applied_updates: 0,
          total_applied_deletes: 0,
        },
        tables: [WAITING_ROW],
        total: 1,
      }),
    )

    render(<TableStatisticsPanel pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(/audit/)).toBeInTheDocument())

    for (const label of [
      "Captured Inserts",
      "Captured Updates",
      "Captured Deletes",
      "Applied Inserts",
      "Applied Updates",
      "Applied Deletes",
    ]) {
      expect(summaryValue(label)).toBe("No data yet")
    }
    expect(summaryValue("Running")).toBe("0")
    expect(cellUnder("Status")).toBe("No data yet")
  })

  // THE BOUND: "No data yet" is for a table that reported nothing, not for any
  // missing counter. A running row that omits one still shows the dash.
  it("keeps the dash for a missing counter on a table that is running", async () => {
    const { inserts: _omit, ...runningWithoutInserts } = CDC_ROW
    authFetch.mockResolvedValue(
      ok({
        summary: { mode: "cdc", total_tables: 1, tables_running: 1 },
        tables: [runningWithoutInserts],
        total: 1,
      }),
    )

    render(<TableStatisticsPanel pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(/events/)).toBeInTheDocument())

    expect(cellUnder("Captured I")).toBe("—")
    expect(cellUnder("Captured U")).toBe("5")
    expect(cellUnder("Status")).toBe("Running")
    expect(screen.queryByText("No data yet")).toBeNull()
  })
})
