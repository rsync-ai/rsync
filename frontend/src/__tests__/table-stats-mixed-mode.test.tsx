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
