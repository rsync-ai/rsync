/**
 * Table statistics with many tables: pages, sorting and per-viewer column
 * choices. Before this, the grid sat inside a fixed 500px scroll box together
 * with the Summary and the Tables card, so past ~10 tables the rows — and the
 * horizontal scrollbar — were below the fold of a box inside the page, and the
 * only pager was a Previous/Next pair that appeared after 50 tables.
 *
 * Everything asserted here is the REQUEST the panel sends (sort/limit/offset
 * are server-side in `table_stats.go`) or the DOM it renders — never a mocked
 * internal.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor, fireEvent } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

import { TableStatisticsPanel, pageWindow } from "@/components/pipeline/TableStatisticsPanel"

const PREFS_KEY = "rsync.tableStats.prefs.v1"

function ok(body: unknown) {
  return { ok: true, status: 200, json: async () => body } as unknown as Response
}

const ALL_ROWS = Array.from({ length: 60 }, (_, i) => {
  const n = String(i + 1).padStart(2, "0")
  return {
    qualified_name: `public.t${n}`,
    table_name: `t${n}`,
    schema_name: "public",
    mode: "cdc",
    status: "running",
    inserts: i,
    updates: 0,
    deletes: 0,
    total_events: i,
    applied_inserts: i,
    applied_updates: 0,
    applied_deletes: 0,
    applied_total_events: i,
    dlq_rows: 0,
    started_at: "2026-09-24T10:00:00Z",
    updated_at: "2026-09-24T10:00:00Z",
  }
})

/** Serves the table-stats API the way the server does: filter, then page. */
function serve(rows = ALL_ROWS) {
  authFetch.mockImplementation(async (url: string) => {
    const p = new URL(url, "http://localhost").searchParams
    const q = p.get("q") || ""
    const matched = rows.filter((r) => r.qualified_name.includes(q))
    const offset = Number(p.get("offset") || 0)
    const limit = Number(p.get("limit") || 50)
    return ok({
      summary: {
        mode: "cdc",
        total_tables: rows.length,
        tables_completed: 0,
        tables_failed: 0,
        tables_running: rows.length,
      },
      tables: matched.slice(offset, offset + limit),
      total: matched.length,
    })
  })
}

/** The query string of every table-stats request made so far. */
function requests() {
  return authFetch.mock.calls.map(([url]) => new URL(String(url), "http://localhost").searchParams)
}
const lastRequest = () => requests()[requests().length - 1]

function headings() {
  return Array.from(document.querySelectorAll("table thead th")).map((h) => (h.textContent || "").trim())
}

beforeEach(() => {
  vi.clearAllMocks()
  localStorage.clear()
})

describe("Table statistics — pages", () => {
  it("asks for 25 tables a page by default and numbers the pages", async () => {
    serve()
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await screen.findByText("t01")

    expect(requests()).toHaveLength(1)
    expect(lastRequest().get("limit")).toBe("25")
    expect(lastRequest().get("offset")).toBe("0")
    expect(screen.getByText("1–25 of 60")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Page 1" })).toHaveAttribute("aria-current", "page")
    expect(screen.getByRole("button", { name: "Page 3" })).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Previous page" })).toBeDisabled()
  })

  it("Next page asks the server for the next 25, not the next 50", async () => {
    serve()
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await screen.findByText("t01")

    fireEvent.click(screen.getByRole("button", { name: "Next page" }))

    await screen.findByText("t26")
    expect(lastRequest().get("offset")).toBe("25")
    expect(lastRequest().get("limit")).toBe("25")
    expect(screen.queryByText("t01")).toBeNull()
    expect(screen.getByText("26–50 of 60")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Page 2" })).toHaveAttribute("aria-current", "page")
  })

  it("a stored page size is used for the FIRST request, not after a refetch", async () => {
    localStorage.setItem(PREFS_KEY, JSON.stringify({ pageSize: 10, hidden: [] }))
    serve()
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await screen.findByText("t10")

    expect(requests().map((r) => r.get("limit"))).toEqual(["10"])
    expect(screen.queryByText("t11")).toBeNull()
    expect(screen.getByRole("button", { name: "Page 6" })).toBeInTheDocument()
  })

  it("control: a stored value the panel does not offer falls back to the default", async () => {
    localStorage.setItem(PREFS_KEY, JSON.stringify({ pageSize: 7, hidden: ["bogus"] }))
    serve()
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await screen.findByText("t01")

    expect(lastRequest().get("limit")).toBe("25")
    expect(headings()).toContain("Captured I")
  })

  it("a refresh that finds fewer tables moves off the now-empty page", async () => {
    serve()
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await screen.findByText("t01")
    fireEvent.click(screen.getByRole("button", { name: "Page 3" }))
    await screen.findByText("t51")

    serve(ALL_ROWS.slice(0, 30)) // 30 tables left: page 3 (offset 50) no longer exists
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }))

    await screen.findByText("t26")
    expect(lastRequest().get("offset")).toBe("25")
    expect(screen.getByText("26–30 of 30")).toBeInTheDocument()
    expect(screen.queryByText(/No tables match/)).toBeNull()
  })

  it("draws no pager when everything fits on one page", async () => {
    serve(ALL_ROWS.slice(0, 5))
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await screen.findByText("t05")

    expect(screen.queryByRole("navigation", { name: "Table statistics pages" })).toBeNull()
    expect(screen.getByText("1–5 of 5")).toBeInTheDocument()
  })
})

describe("Table statistics — sorting", () => {
  it("clicking Captured I sorts server-side by inserts and returns to page 1", async () => {
    serve()
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await screen.findByText("t01")
    const th = (name: string) => screen.getByRole("button", { name }).closest("th")!

    expect(lastRequest().get("sort")).toBe("qualified_name")
    expect(th("Table")).toHaveAttribute("aria-sort", "ascending")

    fireEvent.click(screen.getByRole("button", { name: "Next page" }))
    await screen.findByText("t26")
    fireEvent.click(screen.getByRole("button", { name: "Captured I" }))

    await waitFor(() => expect(lastRequest().get("sort")).toBe("inserts"))
    expect(lastRequest().get("offset")).toBe("0")
    expect(th("Captured I")).toHaveAttribute("aria-sort", "descending")
    expect(th("Table")).toHaveAttribute("aria-sort", "none")
  })

  it("offers a sort only where the API implements one", async () => {
    serve()
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await screen.findByText("t01")

    for (const name of ["Table", "Status", "Captured I", "Updated"]) {
      expect(screen.getByRole("button", { name })).toBeInTheDocument()
    }
    // table_stats.go has no sort key for these; a button would be a lie.
    for (const name of ["Captured U", "Applied I", "Dropped", "Start"]) {
      expect(screen.queryByRole("button", { name })).toBeNull()
    }
  })
})

describe("Table statistics — columns and search", () => {
  it("hidden column groups leave every row as wide as the header", async () => {
    localStorage.setItem(PREFS_KEY, JSON.stringify({ pageSize: 25, hidden: ["captured", "timing"] }))
    serve()
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await screen.findByText("t01")

    const heads = headings()
    expect(heads).not.toContain("Captured I")
    expect(heads).not.toContain("Start")
    expect(heads).toContain("Applied I")
    // Never hideable: the one counter that reports what did NOT land.
    expect(heads).toContain("Dropped")

    const table = document.querySelector("table")!
    const width = table.querySelectorAll("thead th").length
    for (const row of Array.from(table.querySelectorAll("tbody tr"))) {
      expect(row.querySelectorAll("td")).toHaveLength(width)
    }
  })

  it("keeps the full name on the frozen Table cell", async () => {
    serve()
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    const name = await screen.findByText("t01")

    expect(name.closest("td")).toHaveAttribute("title", "public.t01")
    expect(headings().slice(0, 2)).toEqual(["Schema", "Table"])
  })

  it("a search that matches nothing keeps the search box, so it can be cleared", async () => {
    serve()
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)
    await screen.findByText("t01")

    fireEvent.change(screen.getByRole("textbox", { name: "Search tables" }), { target: { value: "nope" } })

    expect(await screen.findByText(/No tables match/)).toBeInTheDocument()
    expect(screen.getByRole("textbox", { name: "Search tables" })).toHaveValue("nope")
    expect(screen.queryByText(/No CDC activity captured yet/)).toBeNull()
    expect(lastRequest().get("q")).toBe("nope")
  })

  it("control: a pipeline with no tables at all still shows the empty state", async () => {
    serve([])
    render(<TableStatisticsPanel pipelineId="p1" mode="cdc" />)

    expect(await screen.findByText(/No CDC activity captured yet/)).toBeInTheDocument()
    expect(screen.queryByRole("textbox", { name: "Search tables" })).toBeNull()
  })
})

describe("pageWindow", () => {
  it("shows every page up to seven", () => {
    expect(pageWindow(1, 3)).toEqual([1, 2, 3])
    expect(pageWindow(4, 7)).toEqual([1, 2, 3, 4, 5, 6, 7])
  })

  it("keeps first, last and the current page's neighbours, eliding the rest", () => {
    expect(pageWindow(5, 10)).toEqual([1, "gap", 4, 5, 6, "gap", 10])
    expect(pageWindow(1, 10)).toEqual([1, 2, "gap", 10])
    expect(pageWindow(10, 10)).toEqual([1, "gap", 9, 10])
  })

  it("draws a one-page gap as that page, never as an ellipsis", () => {
    expect(pageWindow(4, 10)).toEqual([1, 2, 3, 4, 5, "gap", 10])
  })
})
