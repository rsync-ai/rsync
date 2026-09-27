/**
 * A batch Resume with nothing new at the source finishes with every table at
 * 0 read / 0 written. The Table statistics tab showed only that run, so prod
 * pipeline 9a094389 — which had just moved 75,230 rows (run 05ec379f) — read as
 * "moved nothing" (run e0afbc01). The panel now shows the last run that moved
 * rows, says why, and lets the viewer switch back.
 *
 * Asserted: the REQUESTS the panel sends (which execution_id) and the DOM.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor, fireEvent } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

import { TableStatisticsPanel, batchRunMovedNothing } from "@/components/pipeline/TableStatisticsPanel"

const LATEST = "e0afbc01-0000-0000-0000-000000000000"
const RELOAD = "05ec379f-0000-0000-0000-000000000000"

function ok(body: unknown) {
  return { ok: true, status: 200, json: async () => body } as unknown as Response
}

function row(read: number) {
  return {
    qualified_name: "public.users",
    table_name: "users",
    schema_name: "public",
    mode: "batch",
    status: "completed",
    read_rows: read,
    inserted_rows: read,
    dlq_rows: 0,
    started_at: "2026-09-26T19:55:00Z",
    completed_at: "2026-09-26T19:55:05Z",
    updated_at: "2026-09-26T19:55:05Z",
  }
}

function summary(read: number) {
  return {
    mode: "batch",
    total_tables: 1,
    tables_completed: 1,
    tables_failed: 0,
    tables_running: 0,
    tables_degraded: 0,
    total_read_rows: read,
    total_inserted_rows: read,
    total_dlq_rows: 0,
  }
}

const LAST_DATA_RUN = {
  execution_id: RELOAD,
  tables: 1,
  rows_read: 75230,
  rows_written: 75230,
  finished_at: "2026-09-26T19:55:04Z",
}

/** Serves table-stats per execution: the latest run moved 0, the Reload 75,230. */
function serve({ lastDataRun = LAST_DATA_RUN as unknown }: { lastDataRun?: unknown } = {}) {
  authFetch.mockImplementation(async (url: string) => {
    const ex = new URL(url, "http://localhost").searchParams.get("execution_id")
    const read = ex === RELOAD ? 75230 : 0
    return ok({ summary: summary(read), tables: [row(read)], total: 1, last_data_run: lastDataRun })
  })
}

const requestedExecutions = () =>
  authFetch.mock.calls.map(([url]) => new URL(String(url), "http://localhost").searchParams.get("execution_id"))

beforeEach(() => {
  vi.clearAllMocks()
  localStorage.clear()
})

describe("Table statistics — a run that moved nothing", () => {
  it("shows the last run that moved rows, and says so", async () => {
    serve()
    render(<TableStatisticsPanel pipelineId="p1" executionId={LATEST} mode="batch" />)

    const banner = await screen.findByTestId("table-stats-run-banner")
    expect(banner).toHaveTextContent("The latest run (e0afbc01) found no new rows to copy")
    expect(banner).toHaveTextContent("Showing the last run that moved data: 05ec379f")
    expect(banner).toHaveTextContent("75,230 rows read, 75,230 written")
    await waitFor(() => expect(requestedExecutions().at(-1)).toBe(RELOAD))
    expect(requestedExecutions()[0]).toBe(LATEST)
  })

  it("switches back to the latest run on request, and forward again", async () => {
    serve()
    render(<TableStatisticsPanel pipelineId="p1" executionId={LATEST} mode="batch" />)
    fireEvent.click(await screen.findByRole("button", { name: "Show latest run" }))

    await waitFor(() => expect(requestedExecutions().at(-1)).toBe(LATEST))
    const banner = await screen.findByText(/This run found no new rows to copy/)
    expect(banner.closest("[data-testid=table-stats-run-banner]")).toHaveTextContent("05ec379f")

    fireEvent.click(screen.getByRole("button", { name: "Show that run" }))
    await waitFor(() => expect(requestedExecutions().at(-1)).toBe(RELOAD))
    expect(await screen.findByText(/Showing the last run that moved data/)).toBeInTheDocument()
  })

  it("stays on the latest run when no earlier run moved rows", async () => {
    serve({ lastDataRun: null })
    render(<TableStatisticsPanel pipelineId="p1" executionId={LATEST} mode="batch" />)
    await waitFor(() => expect(authFetch).toHaveBeenCalled())
    await screen.findByText("Summary")
    expect(screen.queryByTestId("table-stats-run-banner")).toBeNull()
    expect(new Set(requestedExecutions())).toEqual(new Set([LATEST]))
  })

  it("stays on a run that moved rows", async () => {
    serve()
    render(<TableStatisticsPanel pipelineId="p1" executionId={RELOAD} mode="batch" />)
    await screen.findByText("Summary")
    expect(screen.queryByTestId("table-stats-run-banner")).toBeNull()
    expect(new Set(requestedExecutions())).toEqual(new Set([RELOAD]))
  })
})

describe("batchRunMovedNothing", () => {
  it("is true only for a finished batch run at zero", () => {
    expect(batchRunMovedNothing(summary(0) as never)).toBe(true)
    expect(batchRunMovedNothing(summary(5) as never)).toBe(false)
    expect(batchRunMovedNothing({ ...summary(0), tables_running: 1 } as never)).toBe(false)
    expect(batchRunMovedNothing({ ...summary(0), tables_failed: 1 } as never)).toBe(false)
    expect(batchRunMovedNothing({ ...summary(0), total_dlq_rows: 3 } as never)).toBe(false)
    expect(batchRunMovedNothing({ ...summary(0), mode: "cdc" } as never)).toBe(false)
    expect(batchRunMovedNothing({ ...summary(0), total_tables: 0 } as never)).toBe(false)
    expect(batchRunMovedNothing(null)).toBe(false)
  })
})
