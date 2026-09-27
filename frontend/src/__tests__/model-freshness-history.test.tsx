import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { cleanup, render, screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { ModelFreshnessHistory } from "@/components/explorer/ModelFreshnessHistory"
import { authFetch } from "@/lib/api/auth-fetch"

// A model's freshness misses, past and present. The api-gateway has kept every one since
// the deadline shipped (saved_query_freshness.go, include_resolved), and the only place it
// ever showed was a badge on the one that is open now. The quiet failures worth guarding:
// another model's miss listed as this one's, a widened deadline read as a fix, and a
// capped workspace list read as the whole history.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

const URL = "/api/v1/explorer/freshness?include_resolved=true"

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

function breach(id: string, overrides: Record<string, unknown> = {}) {
  return {
    breach_id: id,
    saved_query_id: "q-1",
    name: "revenue_rollup",
    target_table: "analytics.revenue_rollup",
    deadline_seconds: 3600,
    cause: "overdue",
    reference_at: "2026-09-20T00:00:00Z",
    never_succeeded: false,
    stale_seconds: 60,
    detected_at: "2026-09-20T01:01:00Z",
    ...overrides,
  }
}

// Newest first, as the route orders them (detected_at DESC).
const OPEN = breach("b-open", { stale_seconds_now: 7200 })
const REBUILT = breach("b-rebuilt", {
  cause: "schedule_paused",
  reference_at: "2026-09-18T00:00:00Z",
  detected_at: "2026-09-18T01:00:30Z",
  resolved_at: "2026-09-18T03:30:00Z",
  resolution: "rebuilt",
})
const WIDENED = breach("b-widened", {
  cause: "no_schedule",
  never_succeeded: true,
  deadline_seconds: 1800,
  reference_at: "2026-09-17T00:00:00Z",
  detected_at: "2026-09-17T00:30:40Z",
  resolved_at: "2026-09-17T00:45:00Z",
  resolution: "deadline_widened",
})
const OTHER_MODEL = breach("b-other", { saved_query_id: "q-2", name: "orders" })

function serve(...responses: Response[]) {
  const mock = authFetch as Mock
  for (const r of responses) mock.mockResolvedValueOnce(r)
}

function bodyRows(): HTMLElement[] {
  const table = screen.getByRole("table", { name: "Freshness misses" })
  return within(table).getAllByRole("row").slice(1)
}

describe("ModelFreshnessHistory", () => {
  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
  })

  it("asks for resolved misses too, and lists only this model's, newest first", async () => {
    serve(res(200, { breaches: [OPEN, OTHER_MODEL, REBUILT, WIDENED], count: 4 }))
    render(<ModelFreshnessHistory savedQueryId="q-1" reloadTick={0} />)

    await screen.findByRole("table", { name: "Freshness misses" })
    expect(authFetch).toHaveBeenCalledWith(URL, { cache: "no-store" })
    const rows = bodyRows()
    expect(rows).toHaveLength(3)
    expect(rows[0]).toHaveTextContent("Still open")
    expect(rows[1]).toHaveTextContent("Rebuilt")
    expect(rows[2]).toHaveTextContent("Deadline widened")
    expect(screen.queryByText(/orders/)).not.toBeInTheDocument()
  })

  it("says how late the table is now while a miss is open, and how late it got once one ended", async () => {
    serve(res(200, { breaches: [OPEN, REBUILT, WIDENED], count: 3 }))
    render(<ModelFreshnessHistory savedQueryId="q-1" reloadTick={0} />)

    await screen.findByRole("table", { name: "Freshness misses" })
    const [open, rebuilt, widened] = bodyRows()
    // Open: the server's figure for now, not the one it recorded at detection.
    expect(open).toHaveTextContent("2h so far")
    expect(open).toHaveTextContent("Its schedule is active but has not rebuilt it")
    // Closed at 03:30 on a table last built at 00:00 under a 1h deadline.
    expect(rebuilt).toHaveTextContent("2h 30m")
    expect(rebuilt).toHaveTextContent("Its schedule is paused")
    expect(widened).toHaveTextContent("15m")
    expect(widened).toHaveTextContent("Nothing is scheduled to rebuild it")
    expect(widened).toHaveTextContent("never rebuilt successfully")
  })

  it("does not call a widened deadline a fix", async () => {
    serve(res(200, { breaches: [WIDENED], count: 1 }))
    render(<ModelFreshnessHistory savedQueryId="q-1" reloadTick={0} />)

    await screen.findByRole("table", { name: "Freshness misses" })
    const [row] = bodyRows()
    expect(row).toHaveTextContent("Deadline widened")
    expect(row).toHaveTextContent("The table was not rebuilt")
  })

  it("says a history from a full workspace list may be missing older misses", async () => {
    const filler = Array.from({ length: 499 }, (_, i) => breach(`b-f${i}`, { saved_query_id: "q-2", resolved_at: "2026-09-19T00:00:00Z", resolution: "rebuilt" }))
    serve(res(200, { breaches: [OPEN, ...filler], count: 500 }))
    render(<ModelFreshnessHistory savedQueryId="q-1" reloadTick={0} />)

    await screen.findByRole("table", { name: "Freshness misses" })
    expect(screen.getByText(/only the workspace's 500 most recent misses/i)).toBeInTheDocument()
  })

  it("says nothing about a cap when the list is not full", async () => {
    serve(res(200, { breaches: [OPEN], count: 1 }))
    render(<ModelFreshnessHistory savedQueryId="q-1" reloadTick={0} />)

    await screen.findByRole("table", { name: "Freshness misses" })
    expect(screen.queryByText(/500 most recent/i)).not.toBeInTheDocument()
  })

  it("says the model has never missed, and where the deadline is set", async () => {
    serve(res(200, { breaches: [OTHER_MODEL], count: 1 }))
    render(<ModelFreshnessHistory savedQueryId="q-1" reloadTick={0} />)

    expect(await screen.findByText("No freshness misses recorded.")).toBeInTheDocument()
    expect(screen.getByText(/freshness deadline in Details/i)).toBeInTheDocument()
    expect(screen.queryByRole("table")).not.toBeInTheDocument()
  })

  it("says the history could not be loaded, rather than that there is none, and retries", async () => {
    serve(res(500, {}), res(200, { breaches: [REBUILT], count: 1 }))
    const user = userEvent.setup()
    render(<ModelFreshnessHistory savedQueryId="q-1" reloadTick={0} />)

    expect(await screen.findByText(/could not load the freshness history \(HTTP 500\)/i)).toBeInTheDocument()
    expect(screen.queryByText("No freshness misses recorded.")).not.toBeInTheDocument()
    await user.click(screen.getByRole("button", { name: "Retry" }))
    await screen.findByRole("table", { name: "Freshness misses" })
    expect(authFetch).toHaveBeenCalledTimes(2)
  })

  it("refuses a body it cannot read, rather than showing an empty history", async () => {
    serve(res(200, { nope: true }))
    render(<ModelFreshnessHistory savedQueryId="q-1" reloadTick={0} />)

    expect(await screen.findByText(/could not read/i)).toBeInTheDocument()
    expect(screen.queryByText("No freshness misses recorded.")).not.toBeInTheDocument()
  })

  it("asks again when the page reloads", async () => {
    serve(res(200, { breaches: [OPEN], count: 1 }), res(200, { breaches: [REBUILT], count: 1 }))
    const { rerender } = render(<ModelFreshnessHistory savedQueryId="q-1" reloadTick={0} />)
    await screen.findByText("Still open")

    rerender(<ModelFreshnessHistory savedQueryId="q-1" reloadTick={1} />)
    expect(await screen.findByText("Rebuilt")).toBeInTheDocument()
    expect(screen.queryByText("Still open")).not.toBeInTheDocument()
    expect(authFetch).toHaveBeenCalledTimes(2)
  })
})
