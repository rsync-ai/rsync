import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import ScheduledQueriesPage from "@/app/(dashboard)/explorer/schedules/page"
import { authFetch } from "@/lib/api/auth-fetch"
import { formatAbsoluteTime } from "@/components/explorer/scheduleTime"

// The bug: the schedules page fetched once, so when a row's next_run_at passed it said
// "due now" and kept saying it — the server already knew the following run, but nothing
// asked. The page now re-asks a few seconds after the earliest next run comes due.
//
// Fake timers with shouldAdvanceTime: testing-library's findBy/waitFor poll on timers,
// which hang under a fully frozen clock. The fixtures carry half-minute pads so the few
// real milliseconds that leak in cannot move a label across a minute boundary.

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/explorer/schedules",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({ role: "admin", can: () => true }),
}))
vi.mock("@/contexts/CurrentUserContext", () => ({
  useCurrentUser: () => ({ user: { id: "u-1" } }),
}))
vi.mock("@/components/explorer/SavedQueryEditDialog", () => ({ SavedQueryEditDialog: () => null }))
vi.mock("@/components/explorer/SavedQueryModelDialog", () => ({ SavedQueryModelDialog: () => null }))

const SCHEDULES = "/api/v1/explorer/schedules"
const NAME = "orders_nightly"
const NOW = new Date("2026-08-15T10:00:00Z").getTime()
const SECOND = 1000
const MINUTE = 60 * SECOND

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

function clockSchedule(nextRunAt: number) {
  return {
    schedule_id: "sch-q-1",
    saved_query_id: "q-1",
    name: NAME,
    connection_id: "c-1",
    schedule_type: "cron",
    schedule_spec: { cron: "0 2 * * *", timezone: "UTC" },
    status: "active",
    materialization: "table",
    target_table: `analytics.${NAME}`,
    statement_class: "select",
    supports_materialization: true,
    next_run_at: new Date(nextRunAt).toISOString(),
    created_by: "u-1",
    created_at: "2026-08-01T00:00:00Z",
    updated_at: "2026-08-01T00:00:00Z",
  }
}

// Each call to the schedules list gets the next response; the last one repeats.
function serve(responses: Response[]) {
  let n = 0
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    if (url !== SCHEDULES) return res(404, {})
    const r = responses[Math.min(n, responses.length - 1)]
    n++
    return r
  })
}

function listed(nextRunAt: number) {
  return res(200, { schedules: [clockSchedule(nextRunAt)], count: 1 })
}

function scheduleFetches(): number {
  return (authFetch as Mock).mock.calls.filter(([url]) => url === SCHEDULES).length
}

function nextRunCell(): HTMLElement {
  const column = screen.getAllByRole("columnheader").findIndex((h) => h.textContent === "Next run")
  expect(column).toBeGreaterThan(-1)
  const row = screen.getByText(NAME).closest("tr")
  if (!row) throw new Error(`no table row for ${NAME}`)
  return within(row).getAllByRole("cell")[column]
}

async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

describe("schedules page: refetch when a next run comes due", () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    vi.setSystemTime(NOW)
  })

  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
    vi.useRealTimers()
  })

  it("re-asks a few seconds after the next run passes and shows the following one", async () => {
    serve([listed(NOW + 4 * MINUTE + 30 * SECOND), listed(NOW + 64 * MINUTE)])
    render(<ScheduledQueriesPage />)
    await screen.findByText(NAME)
    expect(nextRunCell()).toHaveTextContent("in 4m")
    expect(scheduleFetches()).toBe(1)

    // Due, but inside the grace: the server would still report the same instant.
    await advance(4 * MINUTE + 32 * SECOND)
    expect(scheduleFetches()).toBe(1)

    await advance(4 * SECOND)
    expect(scheduleFetches()).toBe(2)
    await waitFor(() => expect(nextRunCell()).toHaveTextContent("in 59m"))
  })

  it("names the time an overdue run was due, not only 'due now', until the refetch lands", async () => {
    const dueAt = NOW - 3 * SECOND
    serve([listed(dueAt), listed(NOW + 30 * MINUTE + 30 * SECOND)])
    render(<ScheduledQueriesPage />)
    await screen.findByText(NAME)

    expect(nextRunCell()).toHaveTextContent(`due ${formatAbsoluteTime(new Date(dueAt).toISOString())}`)
    expect(nextRunCell()).not.toHaveTextContent("due now")

    await advance(6 * SECOND)
    expect(scheduleFetches()).toBe(2)
    await waitFor(() => expect(nextRunCell()).toHaveTextContent("in 30m"))
  })

  it("clears the timer on unmount", async () => {
    serve([listed(NOW + 1 * MINUTE + 30 * SECOND)])
    const { unmount } = render(<ScheduledQueriesPage />)
    await screen.findByText(NAME)
    unmount()

    await advance(10 * MINUTE)
    expect(scheduleFetches()).toBe(1)
  })

  it("re-arms on new data instead of leaving the old timer to fire", async () => {
    serve([listed(NOW + 4 * MINUTE + 30 * SECOND), listed(NOW + 30 * MINUTE + 30 * SECOND)])
    render(<ScheduledQueriesPage />)
    await screen.findByText(NAME)

    // A manual Refresh brings a later next run. The timer armed for the old one must go.
    fireEvent.click(screen.getByRole("button", { name: /refresh/i }))
    await waitFor(() => expect(nextRunCell()).toHaveTextContent("in 30m"))
    expect(scheduleFetches()).toBe(2)

    await advance(5 * MINUTE)
    expect(scheduleFetches()).toBe(2)
  })

  it("keeps the rows through a failed background refetch, and tries again", async () => {
    serve([
      listed(NOW + 1 * MINUTE + 30 * SECOND),
      res(500, {}),
      listed(NOW + 30 * MINUTE + 30 * SECOND),
    ])
    render(<ScheduledQueriesPage />)
    await screen.findByText(NAME)

    await advance(1 * MINUTE + 36 * SECOND)
    expect(scheduleFetches()).toBe(2)
    // Not replaced by an error the user never asked for.
    expect(screen.getByText(NAME)).toBeInTheDocument()
    expect(screen.queryByText(/could not load scheduled queries/i)).not.toBeInTheDocument()

    // And the failure did not end the refetching: the row is still overdue, so the
    // page asks again (after as long as it has been overdue, ~6s here) instead of
    // waiting for someone to press Refresh.
    await advance(8 * SECOND)
    expect(scheduleFetches()).toBe(3)
    await waitFor(() => expect(nextRunCell()).toHaveTextContent("in 28m"))
  })
})
