import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render, screen, waitFor } from "@testing-library/react"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: "admin",
    isLoading: false,
    error: false,
    activeWorkspace: null,
    can: () => true,
    meets: () => true,
  }),
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { SavedQueries, type SavedQuery } from "@/components/explorer/SavedQueries"
import { formatAbsoluteTime } from "@/components/explorer/scheduleTime"

// The Explorer panel's "Next run" badge had the schedules page's bug: fetched once, it
// read "due now" from the moment the run fired until something else reloaded the list.
// Fake timers with shouldAdvanceTime so testing-library's polling still runs; fixtures
// carry half-minute pads so leaked real milliseconds cannot cross a minute boundary.

const CONNECTION_ID = "44444444-4444-4444-4444-444444444444"
const NOW = new Date("2026-08-15T10:00:00Z").getTime()
const SECOND = 1000
const MINUTE = 60 * SECOND

const mockFetch = authFetch as unknown as ReturnType<typeof vi.fn>

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

function query(overrides: Partial<SavedQuery> = {}): SavedQuery {
  return {
    id: "33333333-3333-3333-3333-333333333333",
    connection_id: CONNECTION_ID,
    name: "Daily MRR",
    sql_text: "SELECT 1",
    statement_class: "read",
    visibility: "workspace",
    created_by: "11111111-1111-1111-1111-111111111111",
    created_at: "2026-08-13T00:00:00Z",
    updated_at: "2026-08-13T00:00:00Z",
    materialization: "table",
    target_table: "analytics.daily_mrr",
    ...overrides,
  }
}

function scheduled(nextRunAt: number, overrides: Partial<SavedQuery> = {}) {
  return query({
    schedule_status: "active",
    next_run_at: new Date(nextRunAt).toISOString(),
    ...overrides,
  })
}

// Each list call gets the next response; the last one repeats.
function serve(lists: SavedQuery[][]) {
  let n = 0
  mockFetch.mockImplementation(async () => {
    const items = lists[Math.min(n, lists.length - 1)]
    n++
    return res(200, { saved_queries: items, count: items.length })
  })
}

function listFetches(): number {
  return mockFetch.mock.calls.filter(([url]) => String(url).startsWith("/api/v1/explorer/saved?")).length
}

async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

function renderPanel() {
  return render(<SavedQueries connectionId={CONNECTION_ID} currentSql="SELECT 1" onLoad={vi.fn()} />)
}

describe("SavedQueries next-run badge: refetch when a run comes due", () => {
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
    serve([[scheduled(NOW + 2 * MINUTE + 30 * SECOND)], [scheduled(NOW + 62 * MINUTE)]])
    renderPanel()
    await screen.findByText("in 2m")
    expect(listFetches()).toBe(1)

    await advance(2 * MINUTE + 32 * SECOND)
    expect(listFetches()).toBe(1)

    await advance(4 * SECOND)
    expect(listFetches()).toBe(2)
    await waitFor(() => expect(screen.getByText("in 59m")).toBeInTheDocument())
  })

  it("names the time an overdue run was due instead of only 'due now'", async () => {
    const dueAt = NOW - 3 * SECOND
    serve([[scheduled(dueAt)]])
    renderPanel()

    const label = `due ${formatAbsoluteTime(new Date(dueAt).toISOString())}`
    await screen.findByText(label)
    expect(screen.queryByText("due now")).not.toBeInTheDocument()
  })

  it("does not refetch for a schedule that is not active", async () => {
    // The badge is shown for active schedules only, so a paused row's stale time must
    // not cost a request — let alone one every few seconds.
    serve([[scheduled(NOW + 1 * MINUTE + 30 * SECOND, { schedule_status: "paused" })]])
    renderPanel()
    await screen.findByText("Daily MRR")

    await advance(10 * MINUTE)
    expect(listFetches()).toBe(1)
  })

  it("clears the timer on unmount", async () => {
    serve([[scheduled(NOW + 1 * MINUTE + 30 * SECOND)]])
    const { unmount } = renderPanel()
    await screen.findByText("in 1m")
    unmount()

    await advance(10 * MINUTE)
    expect(listFetches()).toBe(1)
  })
})
