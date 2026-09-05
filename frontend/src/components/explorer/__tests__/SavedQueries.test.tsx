import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"

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

// The row badges are the only place an unattended rebuild is visible without opening
// a dialog. Nobody opens a dialog to check on something they believe is fine, so a
// row that under-reports is a schedule nobody looks at.

const CONNECTION_ID = "44444444-4444-4444-4444-444444444444"

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

const mockFetch = authFetch as unknown as ReturnType<typeof vi.fn>

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
    ...overrides,
  }
}

function renderList(items: SavedQuery[]) {
  mockFetch.mockResolvedValue(res(200, { saved_queries: items, count: items.length }))
  return render(
    <SavedQueries connectionId={CONNECTION_ID} currentSql="SELECT 1" onLoad={vi.fn()} />
  )
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe("SavedQueries row badges", () => {
  // The entry point to scheduling was a bare 24px icon, and it was reported as
  // "there are no options to schedule". The label is the fix, so it is pinned.
  it("labels the schedule entry point with the word people look for", async () => {
    renderList([query()])
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /schedule daily mrr/i })).toBeInTheDocument()
    })
    expect(screen.getByText("Schedule")).toBeInTheDocument()
  })

  it("shows when the next run is due for an active schedule", async () => {
    // A real-clock offset, deliberately not vi.useFakeTimers(): waitFor needs the
    // real clock to poll, so faking it here hangs the test for its whole timeout —
    // and, because the restore sits after the await, leaves the fake clock
    // installed for every test that follows. The 1-minute pad past 3h absorbs
    // execution drift, so the floor lands on 3h without pinning the clock.
    const nextRun = new Date(Date.now() + 3 * 60 * 60 * 1000 + 60 * 1000).toISOString()
    renderList([
      query({
        materialization: "table",
        target_table: "analytics.daily_mrr",
        schedule_status: "active",
        next_run_at: nextRun,
      }),
    ])

    await waitFor(() => {
      expect(screen.getByText("in 3h")).toBeInTheDocument()
    })
    // The target still gets its own badge; the next run does not displace it.
    expect(screen.getByText("analytics.daily_mrr")).toBeInTheDocument()
  })

  // The bug: the badge block returned null unless materialization was "table", so
  // clearing the target on a query with a LIVE schedule made the row render exactly
  // like an inert one — while the schedule kept firing and failing every hour.
  it("still reports a live schedule after its target table is cleared", async () => {
    renderList([query({ materialization: "none", target_table: "", schedule_status: "active" })])

    await waitFor(() => {
      expect(screen.getByText("does nothing")).toBeInTheDocument()
    })
    expect(screen.getByText("scheduled")).toBeInTheDocument()
  })

  // A statement model has no target table BY DESIGN — the MERGE names its own
  // destination. Reusing the empty-target warning for it would tell the truth about
  // the column and a lie about the query, so the two states get different badges.
  it("reports a statement model as doing something, not as missing a target", async () => {
    renderList([
      query({
        statement_class: "dml_write",
        materialization: "statement",
        target_table: "",
        schedule_status: "active",
      }),
    ])

    await waitFor(() => {
      expect(screen.getByText("runs as written")).toBeInTheDocument()
    })
    expect(screen.getByText("scheduled")).toBeInTheDocument()
    expect(screen.queryByText("does nothing")).not.toBeInTheDocument()
  })

  // PATCH /explorer/saved/:id shipped with the feature and nothing ever called it:
  // changing a saved query's SQL meant deleting it and saving a new one, which drops
  // its schedule, its target table and its version history.
  it("offers a labelled edit affordance on every row", async () => {
    renderList([query()])
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /edit daily mrr/i })).toBeInTheDocument()
    })
    expect(screen.getByText("Edit")).toBeInTheDocument()
  })

  it("still reports a failed last run after its target table is cleared", async () => {
    renderList([
      query({
        materialization: "none",
        target_table: "",
        last_run_status: "failed",
        last_run_error: "relation does not exist",
      }),
    ])

    await waitFor(() => {
      expect(screen.getByText(/last run failed/i)).toBeInTheDocument()
    })
  })

  it("renders no model badges for a plain saved query", async () => {
    renderList([query()])
    await waitFor(() => {
      expect(screen.getByText("Daily MRR")).toBeInTheDocument()
    })
    expect(screen.queryByText("scheduled")).not.toBeInTheDocument()
    expect(screen.queryByText("does nothing")).not.toBeInTheDocument()
    expect(screen.queryByText("runs as written")).not.toBeInTheDocument()
    expect(screen.queryByText(/last run failed/i)).not.toBeInTheDocument()
  })
})
