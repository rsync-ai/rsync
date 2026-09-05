import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))

// Both identities are knobs: the dialog's gate is "the creator, or an admin", and a
// test that fixed either one would only ever exercise half of it.
const mockUser = { value: "11111111-1111-1111-1111-111111111111" }
const mockRole = { value: "member" }
vi.mock("@/contexts/CurrentUserContext", () => ({
  useCurrentUser: () => ({ user: { id: mockUser.value }, role: "user", isLoading: false }),
}))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: mockRole.value,
    isLoading: false,
    error: false,
    activeWorkspace: null,
    can: () => mockRole.value === "admin" || mockRole.value === "owner",
    meets: (min: string) => {
      const rank: Record<string, number> = { viewer: 1, member: 2, admin: 3, owner: 4 }
      return (rank[mockRole.value] ?? 0) >= (rank[min] ?? 99)
    },
  }),
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { toast } from "sonner"
import { SavedQueryEditDialog } from "@/components/explorer/SavedQueryEditDialog"

// PATCH /explorer/saved/:id shipped with the feature and nothing in the UI called it.
// The properties pinned here are the ones whose failure silently destroys someone
// else's work:
//
//  1. the form is seeded from a fresh read, not from a list row that may be minutes
//     stale — saving stale text reverts a teammate's edit without either of them
//     seeing it happen;
//  2. only changed fields are sent, because PATCH takes pointers server-side and
//     every present field writes a version snapshot;
//  3. a failed load never falls through to a blank form, which would invite the user
//     to save that blankness over the real query.

const QUERY_ID = "33333333-3333-3333-3333-333333333333"
const CREATOR = "11111111-1111-1111-1111-111111111111"

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

const mockFetch = authFetch as unknown as ReturnType<typeof vi.fn>

function loaded(overrides: Record<string, unknown> = {}) {
  return {
    id: QUERY_ID,
    name: "Daily MRR",
    description: "Revenue by day",
    sql_text: "SELECT 1",
    visibility: "workspace",
    created_by: CREATOR,
    statement_class: "read",
    materialization: "table",
    schedule_status: "",
    ...overrides,
  }
}

function renderDialog() {
  return render(
    <SavedQueryEditDialog
      savedQueryId={QUERY_ID}
      open
      onOpenChange={vi.fn()}
      onSaved={vi.fn()}
    />
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  mockUser.value = CREATOR
  mockRole.value = "member"
})

describe("SavedQueryEditDialog", () => {
  it("seeds the form from a fresh read of the query", async () => {
    mockFetch.mockResolvedValue(res(200, loaded()))

    renderDialog()

    await waitFor(() => {
      expect(screen.getByLabelText("Name")).toHaveValue("Daily MRR")
    })
    expect(screen.getByLabelText("SQL")).toHaveValue("SELECT 1")
    // The read is the point: a cached row is exactly how one person's edit
    // silently reverts another's.
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining(`/explorer/saved/${QUERY_ID}`),
      expect.objectContaining({ cache: "no-store" })
    )
  })

  // PATCH takes pointers server-side: a field that is present is written, and every
  // write snapshots the previous version. Resending untouched fields fills the
  // history with edits nobody made.
  it("sends only the fields that changed", async () => {
    const bodies: unknown[] = []
    mockFetch.mockImplementation(async (url: string, init?: RequestInit) => {
      if ((init?.method ?? "GET") === "GET") return res(200, loaded())
      bodies.push(JSON.parse(String(init?.body)))
      return res(200, loaded({ name: "Daily MRR v2" }))
    })

    const user = userEvent.setup()
    renderDialog()

    await waitFor(() => {
      expect(screen.getByLabelText("Name")).toHaveValue("Daily MRR")
    })

    await user.clear(screen.getByLabelText("Name"))
    await user.type(screen.getByLabelText("Name"), "Daily MRR v2")
    await user.click(screen.getByRole("button", { name: /save changes/i }))

    await waitFor(() => {
      expect(bodies.length).toBe(1)
    })
    expect(bodies[0]).toEqual({ name: "Daily MRR v2" })
  })

  // Nothing changed means nothing to write — and a PATCH with an empty body still
  // costs a version snapshot.
  it("offers no save until something actually changes", async () => {
    mockFetch.mockResolvedValue(res(200, loaded()))

    renderDialog()

    await waitFor(() => {
      expect(screen.getByLabelText("Name")).toHaveValue("Daily MRR")
    })
    expect(screen.getByRole("button", { name: /save changes/i })).toBeDisabled()
  })

  // Mirrors UpdateSavedQuery's rule. The server still decides; this stops the dialog
  // offering a Save whose only outcome is a 403, and says why instead.
  it("is read-only for someone who neither created it nor is an admin", async () => {
    mockUser.value = "99999999-9999-9999-9999-999999999999"
    mockRole.value = "member"
    mockFetch.mockResolvedValue(res(200, loaded()))

    renderDialog()

    await waitFor(() => {
      expect(screen.getByText(/only the person who saved this query, or a workspace admin/i))
        .toBeInTheDocument()
    })
    expect(screen.getByLabelText("SQL")).toBeDisabled()
    expect(screen.getByRole("button", { name: /save changes/i })).toBeDisabled()
  })

  it("lets a workspace admin edit someone else's query", async () => {
    mockUser.value = "99999999-9999-9999-9999-999999999999"
    mockRole.value = "admin"
    mockFetch.mockResolvedValue(res(200, loaded()))

    renderDialog()

    await waitFor(() => {
      expect(screen.getByLabelText("SQL")).toBeEnabled()
    })
    expect(screen.queryByText(/only the person who saved this query/i)).not.toBeInTheDocument()
  })

  // Editing the SQL of a query that runs unattended changes what runs unattended, so
  // since migration 092 the server parks that edit for an admin instead of applying
  // it. The dialog has to say so BEFORE the click, and the button has to name what
  // the click actually does.
  it("says the SQL edit needs approval when the query is on a schedule", async () => {
    mockFetch.mockResolvedValue(res(200, loaded({ schedule_status: "active" })))

    const user = userEvent.setup()
    renderDialog()

    await waitFor(() => {
      expect(screen.getByLabelText("SQL")).toHaveValue("SELECT 1")
    })
    expect(screen.queryByText(/this query runs on a schedule/i)).not.toBeInTheDocument()
    expect(screen.getByRole("button", { name: /save changes/i })).toBeInTheDocument()

    await user.type(screen.getByLabelText("SQL"), " FROM orders")

    expect(await screen.findByText(/needs a workspace admin.s approval/i)).toBeInTheDocument()
    expect(screen.getByText(/keeps running the current SQL/i)).toBeInTheDocument()
    // The button promises review, not a save.
    expect(screen.getByRole("button", { name: /send for approval/i })).toBeEnabled()
    expect(screen.queryByRole("button", { name: /save changes/i })).not.toBeInTheDocument()
  })

  // A PAUSED schedule is still a schedule, and the server gates it (the probe covers
  // status IN ('active','paused')). If the dialog treated paused as unscheduled it
  // would promise an immediate save the server will not perform.
  it("treats a paused schedule as needing approval too", async () => {
    mockFetch.mockResolvedValue(res(200, loaded({ schedule_status: "paused" })))

    const user = userEvent.setup()
    renderDialog()

    await waitFor(() => expect(screen.getByLabelText("SQL")).toHaveValue("SELECT 1"))
    await user.type(screen.getByLabelText("SQL"), " FROM orders")

    expect(await screen.findByText(/needs a workspace admin.s approval/i)).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /send for approval/i })).toBeInTheDocument()
  })

  // The failure this pins is a sentence, and it is the whole point of the gate: the
  // server answers 200 for a proposal, so a dialog that reads only res.ok reports
  // "Updated" and the author leaves believing the schedule now runs their new SQL.
  it("never reports a parked SQL edit as an update", async () => {
    mockFetch.mockImplementation(async (_url: string, init?: RequestInit) => {
      if ((init?.method ?? "GET") === "GET") return res(200, loaded({ schedule_status: "active" }))
      return res(200, {
        id: QUERY_ID,
        name: "Daily MRR",
        pending_approval: {
          id: "44444444-4444-4444-4444-444444444444",
          statement_class: "read",
          reason: "This query is on a schedule, so the SQL change needs a workspace admin's approval.",
        },
      })
    })

    const user = userEvent.setup()
    renderDialog()

    await waitFor(() => expect(screen.getByLabelText("SQL")).toHaveValue("SELECT 1"))
    await user.type(screen.getByLabelText("SQL"), " FROM orders")
    await user.click(screen.getByRole("button", { name: /send for approval/i }))

    await waitFor(() => expect(toast.success).toHaveBeenCalled())
    const [message] = (toast.success as unknown as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(message).toMatch(/approval/i)
    expect(message).not.toMatch(/updated/i)
    expect(toast.error).not.toHaveBeenCalled()
  })

  it("sends the reviewer note with a parked edit, and only then", async () => {
    const patches: string[] = []
    mockFetch.mockImplementation(async (_url: string, init?: RequestInit) => {
      if ((init?.method ?? "GET") === "GET") return res(200, loaded({ schedule_status: "active" }))
      patches.push(String(init?.body ?? ""))
      return res(200, { pending_approval: { id: "x", statement_class: "read", reason: "r" } })
    })

    const user = userEvent.setup()
    renderDialog()

    await waitFor(() => expect(screen.getByLabelText("SQL")).toHaveValue("SELECT 1"))
    await user.type(screen.getByLabelText("SQL"), " FROM orders")
    await user.type(screen.getByLabelText(/why this should change/i), "Region filter was wrong")
    await user.click(screen.getByRole("button", { name: /send for approval/i }))

    await waitFor(() => expect(patches).toHaveLength(1))
    const body = JSON.parse(patches[0])
    expect(body.note).toBe("Region filter was wrong")
    expect(body.sql_text).toBe("SELECT 1 FROM orders")
  })

  // The other half of the gate: an UNSCHEDULED query must not acquire a review step.
  // Making every edit go through approval is the easy over-correction, and it would
  // teach people to route around the gate.
  it("applies an edit immediately when nothing is scheduled", async () => {
    mockFetch.mockImplementation(async (_url: string, init?: RequestInit) => {
      if ((init?.method ?? "GET") === "GET") return res(200, loaded())
      return res(200, { id: QUERY_ID, name: "Daily MRR" })
    })

    const user = userEvent.setup()
    renderDialog()

    await waitFor(() => expect(screen.getByLabelText("SQL")).toHaveValue("SELECT 1"))
    await user.type(screen.getByLabelText("SQL"), " FROM orders")

    expect(screen.queryByText(/needs a workspace admin.s approval/i)).not.toBeInTheDocument()
    expect(screen.queryByLabelText(/why this should change/i)).not.toBeInTheDocument()

    await user.click(screen.getByRole("button", { name: /save changes/i }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Updated "Daily MRR"'))
  })

  // A blank form over a query that failed to load is an invitation to save that
  // blankness over the real thing.
  it("does not offer an empty form when the query could not be loaded", async () => {
    mockFetch.mockResolvedValue(res(500, { error: "database is unavailable" }))

    renderDialog()

    await waitFor(() => {
      expect(screen.getByText(/could not load this query/i)).toBeInTheDocument()
    })
    expect(screen.queryByLabelText("SQL")).not.toBeInTheDocument()
    expect(screen.getByText(/the query itself has not been changed/i)).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /save changes/i })).toBeDisabled()
    expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument()
  })

  it("reports the server's reason when a save is refused", async () => {
    mockFetch.mockImplementation(async (url: string, init?: RequestInit) => {
      if ((init?.method ?? "GET") === "GET") return res(200, loaded())
      return res(403, { error: "only the creator or a workspace admin can edit this query" })
    })

    const user = userEvent.setup()
    renderDialog()

    await waitFor(() => {
      expect(screen.getByLabelText("Name")).toHaveValue("Daily MRR")
    })

    await user.type(screen.getByLabelText("Name"), " nightly")
    await user.click(screen.getByRole("button", { name: /save changes/i }))

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "only the creator or a workspace admin can edit this query"
      )
    })
    expect(toast.success).not.toHaveBeenCalled()
  })
})
