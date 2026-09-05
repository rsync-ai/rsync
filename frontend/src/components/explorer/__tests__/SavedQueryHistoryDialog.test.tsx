import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))

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
    activeWorkspace: { id: "ws-1", name: "Acme", role: mockRole.value },
    can: () => mockRole.value === "admin" || mockRole.value === "owner",
    meets: (min: string) => {
      const rank: Record<string, number> = { viewer: 1, member: 2, admin: 3, owner: 4 }
      return (rank[mockRole.value] ?? 0) >= (rank[min] ?? 99)
    },
  }),
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { toast } from "sonner"
import { SavedQueryHistoryDialog } from "@/components/explorer/SavedQueryHistoryDialog"

// This panel is the only place a proposed edit to a scheduled query gets reviewed, so
// its failures are approval failures, not display bugs. What is pinned here:
//
//  1. a member cannot see approve/reject controls — the gate's actual teeth are that
//     proposing and approving are different rights;
//  2. approving a stale proposal warns first, because approving one silently reverts
//     whatever landed in between;
//  3. restore goes through the normal edit path, so on a scheduled query it becomes a
//     proposal and must NOT be reported as if it had been restored.

const QUERY_ID = "33333333-3333-3333-3333-333333333333"
const ME = "11111111-1111-1111-1111-111111111111"
const SOMEONE_ELSE = "22222222-2222-2222-2222-222222222222"

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

const mockFetch = authFetch as unknown as ReturnType<typeof vi.fn>

const VERSIONS = [
  {
    version: 2,
    name: "Daily MRR",
    sql_text: "SELECT 1\nFROM orders",
    statement_class: "read",
    changed_by: SOMEONE_ELSE,
    created_at: "2026-08-01T10:00:00Z",
  },
  {
    version: 1,
    name: "MRR",
    sql_text: "SELECT 1",
    statement_class: "read",
    changed_by: ME,
    created_at: "2026-07-01T10:00:00Z",
  },
]

const CURRENT = {
  name: "Daily MRR",
  sql_text: "SELECT 1\nFROM orders\nWHERE region = 'apac'",
  statement_class: "read",
  updated_at: "2026-08-10T10:00:00Z",
}

/** Wires the three endpoints this panel touches. `history` overrides the body. */
function wire(history: Record<string, unknown> = {}, onPatch?: (body: string) => unknown) {
  mockFetch.mockImplementation(async (url: string, init?: RequestInit) => {
    if (url.includes("/members")) {
      return res(200, { members: [{ user_id: SOMEONE_ELSE, email: "dana@acme.test" }] })
    }
    if (url.includes("/versions")) {
      return res(200, { versions: VERSIONS, count: VERSIONS.length, current: CURRENT, pending_edit: null, ...history })
    }
    if (url.includes("/pending/")) return res(200, { status: "ok" })
    if ((init?.method ?? "GET") === "PATCH") {
      return res(200, onPatch?.(String(init?.body ?? "")) ?? { id: QUERY_ID })
    }
    return res(404, { error: "unexpected" })
  })
}

function renderPanel() {
  return render(
    <SavedQueryHistoryDialog
      savedQueryId={QUERY_ID}
      savedQueryName="Daily MRR"
      open
      onOpenChange={vi.fn()}
      onChanged={vi.fn()}
    />,
  )
}

const pendingEdit = (overrides: Record<string, unknown> = {}) => ({
  id: "44444444-4444-4444-4444-444444444444",
  sql_text: "SELECT 1\nFROM orders\nWHERE region = 'emea'",
  statement_class: "read",
  note: "APAC was the wrong region",
  status: "pending",
  proposed_by: SOMEONE_ELSE,
  proposed_at: "2026-08-15T10:00:00Z",
  stale: false,
  ...overrides,
})

beforeEach(() => {
  vi.clearAllMocks()
  mockUser.value = ME
  mockRole.value = "member"
})

describe("SavedQueryHistoryDialog", () => {
  it("lists earlier versions and diffs the selected one against what runs now", async () => {
    wire()
    renderPanel()

    await waitFor(() => expect(screen.getByRole("button", { name: "v2" })).toBeInTheDocument())
    expect(screen.getByRole("button", { name: "v1" })).toBeInTheDocument()

    // v2 is selected by default and differs from current by the added WHERE clause.
    expect(screen.getByRole("button", { name: "v2" })).toHaveAttribute("aria-pressed", "true")
    expect(await screen.findByText(/\+1 added/)).toBeInTheDocument()
    expect(screen.getByText(/0 removed/)).toBeInTheDocument()
    expect(screen.getByText(/WHERE region = 'apac'/)).toBeInTheDocument()
  })

  it("names the author, and says 'you' for the reader's own edits", async () => {
    wire()
    renderPanel()

    await waitFor(() => expect(screen.getByRole("button", { name: "v2" })).toBeInTheDocument())
    // v2 was changed by someone whose email the roster resolves.
    expect(screen.getByText(/by dana@acme.test/)).toBeInTheDocument()

    await userEvent.setup().click(screen.getByRole("button", { name: "v1" }))
    expect(await screen.findByText(/by you/)).toBeInTheDocument()
  })

  it("still renders history when the member roster cannot be read", async () => {
    // The roster is a nicety; the history is the thing that was asked for. Losing the
    // former must not cost the latter.
    mockFetch.mockImplementation(async (url: string) => {
      if (url.includes("/members")) return res(403, { error: "nope" })
      if (url.includes("/versions")) {
        return res(200, { versions: VERSIONS, current: CURRENT, pending_edit: null })
      }
      return res(404, {})
    })
    renderPanel()

    await waitFor(() => expect(screen.getByRole("button", { name: "v2" })).toBeInTheDocument())
    expect(screen.getByText(/a former member/)).toBeInTheDocument()
  })

  it("says there is nothing to compare rather than showing an empty list", async () => {
    wire({ versions: [] })
    renderPanel()

    expect(await screen.findByText(/has not been edited since it was saved/i)).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /restore this version/i })).not.toBeInTheDocument()
  })

  describe("a proposed edit awaiting review", () => {
    it("shows a member what is pending but offers them no verdict", async () => {
      // The gate's teeth. A member may propose; only an admin may approve. If these
      // buttons rendered for a member the server would still refuse — but the UI would
      // have told them the rule is something other than what it is.
      mockRole.value = "member"
      wire({ pending_edit: pendingEdit() })
      renderPanel()

      expect(await screen.findByText(/waiting for approval/i)).toBeInTheDocument()
      expect(screen.getByText(/APAC was the wrong region/)).toBeInTheDocument()
      expect(screen.getByText(/a workspace admin has to approve this/i)).toBeInTheDocument()
      expect(screen.queryByRole("button", { name: /approve and apply/i })).not.toBeInTheDocument()
      expect(screen.queryByRole("button", { name: /^reject$/i })).not.toBeInTheDocument()
    })

    it("diffs the proposal against what is running, not against nothing", async () => {
      mockRole.value = "admin"
      wire({ pending_edit: pendingEdit() })
      renderPanel()

      const section = (await screen.findByText(/waiting for approval/i)).closest("section")!
      // Direction matters and is asserted, not just presence: base is what runs today
      // and next is the proposal, so the diff shows what approving would do. Reversed,
      // every addition would read as a removal.
      expect(
        within(section).getByText("Line-by-line differences between Running now and Proposed"),
      ).toBeInTheDocument()
      expect(within(section).getByText(/WHERE region = 'emea'/)).toBeInTheDocument()
      expect(within(section).getByText(/WHERE region = 'apac'/)).toBeInTheDocument()
    })

    it("warns before an admin approves a proposal the query has moved past", async () => {
      mockRole.value = "admin"
      wire({ pending_edit: pendingEdit({ stale: true }) })
      renderPanel()

      expect(await screen.findByText(/the query changed after this was proposed/i)).toBeInTheDocument()
      expect(screen.getByText(/including undoing whatever landed in between/i)).toBeInTheDocument()
    })

    it("tells an admin when the proposal under review is their own", async () => {
      mockRole.value = "admin"
      mockUser.value = SOMEONE_ELSE
      wire({ pending_edit: pendingEdit({ proposed_by: SOMEONE_ELSE }) })
      renderPanel()

      expect(await screen.findByText(/this is your own proposal/i)).toBeInTheDocument()
      // Still approvable: a single-admin workspace would otherwise be unable to edit
      // its own scheduled queries at all.
      expect(screen.getByRole("button", { name: /approve and apply/i })).toBeEnabled()
    })

    it("posts the verdict and the reviewer's note to the approve endpoint", async () => {
      mockRole.value = "admin"
      wire({ pending_edit: pendingEdit() })
      const user = userEvent.setup()
      renderPanel()

      await user.type(await screen.findByLabelText(/note for the proposer/i), "Checked the region codes")
      await user.click(screen.getByRole("button", { name: /approve and apply/i }))

      await waitFor(() => {
        const call = mockFetch.mock.calls.find((c: unknown[]) => String(c[0]).includes("/pending/approve"))
        expect(call).toBeTruthy()
        expect(JSON.parse(String((call![1] as RequestInit).body))).toEqual({
          note: "Checked the region codes",
        })
      })
      expect(toast.success).toHaveBeenCalled()
    })

    it("posts to the reject endpoint and keeps the proposal as history", async () => {
      mockRole.value = "admin"
      wire({ pending_edit: pendingEdit() })
      const user = userEvent.setup()
      renderPanel()

      await user.click(await screen.findByRole("button", { name: /^reject$/i }))

      await waitFor(() => {
        expect(
          mockFetch.mock.calls.some((c: unknown[]) => String(c[0]).includes("/pending/reject")),
        ).toBe(true)
      })
      const [message] = (toast.success as unknown as ReturnType<typeof vi.fn>).mock.calls[0]
      expect(message).toMatch(/unchanged/i)
    })

    it("reports the server's reason when a verdict is refused", async () => {
      mockRole.value = "admin"
      mockFetch.mockImplementation(async (url: string) => {
        if (url.includes("/members")) return res(200, { members: [] })
        if (url.includes("/versions")) {
          return res(200, { versions: VERSIONS, current: CURRENT, pending_edit: pendingEdit() })
        }
        return res(403, { error: "admin role required" })
      })
      const user = userEvent.setup()
      renderPanel()

      await user.click(await screen.findByRole("button", { name: /approve and apply/i }))

      await waitFor(() => expect(toast.error).toHaveBeenCalledWith("admin role required"))
      expect(toast.success).not.toHaveBeenCalled()
    })
  })

  describe("restoring an earlier version", () => {
    it("sends the older SQL through the ordinary edit path", async () => {
      // Not a rewind endpoint. Going through PATCH is what keeps the history
      // append-only — the restore snapshots the text it replaced, so it is undoable.
      const bodies: string[] = []
      wire({}, (body) => {
        bodies.push(body)
        return { id: QUERY_ID }
      })
      const user = userEvent.setup()
      renderPanel()

      await user.click(await screen.findByRole("button", { name: "v1" }))
      await user.click(screen.getByRole("button", { name: /restore this version/i }))

      await waitFor(() => expect(bodies).toHaveLength(1))
      const body = JSON.parse(bodies[0])
      expect(body.sql_text).toBe("SELECT 1")
      expect(body.note).toMatch(/restore of version 1/i)
      expect(toast.success).toHaveBeenCalledWith("Restored version 1.")
    })

    it("never claims a restore happened when it was parked for approval", async () => {
      // The same lie the edit dialog must not tell, from the other entry point: the
      // server answers 200 with pending_approval, and "Restored version 1" would send
      // someone away believing the schedule reverted. It has not.
      wire({}, () => ({
        pending_approval: { id: "p1", statement_class: "read", reason: "Needs an admin's approval." },
      }))
      const user = userEvent.setup()
      renderPanel()

      await user.click(await screen.findByRole("button", { name: "v1" }))
      await user.click(screen.getByRole("button", { name: /restore this version/i }))

      await waitFor(() => expect(toast.success).toHaveBeenCalled())
      const [message] = (toast.success as unknown as ReturnType<typeof vi.fn>).mock.calls[0]
      expect(message).toMatch(/submitted for approval/i)
      expect(message).not.toMatch(/^Restored/)
    })

    it("does not offer to restore the version that is already running", async () => {
      wire({
        versions: [{ ...VERSIONS[0], sql_text: CURRENT.sql_text }],
      })
      renderPanel()

      await waitFor(() => expect(screen.getByRole("button", { name: "v2" })).toBeInTheDocument())
      expect(screen.getByRole("button", { name: /restore this version/i })).toBeDisabled()
      expect(screen.getByText(/character-for-character the same/i)).toBeInTheDocument()
    })
  })

  it("does not fall through to an empty panel when history cannot be read", async () => {
    mockFetch.mockImplementation(async (url: string) => {
      if (url.includes("/members")) return res(200, { members: [] })
      return res(500, { error: "database is unavailable" })
    })
    renderPanel()

    expect(await screen.findByText(/could not load the history/i)).toBeInTheDocument()
    expect(screen.getByText(/the query itself has not been changed/i)).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument()
  })
})
