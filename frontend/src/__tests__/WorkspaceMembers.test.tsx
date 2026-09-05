import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, fireEvent, waitFor } from "@testing-library/react"
import type { Mock } from "vitest"

import { WorkspaceMembers } from "@/components/workspace/WorkspaceMembers"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"

// P5b-UI #1: the admin surface for multi-user. Lists the active workspace's
// members + pending invites and lets an admin/owner invite a teammate. The
// backend endpoints (members/invites CRUD) already exist; nothing surfaced them.
// We mock authFetch (the central client wrapper) so the component logic is the
// unit under test, and sonner so toasts are assertable.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

// P5b-UI #3: deleting a workspace navigates away and re-points the active selection.
// The component newly calls useRouter() and the active-workspace setters, so mock the
// router and spy on the setters — while keeping the REAL pure helper
// (resolveActiveAfterDelete) so its decision is exercised, not stubbed.
const { pushMock, getActiveMock, setActiveMock } = vi.hoisted(() => ({
  pushMock: vi.fn(),
  getActiveMock: vi.fn(),
  setActiveMock: vi.fn(),
}))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: pushMock, refresh: vi.fn() }),
}))
vi.mock("@/lib/workspace/active-workspace", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/workspace/active-workspace")>()
  return { ...actual, getActiveWorkspaceId: getActiveMock, setActiveWorkspaceId: setActiveMock }
})

import { toast } from "sonner"

// Radix primitives probe these jsdom-absent APIs; stub so render never throws.
beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  if (!Element.prototype.hasPointerCapture) {
    Element.prototype.hasPointerCapture = () => false
  }
  if (!Element.prototype.setPointerCapture) {
    Element.prototype.setPointerCapture = () => {}
  }
  if (!Element.prototype.releasePointerCapture) {
    Element.prototype.releasePointerCapture = () => {}
  }
  if (!Element.prototype.scrollIntoView) {
    Element.prototype.scrollIntoView = () => {}
  }
})

const WS_ID = "ws-1"

const MEMBERS = [
  { id: "m1", workspace_id: WS_ID, user_id: "u1", email: "alice@acme.com", role: "owner", created_at: "2026-01-01T00:00:00Z" },
  { id: "m2", workspace_id: WS_ID, user_id: "u2", email: "dave@acme.com", role: "member", created_at: "2026-02-01T00:00:00Z" },
]

const INVITES = [
  { id: "i1", workspace_id: WS_ID, email: "bob@acme.com", role: "member", status: "pending", expires_at: "2026-12-01T00:00:00Z", created_at: "2026-06-01T00:00:00Z" },
  // A second pending invite so revoke can be proven row-scoped (uses the opened
  // row's id, not pendingInvites[0]).
  { id: "i3", workspace_id: WS_ID, email: "frank@acme.com", role: "viewer", status: "pending", expires_at: "2026-12-05T00:00:00Z", created_at: "2026-06-05T00:00:00Z" },
  // A non-pending invite the backend still returns (ListWorkspaceInvites has no
  // status filter); the Pending table must exclude it.
  { id: "i9", workspace_id: WS_ID, email: "revoked@acme.com", role: "member", status: "revoked", expires_at: "2026-12-01T00:00:00Z", created_at: "2026-05-01T00:00:00Z" },
]

// The current user id returned by GET /auth/me; tests mutate this before render to
// exercise the self-guard (own row) vs managing others.
let meUserId = "u-admin"

function installFetch() {
  ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
    const u = String(url)
    const method = (opts?.method ?? "GET").toUpperCase()
    if (u.includes("/auth/me")) {
      return { ok: true, status: 200, json: async () => ({ user_id: meUserId, email: "me@acme.com" }) }
    }
    // Per-member mutations — match BEFORE the generic /members list branch.
    if (/\/members\/[^/]+\/role$/.test(u) && method === "POST") {
      return { ok: true, status: 200, json: async () => ({ user_id: "u2", role: "admin", status: "updated" }) }
    }
    if (/\/members\/[^/]+$/.test(u) && method === "DELETE") {
      return { ok: true, status: 200, json: async () => ({ user_id: "u2", status: "removed" }) }
    }
    if (/\/invites\/[^/]+$/.test(u) && method === "DELETE") {
      return { ok: true, status: 200, json: async () => ({ status: "revoked" }) }
    }
    if (u.includes("/members") && method === "GET") {
      return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
    }
    if (u.includes("/invites") && method === "GET") {
      return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
    }
    if (u.includes("/invites") && method === "POST") {
      return {
        ok: true,
        status: 201,
        json: async () => ({
          id: "i2",
          workspace_id: WS_ID,
          email: "carol@acme.com",
          role: "member",
          status: "pending",
          accept_url: "http://localhost:3000/invite/tok-xyz",
          expires_at: "2026-12-09T00:00:00Z",
          created_at: "2026-06-09T00:00:00Z",
        }),
      }
    }
    // Workspace-level: DELETE /workspaces/:id (204) and the LIST used to re-point
    // the active selection after a delete (returns survivors, ws-1 already gone).
    if (/\/workspaces\/[^/]+$/.test(u) && method === "DELETE") {
      return { ok: true, status: 204, json: async () => ({}) }
    }
    if (/\/workspaces$/.test(u) && method === "GET") {
      return {
        ok: true,
        status: 200,
        json: async () => ({ workspaces: [{ id: "ws-2", name: "Team Two" }, { id: "ws-personal", name: "Personal" }] }),
      }
    }
    return { ok: true, status: 200, json: async () => ({}) }
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  meUserId = "u-admin"
  // Default: the workspace being managed (ws-1) is also the active one — the members
  // page only ever renders the active workspace. Tests override for the off-path.
  getActiveMock.mockReturnValue("ws-1")
  installFetch()
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe("WorkspaceMembers", () => {
  it("lists the workspace members", async () => {
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    expect(await screen.findByText("alice@acme.com")).toBeInTheDocument()
    expect(screen.getByText("dave@acme.com")).toBeInTheDocument()
    expect(authFetch).toHaveBeenCalledWith(API_ENDPOINTS.WORKSPACES.MEMBERS(WS_ID))
    // owner outranks admin, so the >= gate must still surface the invite action
    // (pins canManage = roleRank >= roleRank("admin") at the owner boundary).
    expect(screen.getByRole("button", { name: /invite member/i })).toBeInTheDocument()
  })

  it("lists pending invites", async () => {
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    expect(await screen.findByText("bob@acme.com")).toBeInTheDocument()
    expect(screen.getByText("frank@acme.com")).toBeInTheDocument()
    // exact "pending" = the status badge (the card title is "Pending invitations"),
    // one per pending row
    expect(screen.getAllByText("pending")).toHaveLength(2)
    // the revoked invite must NOT appear in the Pending table (filter is load-bearing:
    // the backend returns non-pending invites too)
    expect(screen.queryByText("revoked@acme.com")).toBeNull()
  })

  it("hides the invite action from members and viewers", async () => {
    const { rerender } = render(
      <WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="member" />,
    )
    await screen.findByText("alice@acme.com")
    expect(screen.queryByRole("button", { name: /invite member/i })).toBeNull()
    // ...and no per-row management menu either (canManage gates the actions column)
    expect(screen.queryByRole("button", { name: /manage dave@acme\.com/i })).toBeNull()

    rerender(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="viewer" />)
    await screen.findByText("alice@acme.com")
    expect(screen.queryByRole("button", { name: /invite member/i })).toBeNull()
    expect(screen.queryByRole("button", { name: /manage dave@acme\.com/i })).toBeNull()
  })

  it("shows the invite action to admins", async () => {
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("alice@acme.com")
    expect(screen.getByRole("button", { name: /invite member/i })).toBeInTheDocument()
  })

  it("creates an invite and surfaces the shareable accept URL", async () => {
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("alice@acme.com")

    fireEvent.click(screen.getByRole("button", { name: /invite member/i }))

    const emailInput = await screen.findByPlaceholderText(/teammate@/i)
    fireEvent.change(emailInput, { target: { value: "carol@acme.com" } })
    fireEvent.click(screen.getByRole("button", { name: /send invite/i }))

    // POSTs to the invites endpoint with the email + a role.
    await waitFor(() => {
      const postCall = (authFetch as Mock).mock.calls.find(
        (c) => String(c[0]).includes("/invites") && (c[1]?.method ?? "").toUpperCase() === "POST",
      )
      expect(postCall).toBeTruthy()
      const body = JSON.parse(String(postCall![1].body))
      expect(body.email).toBe("carol@acme.com")
      expect(body.role).toBe("member")
    })

    // The accept URL is shown (email may be unconfigured server-side, so it is the
    // guaranteed share channel) and a success toast fires.
    expect(await screen.findByDisplayValue("http://localhost:3000/invite/tok-xyz")).toBeInTheDocument()
    expect(toast.success).toHaveBeenCalled()

    // The pending table is refreshed after a successful invite: a second GET /invites
    // fires (mount + post-success refetch). Pins the void loadInvites() on success.
    await waitFor(() => {
      const getInvites = (authFetch as Mock).mock.calls.filter(
        (c) => /\/invites$/.test(String(c[0])) && (c[1]?.method ?? "GET").toUpperCase() === "GET",
      )
      expect(getInvites.length).toBeGreaterThanOrEqual(2)
    })
  })

  it("invites with the role chosen in the Select, not just the default", async () => {
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("alice@acme.com")

    fireEvent.click(screen.getByRole("button", { name: /invite member/i }))
    fireEvent.change(await screen.findByPlaceholderText(/teammate@/i), {
      target: { value: "carol@acme.com" },
    })

    // Open the Radix Select via keyboard (reliable in jsdom) and pick a non-default role.
    const roleTrigger = screen.getByRole("combobox")
    roleTrigger.focus()
    fireEvent.keyDown(roleTrigger, { key: "Enter" })
    fireEvent.click(await screen.findByRole("option", { name: "Admin" }))

    fireEvent.click(screen.getByRole("button", { name: /send invite/i }))

    // The POST must carry the SELECTED role, not the 'member' default — this pins
    // onValueChange={setInviteRole} and the `role: inviteRole` body field.
    await waitFor(() => {
      const postCall = (authFetch as Mock).mock.calls.find(
        (c) => String(c[0]).includes("/invites") && (c[1]?.method ?? "").toUpperCase() === "POST",
      )
      expect(postCall).toBeTruthy()
      const body = JSON.parse(String(postCall![1].body))
      expect(body.role).toBe("admin")
    })
  })

  it("shows an error with retry when members fail to load", async () => {
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/members")) return { ok: false, status: 500, json: async () => ({}) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: [] }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)

    // A failed load must surface a real error + retry, NOT the benign "No members yet."
    expect(await screen.findByText(/couldn.t load members/i)).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument()
    expect(screen.queryByText("No members yet.")).toBeNull()
  })

  it("surfaces an error toast when the invite fails", async () => {
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/members")) return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
      if (u.includes("/invites") && method === "POST")
        return { ok: false, status: 409, json: async () => ({ error: "already a member" }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("alice@acme.com")
    fireEvent.click(screen.getByRole("button", { name: /invite member/i }))
    fireEvent.change(await screen.findByPlaceholderText(/teammate@/i), { target: { value: "alice@acme.com" } })
    fireEvent.click(screen.getByRole("button", { name: /send invite/i }))

    await waitFor(() => expect(toast.error).toHaveBeenCalledWith("already a member"))
  })

  // --- P5b-UI #1 mutations: change role / remove member / revoke invite ---

  async function openRowMenu(name: RegExp) {
    const trigger = await screen.findByRole("button", { name })
    trigger.focus()
    fireEvent.keyDown(trigger, { key: "Enter" })
  }

  it("changes a member's role via the row actions menu", async () => {
    meUserId = "u-admin" // not in the member list → no self-row collision
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("dave@acme.com")

    await openRowMenu(/manage dave@acme\.com/i)
    fireEvent.click(await screen.findByRole("menuitem", { name: /make admin/i }))

    // POSTs the new role to the per-member role endpoint for dave (u2).
    await waitFor(() => {
      const call = (authFetch as Mock).mock.calls.find(
        (c) => /\/members\/u2\/role$/.test(String(c[0])) && (c[1]?.method ?? "").toUpperCase() === "POST",
      )
      expect(call).toBeTruthy()
      expect(JSON.parse(String(call![1].body)).role).toBe("admin")
    })
    expect(toast.success).toHaveBeenCalled()
  })

  it("removes a member after confirming in the alert dialog", async () => {
    meUserId = "u-admin"
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("dave@acme.com")

    await openRowMenu(/manage dave@acme\.com/i)
    fireEvent.click(await screen.findByRole("menuitem", { name: /remove from workspace/i }))
    // confirm step (destructive) — the action button is distinct from the menu item
    fireEvent.click(await screen.findByRole("button", { name: /^remove member$/i }))

    await waitFor(() => {
      const call = (authFetch as Mock).mock.calls.find(
        (c) => /\/members\/u2$/.test(String(c[0])) && (c[1]?.method ?? "").toUpperCase() === "DELETE",
      )
      expect(call).toBeTruthy()
    })
    expect(toast.success).toHaveBeenCalled()
    // on success the confirm dialog closes (pins setConfirmAction(null) on the ok path)
    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull())
  })

  it("revokes a pending invite after confirming (row-scoped to the opened row)", async () => {
    meUserId = "u-admin"
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("frank@acme.com")

    // open the SECOND pending invite's menu — proves the id is the opened row's, not [0]
    await openRowMenu(/manage invite for frank@acme\.com/i)
    fireEvent.click(await screen.findByRole("menuitem", { name: /revoke invite/i }))
    fireEvent.click(await screen.findByRole("button", { name: /^revoke invite$/i }))

    await waitFor(() => {
      const call = (authFetch as Mock).mock.calls.find(
        (c) => /\/invites\/i3$/.test(String(c[0])) && (c[1]?.method ?? "").toUpperCase() === "DELETE",
      )
      expect(call).toBeTruthy()
    })
    // and NOT the first pending invite
    expect(
      (authFetch as Mock).mock.calls.find(
        (c) => /\/invites\/i1$/.test(String(c[0])) && (c[1]?.method ?? "").toUpperCase() === "DELETE",
      ),
    ).toBeUndefined()
    expect(toast.success).toHaveBeenCalled()
  })

  it("hides row actions for the current user's own row (self-guard) but shows them for others", async () => {
    meUserId = "u1" // alice (owner) is the current user
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("alice@acme.com")

    // self row: no management menu
    await waitFor(() => expect(screen.queryByRole("button", { name: /manage alice@acme\.com/i })).toBeNull())
    // another member: owner can manage them
    expect(screen.getByRole("button", { name: /manage dave@acme\.com/i })).toBeInTheDocument()
  })

  it("hides actions on an owner row for a non-owner admin (owner-only guard)", async () => {
    meUserId = "u-admin"
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("alice@acme.com")

    // alice is an owner; a mere admin cannot manage an owner
    await waitFor(() => expect(screen.queryByRole("button", { name: /manage alice@acme\.com/i })).toBeNull())
    // dave (member) is manageable by the admin
    expect(screen.getByRole("button", { name: /manage dave@acme\.com/i })).toBeInTheDocument()
  })

  it("surfaces the backend error when a mutation fails (e.g. last-owner guard)", async () => {
    meUserId = "u-admin"
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/auth/me")) return { ok: true, status: 200, json: async () => ({ user_id: "u-admin", email: "me@acme.com" }) }
      if (/\/members\/[^/]+$/.test(u) && method === "DELETE")
        return { ok: false, status: 409, json: async () => ({ error: "workspace must have at least one owner" }) }
      if (u.includes("/members") && method === "GET") return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("dave@acme.com")

    await openRowMenu(/manage dave@acme\.com/i)
    fireEvent.click(await screen.findByRole("menuitem", { name: /remove from workspace/i }))
    fireEvent.click(await screen.findByRole("button", { name: /^remove member$/i }))

    await waitFor(() => expect(toast.error).toHaveBeenCalledWith("workspace must have at least one owner"))
    // on error the confirm dialog STAYS open (pins e.preventDefault() on AlertDialogAction)
    expect(screen.getByRole("alertdialog")).toBeInTheDocument()
  })

  it("surfaces the backend error when a role change fails", async () => {
    meUserId = "u-admin"
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/auth/me")) return { ok: true, status: 200, json: async () => ({ user_id: "u-admin", email: "me@acme.com" }) }
      if (/\/members\/[^/]+\/role$/.test(u) && method === "POST")
        return { ok: false, status: 409, json: async () => ({ error: "workspace must have at least one owner" }) }
      if (u.includes("/members") && method === "GET") return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("dave@acme.com")

    await openRowMenu(/manage dave@acme\.com/i)
    fireEvent.click(await screen.findByRole("menuitem", { name: /make admin/i }))

    await waitFor(() => expect(toast.error).toHaveBeenCalledWith("workspace must have at least one owner"))
  })

  it("offers 'Make Owner' to an owner caller", async () => {
    meUserId = "u-admin"
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("dave@acme.com")

    await openRowMenu(/manage dave@acme\.com/i)
    expect(await screen.findByRole("menuitem", { name: /make owner/i })).toBeInTheDocument()
  })

  it("does NOT offer 'Make Owner' to a non-owner admin caller", async () => {
    meUserId = "u-admin"
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("dave@acme.com")

    await openRowMenu(/manage dave@acme\.com/i)
    // menu is open (a non-owner role item is present) but "Make Owner" must be absent
    expect(await screen.findByRole("menuitem", { name: /make admin/i })).toBeInTheDocument()
    expect(screen.queryByRole("menuitem", { name: /make owner/i })).toBeNull()
  })

  it("cancelling the destructive confirm dialog does not mutate", async () => {
    meUserId = "u-admin"
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("dave@acme.com")

    await openRowMenu(/manage dave@acme\.com/i)
    fireEvent.click(await screen.findByRole("menuitem", { name: /remove from workspace/i }))
    fireEvent.click(await screen.findByRole("button", { name: /^cancel$/i }))

    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull())
    expect(
      (authFetch as Mock).mock.calls.find(
        (c) => /\/members\/u2$/.test(String(c[0])) && (c[1]?.method ?? "").toUpperCase() === "DELETE",
      ),
    ).toBeUndefined()
  })

  it("fails closed: hides member-row actions when the current user is unknown (/auth/me fails)", async () => {
    meUserId = "u-admin"
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/auth/me")) return { ok: false, status: 401, json: async () => ({}) }
      if (u.includes("/members") && method === "GET") return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("dave@acme.com")

    // identity unknown → no member-row management menu (can't reliably honor the self-guard)
    await waitFor(() => expect(screen.queryByRole("button", { name: /manage dave@acme\.com/i })).toBeNull())
  })

  it("disables a member's actions trigger while a role change is in flight (re-entrancy guard)", async () => {
    meUserId = "u-admin"
    let resolveRolePost: (v: unknown) => void = () => {}
    const rolePost = new Promise((r) => {
      resolveRolePost = r
    })
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/auth/me")) return { ok: true, status: 200, json: async () => ({ user_id: "u-admin", email: "me@acme.com" }) }
      if (/\/members\/[^/]+\/role$/.test(u) && method === "POST") {
        await rolePost // hang until the test releases it
        return { ok: true, status: 200, json: async () => ({ user_id: "u2", role: "admin", status: "updated" }) }
      }
      if (u.includes("/members") && method === "GET") return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("dave@acme.com")

    await openRowMenu(/manage dave@acme\.com/i)
    fireEvent.click(await screen.findByRole("menuitem", { name: /make admin/i }))

    // while the POST is in flight the row's trigger is disabled (no duplicate POST)
    await waitFor(() => expect(screen.getByRole("button", { name: /manage dave@acme\.com/i })).toBeDisabled())

    resolveRolePost(undefined)
    await waitFor(() => expect(screen.getByRole("button", { name: /manage dave@acme\.com/i })).not.toBeDisabled())
  })

  // --- P5b-UI #3: delete workspace + re-point the active selection ---

  it("shows the Delete workspace control only to owners", async () => {
    const { rerender } = render(
      <WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />,
    )
    await screen.findByText("alice@acme.com")
    // a non-owner admin must not see a delete control (the backend is owner-only)
    expect(screen.queryByRole("button", { name: /delete workspace/i })).toBeNull()

    rerender(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("alice@acme.com")
    expect(screen.getByRole("button", { name: /delete workspace/i })).toBeInTheDocument()
  })

  it("keeps the permanent-delete button disabled until the workspace name is typed exactly", async () => {
    getActiveMock.mockReturnValue("ws-1")
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("alice@acme.com")

    fireEvent.click(screen.getByRole("button", { name: /delete workspace/i }))
    const confirm = await screen.findByRole("button", { name: /delete permanently/i })
    // armed only by the exact name — a mis-click can't delete
    expect(confirm).toBeDisabled()

    const input = screen.getByLabelText(/type .*to confirm/i)
    fireEvent.change(input, { target: { value: "Acm" } }) // partial
    expect(confirm).toBeDisabled()

    fireEvent.change(input, { target: { value: "Acme" } }) // exact
    expect(confirm).toBeEnabled()
  })

  it("deletes the workspace and re-points the active selection to a surviving workspace", async () => {
    getActiveMock.mockReturnValue("ws-1") // ws-1 (this page's workspace) is the ACTIVE one
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("alice@acme.com")

    fireEvent.click(screen.getByRole("button", { name: /delete workspace/i }))
    // confirm — type the workspace name to arm the destructive action, then confirm
    fireEvent.change(await screen.findByLabelText(/type .*to confirm/i), { target: { value: "Acme" } })
    fireEvent.click(screen.getByRole("button", { name: /delete permanently/i }))

    // DELETEs the workspace by id
    await waitFor(() => {
      const call = (authFetch as Mock).mock.calls.find(
        (c) => /\/workspaces\/ws-1$/.test(String(c[0])) && (c[1]?.method ?? "").toUpperCase() === "DELETE",
      )
      expect(call).toBeTruthy()
    })
    // ws-1 was active → re-point to the first surviving workspace (ws-2)
    await waitFor(() => expect(setActiveMock).toHaveBeenCalledWith("ws-2"))
    expect(toast.success).toHaveBeenCalled()
    // and leaves the now-deleted workspace's members page
    expect(pushMock).toHaveBeenCalledWith("/")
  })

  it("does NOT change the active selection when the deleted workspace was not the active one", async () => {
    getActiveMock.mockReturnValue("ws-other") // a DIFFERENT workspace is active
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("alice@acme.com")

    fireEvent.click(screen.getByRole("button", { name: /delete workspace/i }))
    fireEvent.change(await screen.findByLabelText(/type .*to confirm/i), { target: { value: "Acme" } })
    fireEvent.click(screen.getByRole("button", { name: /delete permanently/i }))

    await waitFor(() => expect(toast.success).toHaveBeenCalled())
    // ws-1 wasn't active → the active selection is left alone
    expect(setActiveMock).not.toHaveBeenCalled()
    expect(pushMock).toHaveBeenCalledWith("/")
  })

  it("surfaces the backend error and stays put when deletion is blocked (e.g. non-empty)", async () => {
    getActiveMock.mockReturnValue("ws-1")
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/auth/me")) return { ok: true, status: 200, json: async () => ({ user_id: "u-admin", email: "me@acme.com" }) }
      if (/\/workspaces\/[^/]+$/.test(u) && method === "DELETE")
        return { ok: false, status: 409, json: async () => ({ error: "workspace not empty; remove all pipelines and connections first" }) }
      if (u.includes("/members") && method === "GET") return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("alice@acme.com")

    fireEvent.click(screen.getByRole("button", { name: /delete workspace/i }))
    fireEvent.change(await screen.findByLabelText(/type .*to confirm/i), { target: { value: "Acme" } })
    fireEvent.click(screen.getByRole("button", { name: /delete permanently/i }))

    await waitFor(() =>
      expect(toast.error).toHaveBeenCalledWith("workspace not empty; remove all pipelines and connections first"),
    )
    // blocked → selection untouched, no navigation, and the confirm dialog stays open
    expect(setActiveMock).not.toHaveBeenCalled()
    expect(pushMock).not.toHaveBeenCalled()
    expect(screen.getByRole("alertdialog")).toBeInTheDocument()
  })

  it("guards against a double-submit while the delete is in flight", async () => {
    getActiveMock.mockReturnValue("ws-1")
    let release: (v?: unknown) => void = () => {}
    const gate = new Promise((r) => {
      release = r
    })
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/auth/me")) return { ok: true, status: 200, json: async () => ({ user_id: "u-admin", email: "me@acme.com" }) }
      if (/\/workspaces\/[^/]+$/.test(u) && method === "DELETE") {
        await gate // hang the DELETE until the test releases it
        return { ok: true, status: 204, json: async () => ({}) }
      }
      if (/\/workspaces$/.test(u) && method === "GET")
        return { ok: true, status: 200, json: async () => ({ workspaces: [{ id: "ws-2", name: "Team Two" }] }) }
      if (u.includes("/members") && method === "GET") return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("alice@acme.com")

    fireEvent.click(screen.getByRole("button", { name: /delete workspace/i }))
    fireEvent.change(await screen.findByLabelText(/type .*to confirm/i), { target: { value: "Acme" } })
    const confirm = await screen.findByRole("button", { name: /delete permanently/i })
    fireEvent.click(confirm)
    // the action disables while the DELETE is in flight (UI-level half of the guard)
    await waitFor(() => expect(screen.getByRole("button", { name: /delete permanently/i })).toBeDisabled())
    fireEvent.click(confirm) // second click while the first DELETE is still in flight
    release()

    await waitFor(() => expect(setActiveMock).toHaveBeenCalledWith("ws-2"))
    // and the in-flight guard (deletingWorkspace) ensures exactly one DELETE fired
    const deletes = (authFetch as Mock).mock.calls.filter(
      (c) => /\/workspaces\/ws-1$/.test(String(c[0])) && (c[1]?.method ?? "").toUpperCase() === "DELETE",
    )
    expect(deletes).toHaveLength(1)
  })

  it("surfaces a 403 and stays put when a non-owner delete is rejected by the backend", async () => {
    // Defence in depth: the control is owner-gated in the UI, but if the backend still
    // returns 403 the error must surface and nothing destructive (re-point / nav) runs.
    getActiveMock.mockReturnValue("ws-1")
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/auth/me")) return { ok: true, status: 200, json: async () => ({ user_id: "u-admin", email: "me@acme.com" }) }
      if (/\/workspaces\/[^/]+$/.test(u) && method === "DELETE")
        return { ok: false, status: 403, json: async () => ({ error: "forbidden" }) }
      if (u.includes("/members") && method === "GET") return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("alice@acme.com")

    fireEvent.click(screen.getByRole("button", { name: /delete workspace/i }))
    fireEvent.change(await screen.findByLabelText(/type .*to confirm/i), { target: { value: "Acme" } })
    fireEvent.click(screen.getByRole("button", { name: /delete permanently/i }))

    await waitFor(() => expect(toast.error).toHaveBeenCalledWith("Only the workspace owner can delete it."))
    expect(setActiveMock).not.toHaveBeenCalled()
    expect(pushMock).not.toHaveBeenCalled()
    expect(screen.getByRole("alertdialog")).toBeInTheDocument()
  })

  it("clears the active selection when deleting the only remaining workspace", async () => {
    // No survivors (the post-delete list is empty) → resolveActiveAfterDelete returns
    // null, so the component clears the selection rather than leaving a tombstone.
    getActiveMock.mockReturnValue("ws-1")
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/auth/me")) return { ok: true, status: 200, json: async () => ({ user_id: "u-admin", email: "me@acme.com" }) }
      if (/\/workspaces\/[^/]+$/.test(u) && method === "DELETE") return { ok: true, status: 204, json: async () => ({}) }
      if (/\/workspaces$/.test(u) && method === "GET") return { ok: true, status: 200, json: async () => ({ workspaces: [] }) }
      if (u.includes("/members") && method === "GET") return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("alice@acme.com")

    fireEvent.click(screen.getByRole("button", { name: /delete workspace/i }))
    fireEvent.change(await screen.findByLabelText(/type .*to confirm/i), { target: { value: "Acme" } })
    fireEvent.click(screen.getByRole("button", { name: /delete permanently/i }))

    await waitFor(() => expect(setActiveMock).toHaveBeenCalledWith(null))
    expect(toast.success).toHaveBeenCalled()
    expect(pushMock).toHaveBeenCalledWith("/")
  })

  it("shows an actionable fallback when a 409 carries no backend message", async () => {
    // The backend normally sends a specific 409 message; if it is ever absent the
    // fallback must still tell the owner what to do. Saved queries are named
    // explicitly: the emptiness guard counts them (they cascade, taking their
    // schedules and run history), so a fallback that lists only connections and
    // pipelines sends the owner to clear both, retry, and hit the same 409 with no
    // idea what is still holding the workspace.
    getActiveMock.mockReturnValue("ws-1")
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (u.includes("/auth/me")) return { ok: true, status: 200, json: async () => ({ user_id: "u-admin", email: "me@acme.com" }) }
      if (/\/workspaces\/[^/]+$/.test(u) && method === "DELETE")
        return { ok: false, status: 409, json: async () => ({}) } // malformed: no error field
      if (u.includes("/members") && method === "GET") return { ok: true, status: 200, json: async () => ({ members: MEMBERS }) }
      if (u.includes("/invites") && method === "GET") return { ok: true, status: 200, json: async () => ({ invites: INVITES }) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="owner" />)
    await screen.findByText("alice@acme.com")

    fireEvent.click(screen.getByRole("button", { name: /delete workspace/i }))
    fireEvent.change(await screen.findByLabelText(/type .*to confirm/i), { target: { value: "Acme" } })
    fireEvent.click(screen.getByRole("button", { name: /delete permanently/i }))

    await waitFor(() =>
      expect(
        (toast.error as Mock).mock.calls.some((c) =>
          /connections, pipelines and saved queries/i.test(String(c[0])),
        ),
      ).toBe(true),
    )
  })
})

// A personal workspace is the auto-provisioned identity anchor: the backend refuses
// to modify it at all, so CreateWorkspaceInvite answers 409 "personal workspaces
// cannot be modified" BEFORE it even parses the email — no address can ever succeed.
// The Invite button used to be gated on role alone, so an owner of their own personal
// workspace saw it, filled in the whole dialog, and learned the rule from a server
// error. `isPersonal` was already a prop, consulted at exactly one place (the delete
// gate) one line away. These tests pin the invite path onto the same flag.
describe("WorkspaceMembers — personal workspace", () => {
  it("hides the invite control on a personal workspace and says where teammates go instead", async () => {
    render(
      <WorkspaceMembers workspaceId={WS_ID} workspaceName="Rahul" currentRole="owner" isPersonal />,
    )
    await screen.findByText("alice@acme.com")

    // Owner role would otherwise pass the canManage gate — isPersonal is what removes it.
    expect(screen.queryByRole("button", { name: /invite member/i })).toBeNull()
    // Dead-end UI is worse than no UI: the copy has to name the way forward.
    expect(
      screen.getByText(/create a team workspace from the workspace switcher/i),
    ).toBeInTheDocument()
    expect(screen.getByText(/personal workspace is just for you/i)).toBeInTheDocument()
    // Sibling gate, same flag: a personal workspace is undeletable (backend 409).
    expect(screen.queryByRole("button", { name: /delete workspace/i })).toBeNull()
  })

  it("shows the invite control on a team workspace for an admin", async () => {
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="admin" />)
    await screen.findByText("alice@acme.com")

    // Pins that the isPersonal gate did not tighten the normal path.
    expect(screen.getByRole("button", { name: /invite member/i })).toBeInTheDocument()
    expect(screen.queryByText(/create a team workspace from the workspace switcher/i)).toBeNull()
    expect(screen.getByText(/People with access to "Acme"/i)).toBeInTheDocument()
  })

  it("still hides the invite control from a viewer on a team workspace", async () => {
    render(<WorkspaceMembers workspaceId={WS_ID} workspaceName="Acme" currentRole="viewer" />)
    await screen.findByText("alice@acme.com")

    // canInvite = canManage && !isPersonal — the role half must keep working on its own,
    // and a viewer must not get the personal-workspace explanation, which would be wrong.
    expect(screen.queryByRole("button", { name: /invite member/i })).toBeNull()
    expect(screen.queryByText(/create a team workspace from the workspace switcher/i)).toBeNull()
  })
})
