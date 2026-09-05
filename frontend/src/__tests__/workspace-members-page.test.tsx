import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, waitFor, act } from "@testing-library/react"
import type { Mock } from "vitest"

import WorkspaceMembersPage from "@/app/(dashboard)/workspace/members/page"
import { authFetch } from "@/lib/api/auth-fetch"
import { ACTIVE_WORKSPACE_KEY, setActiveWorkspaceId } from "@/lib/workspace/active-workspace"

// P5b-UI #1 (review remediation): the /workspace/members page resolves the ACTIVE
// workspace and forwards its role to the gated component. It must (a) pick the saved
// id over list[0], (b) re-resolve when the active workspace is switched in-place
// (else it shows stale data and an invite would target the OLD workspace), and
// (c) distinguish a failed workspace-list fetch from a genuinely empty list.
// We use the REAL active-workspace module so the switch event wiring is exercised.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
// The rendered WorkspaceMembers now calls useRouter() (delete-workspace navigation),
// which throws outside the app-router provider in jsdom — stub it.
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }) }))

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  if (!Element.prototype.hasPointerCapture) Element.prototype.hasPointerCapture = () => false
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {}
})

// jsdom's localStorage is not fully functional under this harness, so install a
// minimal in-memory Storage before each test (same pattern as active-workspace.test).
function installMemoryStorage() {
  const store: Record<string, string> = {}
  const ls: Storage = {
    getItem: (k) => (k in store ? store[k] : null),
    setItem: (k, v) => {
      store[k] = String(v)
    },
    removeItem: (k) => {
      delete store[k]
    },
    clear: () => {
      for (const k of Object.keys(store)) delete store[k]
    },
    key: (i) => Object.keys(store)[i] ?? null,
    get length() {
      return Object.keys(store).length
    },
  }
  Object.defineProperty(window, "localStorage", { value: ls, configurable: true, writable: true })
}

interface WsFixture {
  id: string
  name: string
  role: string
}

function installPageFetch(workspaces: WsFixture[]) {
  ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
    const u = String(url)
    const method = (opts?.method ?? "GET").toUpperCase()
    if (/\/workspaces$/.test(u) && method === "GET") {
      return { ok: true, status: 200, json: async () => ({ workspaces }) }
    }
    if (u.includes("/members")) {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          members: [{ id: "self", user_id: "me", email: "me@acme.com", role: "owner", created_at: "2026-01-01T00:00:00Z" }],
        }),
      }
    }
    if (u.includes("/invites") && method === "GET") {
      return { ok: true, status: 200, json: async () => ({ invites: [] }) }
    }
    return { ok: true, status: 200, json: async () => ({}) }
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  installMemoryStorage()
  document.cookie = `${ACTIVE_WORKSPACE_KEY}=; path=/; max-age=0; SameSite=Lax`
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe("WorkspaceMembersPage", () => {
  it("resolves the saved active workspace (not list[0]) and re-resolves on an in-place switch", async () => {
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-a")
    installPageFetch([
      { id: "ws-b", name: "Beta", role: "admin" }, // list[0] — must NOT be chosen
      { id: "ws-a", name: "Alpha", role: "viewer" }, // saved — must be chosen
    ])

    render(<WorkspaceMembersPage />)
    await screen.findByText("Members")

    // Saved workspace (viewer) resolved → no invite button. Kills "always list[0]"
    // (which would be admin and show the button).
    await waitFor(() =>
      expect(screen.queryByRole("button", { name: /invite member/i })).toBeNull(),
    )

    // Switch active workspace in-place (as the header switcher does). The page must
    // re-resolve to ws-b (admin); otherwise it stays on stale ws-a and any invite
    // would target the OLD workspace.
    await act(async () => {
      setActiveWorkspaceId("ws-b")
    })
    await waitFor(() =>
      expect(screen.getByRole("button", { name: /invite member/i })).toBeInTheDocument(),
    )
  })

  it("falls back to the first workspace when nothing is saved", async () => {
    installPageFetch([
      { id: "ws-b", name: "Beta", role: "admin" },
      { id: "ws-a", name: "Alpha", role: "viewer" },
    ])

    render(<WorkspaceMembersPage />)
    await screen.findByText("Members")
    // list[0] is admin → invite action visible (role forwarded from the chosen ws).
    expect(screen.getByRole("button", { name: /invite member/i })).toBeInTheDocument()
  })

  it("shows an error with retry when the workspace list fails to load", async () => {
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (/\/workspaces$/.test(u) && method === "GET") return { ok: false, status: 500, json: async () => ({}) }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<WorkspaceMembersPage />)

    // A failed LIST must read as an error + retry, NOT "No workspace selected"
    // (which misdirects the user to the equally-broken switcher).
    expect(await screen.findByText(/couldn.t load your workspaces/i)).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument()
    expect(screen.queryByText(/no workspace selected/i)).toBeNull()
  })
})
