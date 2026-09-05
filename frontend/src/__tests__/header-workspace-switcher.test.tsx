import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, waitFor, act, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import type { Mock } from "vitest"
import { toast } from "sonner"

import { Header } from "@/components/layout/Header"
import { authFetch } from "@/lib/api/auth-fetch"
import { ACTIVE_WORKSPACE_KEY, getActiveWorkspaceId, setActiveWorkspaceId } from "@/lib/workspace/active-workspace"

// P5b-UI #3 (final piece): when a workspace is deleted, the delete flow re-points the
// active selection (setActiveWorkspaceId), which dispatches rsync:active-workspace-changed.
// The header switcher must re-sync its list on that signal so the deleted workspace drops
// out — otherwise the user could re-select a tombstone and every request would 404.
// We use the REAL active-workspace module so the event wiring is exercised end to end.

// Hoisted so the vi.mock factories (which run before module init) can close over
// them; `pathname` is mutable per test to place the header on different routes.
const { replaceMock, refreshMock, nav } = vi.hoisted(() => ({
  replaceMock: vi.fn(),
  refreshMock: vi.fn(),
  nav: { pathname: "/" },
}))

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: replaceMock, refresh: refreshMock }),
  usePathname: () => nav.pathname,
}))
vi.mock("next-themes", () => ({
  useTheme: () => ({ theme: "light", setTheme: vi.fn(), resolvedTheme: "light" }),
}))
vi.mock("@/lib/auth", () => ({ logout: vi.fn(), getUserEmail: () => null }))
vi.mock("@/lib/store/useUIStore", () => ({
  useUIStore: (selector: (s: Record<string, unknown>) => unknown) =>
    selector({ sidebarCollapsed: false, setSidebarOpen: () => {}, toggleCommandPalette: () => {} }),
}))

// Radix primitives probe these jsdom-absent APIs; stub so render never throws.
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
// minimal in-memory Storage (same pattern as active-workspace.test).
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

beforeEach(() => {
  vi.clearAllMocks()
  installMemoryStorage()
  nav.pathname = "/"
  document.cookie = `${ACTIVE_WORKSPACE_KEY}=; path=/; max-age=0; SameSite=Lax`
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe("Header workspace switcher", () => {
  it("re-syncs the switcher when the active workspace changes, dropping a deleted workspace", async () => {
    // ws-a is the active workspace; the list initially has both.
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-a")
    let listPayload = [
      { id: "ws-a", name: "Team A", slug: "team-a", role: "owner" },
      { id: "ws-b", name: "Team B", slug: "team-b", role: "owner" },
    ]
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (/\/workspaces$/.test(u) && method === "GET") {
        return { ok: true, status: 200, json: async () => ({ workspaces: listPayload }) }
      }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<Header />)

    // The switcher trigger shows the active workspace name.
    expect(await screen.findByText("Team A")).toBeInTheDocument()

    // ws-a is deleted elsewhere: the list now omits it and the active selection moves
    // to a survivor (exactly what the delete flow does via setActiveWorkspaceId).
    listPayload = [{ id: "ws-b", name: "Team B", slug: "team-b", role: "owner" }]
    await act(async () => {
      setActiveWorkspaceId("ws-b") // dispatches rsync:active-workspace-changed
    })

    // The header must re-fetch and re-resolve: now showing Team B, with Team A gone.
    expect(await screen.findByText("Team B")).toBeInTheDocument()
    await waitFor(() => expect(screen.queryByText("Team A")).toBeNull())
  })

  // Switching workspaces while standing on a resource detail page used to leave
  // the user on that URL, which then re-rendered as "Pipeline not found" — the id
  // belongs to the workspace they just left, and the gateway 404s it by design.
  // The header now routes them to the equivalent list in the new workspace.
  async function switchToTeamB() {
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-a")
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (/\/workspaces$/.test(u) && method === "GET") {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            workspaces: [
              { id: "ws-a", name: "Team A", slug: "team-a", role: "owner" },
              { id: "ws-b", name: "Team B", slug: "team-b", role: "owner" },
            ],
          }),
        }
      }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<Header />)
    const user = userEvent.setup()
    await user.click(await screen.findByRole("button", { name: "Switch workspace" }))
    await user.click(await screen.findByRole("menuitem", { name: /Team B/ }))
  }

  it("leaves a workspace-scoped detail route for the list when the workspace changes", async () => {
    nav.pathname = "/pipelines/2cb685ed-4cf7-445b-9f77-071794d25423"

    await switchToTeamB()

    await waitFor(() => expect(replaceMock).toHaveBeenCalledWith("/pipelines"))
    expect(getActiveWorkspaceId()).toBe("ws-b")
    // A silent teleport reads as a bug, so the move is announced.
    expect(toast.info).toHaveBeenCalledWith(
      "Switched to Team B",
      expect.objectContaining({ description: expect.stringContaining("pipelines") }),
    )
    // refresh() would re-render the dead route under the new workspace — the
    // exact behaviour that produced the 404.
    expect(refreshMock).not.toHaveBeenCalled()
  })

  it("stays put and just refreshes on a route that is valid in every workspace", async () => {
    nav.pathname = "/pipelines"

    await switchToTeamB()

    await waitFor(() => expect(refreshMock).toHaveBeenCalled())
    expect(replaceMock).not.toHaveBeenCalled()
    expect(toast.info).not.toHaveBeenCalled()
  })

  // The redirect used to live inside switchWorkspace, so it only fired for the
  // click path. Anything else that re-points the selection — another tab's
  // switcher (storage event), or the delete flow calling setActiveWorkspaceId —
  // left this tab parked on a detail id the new workspace does not own. Owning
  // the redirect from the change subscription covers all three with one path.
  it("leaves a dead detail route when the workspace changes in ANOTHER tab", async () => {
    nav.pathname = "/connections/2cb685ed-4cf7-445b-9f77-071794d25423"
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-a")
    ;(authFetch as Mock).mockImplementation(async (url: string) => {
      if (/\/workspaces$/.test(String(url))) {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            workspaces: [
              { id: "ws-a", name: "Team A", slug: "team-a", role: "owner" },
              { id: "ws-b", name: "Team B", slug: "team-b", role: "owner" },
            ],
          }),
        }
      }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<Header />)
    await screen.findByText("Team A")

    // Exactly what the browser delivers to a background tab: the value is already
    // written, and only a storage event announces it. No custom event, no click.
    await act(async () => {
      window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-b")
      window.dispatchEvent(new StorageEvent("storage", { key: ACTIVE_WORKSPACE_KEY, newValue: "ws-b" }))
    })

    await waitFor(() => expect(replaceMock).toHaveBeenCalledWith("/connections"))
  })

  // The subscription closes over `pathname`; without it in the dep array the
  // handler keeps the pathname from first render forever, so a user who lands on
  // a detail route after mount gets no redirect at all.
  it("redirects using the CURRENT pathname, not the one from first render", async () => {
    nav.pathname = "/settings"
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-a")
    ;(authFetch as Mock).mockImplementation(async (url: string) => {
      if (/\/workspaces$/.test(String(url))) {
        return {
          ok: true,
          status: 200,
          json: async () => ({ workspaces: [{ id: "ws-a", name: "Team A", slug: "team-a", role: "owner" }] }),
        }
      }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    const { rerender } = render(<Header />)
    await screen.findByText("Team A")

    // Client-side navigation to a detail route: same mounted Header, new pathname.
    nav.pathname = "/executions/2cb685ed-4cf7-445b-9f77-071794d25423"
    rerender(<Header />)

    await act(async () => {
      setActiveWorkspaceId("ws-b")
    })

    await waitFor(() => expect(replaceMock).toHaveBeenCalledWith("/executions"))
  })

  it("clears the active selection when the resolved workspace list is empty (no tombstone)", async () => {
    // A stored active workspace that no longer appears in the list must be cleared,
    // not left behind — otherwise the next request sends a stale X-Workspace-ID.
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-a")
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (/\/workspaces$/.test(u) && method === "GET") {
        return { ok: true, status: 200, json: async () => ({ workspaces: [] }) }
      }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<Header />)

    await waitFor(() => expect(getActiveWorkspaceId()).toBeNull())
  })

  // The personal workspace is auto-provisioned at signup and is the only one that
  // cannot be renamed, invited into, left or deleted. The switcher used to render
  // every row identically, so the user had no way to tell which workspace those
  // rules applied to — they found out from a server error. is_personal already
  // rides along on the list payload; the tag just surfaces it.
  it("tags the personal workspace in the switcher and leaves team workspaces untagged", async () => {
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-team")
    ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (/\/workspaces$/.test(u) && method === "GET") {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            workspaces: [
              { id: "ws-team", name: "Team A", slug: "team-a", role: "owner", is_personal: false },
              { id: "ws-me", name: "Rahul", slug: "rahul-1a2b", role: "owner", is_personal: true },
            ],
          }),
        }
      }
      return { ok: true, status: 200, json: async () => ({}) }
    })

    render(<Header />)
    const user = userEvent.setup()
    await user.click(await screen.findByRole("button", { name: "Switch workspace" }))

    // Scope to each row rather than matching the accessible name, so the assertion
    // pins WHICH row carries the tag — a tag on every row would pass a page-wide check.
    const personalRow = await screen.findByRole("menuitem", { name: /Rahul/ })
    expect(within(personalRow).getByText("Personal")).toBeInTheDocument()

    const teamRow = screen.getByRole("menuitem", { name: /Team A/ })
    expect(within(teamRow).queryByText("Personal")).toBeNull()
  })
})
