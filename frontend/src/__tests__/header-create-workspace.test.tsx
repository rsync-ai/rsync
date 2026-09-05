import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import type { Mock } from "vitest"

import { Header } from "@/components/layout/Header"
import { authFetch } from "@/lib/api/auth-fetch"
import { ACTIVE_WORKSPACE_KEY } from "@/lib/workspace/active-workspace"

// P0 creation dialog v2 + nav: the New Workspace dialog explains what a workspace is,
// previews the slug live (mirroring the backend), and after creating shows next-step
// CTAs (invite team / first connection). The switcher's settings entry points at the
// new /workspace/settings hub.

const pushMock = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: pushMock, replace: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/",
}))
vi.mock("next-themes", () => ({
  useTheme: () => ({ theme: "light", setTheme: vi.fn(), resolvedTheme: "light" }),
}))
vi.mock("@/lib/auth", () => ({ logout: vi.fn(), getUserEmail: () => null }))
vi.mock("@/lib/store/useUIStore", () => ({
  useUIStore: (selector: (s: Record<string, unknown>) => unknown) =>
    selector({ sidebarCollapsed: false, setSidebarOpen: () => {}, toggleCommandPalette: () => {} }),
}))

const mockFetch = authFetch as unknown as Mock

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  if (!Element.prototype.hasPointerCapture) Element.prototype.hasPointerCapture = () => false
  if (!Element.prototype.setPointerCapture) Element.prototype.setPointerCapture = () => {}
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {}
})

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
  pushMock.mockReset()
  installMemoryStorage()
  document.cookie = `${ACTIVE_WORKSPACE_KEY}=; path=/; max-age=0; SameSite=Lax`
})

afterEach(() => vi.restoreAllMocks())

// One existing workspace so the switcher renders; POST creates a new one.
function wireFetch() {
  mockFetch.mockImplementation(async (url: string, opts?: RequestInit) => {
    const u = String(url)
    const method = (opts?.method ?? "GET").toUpperCase()
    if (/\/workspaces$/.test(u) && method === "GET") {
      return { ok: true, status: 200, json: async () => ({ workspaces: [{ id: "ws-a", name: "Team A", slug: "team-a", role: "owner" }] }) }
    }
    if (/\/workspaces$/.test(u) && method === "POST") {
      const body = JSON.parse(String(opts?.body ?? "{}"))
      return { ok: true, status: 201, json: async () => ({ id: "ws-new", name: body.name, slug: "new-team", role: "owner" }) }
    }
    return { ok: true, status: 200, json: async () => ({}) }
  })
}

async function openCreateDialog(user: ReturnType<typeof userEvent.setup>) {
  await screen.findByText("Team A") // switcher mounted
  await user.click(screen.getByRole("button", { name: /switch workspace/i }))
  await user.click(await screen.findByText(/new workspace/i))
}

describe("Header — New Workspace dialog v2", () => {
  it("explains what a workspace is", async () => {
    const user = userEvent.setup()
    wireFetch()
    render(<Header />)
    await openCreateDialog(user)
    expect(await screen.findByText(/connections, pipelines/i)).toBeInTheDocument()
  })

  it("previews the slug live as you type (mirroring the backend)", async () => {
    const user = userEvent.setup()
    wireFetch()
    render(<Header />)
    await openCreateDialog(user)
    const input = await screen.findByPlaceholderText(/acme/i)
    await user.type(input, "Acme Corp")
    expect((await screen.findByTestId("slug-preview")).textContent).toContain("acme-corp")
  })

  it("after creating, shows next-step CTAs and can jump to inviting the team", async () => {
    const user = userEvent.setup()
    wireFetch()
    render(<Header />)
    await openCreateDialog(user)
    const input = await screen.findByPlaceholderText(/acme/i)
    await user.type(input, "New Team")
    await user.click(screen.getByRole("button", { name: /^create$/i }))
    // Next-steps view appears with the two CTAs.
    expect(await screen.findByText(/invite your team/i)).toBeInTheDocument()
    expect(screen.getByText(/first connection/i)).toBeInTheDocument()
    await user.click(screen.getByText(/invite your team/i))
    await waitFor(() => expect(pushMock).toHaveBeenCalledWith("/workspace/settings?tab=members"))
  })

  it("surfaces a toast.error and stays on the form when creation fails", async () => {
    const { toast } = await import("sonner")
    const user = userEvent.setup()
    mockFetch.mockImplementation(async (url: string, opts?: RequestInit) => {
      const u = String(url)
      const method = (opts?.method ?? "GET").toUpperCase()
      if (/\/workspaces$/.test(u) && method === "GET") {
        return { ok: true, status: 200, json: async () => ({ workspaces: [{ id: "ws-a", name: "Team A", slug: "team-a", role: "owner" }] }) }
      }
      if (/\/workspaces$/.test(u) && method === "POST") {
        return { ok: false, status: 409, json: async () => ({ error: "workspace name taken" }) }
      }
      return { ok: true, status: 200, json: async () => ({}) }
    })
    render(<Header />)
    await openCreateDialog(user)
    const input = await screen.findByPlaceholderText(/acme/i)
    await user.type(input, "Dupe")
    await user.click(screen.getByRole("button", { name: /^create$/i }))
    await waitFor(() => expect(toast.error as Mock).toHaveBeenCalledWith("workspace name taken"))
    // still on the create form (no next-steps view), so the user can correct it
    expect(screen.queryByText(/invite your team/i)).toBeNull()
    expect(pushMock).not.toHaveBeenCalled()
  })

  it("the switcher links to the workspace settings hub", async () => {
    const user = userEvent.setup()
    wireFetch()
    render(<Header />)
    await screen.findByText("Team A")
    await user.click(screen.getByRole("button", { name: /switch workspace/i }))
    await user.click(await screen.findByText(/workspace settings/i))
    await waitFor(() => expect(pushMock).toHaveBeenCalledWith("/workspace/settings"))
  })
})
