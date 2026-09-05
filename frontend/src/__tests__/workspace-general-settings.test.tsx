import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen, fireEvent, waitFor } from "@testing-library/react"

import { WorkspaceGeneralSettings } from "@/components/workspace/WorkspaceGeneralSettings"
import { authFetch } from "@/lib/api/auth-fetch"
import { ACTIVE_WORKSPACE_KEY, setActiveWorkspaceId } from "@/lib/workspace/active-workspace"

// P0 General tab: rename (admin+) and leave (any member, hidden on personal).
// Both call existing backend endpoints (PATCH /workspaces/:id, DELETE
// /workspaces/:id/members/:self). The backend independently enforces every guard;
// this component mirrors them for UX and surfaces the 409 last-owner case.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
const pushMock = vi.fn()
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: pushMock, refresh: vi.fn() }) }))

const mockFetch = authFetch as unknown as Mock

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

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

beforeEach(() => {
  installMemoryStorage()
  document.cookie = `${ACTIVE_WORKSPACE_KEY}=; path=/; max-age=0`
  mockFetch.mockReset()
  pushMock.mockReset()
})

afterEach(() => {
  vi.restoreAllMocks()
})

// Default authFetch router: /auth/me -> self id; everything else 200/204.
function routeFetch(handler?: (url: string, init?: RequestInit) => unknown) {
  mockFetch.mockImplementation((url: string, init?: RequestInit) => {
    if (typeof url === "string" && url.endsWith("/auth/me")) {
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ user_id: "u-self" }) } as Response)
    }
    const custom = handler?.(url, init)
    if (custom) return Promise.resolve(custom as Response)
    return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({}) } as Response)
  })
}

function renderGeneral(overrides: Partial<React.ComponentProps<typeof WorkspaceGeneralSettings>> = {}) {
  return render(
    <WorkspaceGeneralSettings
      workspaceId="ws-1"
      workspaceName="Acme"
      slug="acme"
      currentRole="owner"
      isPersonal={false}
      {...overrides}
    />,
  )
}

describe("WorkspaceGeneralSettings — rename", () => {
  it("an admin can rename: PATCHes /workspaces/:id with the new name", async () => {
    routeFetch()
    renderGeneral({ currentRole: "admin" })
    const input = (await screen.findByLabelText(/workspace name/i)) as HTMLInputElement
    expect(input.value).toBe("Acme")
    fireEvent.change(input, { target: { value: "Acme Renamed" } })
    fireEvent.click(screen.getByRole("button", { name: /save/i }))
    await waitFor(() => {
      const patch = mockFetch.mock.calls.find(
        ([u, i]) => typeof u === "string" && u.includes("/workspaces/ws-1") && i?.method === "PATCH",
      )
      expect(patch).toBeTruthy()
      expect(JSON.parse((patch![1] as RequestInit).body as string)).toEqual({ name: "Acme Renamed" })
    })
    // and the success is surfaced to the user
    const { toast } = await import("sonner")
    await waitFor(() => expect(toast.success as Mock).toHaveBeenCalled())
  })

  it("surfaces a toast.error when the rename request fails", async () => {
    const { toast } = await import("sonner")
    routeFetch((url, init) => {
      if (url.includes("/workspaces/ws-1") && init?.method === "PATCH") {
        return { ok: false, status: 500, json: () => Promise.resolve({ error: "rename failed" }) }
      }
      return undefined
    })
    renderGeneral({ currentRole: "admin" })
    const input = (await screen.findByLabelText(/workspace name/i)) as HTMLInputElement
    fireEvent.change(input, { target: { value: "Acme Renamed" } })
    fireEvent.click(screen.getByRole("button", { name: /save/i }))
    await waitFor(() => expect(toast.error as Mock).toHaveBeenCalledWith("rename failed"))
  })

  it("Save is disabled until the name actually changes", async () => {
    routeFetch()
    renderGeneral({ currentRole: "admin" })
    await screen.findByLabelText(/workspace name/i)
    expect(screen.getByRole("button", { name: /save/i })).toBeDisabled()
  })

  it("a viewer cannot rename — the field is read-only and Save is not actionable", async () => {
    routeFetch()
    renderGeneral({ currentRole: "viewer" })
    const input = (await screen.findByLabelText(/workspace name/i)) as HTMLInputElement
    expect(input).toBeDisabled()
    const save = screen.queryByRole("button", { name: /save/i })
    expect(save === null || (save as HTMLButtonElement).disabled).toBe(true)
  })

  it("shows the slug read-only", async () => {
    routeFetch()
    renderGeneral({ currentRole: "admin" })
    expect(await screen.findByText("acme")).toBeInTheDocument()
  })
})

describe("WorkspaceGeneralSettings — leave", () => {
  it("a member can leave: confirms, DELETEs self membership, then navigates home", async () => {
    setActiveWorkspaceId("ws-1")
    routeFetch((url, init) => {
      if (url.includes("/workspaces/ws-1/members/u-self") && init?.method === "DELETE") {
        return { ok: true, status: 204, json: () => Promise.resolve({}) }
      }
      if (url.endsWith("/api/v1/workspaces") || url.endsWith("/workspaces")) {
        return { ok: true, status: 200, json: () => Promise.resolve({ workspaces: [{ id: "ws-2", name: "Other", role: "member" }] }) }
      }
      return undefined
    })
    renderGeneral({ currentRole: "member" })
    fireEvent.click(await screen.findByRole("button", { name: /leave workspace/i }))
    fireEvent.click(await screen.findByRole("button", { name: /^leave$/i }))
    await waitFor(() => expect(pushMock).toHaveBeenCalledWith("/"))
  })

  it("does not show Leave on a personal workspace", async () => {
    routeFetch()
    renderGeneral({ currentRole: "owner", isPersonal: true })
    await screen.findByLabelText(/workspace name/i)
    expect(screen.queryByRole("button", { name: /leave workspace/i })).toBeNull()
  })

  it("on a 409 (last owner) it keeps the dialog open and explains the next step", async () => {
    const { toast } = await import("sonner")
    setActiveWorkspaceId("ws-1")
    routeFetch((url, init) => {
      if (url.includes("/workspaces/ws-1/members/u-self") && init?.method === "DELETE") {
        return { ok: false, status: 409, json: () => Promise.resolve({ error: "workspace must have at least one owner" }) }
      }
      return undefined
    })
    renderGeneral({ currentRole: "owner", isPersonal: false })
    fireEvent.click(await screen.findByRole("button", { name: /leave workspace/i }))
    fireEvent.click(await screen.findByRole("button", { name: /^leave$/i }))
    await waitFor(() => expect((toast.error as Mock)).toHaveBeenCalled())
    expect(pushMock).not.toHaveBeenCalled()
  })
})
