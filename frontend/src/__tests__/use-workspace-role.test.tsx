import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen, waitFor, act } from "@testing-library/react"

import { WorkspaceProvider, useWorkspace, useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { authFetch } from "@/lib/api/auth-fetch"
import { ACTIVE_WORKSPACE_KEY, setActiveWorkspaceId } from "@/lib/workspace/active-workspace"

// P0 keystone hook: a shared context so every client component agrees on the active
// workspace and the caller's role in it (previously each component re-fetched
// /workspaces independently). useWorkspaceRole() exposes the active role plus a
// fail-closed can(action); useWorkspace() exposes the list/active/refresh. Outside a
// provider the hook must fail closed (no permissions) rather than throw, so isolated
// components don't crash.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

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

const WORKSPACES = [
  { id: "ws-owner", name: "Acme", slug: "acme", role: "owner", is_personal: false },
  { id: "ws-viewer", name: "ReadOnly", slug: "readonly", role: "viewer", is_personal: false },
]

function jsonOk(body: unknown) {
  return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(body) } as Response)
}

function listResponse(workspaces = WORKSPACES) {
  return jsonOk({ workspaces })
}

beforeEach(() => {
  installMemoryStorage()
  document.cookie = `${ACTIVE_WORKSPACE_KEY}=; path=/; max-age=0`
  mockFetch.mockReset()
})

afterEach(() => {
  vi.restoreAllMocks()
})

function Probe() {
  const { role, can, isLoading, error } = useWorkspaceRole()
  const { activeWorkspace, workspaces } = useWorkspace()
  return (
    <div>
      <span data-testid="loading">{String(isLoading)}</span>
      <span data-testid="error">{String(error)}</span>
      <span data-testid="role">{role || "none"}</span>
      <span data-testid="active">{activeWorkspace?.id ?? "none"}</span>
      <span data-testid="personal">{String(activeWorkspace?.is_personal ?? "?")}</span>
      <span data-testid="count">{workspaces.length}</span>
      <span data-testid="can-manage">{String(can("manage_members"))}</span>
      <span data-testid="can-delete">{String(can("delete_workspace"))}</span>
      <span data-testid="can-view">{String(can("view"))}</span>
    </div>
  )
}

describe("WorkspaceProvider + useWorkspaceRole", () => {
  it("fetches the workspace list and resolves the saved active workspace + its role", async () => {
    setActiveWorkspaceId("ws-viewer")
    mockFetch.mockImplementation(() => listResponse())
    render(
      <WorkspaceProvider>
        <Probe />
      </WorkspaceProvider>,
    )
    await waitFor(() => expect(screen.getByTestId("loading").textContent).toBe("false"))
    expect(screen.getByTestId("active").textContent).toBe("ws-viewer")
    expect(screen.getByTestId("role").textContent).toBe("viewer")
    expect(screen.getByTestId("count").textContent).toBe("2")
  })

  it("can() reflects the active role — owner can manage & delete", async () => {
    setActiveWorkspaceId("ws-owner")
    mockFetch.mockImplementation(() => listResponse())
    render(
      <WorkspaceProvider>
        <Probe />
      </WorkspaceProvider>,
    )
    await waitFor(() => expect(screen.getByTestId("role").textContent).toBe("owner"))
    expect(screen.getByTestId("can-manage").textContent).toBe("true")
    expect(screen.getByTestId("can-delete").textContent).toBe("true")
  })

  it("a viewer can view but cannot manage members or delete", async () => {
    setActiveWorkspaceId("ws-viewer")
    mockFetch.mockImplementation(() => listResponse())
    render(
      <WorkspaceProvider>
        <Probe />
      </WorkspaceProvider>,
    )
    await waitFor(() => expect(screen.getByTestId("role").textContent).toBe("viewer"))
    expect(screen.getByTestId("can-view").textContent).toBe("true")
    expect(screen.getByTestId("can-manage").textContent).toBe("false")
    expect(screen.getByTestId("can-delete").textContent).toBe("false")
  })

  it("defaults to the first workspace when no active id is saved", async () => {
    mockFetch.mockImplementation(() => listResponse())
    render(
      <WorkspaceProvider>
        <Probe />
      </WorkspaceProvider>,
    )
    await waitFor(() => expect(screen.getByTestId("loading").textContent).toBe("false"))
    expect(screen.getByTestId("active").textContent).toBe("ws-owner")
    expect(screen.getByTestId("role").textContent).toBe("owner")
  })

  it("re-resolves role when the active workspace is switched in place", async () => {
    setActiveWorkspaceId("ws-owner")
    mockFetch.mockImplementation(() => listResponse())
    render(
      <WorkspaceProvider>
        <Probe />
      </WorkspaceProvider>,
    )
    await waitFor(() => expect(screen.getByTestId("role").textContent).toBe("owner"))
    // Switch to the viewer workspace — the provider listens for the change event.
    act(() => setActiveWorkspaceId("ws-viewer"))
    await waitFor(() => expect(screen.getByTestId("role").textContent).toBe("viewer"))
    expect(screen.getByTestId("can-delete").textContent).toBe("false")
  })

  it("surfaces is_personal on the active workspace (for action hiding)", async () => {
    setActiveWorkspaceId("ws-owner")
    mockFetch.mockImplementation(() =>
      listResponse([{ id: "ws-owner", name: "me", slug: "me", role: "owner", is_personal: true }]),
    )
    render(
      <WorkspaceProvider>
        <Probe />
      </WorkspaceProvider>,
    )
    await waitFor(() => expect(screen.getByTestId("loading").textContent).toBe("false"))
    expect(screen.getByTestId("personal").textContent).toBe("true")
  })

  it("sets error=true when the list fetch fails", async () => {
    mockFetch.mockImplementation(() =>
      Promise.resolve({ ok: false, status: 500, json: () => Promise.resolve({}) } as Response),
    )
    render(
      <WorkspaceProvider>
        <Probe />
      </WorkspaceProvider>,
    )
    await waitFor(() => expect(screen.getByTestId("loading").textContent).toBe("false"))
    expect(screen.getByTestId("error").textContent).toBe("true")
    expect(screen.getByTestId("role").textContent).toBe("none")
  })

  it("fails closed when used outside a provider (no permissions, no throw)", () => {
    render(<Probe />)
    expect(screen.getByTestId("role").textContent).toBe("none")
    expect(screen.getByTestId("can-manage").textContent).toBe("false")
    expect(screen.getByTestId("can-view").textContent).toBe("false")
    expect(screen.getByTestId("loading").textContent).toBe("false")
  })
})
