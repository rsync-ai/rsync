import { beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { act, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import ExplorerPage from "@/app/(dashboard)/explorer/page"
import { authFetch } from "@/lib/api/auth-fetch"
import { ACTIVE_WORKSPACE_EVENT } from "@/lib/workspace/active-workspace"

// The Data Explorer loads the workspace's connection list once, and again only when the
// workspace changes. The loader used to depend on the selected connection, so every pick
// in the connection menu fetched the whole list again; and the workspace-switch reload
// only picked the new workspace's first connection because that extra fetch ran after it.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn(), message: vi.fn() },
}))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn(), replace: vi.fn() }),
  usePathname: () => "/explorer",
  useSearchParams: () => new URLSearchParams(),
}))
vi.mock("next-themes", () => ({
  useTheme: () => ({ resolvedTheme: "light", setTheme: vi.fn() }),
}))
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
vi.mock("@/contexts/CurrentUserContext", () => ({
  useCurrentUser: () => ({ user: { id: "u-1" } }),
}))

const A = "11111111-1111-1111-1111-111111111111"
const B = "22222222-2222-2222-2222-222222222222"
const C = "33333333-3333-3333-3333-333333333333"
const mockFetch = authFetch as unknown as Mock

function pg(id: string, name: string) {
  return {
    id,
    name,
    connector_type: "postgresql",
    type: "source",
    status: "active",
    is_connected: true,
    supports_explorer: true,
    explorer_mode: "sql",
  }
}

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

function serve(connections: ReturnType<typeof pg>[]) {
  mockFetch.mockImplementation(async (url: string) => {
    if (/\/api\/v1\/connections$/.test(url)) return res(200, { connections })
    if (url.includes("/schema-index")) return res(200, { tables: [], foreign_keys: [] })
    return res(200, {})
  })
}

const listCalls = () => mockFetch.mock.calls.filter(([url]) => /\/api\/v1\/connections$/.test(String(url))).length
const schemaCalls = (id: string) =>
  mockFetch.mock.calls.filter(([url]) => String(url).includes(`/connections/${id}/schema-index`)).length

function connectionPicker() {
  const trigger = screen.getAllByRole("combobox").find((el) => /warehouse/.test(el.textContent ?? ""))
  expect(trigger).toBeDefined()
  return trigger as HTMLElement
}

describe("Explorer connection list", () => {
  beforeEach(() => {
    mockFetch.mockReset()
  })

  it("is fetched once, not again on every connection picked", async () => {
    serve([pg(A, "a warehouse"), pg(B, "b warehouse")])
    const user = userEvent.setup()
    render(<ExplorerPage />)
    await waitFor(() => expect(schemaCalls(A)).toBe(1))

    await user.click(connectionPicker())
    await user.click(await screen.findByRole("option", { name: /b warehouse/ }))
    await waitFor(() => expect(schemaCalls(B)).toBe(1))

    expect(listCalls()).toBe(1)
  })

  it("is fetched once more on a workspace change, which opens that workspace's first connection", async () => {
    serve([pg(A, "a warehouse")])
    render(<ExplorerPage />)
    await waitFor(() => expect(schemaCalls(A)).toBe(1))

    serve([pg(C, "c warehouse")])
    act(() => {
      window.dispatchEvent(new Event(ACTIVE_WORKSPACE_EVENT))
    })
    await waitFor(() => expect(schemaCalls(C)).toBe(1))
    expect(connectionPicker()).toHaveTextContent("c warehouse")
    expect(listCalls()).toBe(2)
  })
})
