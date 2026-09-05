import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import WorkspaceSettingsPage from "@/app/(dashboard)/workspace/settings/page"
import { useWorkspace } from "@/contexts/WorkspaceContext"

// P0 settings hub: composes the active workspace's General / Members / Roles tabs.
// The child components are unit-tested independently, so here we stub them and
// assert the page wires the active workspace + role through, gates on load state,
// and shows the role banner.

vi.mock("@/contexts/WorkspaceContext", () => ({ useWorkspace: vi.fn() }))
vi.mock("@/components/workspace/WorkspaceGeneralSettings", () => ({
  WorkspaceGeneralSettings: (p: { workspaceId: string; currentRole: string; isPersonal?: boolean }) => (
    <div data-testid="general" data-id={p.workspaceId} data-role={p.currentRole} data-personal={String(p.isPersonal)} />
  ),
}))
vi.mock("@/components/workspace/WorkspaceMembers", () => ({
  WorkspaceMembers: (p: { workspaceId: string; currentRole: string }) => (
    <div data-testid="members" data-id={p.workspaceId} data-role={p.currentRole} />
  ),
}))
vi.mock("@/components/workspace/WorkspaceRolesMatrix", () => ({
  WorkspaceRolesMatrix: (p: { currentRole?: string }) => <div data-testid="matrix" data-role={p.currentRole} />,
}))

let searchParams = new URLSearchParams("")
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn() }),
  useSearchParams: () => searchParams,
}))

const mockUseWorkspace = useWorkspace as unknown as Mock

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  // Radix Tabs uses pointer capture + scrollIntoView, which jsdom lacks.
  if (!Element.prototype.hasPointerCapture) Element.prototype.hasPointerCapture = () => false
  if (!Element.prototype.setPointerCapture) Element.prototype.setPointerCapture = () => {}
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {}
})

beforeEach(() => {
  searchParams = new URLSearchParams("")
  mockUseWorkspace.mockReset()
})

afterEach(() => vi.restoreAllMocks())

function ctx(over: Record<string, unknown> = {}) {
  return {
    workspaces: [],
    activeWorkspace: { id: "ws-1", name: "Acme", slug: "acme", role: "admin", is_personal: false },
    role: "admin",
    isLoading: false,
    error: false,
    refresh: vi.fn(),
    can: () => true,
    meets: () => true,
    ...over,
  }
}

describe("WorkspaceSettingsPage", () => {
  it("shows a loading state while the workspace context loads", () => {
    mockUseWorkspace.mockReturnValue(ctx({ activeWorkspace: null, isLoading: true }))
    render(<WorkspaceSettingsPage />)
    expect(screen.getByText(/loading/i)).toBeInTheDocument()
  })

  it("shows an error state when the workspace list fails to load", () => {
    mockUseWorkspace.mockReturnValue(ctx({ activeWorkspace: null, error: true }))
    render(<WorkspaceSettingsPage />)
    expect(screen.getByText(/couldn.t load/i)).toBeInTheDocument()
  })

  it("prompts to pick a workspace when none is active", () => {
    mockUseWorkspace.mockReturnValue(ctx({ activeWorkspace: null }))
    render(<WorkspaceSettingsPage />)
    expect(screen.getByText(/no workspace selected/i)).toBeInTheDocument()
  })

  it("renders the General tab by default and wires the active workspace + role", () => {
    mockUseWorkspace.mockReturnValue(ctx())
    render(<WorkspaceSettingsPage />)
    const general = screen.getByTestId("general")
    expect(general.getAttribute("data-id")).toBe("ws-1")
    expect(general.getAttribute("data-role")).toBe("admin")
    expect(general.getAttribute("data-personal")).toBe("false")
  })

  it("exposes General, Members and Roles tabs", () => {
    mockUseWorkspace.mockReturnValue(ctx())
    render(<WorkspaceSettingsPage />)
    expect(screen.getByRole("tab", { name: /general/i })).toBeInTheDocument()
    expect(screen.getByRole("tab", { name: /members/i })).toBeInTheDocument()
    expect(screen.getByRole("tab", { name: /roles/i })).toBeInTheDocument()
  })

  it("honors ?tab=members to open the Members tab initially", () => {
    searchParams = new URLSearchParams("tab=members")
    mockUseWorkspace.mockReturnValue(ctx())
    render(<WorkspaceSettingsPage />)
    const members = screen.getByTestId("members")
    expect(members.getAttribute("data-id")).toBe("ws-1")
    expect(members.getAttribute("data-role")).toBe("admin")
  })

  it("switches to the Roles tab on click", async () => {
    const user = userEvent.setup()
    mockUseWorkspace.mockReturnValue(ctx())
    render(<WorkspaceSettingsPage />)
    await user.click(screen.getByRole("tab", { name: /roles/i }))
    const matrix = await screen.findByTestId("matrix")
    expect(matrix.getAttribute("data-role")).toBe("admin")
  })

  it("shows a role banner reflecting the caller's role", () => {
    mockUseWorkspace.mockReturnValue(ctx({ activeWorkspace: { id: "ws-1", name: "Acme", slug: "acme", role: "viewer", is_personal: false }, role: "viewer" }))
    render(<WorkspaceSettingsPage />)
    expect(screen.getByText(/viewer/i)).toBeInTheDocument()
  })
})
