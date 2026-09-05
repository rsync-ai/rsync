import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen } from "@testing-library/react"

import { Sidebar } from "@/components/layout/Sidebar"

// P0 nav: the sidebar gains a "Workspace" entry pointing at the settings hub. The
// "Admin" section stays gated on the PLATFORM role (auth/me role), which is distinct
// from a workspace role even though both can be the string "admin".

vi.mock("next/navigation", () => ({ usePathname: () => "/" }))
vi.mock("@/lib/store/useUIStore", () => ({
  useUIStore: (selector: (s: Record<string, unknown>) => unknown) =>
    selector({
      sidebarCollapsed: false,
      sidebarOpen: false,
      setSidebarOpen: () => {},
      toggleSidebarCollapsed: () => {},
    }),
}))

beforeEach(() => vi.clearAllMocks())
afterEach(() => vi.restoreAllMocks())

describe("Sidebar — workspace nav", () => {
  it("renders a Workspace link to the settings hub", () => {
    render(<Sidebar role="viewer" />)
    const link = screen.getByRole("link", { name: /workspace/i })
    expect(link).toHaveAttribute("href", "/workspace/settings")
  })

  it("hides the Admin section for a non-platform-admin", () => {
    render(<Sidebar role="member" />)
    expect(screen.queryByRole("link", { name: /^admin$/i })).toBeNull()
  })

  it("shows the Admin section for a platform admin", () => {
    render(<Sidebar role="admin" />)
    expect(screen.getByRole("link", { name: /^admin$/i })).toBeInTheDocument()
  })
})
