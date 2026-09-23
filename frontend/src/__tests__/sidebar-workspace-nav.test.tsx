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

describe("Sidebar — fits above the fold (#56)", () => {
  it("groups the links under three headings instead of eight", () => {
    render(<Sidebar role="admin" />)
    const headings = screen.getAllByRole("heading", { level: 4 }).map((h) => h.textContent)
    expect(headings).toEqual(["Pipelines", "Data", "Manage"])
  })

  it("keeps every destination, Settings and Admin in the same group as Workspace", () => {
    render(<Sidebar role="admin" />)
    const names = screen.getAllByRole("link").map((a) => a.getAttribute("aria-label")).filter(Boolean)
    expect(names).toEqual(
      expect.arrayContaining([
        "Home", "Data Pipeline", "All Pipelines", "Executions", "Explorer",
        "Scheduled Queries", "Connections", "Connectors", "Workspace", "Settings", "Admin",
      ]),
    )
    const manage = screen.getByRole("heading", { name: "Manage" }).parentElement as HTMLElement
    const inManage = Array.from(manage.querySelectorAll("a")).map((a) => a.getAttribute("aria-label"))
    expect(inManage).toEqual(expect.arrayContaining(["Workspace", "Settings", "Admin"]))
  })
})

// PII Management shipped with a working /pii page and no way to reach it: the nav
// check above is an `arrayContaining`, so a missing row passes it. These assert the
// row by name, which is the failure the loose check could not produce.
describe("Sidebar — PII Management is reachable from the nav", () => {
  it("links to /pii from the Data group", () => {
    render(<Sidebar role="admin" />)
    expect(screen.getByRole("link", { name: "PII Management" })).toHaveAttribute("href", "/pii")
    const data = screen.getByRole("heading", { name: "Data" }).parentElement as HTMLElement
    const inData = Array.from(data.querySelectorAll("a")).map((a) => a.getAttribute("aria-label"))
    expect(inData).toContain("PII Management")
  })

  it("shows the row to a non-admin, because /pii carries no role gate of its own", () => {
    render(<Sidebar role="viewer" />)
    expect(screen.getByRole("link", { name: "PII Management" })).toBeInTheDocument()
  })
})
