import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen } from "@testing-library/react"

import { AdminNav } from "@/components/admin/AdminNav"
import { Sidebar } from "@/components/layout/Sidebar"
import { UsagePanelGate } from "@/components/usage/UsagePanelGate"
import { featureFlagsManager } from "@/config/features"

// The Usage panel is a BILLING surface — plan, pipeline and query limits, trial
// expiry, metered transfer GB. A self-host runs the same prebuilt frontend image
// as cloud with plan enforcement off (RSYNC_BILLING_ENFORCED=false in
// docker-compose.quickstart.yml and the Helm chart), so the api-gateway turns
// usage_panel off over /api/v1/features and the UI has to follow.
//
// Three things are pinned here, and the third is the one that is easy to lose:
//   1. flag off  -> no nav entry, and the page renders an explanation, not the panel
//   2. flag on   -> both come back (the denominator; without it case 1 passes
//                   just as well when the whole component fails to render)
//   3. unresolved-> ALSO not visible. The build-time default is the cloud answer,
//                   so a boolean that starts there would flash the panel on a
//                   self-host for one frame before withdrawing it.

vi.mock("next/navigation", () => ({
  usePathname: () => "/",
  useRouter: () => ({ push: vi.fn() }),
}))
vi.mock("@/lib/store/useUIStore", () => ({
  useUIStore: (selector: (s: Record<string, unknown>) => unknown) =>
    selector({
      sidebarCollapsed: false,
      sidebarOpen: false,
      setSidebarOpen: () => {},
      toggleSidebarCollapsed: () => {},
    }),
}))

// resetFlags() also clears the resolved bit, so every case starts in 'loading'
// and has to opt in to a resolved answer.
beforeEach(() => featureFlagsManager.resetFlags())
afterEach(() => {
  featureFlagsManager.resetFlags()
  vi.restoreAllMocks()
})

function resolveWith(usagePanel: boolean) {
  featureFlagsManager.updateFlags({ usagePanel })
  featureFlagsManager.markResolved()
}

describe("usage panel flag — navigation", () => {
  it("hides the sidebar Usage entry when the API says the panel is off", () => {
    resolveWith(false)
    render(<Sidebar role="admin" />)
    expect(screen.queryByRole("link", { name: /^usage$/i })).toBeNull()
    // Denominator: its section-mate is still there, so the sidebar did render.
    expect(screen.getByRole("link", { name: /^workspace$/i })).toBeInTheDocument()
  })

  it("shows the sidebar Usage entry when the API says the panel is on", () => {
    resolveWith(true)
    render(<Sidebar role="admin" />)
    expect(screen.getByRole("link", { name: /^usage$/i })).toHaveAttribute("href", "/usage")
  })

  it("hides the sidebar Usage entry while the flags are still unresolved", () => {
    render(<Sidebar role="admin" />)
    expect(screen.queryByRole("link", { name: /^usage$/i })).toBeNull()
    expect(screen.getByRole("link", { name: /^workspace$/i })).toBeInTheDocument()
  })

  it("hides the admin Usage tab when the panel is off and restores it when on", () => {
    resolveWith(false)
    const { unmount } = render(<AdminNav />)
    expect(screen.queryByRole("button", { name: /^usage$/i })).toBeNull()
    expect(screen.getByRole("button", { name: /^users$/i })).toBeInTheDocument()
    unmount()

    resolveWith(true)
    render(<AdminNav />)
    expect(screen.getByRole("button", { name: /^usage$/i })).toBeInTheDocument()
  })
})

describe("usage panel flag — the pages themselves", () => {
  it("does not mount the panel when the flag is off, and says why", () => {
    resolveWith(false)
    render(
      <UsagePanelGate>
        <div>plan meter</div>
      </UsagePanelGate>
    )
    expect(screen.queryByText("plan meter")).toBeNull()
    expect(screen.getByText(/does not enforce plan limits/i)).toBeInTheDocument()
    // Names the escape hatch, so a self-hoster who wants the page can find it.
    expect(screen.getByText(/FEATURE_USAGE_PANEL=true/)).toBeInTheDocument()
  })

  it("mounts the panel when the flag is on", () => {
    resolveWith(true)
    render(
      <UsagePanelGate>
        <div>plan meter</div>
      </UsagePanelGate>
    )
    expect(screen.getByText("plan meter")).toBeInTheDocument()
  })

  it("mounts neither the panel nor the off-state copy while unresolved", () => {
    render(
      <UsagePanelGate>
        <div>plan meter</div>
      </UsagePanelGate>
    )
    expect(screen.queryByText("plan meter")).toBeNull()
    expect(screen.queryByText(/does not enforce plan limits/i)).toBeNull()
  })

  it("keeps the admin nav reachable on a gated admin page", () => {
    resolveWith(false)
    render(
      <UsagePanelGate nav={<AdminNav />}>
        <div>platform plan meter</div>
      </UsagePanelGate>
    )
    expect(screen.queryByText("platform plan meter")).toBeNull()
    expect(screen.getByRole("button", { name: /^users$/i })).toBeInTheDocument()
  })
})
