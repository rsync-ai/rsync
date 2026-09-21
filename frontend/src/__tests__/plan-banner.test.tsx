import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, fireEvent, waitFor } from "@testing-library/react"
import type { Mock } from "vitest"

import { PlanBanner } from "@/components/layout/PlanBanner"
import { authFetch } from "@/lib/api/auth-fetch"
import { notifyPipelinesChanged } from "@/lib/plan/plan-events"

// The banner's "Upgrade to Pro" used to be a bare mailto anchor — a dead click on
// machines with no mail client. It now opens the in-app UpgradeModal. These tests
// pin: (1) the free-plan banner renders, (2) clicking the CTA opens the contact
// dialog (not a silent mailto), (3) a Pro user sees no banner.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  if (!Element.prototype.hasPointerCapture) Element.prototype.hasPointerCapture = () => false
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {}
})

beforeEach(() => vi.clearAllMocks())
afterEach(() => vi.restoreAllMocks())

function mockPlan(plan: Record<string, unknown>) {
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    if (String(url).endsWith("/usage/plan")) {
      return { ok: true, status: 200, json: async () => plan }
    }
    return { ok: true, status: 200, json: async () => ({}) }
  })
}

describe("PlanBanner — Upgrade to Pro", () => {
  it("shows the free-plan banner and opens the contact dialog on click", async () => {
    mockPlan({ plan: "free", pipelines_limit: 2, pipelines_used: 0 })
    render(<PlanBanner />)

    expect(await screen.findByText(/Free plan — 0\/2 pipelines used/)).toBeInTheDocument()

    // The CTA is a real button (not a bare mailto anchor).
    fireEvent.click(screen.getByRole("button", { name: /upgrade to pro/i }))

    // Clicking surfaces the in-app dialog with a clear contact-the-team path.
    expect(await screen.findByText(/contact the rsync team/i)).toBeInTheDocument()
    expect(screen.getByText("sales@rsync.ai")).toBeInTheDocument()
  })

  it("renders nothing for a pro plan", async () => {
    mockPlan({ plan: "pro" })
    const { container } = render(<PlanBanner />)
    await waitFor(() => expect(authFetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })
})

// Issues #7/#21: the banner undercounted (0/2 with one pipeline running) because
// it read /auth/me once when the persistent layout mounted. It now reads the
// active-workspace meter (/api/v1/usage/plan, the create gate's own count) and
// re-reads when a pipeline is created or deleted, or the tab regains focus.
describe("PlanBanner — pipeline meter stays current", () => {
  it("reads the active-workspace meter, not /auth/me", async () => {
    mockPlan({ plan: "free", pipelines_limit: 2, pipelines_used: 1 })
    render(<PlanBanner />)
    expect(await screen.findByText(/Free plan — 1\/2 pipelines used/)).toBeInTheDocument()
    const urls = (authFetch as Mock).mock.calls.map((c) => String(c[0]))
    expect(urls).toContain("/api/v1/usage/plan")
    expect(urls).not.toContain("/api/v1/auth/me")
  })

  it("refetches after a pipeline is created and on focus", async () => {
    let used = 0
    ;(authFetch as Mock).mockImplementation(async (url: string) => {
      if (String(url).endsWith("/usage/plan")) {
        return { ok: true, status: 200, json: async () => ({ plan: "free", pipelines_limit: 2, pipelines_used: used }) }
      }
      return { ok: true, status: 200, json: async () => ({}) }
    })
    render(<PlanBanner />)
    expect(await screen.findByText(/Free plan — 0\/2 pipelines used/)).toBeInTheDocument()

    used = 1
    notifyPipelinesChanged()
    expect(await screen.findByText(/Free plan — 1\/2 pipelines used/)).toBeInTheDocument()

    used = 2
    window.dispatchEvent(new Event("focus"))
    expect(await screen.findByText(/Free plan — 2\/2 pipelines used/)).toBeInTheDocument()
  })

  it("falls back to /auth/me when the gateway has no /usage/plan route", async () => {
    ;(authFetch as Mock).mockImplementation(async (url: string) => {
      if (String(url).endsWith("/usage/plan")) return { ok: false, status: 404, json: async () => ({}) }
      if (String(url).endsWith("/auth/me")) {
        return { ok: true, status: 200, json: async () => ({ plan: "free", pipelines_limit: 2, pipelines_used: 2 }) }
      }
      return { ok: true, status: 200, json: async () => ({}) }
    })
    render(<PlanBanner />)
    expect(await screen.findByText(/Free plan — 2\/2 pipelines used/)).toBeInTheDocument()
  })
})
