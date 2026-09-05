import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, fireEvent, waitFor } from "@testing-library/react"
import type { Mock } from "vitest"

import { PlanBanner } from "@/components/layout/PlanBanner"
import { authFetch } from "@/lib/api/auth-fetch"

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
    if (String(url).endsWith("/auth/me")) {
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
