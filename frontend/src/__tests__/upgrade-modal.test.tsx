import { afterEach, beforeAll, describe, expect, it, vi } from "vitest"
import { render, screen, fireEvent } from "@testing-library/react"

import { UpgradeModal } from "@/components/plan/UpgradeModal"
import { authFetch } from "@/lib/api/auth-fetch"

// The upgrade dialog is shared by two triggers: the 402 plan-limit flows (pass a
// `payload`) and the proactive plan banner (pass `open`). These tests pin both
// paths — especially that generalizing the props did NOT change the 402 behavior —
// and that the primary CTA POSTs an upgrade request so the team is emailed
// automatically (no manual mail step), falling back to copying the sales@ address
// only when the backend can't auto-send.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
const mockAuthFetch = vi.mocked(authFetch)

// Dialog is a plain conditional render, but stub jsdom-absent APIs defensively.
beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  if (!Element.prototype.hasPointerCapture) Element.prototype.hasPointerCapture = () => false
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {}
})

afterEach(() => {
  vi.restoreAllMocks()
  mockAuthFetch.mockReset()
})

describe("UpgradeModal", () => {
  it("is closed when neither open nor payload is provided (default = old !!payload)", () => {
    render(<UpgradeModal onClose={() => {}} />)
    expect(screen.queryByText(/pro plan includes/i)).toBeNull()
  })

  it("stays closed when open is explicitly false even with a payload (respects `??`, not `||`)", () => {
    render(
      <UpgradeModal
        open={false}
        payload={{ error: "pipeline_limit_reached", message: "x", used: 2, limit: 2, plan: "free" }}
        onClose={() => {}}
      />,
    )
    expect(screen.queryByText(/pipeline limit reached/i)).toBeNull()
  })

  it("opens proactively (no payload) with Upgrade-to-Pro copy + the sales@ contact", () => {
    render(<UpgradeModal open onClose={() => {}} />)
    expect(screen.getByText("Upgrade to Pro")).toBeInTheDocument()
    expect(screen.getByText(/pro unlocks unlimited pipelines/i)).toBeInTheDocument()
    // Address shown as selectable text.
    expect(screen.getByText("sales@rsync.ai")).toBeInTheDocument()
    // Primary CTA is an in-app button, not a mailto link (no external-app redirect).
    expect(screen.getByRole("button", { name: /contact the rsync team/i })).toBeInTheDocument()
    expect(screen.queryByRole("link", { name: /contact the rsync team/i })).toBeNull()
    // Proactive dismiss label.
    expect(screen.getByRole("button", { name: /maybe later/i })).toBeInTheDocument()
  })

  it("POSTs an upgrade request so the team is emailed automatically, then confirms 'Request sent'", async () => {
    mockAuthFetch.mockResolvedValue({
      ok: true,
      json: async () => ({ status: "sent" }),
    } as Response)
    render(<UpgradeModal open onClose={() => {}} />)
    fireEvent.click(screen.getByRole("button", { name: /contact the rsync team/i }))
    expect(mockAuthFetch).toHaveBeenCalledWith("/api/v1/plan/upgrade-request", { method: "POST" })
    // Button flips to the sent confirmation; no clipboard / mailto involved.
    expect(await screen.findByRole("button", { name: /request sent/i })).toBeInTheDocument()
  })

  it("falls back to copying the address when the backend can't auto-send (self-hosted)", async () => {
    mockAuthFetch.mockResolvedValue({
      ok: true,
      json: async () => ({ status: "unconfigured" }),
    } as Response)
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })
    render(<UpgradeModal open onClose={() => {}} />)
    fireEvent.click(screen.getByRole("button", { name: /contact the rsync team/i }))
    expect(
      await screen.findByRole("button", { name: /copied sales@rsync\.ai/i }),
    ).toBeInTheDocument()
    expect(writeText).toHaveBeenCalledWith("sales@rsync.ai")
  })

  // A self-hoster sets UPGRADE_SALES_EMAIL so upgrade requests reach their own team.
  // The backend honours it and returns it as `contact_email`; before this fix the
  // dialog discarded that field and kept telling their users to email sales@rsync.ai.
  it("adopts the contact address the backend reports (operator set UPGRADE_SALES_EMAIL)", async () => {
    mockAuthFetch.mockResolvedValue({
      ok: true,
      json: async () => ({ status: "unconfigured", contact_email: "billing@acme.example" }),
    } as Response)
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })
    render(<UpgradeModal open onClose={() => {}} />)
    fireEvent.click(screen.getByRole("button", { name: /contact the rsync team/i }))

    expect(
      await screen.findByRole("button", { name: /copied billing@acme\.example/i }),
    ).toBeInTheDocument()
    // Copied on the same click that learned the address, not one click late.
    expect(writeText).toHaveBeenCalledWith("billing@acme.example")
    expect(writeText).not.toHaveBeenCalledWith("sales@rsync.ai")
    // And the selectable text follows too, so both places agree.
    expect(screen.getByText("billing@acme.example")).toBeInTheDocument()
    expect(screen.queryByText("sales@rsync.ai")).toBeNull()
  })

  it("renders the 402 limit-reached payload unchanged (title, count, and Cancel)", () => {
    render(
      <UpgradeModal
        payload={{ error: "pipeline_limit_reached", message: "x", used: 2, limit: 2, plan: "free" }}
        onClose={() => {}}
      />,
    )
    expect(screen.getByText(/pipeline limit reached/i)).toBeInTheDocument()
    expect(screen.getByText(/you've used 2 of 2 allowed pipelines/i)).toBeInTheDocument()
    // 402 flows keep "Cancel", not the proactive "Maybe later".
    expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument()
    // Contact address fully migrated off hello@.
    expect(screen.queryByText("hello@rsync.ai")).toBeNull()
  })

  it("renders the trial-expired payload", () => {
    render(<UpgradeModal payload={{ error: "trial_expired", message: "x" }} onClose={() => {}} />)
    expect(screen.getByText("Trial expired")).toBeInTheDocument()
  })

  it("calls onClose when the dismiss button is clicked", () => {
    const onClose = vi.fn()
    render(<UpgradeModal open onClose={onClose} />)
    fireEvent.click(screen.getByRole("button", { name: /maybe later/i }))
    expect(onClose).toHaveBeenCalledTimes(1)
  })
})
