import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { act, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import type { Mock } from "vitest"

import { Header } from "@/components/layout/Header"
import { authFetch } from "@/lib/api/auth-fetch"
import type { Notification as AppNotification } from "@/lib/api/notifications"
import { ACTIVE_WORKSPACE_EVENT, ACTIVE_WORKSPACE_KEY } from "@/lib/workspace/active-workspace"

// The header bell polls /notifications/unread-count on mount and shows the count
// as a badge. These pin that wiring: the badge reflects the backend count and
// caps at "9+". (Mark-read lives in the notifications API tests and the Go
// handler tests — this covers the count→badge path end to end.)
//
// The dropdown tests below pin the readability contract: a row shows which
// pipeline it's about, what it means for the user's data, and what to do next —
// and never renders an internal identifier as the headline.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/",
}))
vi.mock("next-themes", () => ({
  useTheme: () => ({ theme: "light", setTheme: vi.fn(), resolvedTheme: "light" }),
}))
vi.mock("@/lib/auth", () => ({ logout: vi.fn(), getUserEmail: () => null }))
vi.mock("@/lib/store/useUIStore", () => ({
  useUIStore: (selector: (s: Record<string, unknown>) => unknown) =>
    selector({ sidebarCollapsed: false, setSidebarOpen: () => {}, toggleCommandPalette: () => {} }),
}))

// Radix primitives probe these jsdom-absent APIs; stub so render never throws.
beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  if (!Element.prototype.hasPointerCapture) Element.prototype.hasPointerCapture = () => false
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {}
})

// Route authFetch by URL: unread-count drives the badge, the LIST endpoint fills
// the dropdown (fetched on open); /workspaces returns an empty list so the
// switcher stays out of the way.
function mockAuthFetch(unreadCount: number, notifications: Partial<AppNotification>[] = []) {
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/notifications/unread-count")) {
      return { ok: true, status: 200, json: async () => ({ unread_count: unreadCount }) }
    }
    if (u.includes("/notifications")) {
      return {
        ok: true,
        status: 200,
        json: async () => ({ notifications, unread_count: unreadCount }),
      }
    }
    if (/\/workspaces$/.test(u)) {
      return { ok: true, status: 200, json: async () => ({ workspaces: [] }) }
    }
    return { ok: true, status: 200, json: async () => ({}) }
  })
}

// A row exactly as the notifier now writes it: human title, owning pipeline,
// plain-language detail, impact line, action verb, code kept as a reference.
const driftRow: Partial<AppNotification> = {
  id: "n1",
  pipeline_id: "pl-1",
  type: "structured_error_notification",
  severity: "warning",
  title: "Approval needed: your source schema changed",
  message: 'A new column "status" was added to pipeline_test.categories.',
  pipeline_name: "orders-sync",
  impact: "Your existing data keeps syncing. The change won't be applied until you approve it.",
  action_label: "Review change",
  reference_code: "SCHEMA_DRIFT_DETECTED",
  action_url: "/pipelines/pl-1/schema-changes",
  read: false,
  read_at: null,
  created_at: new Date().toISOString(),
}

beforeEach(() => {
  vi.clearAllMocks()
  document.cookie = `${ACTIVE_WORKSPACE_KEY}=; path=/; max-age=0; SameSite=Lax`
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe("Header notification bell", () => {
  it("shows the polled unread count on the bell badge", async () => {
    mockAuthFetch(3)
    render(<Header />)

    // Bell trigger is accessible, and the badge reflects the backend count.
    expect(await screen.findByLabelText("Notifications")).toBeInTheDocument()
    expect(await screen.findByText("3")).toBeInTheDocument()

    await waitFor(() =>
      expect(
        (authFetch as Mock).mock.calls.some((c) =>
          String(c[0]).includes("/notifications/unread-count"),
        ),
      ).toBe(true),
    )
  })

  it("caps the badge at 9+ when there are more than 9 unread", async () => {
    mockAuthFetch(15)
    render(<Header />)

    expect(await screen.findByText("9+")).toBeInTheDocument()
  })

  it("renders a row a non-technical user can act on: pipeline, impact and next step", async () => {
    mockAuthFetch(1, [driftRow])
    render(<Header />)

    await userEvent.click(await screen.findByLabelText("Notifications"))

    // What happened, which pipeline, what it means, what to do.
    expect(await screen.findByText(driftRow.title as string)).toBeInTheDocument()
    expect(screen.getByText("orders-sync")).toBeInTheDocument()
    expect(screen.getByText(driftRow.impact as string)).toBeInTheDocument()
    expect(screen.getByText(/Review change/)).toBeInTheDocument()
  })

  it("never renders an internal identifier as the headline", async () => {
    // Guards the reported bug: the bell showed "rsync.healer.results" and
    // "LEGACY_UNCLASSIFIED" as titles. Copy now comes resolved from the server,
    // and the code survives only as a small support reference on failures.
    mockAuthFetch(1, [
      {
        ...driftRow,
        severity: "critical",
        title: "We couldn't reach your PostgreSQL source",
        reference_code: "AUTH_TOKEN_EXPIRED",
        action_label: "Reconnect",
      },
    ])
    render(<Header />)

    await userEvent.click(await screen.findByLabelText("Notifications"))

    const headline = await screen.findByText("We couldn't reach your PostgreSQL source")
    expect(headline.textContent).not.toMatch(/rsync\.|_/)
    // The code is present but demoted — it is not the headline element.
    expect(screen.getByText("AUTH_TOKEN_EXPIRED")).not.toBe(headline)
  })

  it("re-reads the badge count when the active workspace changes", async () => {
    // Found on staging: the count is workspace-scoped, but the badge only refreshed
    // on its 30s tick — so after a switch it kept showing the *previous* workspace's
    // unread number (Beta's 3 while Alpha, with 0 unread, was active).
    mockAuthFetch(3)
    render(<Header />)
    expect(await screen.findByText("3")).toBeInTheDocument()

    // The workspace we switch to has nothing unread.
    mockAuthFetch(0)
    window.dispatchEvent(new Event(ACTIVE_WORKSPACE_EVENT))

    await waitFor(() => expect(screen.queryByText("3")).not.toBeInTheDocument())
  })

  it("drops the previous workspace's rows from the open dropdown on a switch", async () => {
    // The badge already re-polled on switch, but the LIST is only fetched when the
    // dropdown opens — so an open bell kept rendering the old workspace's rows,
    // pipeline names and all, under the new workspace's header. Clearing on the
    // change signal is what makes the rows disappear rather than go stale.
    mockAuthFetch(1, [driftRow])
    render(<Header />)

    await userEvent.click(await screen.findByLabelText("Notifications"))
    expect(await screen.findByText("orders-sync")).toBeInTheDocument()

    mockAuthFetch(0, [])
    await act(async () => {
      window.dispatchEvent(new Event(ACTIVE_WORKSPACE_EVENT))
    })

    await waitFor(() => expect(screen.queryByText("orders-sync")).not.toBeInTheDocument())
  })

  it("falls back to a usable row for pre-catalog notifications", async () => {
    // Rows written before the catalog have no impact/action_label/pipeline_name.
    mockAuthFetch(1, [
      {
        ...driftRow,
        pipeline_name: "",
        impact: "",
        action_label: "",
        reference_code: "",
      },
    ])
    render(<Header />)

    await userEvent.click(await screen.findByLabelText("Notifications"))

    expect(await screen.findByText(driftRow.title as string)).toBeInTheDocument()
    expect(screen.getByText(/View pipeline/)).toBeInTheDocument()
  })
})
