import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"

import AdminSettingsPage from "@/app/(dashboard)/admin/settings/page"
import { authFetch } from "@/lib/api/auth-fetch"

// F-260. The page handles 401, 403 and 429 by name and lets every other status
// fall through to `await res.json()` — which succeeds, because the handler
// returns `{"error": "..."}` on failure (admin_settings.go:17 and :23). So the
// component's catch never runs, `json.settings?.registration_mode` is undefined,
// and `|| "open"` selects **Open Registration**.
//
// The harm is not the wrong pixel. The form stays live, so pressing Save writes
// that fabricated default back — turning an invite-only instance into an open
// one from a read that never came back.

// AdminNav (rendered above the card) calls both of these; without them React
// throws "app router is not mounted" and every case fails for a reason that has
// nothing to do with the defect under test.
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/admin/settings",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("@/lib/api/admin", () => ({
  adminGetSettings: vi.fn(),
  adminUpdateSettings: vi.fn(),
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const mockFetch = authFetch as unknown as Mock

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

beforeEach(() => {
  mockFetch.mockReset()
})

const SAVE = /save changes/i
const LOAD_FAILED = /could not load/i

function checked(id: string): string | null {
  return document.getElementById(id)?.getAttribute("aria-checked") ?? null
}

describe("Admin Settings — a failed read must not read as Open Registration", () => {
  it("does not show Open Registration selected when the read returns 500", async () => {
    mockFetch.mockResolvedValue(res(500, { error: "Failed to load settings" }))
    render(<AdminSettingsPage />)

    await waitFor(() => expect(screen.getByText(LOAD_FAILED)).toBeInTheDocument())
    expect(checked("reg-open")).not.toBe("true")
  })

  it("hides Save after a failed read, so the default cannot be written back", async () => {
    mockFetch.mockResolvedValue(res(500, { error: "Failed to load settings" }))
    render(<AdminSettingsPage />)

    await waitFor(() => expect(screen.getByText(LOAD_FAILED)).toBeInTheDocument())
    expect(screen.queryByRole("button", { name: SAVE })).not.toBeInTheDocument()
  })

  it("says the read failed when the request throws", async () => {
    mockFetch.mockRejectedValue(new Error("network down"))
    render(<AdminSettingsPage />)

    await waitFor(() => expect(screen.getByText(LOAD_FAILED)).toBeInTheDocument())
    expect(screen.queryByRole("button", { name: SAVE })).not.toBeInTheDocument()
  })

  it("offers a retry that re-reads, and shows the settings once it succeeds", async () => {
    mockFetch
      .mockResolvedValueOnce(res(500, { error: "Failed to load settings" }))
      .mockResolvedValueOnce(res(200, { settings: { registration_mode: "invite_only" } }))
    render(<AdminSettingsPage />)

    await waitFor(() => expect(screen.getByText(LOAD_FAILED)).toBeInTheDocument())
    screen.getByRole("button", { name: /retry/i }).click()

    await waitFor(() => expect(checked("reg-invite")).toBe("true"))
  })
})

describe("Admin Settings — the successful paths are unchanged", () => {
  it("renders the stored mode on a clean read", async () => {
    mockFetch.mockResolvedValue(res(200, { settings: { registration_mode: "invite_only" } }))
    render(<AdminSettingsPage />)

    await waitFor(() => expect(checked("reg-invite")).toBe("true"))
    expect(checked("reg-open")).toBe("false")
    expect(screen.getByRole("button", { name: SAVE })).toBeInTheDocument()
  })

  it("defaults to open when the read succeeds and the key is genuinely unset", async () => {
    mockFetch.mockResolvedValue(res(200, { settings: {} }))
    render(<AdminSettingsPage />)

    await waitFor(() => expect(checked("reg-open")).toBe("true"))
    expect(screen.queryByText(LOAD_FAILED)).not.toBeInTheDocument()
  })

  it("still shows Access denied on 403 rather than the failed-read card", async () => {
    mockFetch.mockResolvedValue(res(403, { error: "forbidden" }))
    render(<AdminSettingsPage />)

    await waitFor(() => expect(screen.queryByRole("button", { name: SAVE })).not.toBeInTheDocument())
    expect(screen.queryByText(LOAD_FAILED)).not.toBeInTheDocument()
  })
})
