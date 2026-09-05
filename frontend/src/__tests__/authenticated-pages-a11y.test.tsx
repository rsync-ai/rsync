import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import axe from "axe-core"

import SettingsPage from "@/app/(dashboard)/settings/page"
import ConnectionsPage from "@/app/(dashboard)/connections/page"
import { Sidebar } from "@/components/layout/Sidebar"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"

// KI-2. The Playwright a11y suite (`npm run test:a11y`) is the only thing that can
// measure these routes end to end, and it CANNOT run them in CI: the frontend-a11y
// job starts no api-gateway, so `e2e/fixtures/auth.setup.ts` skips on its :5001
// health probe and all four authenticated specs in `e2e/a11y/authenticated_pages.spec.ts`
// self-skip — a skip that reads as a pass in the report.
//
// This file is the guard that actually runs on every PR: `src/__tests__/**` is in the
// vitest `include` list, and ci.yml runs `npm test` in the `frontend` job. It renders
// the same components under jsdom and applies the SAME blocking bar as the Playwright
// spec (`e2e/a11y/authenticated_pages.spec.ts:33` — zero serious/critical violations
// under wcag2a/wcag2aa/wcag21a/wcag21aa), so the two gates agree on what "blocking" means.
//
// SCOPE — this is a name/role/label gate, NOT a contrast gate. `color-contrast` is
// explicitly disabled because jsdom has no layout and no paint: the rule needs real
// computed colours (and `HTMLCanvasElement.getContext`, which jsdom does not implement),
// so it can only ever return `incomplete`, never `violations`. Contrast on these routes
// stays unmeasured until `npm run test:a11y` runs against a live stack.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn(), replace: vi.fn() }),
  usePathname: () => "/settings",
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
// Collapsed rail AND open mobile drawer, so both icon-only Sidebar buttons —
// the collapse toggle and the drawer's X — are in the measured DOM at once.
vi.mock("@/lib/store/useUIStore", () => ({
  useUIStore: (selector: (s: Record<string, unknown>) => unknown) =>
    selector({
      sidebarCollapsed: true,
      sidebarOpen: true,
      setSidebarOpen: () => {},
      toggleSidebarCollapsed: () => {},
    }),
}))

const mockFetch = authFetch as unknown as Mock

function res(body: unknown, status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

type Impact = "minor" | "moderate" | "serious" | "critical"
const BLOCKING: Impact[] = ["serious", "critical"]

/**
 * Runs axe over `container` and returns one string per blocking violation, e.g.
 * `button-name (critical) x3`. An empty array is the pass condition.
 */
async function blockingViolations(container: HTMLElement): Promise<string[]> {
  const results = await axe.run(container, {
    runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"] },
    // See the SCOPE note at the top of this file: jsdom cannot evaluate contrast.
    rules: { "color-contrast": { enabled: false } },
  })
  return results.violations
    .filter((v) => BLOCKING.includes(v.impact as Impact))
    .map((v) => `${v.id} (${v.impact}) x${v.nodes.length}`)
}

beforeAll(() => {
  // Radix (Switch, DropdownMenu, AlertDialog, ScrollArea) observes element size.
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  // The connections page overlays optimistic test results from sessionStorage;
  // Node's storage shim can leave it without the Storage methods (see vitest.setup.ts
  // for the same problem on localStorage).
  if (typeof window.sessionStorage?.getItem !== "function") {
    const store = new Map<string, string>()
    Object.defineProperty(window, "sessionStorage", {
      configurable: true,
      value: {
        get length() {
          return store.size
        },
        key: (i: number) => Array.from(store.keys())[i] ?? null,
        getItem: (k: string) => store.get(k) ?? null,
        setItem: (k: string, v: string) => void store.set(k, String(v)),
        removeItem: (k: string) => void store.delete(k),
        clear: () => store.clear(),
      } as Storage,
    })
  }
})

beforeEach(() => {
  mockFetch.mockReset()
})

describe("authenticated pages — no serious/critical axe violations", () => {
  it("/settings", async () => {
    mockFetch.mockResolvedValue(res({ name: "Ada Lovelace", email: "ada@example.com" }))

    const { container } = render(<SettingsPage />)
    // Wait for the profile GET to land, so the real (non-empty) form is measured.
    await waitFor(() => expect(screen.getByDisplayValue("ada@example.com")).toBeInTheDocument())

    expect(await blockingViolations(container)).toEqual([])
  })

  it("/connections", async () => {
    // A zero-row render would make this case vacuously green: the row-actions
    // button only exists once per connection. Assert the row is present first.
    mockFetch.mockImplementation((url: string) => {
      if (url === API_ENDPOINTS.OAUTH.TOKENS) return Promise.resolve(res({ tokens: [] }))
      if (url === API_ENDPOINTS.CONNECTIONS.LIST) {
        return Promise.resolve(
          res({
            connections: [
              {
                id: "conn-1",
                name: "Analytics Postgres",
                description: "Primary warehouse",
                type: "source",
                connector_type: "postgresql",
                is_connected: true,
                config: {},
                created_at: "2026-01-02T03:04:05Z",
                updated_at: "2026-01-02T03:04:05Z",
              },
            ],
          }),
        )
      }
      return Promise.resolve(res({}))
    })

    const { container } = render(<ConnectionsPage />)
    await waitFor(() => expect(screen.getByText("Analytics Postgres")).toBeInTheDocument())

    expect(await blockingViolations(container)).toEqual([])
  })

  it("sidebar, collapsed rail + mobile drawer", async () => {
    // `sidebarCollapsed` is persisted to localStorage (useUIStore partialize), so any
    // user who collapses the rail carries this state onto every authenticated page —
    // the collapsed branch is the only one where the toggle has no visible text.
    const { container } = render(<Sidebar role="user" />)
    await waitFor(() => expect(screen.getAllByRole("link").length).toBeGreaterThan(0))

    expect(await blockingViolations(container)).toEqual([])
  })
})
