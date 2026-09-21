import { Suspense } from "react"
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

import ConnectionsPage from "@/app/(dashboard)/connections/page"
import ConnectionDetailPage from "@/app/(dashboard)/connections/[id]/page"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"

// ---------------------------------------------------------------------------
// Why did the connection test fail? The gateway stores the scrubbed error in
// last_test_error on every test of a stored connection (connections.go
// TestConnection) and returns it on list and detail GETs. Both pages used to
// drop it: a failed test read "Not tested", and a sessionStorage overlay kept
// a connection "Connected" after a later test failed. These render the real
// pages against what the gateway stores.
// ---------------------------------------------------------------------------

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))
const router = vi.hoisted(() => ({ push: () => {}, refresh: () => {}, replace: () => {} }))
vi.mock("next/navigation", () => ({
  useRouter: () => router,
  useSearchParams: () => new URLSearchParams(),
  usePathname: () => "/connections",
  notFound: vi.fn(),
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
vi.mock("@/components/connectors/ConnectionLogo", () => ({
  ConnectionLogo: () => <span data-testid="logo" />,
}))
vi.mock("@/components/connectors/GenericConnectorForm", () => ({
  GenericConnectorForm: () => <div data-testid="form" />,
}))
vi.mock("@/components/oauth/OAuthConnectButton", () => ({
  OAuthConnectButton: () => <button type="button">Connect</button>,
}))

const mockFetch = authFetch as unknown as Mock
const ERR = 'password authentication failed for user "etl"'
const minutesAgo = (m: number) => new Date(Date.now() - m * 60_000).toISOString()

type Stored = { last_test_status?: string; last_test_error?: string; last_tested_at?: string }

function conn(stored: Stored) {
  return {
    id: "conn-1",
    name: "Orders Postgres",
    description: "",
    type: "source",
    connector_type: "postgresql",
    sync_mode: "batch",
    config: {},
    status: "active",
    is_connected: stored.last_test_status === "success",
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-01T00:00:00Z",
    ...stored,
  }
}

function res(body: unknown, status = 200) {
  return { ok: status < 400, status, headers: { get: () => null }, json: async () => body } as unknown as Response
}

/**
 * A tiny gateway: GET returns the stored row; POST …/test answers with
 * `nextTest` and stores it, as TestConnection does.
 */
function gateway(initial: Stored, nextTest: { success: boolean; error?: string }) {
  let stored = initial
  mockFetch.mockImplementation(async (url: string, init?: RequestInit) => {
    if (url === API_ENDPOINTS.OAUTH.TOKENS) return res({ tokens: [] })
    if (url.includes("/api/v1/connectors/")) return res({}, 404)
    if (url.startsWith(API_ENDPOINTS.CONNECTIONS.TEST_BY_ID("conn-1")) && init?.method === "POST") {
      stored = nextTest.success
        ? { last_test_status: "success", last_test_error: "", last_tested_at: minutesAgo(0) }
        : { last_test_status: "failed", last_test_error: nextTest.error, last_tested_at: minutesAgo(0) }
      return res(nextTest.success ? { success: true, status: "success" } : { success: false, status: "failed", error: nextTest.error })
    }
    if (url === API_ENDPOINTS.CONNECTIONS.LIST) return res({ connections: [conn(stored)] })
    if (url === API_ENDPOINTS.CONNECTIONS.GET("conn-1")) return res(conn(stored))
    return res({})
  })
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
afterEach(() => cleanup())

async function renderDetail() {
  await act(async () => {
    render(
      <Suspense fallback={<div>loading</div>}>
        <ConnectionDetailPage params={Promise.resolve({ id: "conn-1" })} />
      </Suspense>
    )
  })
  await screen.findByRole("heading", { name: "Orders Postgres" })
}

describe("connection page — last test error", () => {
  it("shows the stored error, and clears it when a re-test passes", async () => {
    gateway({ last_test_status: "failed", last_test_error: ERR, last_tested_at: minutesAgo(12) }, { success: true })
    await renderDetail()

    const alert = screen.getByTestId("connection-last-test-failed")
    expect(alert).toHaveTextContent(ERR)
    expect(screen.getByTestId("connection-last-test")).toHaveTextContent("Failed · 12m ago")

    await userEvent.click(within(alert).getByRole("button", { name: "Test again" }))

    await waitFor(() => expect(screen.queryByTestId("connection-last-test-failed")).toBeNull())
    expect(screen.getByTestId("connection-last-test")).toHaveTextContent("Succeeded · just now")
  })

  it("a test that fails from a passing state shows the new error without a reload", async () => {
    gateway({ last_test_status: "success", last_test_error: "", last_tested_at: minutesAgo(30) }, { success: false, error: ERR })
    await renderDetail()
    expect(screen.queryByTestId("connection-last-test-failed")).toBeNull()

    await userEvent.click(screen.getByRole("button", { name: "Test Connection" }))

    expect(await screen.findByTestId("connection-last-test-failed")).toHaveTextContent(ERR)
  })
})

describe("connections list — last test error", () => {
  it("a failed connection reads Test failed with its reason, not Not tested", async () => {
    gateway({ last_test_status: "failed", last_test_error: ERR, last_tested_at: minutesAgo(12) }, { success: false, error: ERR })
    render(<ConnectionsPage />)

    await screen.findByText("Orders Postgres")
    expect(screen.getByText("Test failed")).toBeInTheDocument()
    expect(screen.queryByText("Not tested")).toBeNull()
    expect(screen.getByTestId("connection-last-test-error")).toHaveTextContent(`Last test failed 12m ago: ${ERR}`)
  })

  it("a test that fails turns a Connected row into Test failed with the reason", async () => {
    gateway({ last_test_status: "success", last_test_error: "", last_tested_at: minutesAgo(30) }, { success: false, error: ERR })
    const user = userEvent.setup()
    render(<ConnectionsPage />)
    await screen.findByText("Connected")

    await user.click(screen.getByRole("button", { name: "Actions for Orders Postgres" }))
    await user.click(await screen.findByRole("menuitem", { name: /test connection/i }))

    expect(await screen.findByText("Test failed")).toBeInTheDocument()
    expect(screen.queryByText("Connected")).toBeNull()
    expect(screen.getByTestId("connection-last-test-error")).toHaveTextContent(ERR)
  })

  // #45 prod retest: the cards printed the raw type — "postgresql connection",
  // "Type: postgresql" — where the rest of the app says "PostgreSQL".
  it("names the connector the way the rest of the app does", async () => {
    gateway({ last_test_status: "success", last_test_error: "", last_tested_at: minutesAgo(30) }, { success: true })
    render(<ConnectionsPage />)
    await screen.findByText("Orders Postgres")

    expect(screen.getByText("PostgreSQL connection")).toBeInTheDocument()
    expect(screen.getByText("Type: PostgreSQL")).toBeInTheDocument()
    expect(screen.queryByText(/postgresql connection|Type: postgresql/)).toBeNull()
  })
})
