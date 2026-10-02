import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import {
  ComponentHealthTable,
  ageLabel,
  sortComponents,
  type ComponentHealth,
} from "@/components/admin/ComponentHealthTable"
import AdminHealthPage from "@/app/(dashboard)/admin/health/page"
import { authFetch } from "@/lib/api/auth-fetch"

// sentinel_component_health had an API (monitoring.go GetSentinelHealth) and no reader.
// admin/health now draws it, and the route is admin-only (main.go, AdminRoleMiddleware).

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/admin/health",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
// The page also renders the browser-side probes; with none, the tests stay off the network.
vi.mock("@/lib/diagnostics/browser-connectivity", () => ({ BROWSER_PROBES: [] }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const mockFetch = authFetch as unknown as Mock
const SENTINEL = "/api/v1/monitoring/sentinel/health"

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

function ago(ms: number): string {
  return new Date(Date.now() - ms).toISOString()
}

const MIN = 60 * 1000

function component(over: Partial<ComponentHealth>): ComponentHealth {
  return {
    component_id: "c",
    component_type: "infrastructure",
    status: "healthy",
    last_heartbeat: ago(10 * 1000),
    messages_processed: 0,
    error_count: 0,
    updated_at: ago(10 * 1000),
    ...over,
  }
}

const fleet: ComponentHealth[] = [
  component({ component_id: "infrastructure:postgres" }),
  component({
    component_id: "orchestrator-agent",
    component_type: "agent",
    status: "dead",
    last_heartbeat: ago(3 * MIN),
    messages_processed: 1200,
    error_count: 7,
    consumer_lag: 0,
    updated_at: ago(20 * 1000),
  }),
  component({
    component_id: "rsync.pipeline.events",
    component_type: "kafka_consumer",
    status: "healthy",
    consumer_lag: 4500,
    last_heartbeat: ago(2 * 60 * MIN),
    updated_at: ago(2 * 60 * MIN),
  }),
  component({
    component_id: "mcp_connector:rsync-mcp-postgres-v1",
    component_type: "mcp_connector",
    status: "unhealthy",
    last_error: "dial tcp: connection refused",
  }),
]

function services(): HTMLElement {
  return screen.getByRole("region", { name: "rsync.ai services" })
}

function findServices(): Promise<HTMLElement> {
  return screen.findByRole("region", { name: "rsync.ai services" })
}

function connectors(): HTMLElement {
  return screen.getByRole("region", { name: "Connectors" })
}

function rowFor(id: string): HTMLElement {
  const row = document.querySelector<HTMLElement>(`tr[data-component="${id}"]`)
  if (!row) throw new Error(`no row for ${id}`)
  return row
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

describe("ageLabel and sortComponents", () => {
  const now = Date.parse("2026-09-18T12:00:00Z")

  it("labels ages against the fetch time and gives Go's zero time a dash", () => {
    expect(ageLabel("2026-09-18T11:59:30Z", now)).toBe("just now")
    expect(ageLabel("2026-09-18T11:48:00Z", now)).toBe("12m ago")
    expect(ageLabel("2026-09-18T07:00:00Z", now)).toBe("5h ago")
    expect(ageLabel("2026-09-15T12:00:00Z", now)).toBe("3d ago")
    expect(ageLabel("0001-01-01T00:00:00Z", now)).toBe("—")
    expect(ageLabel("not a time", now)).toBe("—")
  })

  it("puts the worst status first, then the component heard from longest ago", () => {
    const sorted = sortComponents([
      component({ component_id: "ok-new", status: "healthy", last_heartbeat: "2026-09-18T11:59:00Z" }),
      component({ component_id: "ok-old", status: "healthy", last_heartbeat: "2026-09-18T10:00:00Z" }),
      component({ component_id: "weird", status: "exploded" }),
      component({ component_id: "degraded", status: "degraded" }),
      component({ component_id: "dead", status: "dead" }),
      component({ component_id: "unhealthy", status: "unhealthy" }),
    ])
    expect(sorted.map((c) => c.component_id)).toEqual([
      "dead",
      "unhealthy",
      "degraded",
      "weird",
      "ok-old",
      "ok-new",
    ])
  })
})

describe("ComponentHealthTable", () => {
  it("reads the admin route and shows every row, worst first, services before connectors", async () => {
    mockFetch.mockResolvedValue(res(200, { components: fleet, total: fleet.length }))
    render(<ComponentHealthTable refreshToken={0} />)

    await screen.findByText("orchestrator-agent")
    expect(mockFetch).toHaveBeenCalledWith(SENTINEL, { method: "GET" })

    const ids = (root: ParentNode) =>
      [...root.querySelectorAll("tr[data-component]")].map((r) => r.getAttribute("data-component"))
    // The stale consumer is healthy but 2h old, so it reads "no recent check" and sorts above
    // the freshly checked postgres row.
    expect(ids(services())).toEqual(["orchestrator-agent", "rsync.pipeline.events", "infrastructure:postgres"])
    expect(ids(connectors())).toEqual(["mcp_connector:rsync-mcp-postgres-v1"])

    const summary = screen.getByLabelText("Status summary")
    expect(summary).toHaveTextContent("1 dead")
    expect(summary).toHaveTextContent("1 no recent check")
    expect(summary).toHaveTextContent("1 healthy")
    expect(summary).not.toHaveTextContent("unhealthy")
    expect(summary).not.toHaveTextContent("degraded")
    expect(screen.getByLabelText("Connector status summary")).toHaveTextContent("1 unhealthy")
  })

  it("shows counts only where the type measures them, and errors in red", async () => {
    mockFetch.mockResolvedValue(res(200, { components: fleet }))
    render(<ComponentHealthTable refreshToken={0} />)
    await screen.findByText("orchestrator-agent")

    const cells = (id: string) => [...rowFor(id).querySelectorAll("td")].map((td) => td.textContent)

    // Agent: processed, errors and lag all come from its heartbeat; a measured 0 lag is "0".
    const agent = cells("orchestrator-agent")
    expect(agent.slice(3, 6)).toEqual([(1200).toLocaleString(), "7", "0"])
    expect(rowFor("orchestrator-agent").querySelectorAll("td")[4].className).toMatch(/text-red-600/)

    // Kafka consumer: lag only.
    expect(cells("rsync.pipeline.events").slice(3, 6)).toEqual(["—", "—", (4500).toLocaleString()])

    // Infrastructure stores 0 because nothing measured it: a dash, not "0 processed".
    expect(cells("infrastructure:postgres").slice(3, 6)).toEqual(["—", "—", "—"])

    const err = within(rowFor("mcp_connector:rsync-mcp-postgres-v1")).getByText("dial tcp: connection refused")
    expect(err).toHaveAttribute("title", "dial tcp: connection refused")
    expect(err.className).toMatch(/text-red-600/)
  })

  it("marks a row the Sentinel has not rewritten in 5 minutes", async () => {
    mockFetch.mockResolvedValue(res(200, { components: fleet }))
    render(<ComponentHealthTable refreshToken={0} />)
    await screen.findByText("orchestrator-agent")

    expect(within(rowFor("rsync.pipeline.events")).getByText("Row not updated 2h ago")).toBeInTheDocument()
    expect(within(rowFor("infrastructure:postgres")).queryByText(/Row not updated/)).toBeNull()
    // A dead agent's heartbeat is old but its row is fresh: the verdict is current, not stale.
    expect(within(rowFor("orchestrator-agent")).getByText("3m ago")).toBeInTheDocument()
    expect(within(rowFor("orchestrator-agent")).queryByText(/Row not updated/)).toBeNull()
  })

  it("does not show green for a healthy row nobody has re-checked", async () => {
    mockFetch.mockResolvedValue(res(200, { components: fleet }))
    render(<ComponentHealthTable refreshToken={0} />)
    await screen.findByText("orchestrator-agent")

    const stale = within(rowFor("rsync.pipeline.events"))
    expect(stale.getByText("no recent check")).toBeInTheDocument()
    expect(stale.queryByText("healthy")).toBeNull()
    expect(rowFor("rsync.pipeline.events").querySelector("[data-status-dot]")).toHaveAttribute("data-status-dot", "stale")
    expect(within(rowFor("infrastructure:postgres")).getByText("healthy")).toBeInTheDocument()
  })

  it("keeps the last verdict of a stale row that was already down", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        components: [
          component({ component_id: "infrastructure:kafka", status: "unhealthy", updated_at: ago(30 * MIN) }),
        ],
      }),
    )
    render(<ComponentHealthTable refreshToken={0} />)
    await screen.findByText("infrastructure:kafka")

    expect(within(rowFor("infrastructure:kafka")).getByText("unhealthy")).toBeInTheDocument()
    expect(within(rowFor("infrastructure:kafka")).getByText("Row not updated 30m ago")).toBeInTheDocument()
    expect(screen.getByLabelText("Status summary")).toHaveTextContent("1 unhealthy")
  })

  it("filters by type and counts each type", async () => {
    mockFetch.mockResolvedValue(res(200, { components: fleet }))
    render(<ComponentHealthTable refreshToken={0} />)
    await screen.findByText("orchestrator-agent")

    // Connectors have their own section, so they are not a filter here.
    const group = screen.getByRole("group", { name: "Filter by type" })
    expect(within(group).queryByRole("button", { name: /MCP connector/ })).toBeNull()
    expect(within(group).getByRole("button", { name: /All/ })).toHaveTextContent("All3")

    await userEvent.click(within(group).getByRole("button", { name: /Kafka consumer/ }))
    expect(services().querySelectorAll("tr[data-component]")).toHaveLength(1)
    expect(rowFor("rsync.pipeline.events")).toBeInTheDocument()
    expect(within(group).getByRole("button", { name: /Kafka consumer/ })).toHaveAttribute("aria-pressed", "true")

    await userEvent.click(within(group).getByRole("button", { name: /All/ }))
    expect(services().querySelectorAll("tr[data-component]")).toHaveLength(3)
  })

  it("folds connectors away while all of them are up, and opens on request", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        components: [
          component({ component_id: "infrastructure:postgres" }),
          component({ component_id: "mcp_connector:rsync-mcp-postgres-v1", component_type: "mcp_connector" }),
          // A row left by an older pinned version: stale, but not a reason to open the section.
          component({
            component_id: "mcp_connector:rsync-mcp-gcs-v1",
            component_type: "mcp_connector",
            updated_at: ago(3 * 60 * MIN),
          }),
        ],
      }),
    )
    render(<ComponentHealthTable refreshToken={0} />)
    await screen.findByText("infrastructure:postgres")

    expect(connectors().querySelector("tr[data-component]")).toBeNull()
    const summary = screen.getByLabelText("Connector status summary")
    expect(summary).toHaveTextContent("1 healthy")
    expect(summary).toHaveTextContent("1 no recent check")

    const toggle = within(connectors()).getByRole("button", { name: "Show connectors" })
    expect(toggle).toHaveAttribute("aria-expanded", "false")
    await userEvent.click(toggle)
    expect(within(connectors()).getByRole("button", { name: "Hide connectors" })).toHaveAttribute(
      "aria-expanded",
      "true",
    )
    expect(connectors().querySelectorAll("tr[data-component]")).toHaveLength(2)
  })

  it("opens the connectors when a refresh finds one down", async () => {
    const up = component({ component_id: "mcp_connector:rsync-mcp-postgres-v1", component_type: "mcp_connector" })
    mockFetch.mockResolvedValueOnce(res(200, { components: [up] }))
    mockFetch.mockResolvedValueOnce(res(200, { components: [{ ...up, status: "unhealthy" }] }))
    const { rerender } = render(<ComponentHealthTable refreshToken={0} />)
    await screen.findByRole("button", { name: "Show connectors" })

    rerender(<ComponentHealthTable refreshToken={1} />)
    expect(await screen.findByRole("button", { name: "Hide connectors" })).toHaveAttribute("aria-expanded", "true")
    expect(within(rowFor("mcp_connector:rsync-mcp-postgres-v1")).getByText("unhealthy")).toBeInTheDocument()
  })

  it("says so when only connectors have reported", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        components: [
          component({ component_id: "mcp_connector:rsync-mcp-postgres-v1", component_type: "mcp_connector" }),
        ],
      }),
    )
    render(<ComponentHealthTable refreshToken={0} />)
    expect(await within(await findServices()).findByText("No rsync.ai service has reported yet.")).toBeInTheDocument()
    expect(screen.getByLabelText("Connector status summary")).toHaveTextContent("1 healthy")
  })

  it("says the feature is off on a 404", async () => {
    mockFetch.mockResolvedValue(res(404, { error: "not found" }))
    render(<ComponentHealthTable refreshToken={0} />)
    expect(await screen.findByText(/Worker health is off/)).toHaveTextContent("FEATURE_MONITORING_INFRA=true")
  })

  it("says so when nothing has reported", async () => {
    mockFetch.mockResolvedValue(res(200, { components: [], total: 0 }))
    render(<ComponentHealthTable refreshToken={0} />)
    expect(await screen.findByText(/No component has reported yet/)).toBeInTheDocument()
  })

  it("offers a retry after a failed first read", async () => {
    mockFetch.mockResolvedValueOnce(res(500, { error: "Failed to fetch health data" }))
    mockFetch.mockResolvedValueOnce(res(200, { components: fleet }))
    render(<ComponentHealthTable refreshToken={0} />)

    await screen.findByText("Could not load worker health.")
    await userEvent.click(screen.getByRole("button", { name: "Retry" }))
    await screen.findByText("orchestrator-agent")
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it("keeps the last rows when a refresh fails, and says so", async () => {
    mockFetch.mockResolvedValueOnce(res(200, { components: fleet }))
    mockFetch.mockRejectedValueOnce(new Error("network"))
    const { rerender } = render(<ComponentHealthTable refreshToken={0} />)
    await screen.findByText("orchestrator-agent")

    rerender(<ComponentHealthTable refreshToken={1} />)
    expect(await screen.findByRole("status")).toHaveTextContent(/Could not refresh/)
    expect(screen.getByText("orchestrator-agent")).toBeInTheDocument()
  })
})

describe("admin/health page", () => {
  it("draws the worker table under the service cards and refreshes both together", async () => {
    mockFetch.mockImplementation(async (url: string) =>
      url === SENTINEL
        ? res(200, { components: fleet })
        : res(200, { services: [{ service: "postgresql", status: "up", latency_ms: 2 }] }),
    )
    render(<AdminHealthPage />)

    await screen.findByText("orchestrator-agent")
    expect(screen.getByText("Worker health")).toBeInTheDocument()
    const sentinelCalls = () => mockFetch.mock.calls.filter(([u]) => u === SENTINEL).length
    expect(sentinelCalls()).toBe(1)

    await userEvent.click(screen.getByRole("button", { name: /Refresh/ }))
    await waitFor(() => expect(sentinelCalls()).toBe(2))
  })
})
