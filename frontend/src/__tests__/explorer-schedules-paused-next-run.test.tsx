import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { cleanup, render, screen, within } from "@testing-library/react"
import ScheduledQueriesPage from "@/app/(dashboard)/explorer/schedules/page"
import { authFetch } from "@/lib/api/auth-fetch"

// Seen in a browser on 2026-09-15: orders_daily, an after_upstream schedule the gateway
// had auto-paused because its connection config could not be decrypted, still said its
// next run was "When an upstream runs". The fire path refuses a schedule that is not
// active (saved_query_schedules.go, "schedule is " + status), so that cell was promising
// a run that would never come.

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/explorer/schedules",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({ role: "admin", can: () => true }),
}))
vi.mock("@/contexts/CurrentUserContext", () => ({
  useCurrentUser: () => ({ user: { id: "u-1" } }),
}))
vi.mock("@/components/explorer/SavedQueryEditDialog", () => ({ SavedQueryEditDialog: () => null }))
vi.mock("@/components/explorer/SavedQueryModelDialog", () => ({ SavedQueryModelDialog: () => null }))

const SCHEDULES = "/api/v1/explorer/schedules"
const UPSTREAM_CLAIM = "When an upstream runs"

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

function schedule(id: string, name: string, overrides: Record<string, unknown> = {}) {
  const type = (overrides.schedule_type as string | undefined) ?? "after_upstream"
  return {
    schedule_id: `sch-${id}`,
    saved_query_id: id,
    name,
    connection_id: "c-1",
    schedule_type: type,
    schedule_spec: type === "cron" ? { cron: "0 2 * * *", timezone: "UTC" } : {},
    status: "active",
    materialization: "table",
    target_table: `analytics.${name}`,
    statement_class: "select",
    supports_materialization: true,
    upstreams: type === "after_upstream" ? [{ kind: "pipeline", id: "p-1", name: "orders_sync" }] : undefined,
    created_by: "u-1",
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-01T00:00:00Z",
    ...overrides,
  }
}

// The gateway sends next_run_at only for an active schedule (the list handler in
// saved_query_schedules.go), so none of the paused fixtures carries one.
const MANUALLY_PAUSED = schedule("q-1", "orders_weekly", {
  status: "paused",
  paused_at: "2026-09-14T09:00:00Z",
})
const AUTO_PAUSED = schedule("q-2", "orders_daily", {
  status: "paused",
  auto_paused_at: "2026-09-15T08:00:00Z",
  auto_paused_reason: "this model's connection config could not be decrypted",
})
const ACTIVE_AFTER_UPSTREAM = schedule("q-3", "orders_hourly")
const PAUSED_CLOCK = schedule("q-4", "orders_nightly", { schedule_type: "cron", status: "paused" })
const FAN_IN = [
  { kind: "model", id: "m-1", name: "q_two" },
  { kind: "model", id: "m-2", name: "stg_orders" },
]
const WAITS_ON_ALL = schedule("q-5", "d_fanin", { upstreams: FAN_IN, upstream_policy: "all" })
const WAKES_ON_ANY = schedule("q-6", "e_fanin", { upstreams: FAN_IN, upstream_policy: "any" })

async function renderPage(schedules: ReturnType<typeof schedule>[]) {
  ;(authFetch as Mock).mockImplementation(async (url: string) =>
    url === SCHEDULES ? res(200, { schedules, count: schedules.length }) : res(404, {}),
  )
  render(<ScheduledQueriesPage />)
  await screen.findByText(schedules[0].name)
}

function rowFor(name: string): HTMLElement {
  const row = screen.getByText(name).closest("tr")
  if (!row) throw new Error(`no table row for ${name}`)
  return row
}

// Located by header text rather than a fixed index, so a column added before it cannot
// quietly point these assertions at the wrong cell.
function nextRunCell(name: string): HTMLElement {
  const column = screen.getAllByRole("columnheader").findIndex((h) => h.textContent === "Next run")
  expect(column).toBeGreaterThan(-1)
  return within(rowFor(name)).getAllByRole("cell")[column]
}

describe("schedules page: Next run for a paused after_upstream schedule", () => {
  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
  })

  it.each([
    ["paused by hand", MANUALLY_PAUSED],
    ["auto-paused by the gateway", AUTO_PAUSED],
  ])("does not promise a run when %s", async (_label, paused) => {
    await renderPage([paused, ACTIVE_AFTER_UPSTREAM])

    // The row really is on screen as paused, so the cell below is the paused one's.
    expect(within(rowFor(paused.name)).getByText("paused")).toBeInTheDocument()
    expect(nextRunCell(paused.name)).not.toHaveTextContent(UPSTREAM_CLAIM)
    expect(nextRunCell(paused.name)).toHaveTextContent(/^—$/)
  })

  it("reads the same as a paused clock schedule", async () => {
    await renderPage([AUTO_PAUSED, PAUSED_CLOCK])

    expect(nextRunCell(AUTO_PAUSED.name).textContent).toBe(nextRunCell(PAUSED_CLOCK.name).textContent)
  })

  // The control. Without it, a lookup that found the wrong cell would make every
  // "does not say" assertion above pass for nothing.
  it("still says an active after_upstream schedule runs when an upstream runs", async () => {
    await renderPage([ACTIVE_AFTER_UPSTREAM, AUTO_PAUSED])

    expect(nextRunCell(ACTIVE_AFTER_UPSTREAM.name)).toHaveTextContent(UPSTREAM_CLAIM)
  })

  it("still describes the trigger in the Cadence column", async () => {
    await renderPage([AUTO_PAUSED])

    // Pausing stops the runs, not what would trigger them, so the cadence stays put.
    expect(within(rowFor(AUTO_PAUSED.name)).getByText("After orders_sync runs")).toBeInTheDocument()
  })

  // Seen in a browser on 2026-09-16: d_fanin, which the gateway rebuilds only once both
  // upstreams have landed, still said it would run when an upstream runs.
  it("does not say a fan-in on all runs at the next landing", async () => {
    await renderPage([WAITS_ON_ALL, WAKES_ON_ANY])

    expect(within(rowFor(WAITS_ON_ALL.name)).getByText("After all of q_two, stg_orders run")).toBeInTheDocument()
    expect(nextRunCell(WAITS_ON_ALL.name)).toHaveTextContent("When all its upstreams have run")
    expect(nextRunCell(WAITS_ON_ALL.name)).not.toHaveTextContent(UPSTREAM_CLAIM)
    // The control: the same two upstreams on "any" keep the old sentence.
    expect(nextRunCell(WAKES_ON_ANY.name)).toHaveTextContent(UPSTREAM_CLAIM)
  })
})
