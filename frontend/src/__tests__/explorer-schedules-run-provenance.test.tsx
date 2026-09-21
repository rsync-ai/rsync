import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { cleanup, render, screen, within } from "@testing-library/react"
import ModelSchedulePage from "@/app/(dashboard)/explorer/schedules/[id]/page"
import { authFetch } from "@/lib/api/auth-fetch"

// Run history used to print only trigger_source, so a model rebuilt by a chain and one
// that was never rebuilt because its upstream failed looked like "triggered" and nothing.
// Migration 104 stores why a run ran and why a rebuild was skipped; this is the page
// saying it. Run history is on the model's own page; the schedules list links there.

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn(), replace: vi.fn() }),
  usePathname: () => "/explorer/schedules/q-1",
  useParams: () => ({ id: "q-1" }),
  useSearchParams: () => new URLSearchParams(),
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: "admin",
    can: () => true,
    meets: () => true,
    activeWorkspace: { id: "ws-1" },
    isLoading: false,
  }),
}))
vi.mock("@/contexts/CurrentUserContext", () => ({
  useCurrentUser: () => ({ user: { id: "u-1" }, isLoading: false }),
}))
vi.mock("@/components/explorer/SavedQueryEditDialog", () => ({ SavedQueryEditDialog: () => null }))
vi.mock("@/components/explorer/SavedQueryModelDialog", () => ({ SavedQueryModelDialog: () => null }))

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

const SCHEDULE = {
  schedule_id: "sch-1",
  saved_query_id: "q-1",
  name: "revenue_rollup",
  connection_id: "c-1",
  schedule_type: "after_upstream",
  schedule_spec: {},
  status: "active",
  materialization: "table",
  target_table: "analytics.revenue_rollup",
  statement_class: "select",
  supports_materialization: true,
  upstream_policy: "all",
  upstreams: [
    { kind: "pipeline", id: "p-1", name: "orders_sync" },
    { kind: "model", id: "m-1", name: "customer_dim" },
  ],
  created_by: "u-1",
  created_at: "2026-09-01T00:00:00Z",
  updated_at: "2026-09-01T00:00:00Z",
}

function run(overrides: Record<string, unknown>) {
  return {
    saved_query_id: "q-1",
    status: "succeeded",
    started_at: "2026-09-16T10:00:00Z",
    finished_at: "2026-09-16T10:00:02Z",
    duration_ms: 2000,
    ...overrides,
  }
}

const RUNS = [
  run({
    run_id: "r-chain",
    trigger_source: "triggered",
    upstream_kind: "model",
    upstream_name: "customer_dim",
    trigger_depth: 2,
    coalesced_count: 3,
  }),
  run({
    run_id: "r-failed-up",
    trigger_source: "triggered",
    status: "skipped",
    duration_ms: 0,
    upstream_kind: "pipeline",
    upstream_name: "orders_sync",
    skip_reason: "upstream_failed",
    error: "orders_sync failed",
  }),
  run({ run_id: "r-manual", trigger_source: "manual" }),
]

async function openHistory() {
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    if (url === "/api/v1/explorer/schedules?saved_query_id=q-1") return res(200, { schedules: [SCHEDULE], count: 1 })
    if (url.startsWith("/api/v1/explorer/saved/q-1/runs")) return res(200, { runs: RUNS })
    return res(404, {})
  })
  render(<ModelSchedulePage />)
  await screen.findByText("manual")
}

describe("model page: run history provenance", () => {
  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
  })

  it("describes a wait-for-all trigger as all of, not any of", async () => {
    await openHistory()
    expect(screen.getByText("After all of orders_sync, customer_dim run")).toBeInTheDocument()
  })

  it("says what woke a chained rebuild, how deep, and how many it absorbed", async () => {
    await openHistory()
    expect(screen.getByText("after customer_dim (depth 2, 3 coalesced)")).toBeInTheDocument()
  })

  it("says why a rebuild was skipped, and does not paint its message as a failure", async () => {
    await openHistory()
    const reason = screen.getByText("not rebuilt: its upstream failed")
    // Scoped to the run's row: the page's status filter offers "skipped" too.
    expect(within(reason.closest("tr")!).getByText("skipped")).toBeInTheDocument()
    const message = screen.getByText("orders_sync failed")
    expect(message.className).not.toMatch(/text-red/)
  })

  // The control: a real failure keeps its red, so the assertion above is about the skip
  // and not about errors having lost their colour altogether.
  it("still paints a failed run's error red", async () => {
    RUNS.push(run({ run_id: "r-bad", trigger_source: "scheduled", status: "failed", error: "syntax error" }))
    try {
      await openHistory()
      expect(screen.getByText("syntax error").className).toMatch(/text-red/)
    } finally {
      RUNS.pop()
    }
  })
})
