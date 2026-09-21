import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { cleanup, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import ModelSchedulePage from "@/app/(dashboard)/explorer/schedules/[id]/page"
import ScheduledQueriesPage from "@/app/(dashboard)/explorer/schedules/page"
import { authFetch } from "@/lib/api/auth-fetch"

// The per-model page: every run of one model, filterable and paged, with the controls
// to act on it. The failure modes worth guarding are all quiet ones — a page that
// cannot reach run 26, a filter that is shown but not sent, an error drawn as "no runs",
// and a viewer offered buttons whose only outcome is a 403.

const role = vi.hoisted(() => ({ canSchedule: true }))
// Both providers start out loading; the page renders before either has answered.
const loading = vi.hoisted(() => ({ user: false, workspace: false }))
// `search` is the address's query string; `replace` is where a tab click sends the new one;
// `push` is where a click on a list row goes.
const nav = vi.hoisted(() => ({ search: "", replace: vi.fn(), push: vi.fn() }))
// The graph itself is tested in model-lineage-graph.test.tsx. Here only what the page hands it.
const graph = vi.hoisted(() => ({ props: [] as Record<string, unknown>[] }))

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: nav.push, refresh: vi.fn(), replace: nav.replace }),
  usePathname: () => "/explorer/schedules/q-1",
  useParams: () => ({ id: "q-1" }),
  useSearchParams: () => new URLSearchParams(nav.search),
}))
vi.mock("@/components/explorer/ModelLineageGraph", () => ({
  ModelLineageGraph: (props: Record<string, unknown>) => {
    graph.props.push(props)
    return `lineage graph of ${String(props.modelName)}`
  },
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: role.canSchedule ? "admin" : "viewer",
    can: () => role.canSchedule,
    meets: () => role.canSchedule,
    activeWorkspace: loading.workspace ? null : { id: "ws-1" },
    isLoading: loading.workspace,
  }),
}))
vi.mock("@/contexts/CurrentUserContext", () => ({
  useCurrentUser: () => ({ user: loading.user ? null : { id: "u-1" }, isLoading: loading.user }),
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
  connection_name: "warehouse",
  connector_type: "postgresql",
  schedule_type: "cron",
  schedule_spec: { cron: "0 3 * * *", timezone: "UTC" },
  status: "active",
  materialization: "table",
  target_table: "analytics.revenue_rollup",
  statement_class: "select",
  supports_materialization: true,
  created_by: "u-1",
  created_at: "2026-09-01T00:00:00Z",
  updated_at: "2026-09-01T00:00:00Z",
}

function run(id: string, overrides: Record<string, unknown> = {}) {
  return {
    run_id: id,
    saved_query_id: "q-1",
    trigger_source: "scheduled",
    status: "succeeded",
    started_at: "2026-09-16T03:00:00Z",
    finished_at: "2026-09-16T03:01:05Z",
    duration_ms: 65_000,
    ...overrides,
  }
}

type Route = (url: string, init?: RequestInit) => Response | undefined

function serve(route: Route, schedule: Record<string, unknown> | null = SCHEDULE) {
  ;(authFetch as Mock).mockImplementation(async (url: string, init?: RequestInit) => {
    const hit = route(url, init)
    if (hit) return hit
    if (url === "/api/v1/explorer/schedules?saved_query_id=q-1") {
      return res(200, { schedules: schedule ? [schedule] : [], count: schedule ? 1 : 0 })
    }
    if (url === "/api/v1/explorer/freshness") return res(200, { breaches: [] })
    // The Details card's own reads: who the schedule runs as, and the list the lineage
    // count walks. The list route here is the whole workspace's, with no query string.
    if (url === "/api/v1/explorer/saved/q-1/schedule" && schedule) {
      return res(200, { ...schedule, run_as_user_id: "u-1" })
    }
    if (url === "/api/v1/explorer/schedules") {
      return res(200, { schedules: schedule ? [schedule] : [], count: schedule ? 1 : 0 })
    }
    return res(404, {})
  })
}

function requested(url: string): number {
  return (authFetch as Mock).mock.calls.filter(([u]) => u === url).length
}

/** The value cell of a row in the Details card. */
async function detail(label: string): Promise<HTMLElement> {
  const term = await screen.findByText(label, { selector: "dt" })
  return term.nextElementSibling as HTMLElement
}

function runRequests(): URL[] {
  return (authFetch as Mock).mock.calls
    .map(([url]) => String(url))
    .filter((url) => url.startsWith("/api/v1/explorer/saved/q-1/runs"))
    .map((url) => new URL(url, "http://x"))
}

describe("per-model schedule page", () => {
  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
    role.canSchedule = true
    loading.user = false
    loading.workspace = false
    nav.search = ""
    graph.props = []
  })

  it("lists runs as a table with duration, trigger, status and the error", async () => {
    serve((url) =>
      url.startsWith("/api/v1/explorer/saved/q-1/runs")
        ? res(200, {
            runs: [
              run("r-1"),
              run("r-2", { status: "failed", error: "relation missing", duration_ms: 1200 }),
              run("r-3", {
                trigger_source: "triggered",
                status: "skipped",
                skip_reason: "upstream_failed",
                upstream_kind: "pipeline",
                upstream_name: "orders_sync",
                error: "orders_sync failed",
              }),
            ],
          })
        : undefined,
    )
    render(<ModelSchedulePage />)

    const table = await screen.findByRole("table")
    const rows = within(table).getAllByRole("row")
    expect(rows).toHaveLength(4)
    expect(within(rows[1]).getByText("1m 5s")).toBeInTheDocument()
    expect(within(rows[2]).getByText("1.2s")).toBeInTheDocument()
    expect(within(rows[2]).getByText("relation missing").className).toMatch(/text-red/)
    expect(within(rows[3]).getByText("not rebuilt: its upstream failed")).toBeInTheDocument()
    // A skip has no duration worth showing, and its message is not a failure.
    expect(within(rows[3]).getByText("—")).toBeInTheDocument()
    expect(within(rows[3]).getByText("orders_sync failed").className).not.toMatch(/text-red/)

    expect(screen.getByRole("heading", { name: "revenue_rollup" })).toBeInTheDocument()
    expect(screen.getByText("Every day at 03:00 (UTC)")).toBeInTheDocument()
    expect(screen.getByText("analytics.revenue_rollup")).toBeInTheDocument()
    expect(runRequests()[0].searchParams.get("limit")).toBe("25")
  })

  it("says a table rebuild rebuilt the table, where only a DML write has a row count", async () => {
    serve((url) =>
      url.startsWith("/api/v1/explorer/saved/q-1/runs")
        ? res(200, {
            runs: [
              run("r-1", { statement_class: "read", target_table: "analytics.revenue_rollup" }),
              run("r-2", { statement_class: "dml_write", rows_affected: 1 }),
              run("r-3", { status: "failed", target_table: "analytics.revenue_rollup", error: "relation missing" }),
              run("r-4", {
                trigger_source: "triggered",
                status: "skipped",
                skip_reason: "upstream_failed",
                upstream_kind: "pipeline",
                upstream_name: "orders_sync",
                target_table: "analytics.revenue_rollup",
              }),
              // A statement model has no target, and a DDL statement reports no count.
              run("r-5", { statement_class: "ddl" }),
            ],
          })
        : undefined,
    )
    render(<ModelSchedulePage />)

    const rows = within(await screen.findByRole("table")).getAllByRole("row")
    expect(rows).toHaveLength(6)
    expect(within(rows[1]).getByText("Table rebuilt")).toHaveAttribute("title", "Rebuilt analytics.revenue_rollup")
    expect(within(rows[2]).getByText("1 row")).toBeInTheDocument()
    expect(within(rows[2]).queryByText("Table rebuilt")).not.toBeInTheDocument()
    expect(within(rows[3]).queryByText("Table rebuilt")).not.toBeInTheDocument()
    expect(within(rows[4]).queryByText("Table rebuilt")).not.toBeInTheDocument()
    expect(within(rows[5]).queryByText("Table rebuilt")).not.toBeInTheDocument()
  })

  it("wraps the rebuilt table's name after its dot, never mid-word", async () => {
    serve((url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [] }) : undefined))
    render(<ModelSchedulePage />)
    const does = await detail("Does")
    expect(does).toHaveTextContent(/^Rebuilds analytics\.revenue_rollup$/)
    const name = within(does).getByText("analytics.revenue_rollup")
    expect(name.className).not.toMatch(/break-all/)
    expect(name.className).toMatch(/break-words/)
    expect(name.innerHTML).toBe("analytics.<wbr>revenue_rollup")
  })

  it("pages back with the server's cursor, and Newer returns to the first page", async () => {
    serve((url) => {
      if (!url.startsWith("/api/v1/explorer/saved/q-1/runs")) return undefined
      return new URL(url, "http://x").searchParams.get("before")
        ? res(200, { runs: [run("r-old")] })
        : res(200, { runs: [run("r-new")], next_cursor: "2026-09-16T03:01:05Z_r-new" })
    })
    const user = userEvent.setup()
    render(<ModelSchedulePage />)

    await user.click(await screen.findByRole("button", { name: "Older" }))
    await screen.findByText("Page 2")
    // "Page 2" is drawn the moment the cursor moves, but the pager stays disabled until that
    // page's runs arrive. Wait for them, or a slow runner clicks a disabled Newer.
    await waitFor(() => expect(screen.getByRole("button", { name: "Newer" })).toBeEnabled())
    expect(runRequests().at(-1)!.searchParams.get("before")).toBe("2026-09-16T03:01:05Z_r-new")
    expect(screen.getByRole("button", { name: "Older" })).toBeDisabled()

    await user.click(screen.getByRole("button", { name: "Newer" }))
    await screen.findByText("Page 1")
    expect(runRequests().at(-1)!.searchParams.has("before")).toBe(false)
  })

  it("sends the status filter and starts it from the newest run", async () => {
    serve((url) => {
      if (!url.startsWith("/api/v1/explorer/saved/q-1/runs")) return undefined
      const q = new URL(url, "http://x").searchParams
      if (q.get("status") === "failed") return res(200, { runs: [] })
      return q.get("before")
        ? res(200, { runs: [run("r-old")] })
        : res(200, { runs: [run("r-new")], next_cursor: "c1" })
    })
    const user = userEvent.setup()
    render(<ModelSchedulePage />)

    await user.click(await screen.findByRole("button", { name: "Older" }))
    await screen.findByText("Page 2")
    await user.click(screen.getByRole("button", { name: "Failed" }))

    expect(await screen.findByText("No failed runs.")).toBeInTheDocument()
    const last = runRequests().at(-1)!
    expect(last.searchParams.get("status")).toBe("failed")
    expect(last.searchParams.has("before")).toBe(false)
    expect(screen.getByRole("button", { name: "Failed" })).toHaveAttribute("aria-pressed", "true")
  })

  it("draws the table's runs as duration bars above it, and follows the filter", async () => {
    serve((url) => {
      if (!url.startsWith("/api/v1/explorer/saved/q-1/runs")) return undefined
      return new URL(url, "http://x").searchParams.get("status") === "failed"
        ? res(200, { runs: [run("r-f", { status: "failed", duration_ms: 1200 })] })
        : res(200, {
            runs: [
              run("r-3", { status: "skipped", skip_reason: "waiting_on_upstreams", duration_ms: 0 }),
              run("r-2", { status: "failed", duration_ms: 1200 }),
              run("r-1"),
            ],
          })
    })
    const user = userEvent.setup()
    render(<ModelSchedulePage />)
    const chartBars = () =>
      within(screen.getByRole("list", { name: "Run durations, oldest first" })).getAllByRole("listitem")

    const table = await screen.findByRole("table")
    // The same runs as the table, oldest first where the table is newest first.
    expect(chartBars().map((b) => b.dataset.status)).toEqual(["succeeded", "failed", "skipped"])
    expect(within(table).getAllByRole("row")).toHaveLength(4)
    const chart = screen.getByRole("list", { name: "Run durations, oldest first" })
    expect(chart.compareDocumentPosition(table) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()

    await user.click(screen.getByRole("button", { name: "Failed" }))
    await waitFor(() => expect(chartBars().map((b) => b.dataset.status)).toEqual(["failed"]))
    expect(within(screen.getByRole("table")).getAllByRole("row")).toHaveLength(2)
  })

  it("says the history could not be loaded, rather than that there is none", async () => {
    serve((url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(500, {}) : undefined))
    render(<ModelSchedulePage />)
    expect(await screen.findByText("Could not load run history (HTTP 500).")).toBeInTheDocument()
    expect(screen.queryByText("No runs recorded yet.")).not.toBeInTheDocument()
  })

  // A deleted schedule leaves its runs behind, and a link to this page from a run, an
  // upstream or a bookmark still points here. The history is the part worth showing.
  it("still shows the run history of a query whose schedule is gone, without schedule controls", async () => {
    serve(
      (url) =>
        url === "/api/v1/explorer/saved/q-1"
          ? res(200, {
              id: "q-1",
              name: "stg_orders",
              created_by: "u-2",
              created_at: "2026-09-01T00:00:00Z",
              updated_at: "2026-09-01T00:00:00Z",
              materialization: "table",
              target_table: "analytics.stg_orders",
            })
          : url.startsWith("/api/v1/explorer/saved/q-1/runs")
            ? res(200, { runs: [run("r-1", { status: "failed", error: "relation missing" })] })
            : undefined,
      null,
    )
    render(<ModelSchedulePage />)

    const table = await screen.findByRole("table")
    expect(within(table).getByText("relation missing")).toBeInTheDocument()
    expect(screen.getByRole("heading", { name: "stg_orders" })).toBeInTheDocument()
    expect(within(screen.getByRole("status")).getByText("Not scheduled")).toBeInTheDocument()
    expect(screen.getByText("analytics.stg_orders")).toBeInTheDocument()
    expect(runRequests()[0].searchParams.get("limit")).toBe("25")
    // A button that acts on a schedule has nothing to act on.
    for (const name of [/run now/i, /pause/i, /resume/i, /edit schedule/i]) {
      expect(screen.queryByRole("button", { name })).not.toBeInTheDocument()
    }
    expect(screen.getByRole("button", { name: "Failed" })).toBeInTheDocument()
  })

  // The query route answers 404 for a query that does not exist and for another member's
  // private one alike, and the page must not tell those apart either — or ask for runs.
  it("says the query could not be found, and asks for no runs, when the query route 404s", async () => {
    serve(() => undefined, null)
    render(<ModelSchedulePage />)
    expect(await screen.findByText("This query could not be found.")).toBeInTheDocument()
    expect(screen.getByText(/private query of another member/)).toBeInTheDocument()
    expect((authFetch as Mock).mock.calls.some(([url]) => url === "/api/v1/explorer/saved/q-1")).toBe(true)
    expect(runRequests()).toHaveLength(0)
  })

  it("offers a retry, not a not-found, when the query route fails", async () => {
    serve((url) => (url === "/api/v1/explorer/saved/q-1" ? res(500, {}) : undefined), null)
    render(<ModelSchedulePage />)
    expect(await screen.findByText("Could not load this model (HTTP 500).")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument()
    expect(screen.queryByText("This query could not be found.")).not.toBeInTheDocument()
  })

  it("runs now as a POST and reloads the newest runs", async () => {
    serve((url, init) => {
      if (url === "/api/v1/explorer/saved/q-1/run" && init?.method === "POST") {
        return res(200, { target_table: "analytics.revenue_rollup" })
      }
      return url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [run("r-1")] }) : undefined
    })
    const user = userEvent.setup()
    render(<ModelSchedulePage />)
    await screen.findByRole("table")
    const before = runRequests().length

    await user.click(screen.getByRole("button", { name: /run now/i }))
    await waitFor(() => expect(runRequests().length).toBe(before + 1))
    expect(runRequests().at(-1)!.searchParams.has("before")).toBe(false)
  })

  it("pauses through the schedule route", async () => {
    serve((url, init) =>
      url === "/api/v1/explorer/saved/q-1/schedule/pause" && init?.method === "POST"
        ? res(200, {})
        : url.startsWith("/api/v1/explorer/saved/q-1/runs")
          ? res(200, { runs: [] })
          : undefined,
    )
    const user = userEvent.setup()
    render(<ModelSchedulePage />)
    await user.click(await screen.findByRole("button", { name: /pause/i }))
    await waitFor(() =>
      expect((authFetch as Mock).mock.calls.some(([url]) => url === "/api/v1/explorer/saved/q-1/schedule/pause")).toBe(
        true,
      ),
    )
  })

  it("says a fan-in on all runs once all its upstreams have run", async () => {
    const upstreams = [
      { kind: "model", id: "m-1", name: "q_two" },
      { kind: "model", id: "m-2", name: "stg_orders" },
    ]
    serve(
      (url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [] }) : undefined),
      { ...SCHEDULE, schedule_type: "after_upstream", schedule_spec: {}, upstreams, upstream_policy: "all" },
    )
    render(<ModelSchedulePage />)
    const nextRun = (await screen.findByText("Next run")).nextElementSibling
    expect(nextRun).toHaveTextContent(/^When all its upstreams have run$/)
    expect(screen.getByText("After all of q_two, stg_orders run")).toBeInTheDocument()
  })

  it("explains an automatic pause", async () => {
    serve(
      (url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [] }) : undefined),
      { ...SCHEDULE, status: "paused", auto_paused_reason: "3 consecutive failures" },
    )
    render(<ModelSchedulePage />)
    expect(await screen.findByText(/Paused automatically/)).toBeInTheDocument()
    expect(screen.getByText("3 consecutive failures")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /resume/i })).toBeEnabled()
  })

  it("does not offer a viewer controls that can only 403", async () => {
    role.canSchedule = false
    serve((url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [] }) : undefined))
    render(<ModelSchedulePage />)
    expect(await screen.findByRole("button", { name: /run now/i })).toBeDisabled()
    expect(screen.getByRole("button", { name: /pause/i })).toBeDisabled()
    expect(screen.getByRole("button", { name: /edit schedule/i })).toBeDisabled()
  })

  // Control for the gate above: a model whose runs would do nothing is not runnable by
  // anyone, so a disabled Run now is not only a role check.
  it("refuses Run now when a run would do nothing", async () => {
    serve(
      (url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [] }) : undefined),
      { ...SCHEDULE, target_table: "" },
    )
    render(<ModelSchedulePage />)
    expect(await screen.findByRole("button", { name: /run now/i })).toBeDisabled()
    expect(screen.getByRole("button", { name: /pause/i })).toBeEnabled()
  })

  it("opens on Runs, and choosing Graph shows the graph and puts the tab in the address", async () => {
    serve((url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [run("r-1")] }) : undefined))
    const user = userEvent.setup()
    render(<ModelSchedulePage />)

    await screen.findByRole("table")
    expect(screen.getByRole("tab", { name: "Runs" })).toHaveAttribute("aria-selected", "true")
    expect(screen.queryByText("lineage graph of revenue_rollup")).not.toBeInTheDocument()

    await user.click(screen.getByRole("tab", { name: "Graph" }))
    expect(await screen.findByText("lineage graph of revenue_rollup")).toBeInTheDocument()
    expect(screen.queryByRole("table")).not.toBeInTheDocument()
    expect(nav.replace).toHaveBeenCalledWith("/explorer/schedules/q-1?tab=graph", { scroll: false })
    expect(graph.props.at(-1)).toMatchObject({
      modelId: "q-1",
      modelName: "revenue_rollup",
      rootSchedule: expect.objectContaining({ schedule_id: "sch-1" }),
      canSchedule: true,
    })
  })

  // Each node of a graph links to its model's page with ?tab=graph, so following the chain
  // stays on the graph.
  it("opens on the graph when the address says so, and Runs takes the tab back out of it", async () => {
    nav.search = "tab=graph&from=list"
    serve((url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [run("r-1")] }) : undefined))
    const user = userEvent.setup()
    render(<ModelSchedulePage />)

    expect(await screen.findByText("lineage graph of revenue_rollup")).toBeInTheDocument()
    expect(screen.getByRole("tab", { name: "Graph" })).toHaveAttribute("aria-selected", "true")

    await user.click(screen.getByRole("tab", { name: "Runs" }))
    expect(await screen.findByRole("table")).toBeInTheDocument()
    expect(nav.replace).toHaveBeenCalledWith("/explorer/schedules/q-1?from=list", { scroll: false })
  })

  it("reloads the graph with the page's Refresh, and asks what is running while the graph is open", async () => {
    nav.search = "tab=graph"
    serve((url) => {
      if (url.startsWith("/api/v1/explorer/saved/q-1/runs")) return res(200, { runs: [] })
      if (url === "/api/v1/explorer/running") {
        return res(200, { models: [], count: 0, limit: 200, temporal_available: true })
      }
      return undefined
    })
    const user = userEvent.setup()
    render(<ModelSchedulePage />)

    await screen.findByText("lineage graph of revenue_rollup")
    // A clock schedule has no refresh loop of its own, but the models around it may.
    await waitFor(() =>
      expect((authFetch as Mock).mock.calls.some(([url]) => url === "/api/v1/explorer/running")).toBe(true),
    )
    const tick = graph.props.at(-1)!.reloadTick as number

    await user.click(screen.getByRole("button", { name: "Refresh" }))
    await waitFor(() => expect(graph.props.at(-1)!.reloadTick).toBe(tick + 1))
  })

  it("says what the refresh loop is doing for a model that runs after its upstreams", async () => {
    serve(
      (url) => {
        if (url.startsWith("/api/v1/explorer/saved/q-1/runs")) return res(200, { runs: [] })
        if (url === "/api/v1/explorer/running") {
          return res(200, {
            models: [
              {
                saved_query_id: "q-1",
                name: "revenue_rollup",
                workflow_id: "model-refresh-q-1",
                state: "idle",
                detail_available: false,
                message: "no open refresh loop; the last run has closed",
              },
            ],
            count: 1,
            limit: 200,
            temporal_available: true,
          })
        }
        return undefined
      },
      {
        ...SCHEDULE,
        schedule_type: "after_upstream",
        schedule_spec: {},
        upstreams: [{ kind: "model", id: "m-1", name: "q_two" }],
      },
    )
    render(<ModelSchedulePage />)
    expect(await screen.findByText("Idle: no open refresh loop; the last run has closed")).toBeInTheDocument()
  })

  it("starts the Runs and Details cards on one row, with the notices and the tabs above both", async () => {
    serve(
      (url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [run("r-1")] }) : undefined),
      { ...SCHEDULE, status: "paused", auto_paused_reason: "3 consecutive failures" },
    )
    render(<ModelSchedulePage />)
    await screen.findByRole("table")

    // jsdom lays nothing out, so this holds the structure the heights come from: the two
    // cards are the only things in their grid row, and the Details card is not sized to
    // its own content, so the row stretches it to the Runs card's height.
    const details = screen.getByTestId("model-details-card")
    const runs = screen.getByTestId("model-runs-card")
    const row = details.parentElement!
    expect(row).toContainElement(runs)
    expect(row).not.toContainElement(screen.getByRole("tablist"))
    expect(row).not.toContainElement(screen.getByText(/Paused automatically/))
    expect(details.className).not.toMatch(/\bh-fit\b/)
    expect(runs.className).toMatch(/\bh-full\b/)
  })

  it("does not ask what is running for a clock-scheduled model while its runs are shown", async () => {
    serve((url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [run("r-1")] }) : undefined))
    render(<ModelSchedulePage />)
    await screen.findByRole("table")
    expect((authFetch as Mock).mock.calls.some(([url]) => url === "/api/v1/explorer/running")).toBe(false)
  })

  it("says a cron schedule as a sentence, with the expression it was read from under it", async () => {
    serve((url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [] }) : undefined))
    render(<ModelSchedulePage />)
    const trigger = await detail("Trigger")
    expect(within(trigger).getByText("Every day at 03:00 (UTC)").className).not.toMatch(/font-mono/)
    expect(within(trigger).getByText("0 3 * * *").className).toMatch(/font-mono/)
  })

  // Neither day field starred: the gateway's parser and Temporal disagree on which days
  // that means, so no sentence is exact and the expression is shown as typed.
  it("shows a cron it cannot put exactly into words as typed, once", async () => {
    serve(
      (url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [] }) : undefined),
      { ...SCHEDULE, schedule_spec: { cron: "0 9 1 * 1", timezone: "Asia/Kuala_Lumpur" } },
    )
    render(<ModelSchedulePage />)
    const trigger = await detail("Trigger")
    expect(trigger).toHaveTextContent(/^0 9 1 \* 1 \(Asia\/Kuala_Lumpur\)$/)
    expect(within(trigger).getByText("0 9 1 * 1 (Asia/Kuala_Lumpur)").className).toMatch(/font-mono/)
  })

  it("says the schedule runs as you, without reading the roster", async () => {
    serve((url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [] }) : undefined))
    render(<ModelSchedulePage />)
    const runAs = await detail("Run as")
    await waitFor(() => expect(runAs).toHaveTextContent(/^you$/))
    expect(within(runAs).getByText("you")).toHaveAttribute("title", expect.stringMatching(/drops below admin or below what this model's SQL needs/))
    expect(requested("/api/v1/workspaces/ws-1/members")).toBe(0)
  })

  it.each([
    ["names another member by email", res(200, { members: [{ user_id: "u-2", email: "dana@example.com" }] }), "dana@example.com"],
    // The roster lists current members only; run_as_user_id outlives the membership.
    ["calls someone no longer on the roster a former member", res(200, { members: [{ user_id: "u-1", email: "me@example.com" }] }), "a former member"],
    ["still says it is someone else when the roster cannot be read", res(500, {}), "another member"],
  ])("%s", async (_name, roster, label) => {
    serve((url) => {
      if (url.startsWith("/api/v1/explorer/saved/q-1/runs")) return res(200, { runs: [] })
      if (url === "/api/v1/explorer/saved/q-1/schedule") return res(200, { ...SCHEDULE, run_as_user_id: "u-2" })
      if (url === "/api/v1/workspaces/ws-1/members") return roster
      return undefined
    })
    render(<ModelSchedulePage />)
    const runAs = await detail("Run as")
    await waitFor(() => expect(runAs).toHaveTextContent(new RegExp(`^${label}$`)))
  })

  it.each([
    ["the signed-in user", "user"],
    ["the workspace", "workspace"],
  ] as const)("does not ask who runs the schedule until %s has loaded", async (_name, which) => {
    loading[which] = true
    serve((url) => (url.startsWith("/api/v1/explorer/saved/q-1/runs") ? res(200, { runs: [] }) : undefined))
    const { rerender } = render(<ModelSchedulePage />)
    const runAs = await detail("Run as")
    // The lineage count reads the list on its own; once it has, the page has settled.
    await waitFor(() => expect(requested("/api/v1/explorer/schedules")).toBeGreaterThan(0))
    expect(runAs).toHaveTextContent(/^Loading…$/)
    expect(requested("/api/v1/explorer/saved/q-1/schedule")).toBe(0)

    loading[which] = false
    rerender(<ModelSchedulePage />)
    await waitFor(() => expect(runAs).toHaveTextContent(/^you$/))
    expect(requested("/api/v1/explorer/saved/q-1/schedule")).toBe(1)
  })

  it("says who runs the schedule could not be loaded, rather than naming anyone", async () => {
    serve((url) => {
      if (url.startsWith("/api/v1/explorer/saved/q-1/runs")) return res(200, { runs: [] })
      if (url === "/api/v1/explorer/saved/q-1/schedule") return res(500, {})
      return undefined
    })
    render(<ModelSchedulePage />)
    const runAs = await detail("Run as")
    await waitFor(() => expect(runAs).toHaveTextContent("Could not load who runs this schedule (HTTP 500)."))
  })

  it("counts the models and pipelines chained both ways, and the count opens the graph", async () => {
    const root = {
      ...SCHEDULE,
      schedule_type: "after_upstream",
      schedule_spec: {},
      upstreams: [
        { kind: "pipeline", id: "p-1", name: "orders_sync" },
        { kind: "model", id: "m-1", name: "stg_orders" },
      ],
    }
    const downstream = (id: string, upstreamId: string) => ({
      ...SCHEDULE,
      schedule_id: `sch-${id}`,
      saved_query_id: id,
      name: id,
      schedule_type: "after_upstream",
      schedule_spec: {},
      upstreams: [{ kind: "model", id: upstreamId, name: upstreamId }],
    })
    serve(
      (url) => {
        if (url.startsWith("/api/v1/explorer/saved/q-1/runs")) return res(200, { runs: [] })
        if (url === "/api/v1/explorer/schedules") {
          return res(200, { schedules: [root, downstream("d-1", "q-1"), downstream("d-2", "d-1")], count: 3 })
        }
        return undefined
      },
      root,
    )
    const user = userEvent.setup()
    render(<ModelSchedulePage />)

    const button = await screen.findByRole("button", { name: "2 upstream · 2 downstream" })
    await user.click(button)
    expect(screen.getByRole("tab", { name: "Graph" })).toHaveAttribute("aria-selected", "true")
    expect(nav.replace).toHaveBeenCalledWith("/explorer/schedules/q-1?tab=graph", { scroll: false })

    const before = requested("/api/v1/explorer/schedules")
    await user.click(screen.getByRole("button", { name: "Refresh" }))
    await waitFor(() => expect(requested("/api/v1/explorer/schedules")).toBe(before + 1))
  })

  it("counts for a model with no schedule of its own, which can still wake models downstream", async () => {
    serve(
      (url) => {
        if (url === "/api/v1/explorer/saved/q-1") {
          return res(200, { id: "q-1", name: "stg_orders", created_by: "u-2", created_at: "2026-09-01T00:00:00Z", updated_at: "2026-09-01T00:00:00Z" })
        }
        if (url.startsWith("/api/v1/explorer/saved/q-1/runs")) return res(200, { runs: [] })
        if (url === "/api/v1/explorer/schedules") {
          return res(200, {
            schedules: [{ ...SCHEDULE, schedule_id: "sch-d", saved_query_id: "d-1", schedule_type: "after_upstream", schedule_spec: {}, upstreams: [{ kind: "model", id: "q-1" }] }],
            count: 1,
          })
        }
        return undefined
      },
      null,
    )
    render(<ModelSchedulePage />)
    expect(await screen.findByRole("button", { name: "0 upstream · 1 downstream" })).toBeInTheDocument()
    expect(screen.queryByText("Run as")).not.toBeInTheDocument()
  })

  // A capped list can leave out this model's own row; its upstreams still come from the
  // schedule the page holds.
  it("says a count from a capped schedule list is a lower bound, and still counts this model's own upstreams", async () => {
    const root = {
      ...SCHEDULE,
      schedule_type: "after_upstream",
      schedule_spec: {},
      upstreams: [{ kind: "pipeline", id: "p-1", name: "orders_sync" }],
    }
    serve(
      (url) => {
        if (url.startsWith("/api/v1/explorer/saved/q-1/runs")) return res(200, { runs: [] })
        if (url === "/api/v1/explorer/schedules") return res(200, { schedules: [], count: 500 })
        return undefined
      },
      root,
    )
    render(<ModelSchedulePage />)
    const button = await screen.findByRole("button", { name: "At least 1 upstream · at least 0 downstream" })
    expect(button).toHaveAttribute("title", expect.stringMatching(/first 500 schedules/))
  })

  it("says the count could not be loaded, rather than showing zero", async () => {
    serve((url) => {
      if (url.startsWith("/api/v1/explorer/saved/q-1/runs")) return res(200, { runs: [] })
      if (url === "/api/v1/explorer/schedules") return res(503, {})
      return undefined
    })
    render(<ModelSchedulePage />)
    const lineage = await detail("Lineage")
    await waitFor(() => expect(lineage).toHaveTextContent("Could not load the linked models (HTTP 503)."))
    expect(within(lineage).queryByRole("button")).not.toBeInTheDocument()
  })
})

describe("per-model page — freshness deadline", () => {
  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
    role.canSchedule = true
  })

  // The schedules list does not carry the deadline; the row reads the query's own GET.
  function serveDeadline(initial: number | null) {
    let stored = initial
    serve((url, init) => {
      if (url === "/api/v1/explorer/saved/q-1/freshness" && init?.method === "PUT") {
        stored = JSON.parse(String(init.body)).deadline_seconds
        return res(200, { saved_query_id: "q-1", deadline_seconds: stored })
      }
      if (url === "/api/v1/explorer/saved/q-1") {
        return res(200, { id: "q-1", ...(stored == null ? {} : { freshness_deadline_seconds: stored }) })
      }
      return undefined
    })
  }

  it("shows the stored deadline in Details, and a save re-reads the model and its breaches", async () => {
    serveDeadline(21600)
    const user = userEvent.setup()
    render(<ModelSchedulePage />)

    const cell = await detail("Freshness")
    expect(await within(cell).findByText("Alert if older than 6h")).toBeInTheDocument()
    const schedulesBefore = requested("/api/v1/explorer/schedules?saved_query_id=q-1")
    const freshnessBefore = requested("/api/v1/explorer/freshness")

    await user.click(within(cell).getByRole("button", { name: "Edit freshness deadline" }))
    await user.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "1 day" }))
    await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }))

    expect(await within(cell).findByText("Alert if older than 1d")).toBeInTheDocument()
    await waitFor(() => {
      expect(requested("/api/v1/explorer/schedules?saved_query_id=q-1")).toBeGreaterThan(schedulesBefore)
      expect(requested("/api/v1/explorer/freshness")).toBeGreaterThan(freshnessBefore)
    })
  })

  it("locks the control below the model-run role, and says why", async () => {
    role.canSchedule = false
    serveDeadline(null)
    render(<ModelSchedulePage />)

    const cell = await detail("Freshness")
    const set = await within(cell).findByRole("button", { name: "Set freshness deadline" })
    expect(set).toBeDisabled()
    expect(set).toHaveAttribute(
      "title",
      "Setting a freshness deadline needs the admin role or higher. Your role is viewer.",
    )
  })
})

describe("schedules list", () => {
  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
  })

  it("links each model to its own page", async () => {
    ;(authFetch as Mock).mockImplementation(async (url: string) =>
      url === "/api/v1/explorer/schedules" ? res(200, { schedules: [SCHEDULE], count: 1 }) : res(200, {}),
    )
    render(<ScheduledQueriesPage />)
    const link = await screen.findByRole("link", { name: "revenue_rollup" })
    expect(link).toHaveAttribute("href", "/explorer/schedules/q-1")
  })

  it("opens the model's page from anywhere on its row, but not from its Edit buttons", async () => {
    ;(authFetch as Mock).mockImplementation(async (url: string) =>
      url === "/api/v1/explorer/schedules" ? res(200, { schedules: [SCHEDULE], count: 1 }) : res(200, {}),
    )
    const user = userEvent.setup()
    render(<ScheduledQueriesPage />)

    await user.click(await screen.findByText("Every day at 03:00 (UTC)"))
    expect(nav.push).toHaveBeenCalledTimes(1)
    expect(nav.push).toHaveBeenCalledWith("/explorer/schedules/q-1")

    await user.click(screen.getByRole("button", { name: "Edit the SQL of revenue_rollup" }))
    await user.click(screen.getByRole("button", { name: "Edit the schedule of revenue_rollup" }))
    expect(nav.push).toHaveBeenCalledTimes(1)
  })

  it("shows only the last run's outcome, with a failure's error on hover, and reads no run history", async () => {
    const failed = {
      ...SCHEDULE,
      last_run_at: "2026-09-16T03:00:00Z",
      last_run_status: "failed",
      last_run_error: "relation analytics.orders does not exist",
    }
    ;(authFetch as Mock).mockImplementation(async (url: string) =>
      url === "/api/v1/explorer/schedules" ? res(200, { schedules: [failed], count: 1 }) : res(200, {}),
    )
    render(<ScheduledQueriesPage />)

    const badge = await screen.findByText("failed")
    expect(badge.parentElement).toHaveAttribute("title", "relation analytics.orders does not exist")
    expect(screen.queryByText(/run history/i)).not.toBeInTheDocument()
    expect((authFetch as Mock).mock.calls.some(([url]) => String(url).includes("/runs"))).toBe(false)
  })

  it("says a cron as a sentence with the expression on hover, and shows one it cannot word as typed", async () => {
    const unworded = {
      ...SCHEDULE,
      schedule_id: "sch-2",
      saved_query_id: "q-2",
      name: "odd_days",
      schedule_spec: { cron: "0 9 1 * 1", timezone: "UTC" },
    }
    ;(authFetch as Mock).mockImplementation(async (url: string) =>
      url === "/api/v1/explorer/schedules" ? res(200, { schedules: [SCHEDULE, unworded], count: 2 }) : res(200, {}),
    )
    render(<ScheduledQueriesPage />)
    const worded = await screen.findByText("Every day at 03:00 (UTC)")
    expect(worded).toHaveAttribute("title", "0 3 * * *")
    expect(worded.className).not.toMatch(/font-mono/)
    const typed = screen.getByText("0 9 1 * 1 (UTC)")
    expect(typed).not.toHaveAttribute("title")
    expect(typed.className).toMatch(/font-mono/)
  })
})
