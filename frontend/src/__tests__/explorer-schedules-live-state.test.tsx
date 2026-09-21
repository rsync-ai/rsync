import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { act, fireEvent, render, screen, within } from "@testing-library/react"

import ScheduledQueriesPage from "@/app/(dashboard)/explorer/schedules/page"
import { LiveStateDetail } from "@/components/explorer/ModelLiveState"
import { liveCellFor, type Fetched, type RunningModelsResponse } from "@/components/explorer/liveState"
import { authFetch } from "@/lib/api/auth-fetch"

// The schedules page's Now column. Two families of claim are pinned here.
//
// What a row may say: "Idle" only when the server asked the model's refresh loop and was
// told there was none. Orchestration being down, a failed or unreadable check, a model
// the answer left out, and a clock schedule each read as something else.
//
// What the page may cost: /explorer/running is one Temporal query per model on the
// queue that carries real rebuilds, so it is asked every 15 s, only while the tab is
// visible, and never while the previous ask is still out.

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
const RUNNING = "/api/v1/explorer/running"
const FRESHNESS = "/api/v1/explorer/freshness"

const mockFetch = authFetch as unknown as Mock

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

function schedule(id: string, name: string, type = "after_upstream") {
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
  }
}

function refreshState(over: Record<string, unknown> = {}) {
  return {
    saved_query_id: "m-reb",
    phase: "rebuilding",
    queued_completions: 2,
    current: {
      schedule_id: "sch-m-reb",
      upstream_kind: "pipeline",
      upstream_id: "p-1",
      depth: 0,
      coalesced: 1,
      started_at: "2026-09-15T14:02:00Z",
    },
    refreshes_this_run: 4,
    refreshes_per_run: 20,
    ...over,
  }
}

function running(models: unknown[], over: Record<string, unknown> = {}) {
  return { models, count: models.length, limit: 50, temporal_available: true, ...over }
}

const REBUILDING = {
  saved_query_id: "m-reb",
  name: "orders_daily",
  workflow_id: "model-refresh-m-reb",
  state: "running",
  detail_available: true,
  detail: refreshState({ last_error: "relation analytics.stg_orders does not exist" }),
}
const IDLE = {
  saved_query_id: "m-idle",
  name: "revenue_model",
  workflow_id: "model-refresh-m-idle",
  state: "idle",
  detail_available: false,
  message: "no open refresh loop; the last run has closed",
}

function breach(savedQueryId: string, name: string, over: Record<string, unknown> = {}) {
  return {
    breach_id: `b-${savedQueryId}`,
    saved_query_id: savedQueryId,
    name,
    target_table: `analytics.${name}`,
    deadline_seconds: 3600,
    cause: "overdue",
    reference_at: "2026-09-15T10:00:00Z",
    never_succeeded: false,
    stale_seconds: 600,
    stale_seconds_now: 7200,
    detected_at: "2026-09-15T11:10:00Z",
    ...over,
  }
}

type Reply = Response | Promise<Response>

let schedulesBody: unknown
let runningReplies: Array<() => Reply>
let freshnessReply: () => Reply

/** Serves each route; /running replies are taken in order, the last one repeating. */
function serve() {
  mockFetch.mockImplementation((url: string) => {
    if (url === SCHEDULES) return Promise.resolve(res(200, schedulesBody))
    if (url === RUNNING) {
      const next = runningReplies.length > 1 ? runningReplies.shift()! : runningReplies[0]
      return Promise.resolve(next())
    }
    if (url === FRESHNESS) return Promise.resolve(freshnessReply())
    if (url.endsWith("/runs")) return Promise.resolve(res(200, { runs: [] }))
    return Promise.reject(new Error(`unexpected request ${url}`))
  })
}

function calls(url: string): number {
  return mockFetch.mock.calls.filter(([u]) => u === url).length
}

let visibility: DocumentVisibilityState = "visible"

function setVisibility(v: DocumentVisibilityState) {
  visibility = v
  document.dispatchEvent(new Event("visibilitychange"))
}

async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

/** Lets the chained fetch → json → setState → effect → fetch hops all land. */
async function settle() {
  for (let i = 0; i < 5; i++) await advance(0)
}

function row(name: string): HTMLElement {
  const match = screen.getAllByText(name).map((el) => el.closest("tr")).find(Boolean)
  if (!match) throw new Error(`no row for ${name}`)
  return match as HTMLElement
}

// Located by header text, so a column added or removed before it cannot quietly
// point these assertions at the wrong cell.
function nowCell(name: string): HTMLElement {
  const column = screen.getAllByRole("columnheader").findIndex((h) => h.textContent === "Now")
  expect(column).toBeGreaterThan(-1)
  return within(row(name)).getAllByRole("cell")[column]
}

async function renderPage() {
  render(<ScheduledQueriesPage />)
  await settle()
}

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility })
})

afterAll(() => {
  delete (document as unknown as Record<string, unknown>).visibilityState
})

beforeEach(() => {
  // Installed before render, so the page's first timer is already a fake one.
  vi.useFakeTimers()
  visibility = "visible"
  mockFetch.mockReset()
  schedulesBody = {
    schedules: [
      schedule("m-reb", "orders_daily"),
      schedule("m-idle", "revenue_model"),
      schedule("m-clock", "nightly_rollup", "cron"),
    ],
  }
  runningReplies = [() => res(200, running([REBUILDING, IDLE]))]
  freshnessReply = () => res(200, { breaches: [], count: 0 })
  serve()
})

afterEach(() => {
  vi.useRealTimers()
})

describe("schedules page: what each row may say", () => {
  it("shows each state the server reports, and a dash for a clock schedule", async () => {
    schedulesBody = {
      schedules: [
        schedule("m-reb", "orders_daily"),
        schedule("m-wait", "waiting_model"),
        schedule("m-stuck", "stuck_model"),
        schedule("m-idle", "revenue_model"),
        schedule("m-quiet", "quiet_model"),
        schedule("m-unr", "legacy_model"),
        schedule("m-unk", "mystery_model"),
        schedule("m-clock", "nightly_rollup", "cron"),
      ],
    }
    const base = { workflow_id: "wf", detail_available: false }
    runningReplies = [
      () =>
        res(
          200,
          running([
            REBUILDING,
            {
              ...base,
              saved_query_id: "m-wait",
              name: "waiting_model",
              state: "running",
              detail_available: true,
              detail: refreshState({ saved_query_id: "m-wait", phase: "waiting", queued_completions: 0, current: null }),
            },
            {
              ...base,
              saved_query_id: "m-stuck",
              name: "stuck_model",
              state: "running",
              detail_available: true,
              detail: refreshState({
                saved_query_id: "m-stuck",
                phase: "waiting",
                queued_completions: 0,
                current: null,
                last_error: "permission denied for schema analytics",
              }),
            },
            IDLE,
            { ...base, saved_query_id: "m-quiet", name: "quiet_model", state: "running" },
            { ...base, saved_query_id: "m-unr", name: "legacy_model", state: "unreachable", message: "deadline exceeded" },
            { ...base, saved_query_id: "m-unk", name: "mystery_model", state: "unknown" },
            // The route lists after_upstream schedules only. If it ever named a clock
            // schedule, the row must still not claim to know.
            { ...base, saved_query_id: "m-clock", name: "nightly_rollup", state: "idle" },
          ]),
        ),
    ]
    serve()
    await renderPage()

    expect(nowCell("orders_daily").textContent).toBe("Rebuilding · 2 queuedRebuild failed this run")
    // A loop that survived a failed rebuild is not a calm grey Waiting. The failure is
    // never cleared within the run, so the badge says it happened, not that it is failing.
    const failed = within(nowCell("orders_daily")).getByText("Rebuild failed this run")
    expect(failed.closest("[title]")?.getAttribute("title")).toBe(
      "A rebuild failed during this run of the refresh loop: relation analytics.stg_orders does not exist. It is not cleared when a later rebuild succeeds.",
    )
    expect(nowCell("stuck_model").textContent).toBe("WaitingRebuild failed this run")
    expect(nowCell("waiting_model").textContent).toBe("Waiting")
    expect(nowCell("revenue_model").textContent).toBe("Idle")
    expect(nowCell("quiet_model").textContent).toBe("Running · no detail")
    expect(nowCell("legacy_model").textContent).toBe("Unreachable")
    expect(within(nowCell("legacy_model")).getByText("Unreachable").closest("[title]")?.getAttribute("title")).toBe(
      "The refresh loop could not be asked what it is doing (deadline exceeded).",
    )
    expect(nowCell("mystery_model").textContent).toBe("Unknown")
    expect(nowCell("nightly_rollup").textContent).toBe("—")
    expect(screen.getAllByRole("columnheader").map((h) => h.textContent)).toContain("Now")
    expect(screen.queryByText(/Live state unavailable/)).toBeNull()
  })

  it("reads no row as Idle when orchestration is down, even a row the answer calls idle", async () => {
    runningReplies = [() => res(200, running([IDLE], { temporal_available: false }))]
    serve()
    await renderPage()

    expect(screen.getByText("Live state unavailable.")).toBeInTheDocument()
    expect(nowCell("revenue_model").textContent).toBe("Unavailable")
    expect(nowCell("orders_daily").textContent).toBe("Unavailable")
    expect(screen.queryAllByText("Idle")).toHaveLength(0)
  })

  it.each([
    // A well-formed body on a failed status, so it is the status that is being believed.
    ["a failed check, whatever its body says", () => res(503, { ...running([IDLE]), error: "temporal is unavailable" })],
    ["an answer without temporal_available", () => res(200, { models: [IDLE], count: 1, limit: 50 })],
    ["a request that never reached the server", () => Promise.reject(new TypeError("Failed to fetch"))],
  ])("reads no row as Idle after %s", async (_label, reply) => {
    runningReplies = [reply as () => Reply]
    serve()
    await renderPage()

    expect(screen.getByText("Live state unavailable.")).toBeInTheDocument()
    expect(nowCell("revenue_model").textContent).toBe("Unavailable")
    expect(screen.queryAllByText("Idle")).toHaveLength(0)
  })

  it("drops a Rebuilding it can no longer vouch for when a later check fails", async () => {
    runningReplies = [() => res(200, running([REBUILDING, IDLE])), () => res(502, {})]
    serve()
    await renderPage()
    expect(nowCell("orders_daily").textContent).toBe("Rebuilding · 2 queuedRebuild failed this run")

    await advance(15_000)
    await settle()
    expect(nowCell("orders_daily").textContent).toBe("Unavailable")
    expect(screen.getByText("Live state unavailable.")).toBeInTheDocument()
  })

  it("reads Not checked for a model the answer left out, and says when the cap was hit", async () => {
    runningReplies = [() => res(200, running([IDLE], { count: 50, limit: 50 }))]
    serve()
    await renderPage()

    expect(nowCell("orders_daily").textContent).toBe("Not checked")
    expect(nowCell("revenue_model").textContent).toBe("Idle")
    expect(screen.getByText(/checked for the first 50 upstream-triggered models/)).toBeInTheDocument()
  })
})

describe("schedules page: overdue markers", () => {
  it("marks an open breach on its row, ignores a resolved one, and names one with no row", async () => {
    freshnessReply = () =>
      res(200, {
        breaches: [
          breach("m-idle", "revenue_model"),
          breach("m-reb", "orders_daily", { resolved_at: "2026-09-15T12:00:00Z", resolution: "succeeded" }),
          breach("m-clock", "nightly_rollup", { stale_seconds_now: undefined, stale_seconds: 90_000 }),
          breach("m-gone", "ghost_model"),
        ],
        count: 4,
      })
    serve()
    await renderPage()

    expect(nowCell("revenue_model").textContent).toBe("IdleOverdue 2h")
    const badge = within(nowCell("revenue_model")).getByText("Overdue 2h")
    expect(badge.closest("[title]")?.getAttribute("title")).toMatch(/1h freshness deadline by 2h: its schedule is active/)
    expect(nowCell("orders_daily").textContent).not.toMatch(/Overdue/)
    // Overdue is about the table, not the trigger, so a clock schedule can carry one.
    expect(nowCell("nightly_rollup").textContent).toBe("—Overdue 1d 1h")
    expect(screen.getByText(/1 model past its freshness deadline has no row on this page/)).toHaveTextContent(
      "ghost_model",
    )
  })

  it("says when the overdue markers could not be loaded", async () => {
    freshnessReply = () => res(500, { error: "boom" })
    serve()
    await renderPage()

    expect(screen.getByText("Overdue markers unavailable.")).toBeInTheDocument()
    expect(screen.queryByText(/Live state unavailable/)).toBeNull()
  })
})

// The longer account of a row's Now cell. The list used to show it in an expanded
// row; it now lives on the model's own page (explorer-schedule-detail.test.tsx pins
// that it is rendered there), so it is pinned here on its own.
describe("the live-state detail on a model's page", () => {
  const answered = (models: unknown[]): Fetched<RunningModelsResponse> =>
    ({ status: "ok", data: running(models) }) as Fetched<RunningModelsResponse>

  it("says what the loop is doing, and the overdue line", () => {
    render(
      <LiveStateDetail
        cell={liveCellFor("after_upstream", "m-reb", answered([REBUILDING, IDLE]))}
        breach={breach("m-reb", "orders_daily", { never_succeeded: true, cause: "schedule_paused" })}
        upstreams={[{ kind: "pipeline", id: "p-1", name: "orders_sync" }]}
      />,
    )

    expect(screen.getByText(/^Rebuilding since .+, woken by pipeline orders_sync$/)).toBeInTheDocument()
    expect(screen.getByText("2 completions queued · refresh 4 of 20 this run")).toBeInTheDocument()
    expect(screen.getByText(/Last rebuild failure this run: relation analytics.stg_orders does not exist/)).toBeInTheDocument()
    expect(
      screen.getByText(/Past its 1h freshness deadline by 2h — it has never rebuilt successfully; its schedule is paused\./),
    ).toBeInTheDocument()
  })

  it("says why an idle model is idle, in the server's words", () => {
    render(<LiveStateDetail cell={liveCellFor("after_upstream", "m-idle", answered([REBUILDING, IDLE]))} />)
    expect(screen.getByText("Idle: no open refresh loop; the last run has closed")).toBeInTheDocument()
  })

  it("says why an answer on screen was set aside while the page asks again", () => {
    const note = "The last answer is 10m old, so it is not shown while the page asks again."
    render(<LiveStateDetail cell={liveCellFor("after_upstream", "m-reb", { status: "loading", note })} />)
    expect(screen.getByText(`Checking live state… ${note}`)).toBeInTheDocument()
  })
})

describe("schedules page: an answer from before the tab was hidden", () => {
  it("is not shown as current while the page asks again, and says how old it is", async () => {
    let answer!: (r: Response) => void
    runningReplies = [
      () => res(200, running([REBUILDING, IDLE])),
      () => new Promise<Response>((resolve) => (answer = resolve)),
    ]
    serve()
    await renderPage()
    expect(nowCell("orders_daily").textContent).toMatch(/^Rebuilding/)

    act(() => setVisibility("hidden"))
    await advance(10 * 60_000)
    act(() => setVisibility("visible"))
    await settle()
    expect(calls(RUNNING)).toBe(2)

    // Ten minutes ago the loop was rebuilding. Nothing says it still is until the new
    // answer lands, which at the fifty-model cap can take ~14 s.
    const note = "The last answer is 10m old, so it is not shown while the page asks again."
    expect(nowCell("orders_daily").textContent).toBe("Checking…")
    expect(nowCell("revenue_model").textContent).toBe("Checking…")
    expect(within(nowCell("orders_daily")).getByText("Checking…").closest("[title]")?.getAttribute("title")).toBe(note)

    await act(async () => answer(res(200, running([REBUILDING, IDLE]))))
    await settle()
    expect(nowCell("orders_daily").textContent).toMatch(/^Rebuilding/)
    expect(nowCell("revenue_model").textContent).toBe("Idle")
  })

  it("keeps the last answer on screen while a routine check is out", async () => {
    runningReplies = [() => res(200, running([REBUILDING, IDLE])), () => new Promise<Response>(() => {})]
    serve()
    await renderPage()

    await advance(15_000)
    expect(calls(RUNNING)).toBe(2)
    expect(nowCell("orders_daily").textContent).toMatch(/^Rebuilding · 2 queued/)
  })

  it("keeps the last answer on screen when the tab was away for only a few seconds", async () => {
    runningReplies = [() => res(200, running([REBUILDING, IDLE])), () => new Promise<Response>(() => {})]
    serve()
    await renderPage()

    act(() => setVisibility("hidden"))
    await advance(18_000)
    act(() => setVisibility("visible"))
    await settle()
    expect(calls(RUNNING)).toBe(2)
    expect(nowCell("orders_daily").textContent).toMatch(/^Rebuilding · 2 queued/)
  })

  it("keeps an old failed check on screen while it asks again, since Unavailable claims nothing", async () => {
    runningReplies = [() => res(502, {}), () => new Promise<Response>(() => {})]
    serve()
    await renderPage()
    expect(nowCell("orders_daily").textContent).toBe("Unavailable")

    act(() => setVisibility("hidden"))
    await advance(10 * 60_000)
    act(() => setVisibility("visible"))
    await settle()
    expect(calls(RUNNING)).toBe(2)
    expect(nowCell("orders_daily").textContent).toBe("Unavailable")
    expect(screen.getByText("Live state unavailable.")).toBeInTheDocument()
  })
})

describe("schedules page: what looking at it costs", () => {
  it("asks for live state every 15 s and freshness every 60 s", async () => {
    await renderPage()
    expect(calls(RUNNING)).toBe(1)
    expect(calls(FRESHNESS)).toBe(1)

    await advance(14_999)
    expect(calls(RUNNING)).toBe(1)
    await advance(1)
    expect(calls(RUNNING)).toBe(2)

    await advance(45_000)
    expect(calls(RUNNING)).toBe(5)
    expect(calls(FRESHNESS)).toBe(2)
  })

  it("never asks for live state when no row rebuilds after an upstream", async () => {
    schedulesBody = { schedules: [schedule("m-clock", "nightly_rollup", "cron")] }
    await renderPage()
    await advance(60_000)

    expect(calls(RUNNING)).toBe(0)
    expect(calls(FRESHNESS)).toBe(2)
    expect(nowCell("nightly_rollup").textContent).toBe("—")
    expect(screen.queryByText(/Live state unavailable/)).toBeNull()

    fireEvent.click(screen.getByRole("button", { name: /refresh/i }))
    await settle()
    expect(calls(RUNNING)).toBe(0)
    expect(calls(FRESHNESS)).toBe(3)
  })

  it("stops asking while the tab is hidden and picks up again when it is shown", async () => {
    await renderPage()
    expect(calls(RUNNING)).toBe(1)

    act(() => setVisibility("hidden"))
    await advance(15_000)
    await advance(120_000)
    expect(calls(RUNNING)).toBe(1)
    expect(calls(FRESHNESS)).toBe(1)

    act(() => setVisibility("visible"))
    await settle()
    expect(calls(RUNNING)).toBe(2)
    expect(calls(FRESHNESS)).toBe(2)

    await advance(15_000)
    expect(calls(RUNNING)).toBe(3)
  })

  it("asks nothing when the page opens in a hidden tab", async () => {
    visibility = "hidden"
    await renderPage()
    await advance(60_000)

    expect(calls(SCHEDULES)).toBe(1)
    expect(calls(RUNNING)).toBe(0)
    expect(calls(FRESHNESS)).toBe(0)
  })

  it("stops asking once the page is left with a check scheduled", async () => {
    const { unmount } = render(<ScheduledQueriesPage />)
    await settle()
    expect(calls(RUNNING)).toBe(1)

    unmount()
    await advance(120_000)
    expect(calls(RUNNING)).toBe(1)
    expect(calls(FRESHNESS)).toBe(1)
  })

  it("does not resume on becoming visible once the page is left", async () => {
    const { unmount } = render(<ScheduledQueriesPage />)
    await settle()
    act(() => setVisibility("hidden"))
    await advance(60_000)
    expect(calls(RUNNING)).toBe(1)

    unmount()
    act(() => setVisibility("visible"))
    await advance(60_000)
    expect(calls(RUNNING)).toBe(1)
  })

  it("does not schedule another check when the one still out lands after the page is left", async () => {
    let answer!: (r: Response) => void
    runningReplies = [() => new Promise<Response>((resolve) => (answer = resolve)), () => res(200, running([IDLE]))]
    serve()
    const { unmount } = render(<ScheduledQueriesPage />)
    await settle()
    expect(calls(RUNNING)).toBe(1)

    unmount()
    await act(async () => answer(res(200, running([IDLE]))))
    await advance(60_000)
    expect(calls(RUNNING)).toBe(1)
  })

  it("never starts a second check while one is still out, from the timer or from Refresh", async () => {
    let answer!: (r: Response) => void
    runningReplies = [() => new Promise<Response>((resolve) => (answer = resolve)), () => res(200, running([IDLE]))]
    serve()
    await renderPage()
    expect(calls(RUNNING)).toBe(1)

    await advance(60_000)
    expect(calls(RUNNING)).toBe(1)

    fireEvent.click(screen.getByRole("button", { name: /refresh/i }))
    await settle()
    expect(calls(SCHEDULES)).toBe(2)
    expect(calls(RUNNING)).toBe(1)

    await act(async () => answer(res(200, running([REBUILDING, IDLE]))))
    await settle()
    expect(nowCell("orders_daily").textContent).toBe("Rebuilding · 2 queuedRebuild failed this run")

    // The next check is timed from when the last one settled.
    await advance(14_999)
    expect(calls(RUNNING)).toBe(1)
    await advance(1)
    expect(calls(RUNNING)).toBe(2)
  })

  it("keeps asking after the tab returns while a Refresh is still out", async () => {
    // The timer comes due while the tab is hidden, which stops the chain; the tab returns
    // with a Refresh still waiting on its answer. A Refresh schedules nothing after itself,
    // so a tab return that deferred to it would leave the page never asking again.
    let answer!: (r: Response) => void
    runningReplies = [
      () => res(200, running([IDLE])),
      () => new Promise<Response>((resolve) => (answer = resolve)),
      () => res(200, running([IDLE])),
    ]
    serve()
    await renderPage()

    fireEvent.click(screen.getByRole("button", { name: /refresh/i }))
    await settle()
    expect(calls(RUNNING)).toBe(2)

    act(() => setVisibility("hidden"))
    await advance(15_000)
    act(() => setVisibility("visible"))
    await settle()
    // It waits on the check still out rather than starting a second one.
    expect(calls(RUNNING)).toBe(2)

    await act(async () => answer(res(200, running([IDLE]))))
    await settle()
    await advance(15_000)
    expect(calls(RUNNING)).toBe(3)
    await advance(15_000)
    expect(calls(RUNNING)).toBe(4)
  })

  it("times the next check from when a Refresh the timer met lands", async () => {
    let answer!: (r: Response) => void
    runningReplies = [
      () => res(200, running([IDLE])),
      () => new Promise<Response>((resolve) => (answer = resolve)),
      () => res(200, running([IDLE])),
    ]
    serve()
    await renderPage()

    await advance(10_000)
    fireEvent.click(screen.getByRole("button", { name: /refresh/i }))
    await settle()
    expect(calls(RUNNING)).toBe(2)

    // The timer comes due at 15 s with the Refresh still out; the answer lands at 25 s.
    await advance(5_000)
    expect(calls(RUNNING)).toBe(2)
    await advance(10_000)
    await act(async () => answer(res(200, running([IDLE]))))
    await settle()

    await advance(14_999)
    expect(calls(RUNNING)).toBe(2)
    await advance(1)
    expect(calls(RUNNING)).toBe(3)
  })

  it("re-asks live state and freshness when Refresh is pressed", async () => {
    await renderPage()
    expect(calls(RUNNING)).toBe(1)
    expect(calls(FRESHNESS)).toBe(1)

    fireEvent.click(screen.getByRole("button", { name: /refresh/i }))
    await settle()
    expect(calls(RUNNING)).toBe(2)
    expect(calls(FRESHNESS)).toBe(2)
  })
})
