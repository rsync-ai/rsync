import { afterEach, beforeAll, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { ModelLineageGraph, type ModelLineageGraphProps } from "@/components/explorer/ModelLineageGraph"
import type { ScheduledQuery } from "@/components/explorer/scheduledModel"
import { authFetch } from "@/lib/api/auth-fetch"

// The Graph tab draws other people's models. What it must never do is quiet: print a
// private model's name or id that leaked into a downstream's upstream list, flash a name
// before the node's own route has answered, or read "nothing is linked" when the schedule
// list could not be loaded.

const media = vi.hoisted(() => ({ canvas: true }))

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("next-themes", () => ({ useTheme: () => ({ resolvedTheme: "light" }) }))

beforeAll(() => {
  // What React Flow needs from a browser that jsdom does not have.
  class ResizeObserverStub {
    constructor(private cb: ResizeObserverCallback) {}
    observe(target: Element) {
      const contentRect = { width: 800, height: 420 } as DOMRectReadOnly
      this.cb([{ target, contentRect } as ResizeObserverEntry], this as unknown as ResizeObserver)
    }
    unobserve() {}
    disconnect() {}
  }
  class DOMMatrixReadOnlyStub {
    m22: number
    constructor(transform?: string) {
      const scale = transform?.match(/scale\(([\d.]+)\)/)?.[1]
      this.m22 = scale !== undefined ? Number(scale) : 1
    }
  }
  Object.assign(globalThis, { ResizeObserver: ResizeObserverStub, DOMMatrixReadOnly: DOMMatrixReadOnlyStub })
  Object.defineProperties(HTMLElement.prototype, {
    offsetHeight: { configurable: true, get() { return parseFloat(this.style.height) || 1 } },
    offsetWidth: { configurable: true, get() { return parseFloat(this.style.width) || 1 } },
  })
  ;(SVGElement.prototype as unknown as { getBBox: () => DOMRect }).getBBox = () =>
    ({ x: 0, y: 0, width: 0, height: 0 }) as DOMRect
  window.matchMedia = ((query: string) => ({
    matches: media.canvas,
    media: query,
    onchange: null,
    addEventListener: () => {},
    removeEventListener: () => {},
    addListener: () => {},
    removeListener: () => {},
    dispatchEvent: () => false,
  })) as unknown as typeof window.matchMedia
})

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

function sched(id: string, overrides: Partial<ScheduledQuery> = {}): ScheduledQuery {
  return {
    schedule_id: `s-${id}`,
    saved_query_id: id,
    name: id,
    connection_id: "c-1",
    schedule_type: "cron",
    schedule_spec: { cron: "0 3 * * *" },
    status: "active",
    materialization: "table",
    created_by: "u-1",
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-01T00:00:00Z",
    ...overrides,
  }
}

function after(id: string, upstreams: { kind: string; id: string; name?: string }[], overrides: Partial<ScheduledQuery> = {}) {
  return sched(id, { schedule_type: "after_upstream", schedule_spec: {}, upstreams, upstream_policy: "any", ...overrides })
}

// Another member's private model: its row is not in the caller's list, but the root's
// upstream list still carries its id and name.
const PRIVATE_ID = "priv-7f3a91"
const PRIVATE_NAME = "secret_salaries"

const ROOT = after("q-1", [
  { kind: "model", id: PRIVATE_ID, name: PRIVATE_NAME },
  { kind: "pipeline", id: "pl-1", name: "orders sync" },
], { name: "revenue_rollup" })
const DOWN = after("q-down", [{ kind: "model", id: "q-1", name: "revenue_rollup" }], { name: "orders_kpi" })

const runsUrl = (id: string) => `/api/v1/explorer/saved/${id}/runs?limit=25`

type Handler = Response | (() => Response | Promise<Response>)

function serve(routes: Record<string, Handler>) {
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    const hit = routes[url]
    if (hit === undefined) return res(404, {})
    return typeof hit === "function" ? hit() : hit
  })
}

function chainRoutes(overrides: Record<string, Handler> = {}): Record<string, Handler> {
  return {
    "/api/v1/explorer/schedules": res(200, { schedules: [ROOT, DOWN], count: 2 }),
    [runsUrl("q-1")]: res(200, { runs: [{ status: "succeeded", finished_at: "2026-09-16T03:00:00Z" }] }),
    [runsUrl(PRIVATE_ID)]: res(404, { error: "not found" }),
    "/api/v1/pipelines/pl-1": res(200, {
      name: "orders sync",
      status: "active",
      last_execution: { status: "completed", completed_at: "2026-09-16T02:00:00Z" },
    }),
    [runsUrl("q-down")]: res(200, { runs: [{ status: "failed", finished_at: "2026-09-16T03:05:00Z" }] }),
    ...overrides,
  }
}

function props(overrides: Partial<ModelLineageGraphProps> = {}): ModelLineageGraphProps {
  return {
    modelId: "q-1",
    modelName: "revenue_rollup",
    rootSchedule: ROOT,
    canSchedule: true,
    running: { status: "ok", data: { models: [], count: 0, limit: 200, temporal_available: true } },
    reloadTick: 0,
    ...overrides,
  }
}

const calls = () => (authFetch as Mock).mock.calls.map(([url, init]) => ({ url: String(url), init: init as Record<string, unknown> }))

/** The list's row for a title. The run grid under the list names each node as well. */
function listItem(text: string): HTMLElement {
  const item = screen.getAllByText(text).map((el) => el.closest("li")).find((li) => li !== null)
  if (!item) throw new Error(`no list row for ${text}`)
  return item
}

async function findListItem(text: string): Promise<HTMLElement> {
  await screen.findAllByText(text)
  return listItem(text)
}

function deferred() {
  let resolve!: (r: Response) => void
  const promise = new Promise<Response>((r) => (resolve = r))
  return { promise, resolve }
}

describe("ModelLineageGraph", () => {
  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
    media.canvas = true
  })

  it("draws the chain as a canvas, naming only the nodes whose own route answered", async () => {
    serve(chainRoutes())
    const { container } = render(<ModelLineageGraph {...props()} />)

    await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(4))
    expect(screen.getByText("2 upstream · 1 downstream")).toBeInTheDocument()
    expect(container.querySelectorAll(".react-flow__edge")).toHaveLength(3)

    const canvas = container.querySelector(".react-flow")!.parentElement!
    expect(canvas).toHaveAttribute("aria-hidden", "true")
    expect(within(canvas).getByText("A model you can't open")).toBeInTheDocument()
    // A card opens the run panel, which holds the link; the card itself links nowhere.
    expect(canvas.querySelectorAll("a")).toHaveLength(0)
    expect(within(canvas).getByText("orders_kpi")).toBeInTheDocument()
    expect(within(canvas).getByText("orders sync")).toBeInTheDocument()
    expect(within(canvas).getByText("This model")).toBeInTheDocument()
    expect(within(canvas).getByText("revenue_rollup")).toBeInTheDocument()

    // Neither the private model's name nor its id reaches the DOM, in text or in an attribute.
    expect(document.body.innerHTML).not.toContain(PRIVATE_NAME)
    expect(document.body.innerHTML).not.toContain(PRIVATE_ID)

    // The canvas is a picture: nothing in it takes focus or announces React Flow's own ids.
    const focusable = 'a[href], button, input, select, textarea, [tabindex]'
    expect([...canvas.querySelectorAll(focusable)].filter((el) => el.getAttribute("tabindex") !== "-1")).toHaveLength(0)
    for (const el of document.querySelectorAll("[aria-label]")) {
      expect(el.getAttribute("aria-label")).not.toMatch(/\b[ne]\d+\b|Edge from/)
    }
    // The zoom controls sit outside the hidden canvas, so they can be reached.
    for (const name of ["Zoom in", "Zoom out", "Fit the whole chain"]) {
      expect(screen.getByRole("button", { name })).toBeInTheDocument()
    }
  })

  it("lists the chain with links when the screen cannot pan a canvas", async () => {
    media.canvas = false
    serve(chainRoutes())
    render(<ModelLineageGraph {...props()} />)

    const down = await screen.findByRole("link", { name: "orders_kpi" })
    expect(down).toHaveAttribute("href", "/explorer/schedules/q-down?tab=graph")
    expect(screen.getByRole("link", { name: "orders sync" })).toHaveAttribute("href", "/pipelines/pl-1")
    // The model itself is the page: it is marked, not linked.
    expect(screen.queryByRole("link", { name: "revenue_rollup" })).not.toBeInTheDocument()
    expect(listItem("revenue_rollup")).toHaveAttribute("aria-current", "true")

    expect(screen.getByRole("heading", { name: "Upstream (2)" })).toBeInTheDocument()
    expect(screen.getByRole("heading", { name: "Downstream (1)" })).toBeInTheDocument()
    expect(screen.getByRole("status")).toHaveTextContent(/^Graph loaded: 2 upstream, 1 downstream\.$/)
    const downItem = down.closest("li")!
    expect(within(downItem).getByText("Direct downstream")).toBeInTheDocument()
    expect(within(downItem).getByText(/^Failed · /)).toBeInTheDocument()
    const hidden = listItem("A model you can't open")
    expect(within(hidden).getByText("Status hidden")).toBeInTheDocument()
    expect(within(hidden).queryByRole("link")).not.toBeInTheDocument()

    // No canvas, so nothing to toggle to.
    expect(screen.queryByRole("group", { name: "Show the chain as" })).not.toBeInTheDocument()
    expect(document.body.innerHTML).not.toContain(PRIVATE_NAME)
    expect(document.body.innerHTML).not.toContain(PRIVATE_ID)
  })

  it("switches between the canvas and the list", async () => {
    serve(chainRoutes())
    const user = userEvent.setup()
    const { container } = render(<ModelLineageGraph {...props()} />)

    const toggle = await screen.findByRole("group", { name: "Show the chain as" })
    expect(within(toggle).getByRole("button", { name: "Graph" })).toHaveAttribute("aria-pressed", "true")
    await user.click(within(toggle).getByRole("button", { name: "List" }))
    expect(await screen.findByRole("link", { name: "orders_kpi" })).toBeInTheDocument()
    expect(within(toggle).getByRole("button", { name: "List" })).toHaveAttribute("aria-pressed", "true")
    expect(container.querySelector(".react-flow")).toBeNull()
  })

  it("asks each node's own route, under a deadline it can abort, and never a name before every answer is in", async () => {
    media.canvas = false
    const pipeline = deferred()
    serve(chainRoutes({ "/api/v1/pipelines/pl-1": () => pipeline.promise }))
    render(<ModelLineageGraph {...props()} />)

    await waitFor(() => expect(calls().some((c) => c.url === "/api/v1/pipelines/pl-1")).toBe(true))
    // Let every other lookup settle: the graph still waits for the last one.
    await waitFor(() => expect(calls().some((c) => c.url === runsUrl("q-down"))).toBe(true))
    await new Promise((r) => setTimeout(r, 20))
    expect(screen.getByText("Loading the graph…")).toBeInTheDocument()
    expect(screen.getByRole("status")).toHaveTextContent("Loading the graph.")
    expect(screen.queryByText("orders_kpi")).not.toBeInTheDocument()

    pipeline.resolve(res(200, { name: "orders sync", status: "paused", last_execution: null }))
    expect(await screen.findByRole("link", { name: "orders sync" })).toBeInTheDocument()
    expect(screen.getByText("Paused")).toBeInTheDocument()

    expect(calls().map((c) => c.url).sort()).toEqual(
      ["/api/v1/explorer/schedules", runsUrl("q-1"), runsUrl(PRIVATE_ID), "/api/v1/pipelines/pl-1", runsUrl("q-down")].sort(),
    )
    for (const c of calls()) {
      // The deadline is the component's own, over the body as well: authFetch's timeoutMs stops at the headers.
      expect(c.init.timeoutMs).toBeUndefined()
      expect(c.init.signal).toBeInstanceOf(AbortSignal)
      expect(c.init.cache).toBe("no-store")
    }
  })

  it("gives up on a node whose body stalls after its headers, and says its status is unavailable", async () => {
    media.canvas = false
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const stalled = { ...res(200, {}), json: () => new Promise(() => {}) } as unknown as Response
      serve(chainRoutes({ "/api/v1/pipelines/pl-1": stalled }))
      render(<ModelLineageGraph {...props()} />)

      await waitFor(() => expect(calls().some((c) => c.url === "/api/v1/pipelines/pl-1")).toBe(true))
      await vi.advanceTimersByTimeAsync(9_000)
      expect(screen.getByText("Loading the graph…")).toBeInTheDocument()
      await vi.advanceTimersByTimeAsync(1_500)

      const pipeline = await findListItem("A pipeline")
      expect(within(pipeline).getByText("Status unavailable")).toBeInTheDocument()
      expect(screen.getByRole("link", { name: "orders_kpi" })).toBeInTheDocument()
      // The download itself was told to stop.
      const signal = calls().find((c) => c.url === "/api/v1/pipelines/pl-1")!.init.signal as AbortSignal
      expect(signal.aborted).toBe(true)
    } finally {
      vi.useRealTimers()
    }
  })

  it("gives up on a schedule list whose body stalls, and offers Retry", async () => {
    media.canvas = false
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const stalled = { ...res(200, {}), json: () => new Promise(() => {}) } as unknown as Response
      serve(chainRoutes({ "/api/v1/explorer/schedules": stalled }))
      render(<ModelLineageGraph {...props()} />)
      await waitFor(() => expect(calls()).toHaveLength(1))
      await vi.advanceTimersByTimeAsync(15_500)
      expect(await screen.findByRole("alert")).toHaveTextContent("Could not reach the server to load the graph.")
      expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })

  it("reads a pipeline route's own 404 as unknown, not as a pipeline the caller can't open", async () => {
    media.canvas = false
    serve(chainRoutes({ "/api/v1/pipelines/pl-1": res(404, { error: "pipeline_not_found", message: "Pipeline not found" }) }))
    render(<ModelLineageGraph {...props()} />)
    const pipeline = await findListItem("A pipeline")
    expect(within(pipeline).getByText("Status unavailable")).toBeInTheDocument()
    expect(screen.queryByText("A pipeline you can't open")).not.toBeInTheDocument()
    expect(screen.getByText("Some statuses could not be loaded.")).toBeInTheDocument()
    cleanup()

    serve(chainRoutes({ "/api/v1/pipelines/pl-1": res(404, { error: "not found" }) }))
    render(<ModelLineageGraph {...props()} />)
    expect(await findListItem("A pipeline you can't open")).toBeInTheDocument()
  })

  it("asks once for older runs when every recent run is a skip that says nothing", async () => {
    media.canvas = false
    const waiting = { status: "skipped", skip_reason: "waiting_on_upstreams", finished_at: "2026-09-16T03:05:00Z" }
    const cursor = "2026-09-16T03:01:00.5Z_r5"
    const olderUrl = `/api/v1/explorer/saved/q-down/runs?limit=200&before=${encodeURIComponent(cursor)}`
    serve(
      chainRoutes({
        [runsUrl("q-down")]: res(200, { runs: Array(5).fill(waiting), next_cursor: cursor }),
        [olderUrl]: res(200, { runs: [waiting, { status: "failed", finished_at: "2026-09-15T03:00:00Z" }] }),
        // One with no older page stays as it was, and is not asked again.
        [runsUrl("q-1")]: res(200, { runs: [waiting] }),
      }),
    )
    render(<ModelLineageGraph {...props()} />)
    const down = await screen.findByRole("link", { name: "orders_kpi" })
    expect(within(down.closest("li")!).getByText(/^Failed · /)).toBeInTheDocument()
    expect(calls().filter((c) => c.url === olderUrl)).toHaveLength(1)
    expect(calls().filter((c) => c.url.startsWith("/api/v1/explorer/saved/q-1/"))).toHaveLength(1)
  })

  it("keeps the recent runs when the older page fails", async () => {
    media.canvas = false
    const waiting = { status: "skipped", skip_reason: "waiting_on_upstreams", finished_at: "2026-09-16T03:05:00Z" }
    serve(
      chainRoutes({
        [runsUrl("q-down")]: res(200, { runs: [waiting], next_cursor: "c1" }),
        "/api/v1/explorer/saved/q-down/runs?limit=200&before=c1": res(500, {}),
      }),
    )
    render(<ModelLineageGraph {...props()} />)
    const down = await screen.findByRole("link", { name: "orders_kpi" })
    expect(within(down.closest("li")!).getByText(/^Waiting for other upstreams/)).toBeInTheDocument()
  })

  it("loads again when a drawn model it showed as rebuilding stops, since its new run was written after", async () => {
    media.canvas = false
    let downRun = { status: "failed", finished_at: "2026-09-16T03:05:00Z" }
    serve(chainRoutes({ [runsUrl("q-down")]: () => res(200, { runs: [downRun] }) }))
    const rebuilding: ModelLineageGraphProps["running"] = {
      status: "ok",
      data: {
        models: [
          {
            saved_query_id: "q-down",
            name: "orders_kpi",
            workflow_id: "w-1",
            state: "running",
            detail_available: true,
            detail: { saved_query_id: "q-down", queued_completions: 0, current: null, refreshes_this_run: 1, refreshes_per_run: 100, phase: "rebuilding" },
          },
        ],
        count: 1,
        limit: 200,
        temporal_available: true,
      },
    }
    const { rerender } = render(<ModelLineageGraph {...props({ running: rebuilding })} />)
    const down = await screen.findByRole("link", { name: "orders_kpi" })
    expect(within(down.closest("li")!).getByText("Rebuilding now")).toBeInTheDocument()
    const before = calls().length

    // A poll that fails says nothing about whether it stopped.
    rerender(<ModelLineageGraph {...props({ running: { status: "error", message: "x" } })} />)
    await new Promise((r) => setTimeout(r, 20))
    expect(calls()).toHaveLength(before)

    downRun = { status: "succeeded", finished_at: "2026-09-16T04:00:00Z" }
    rerender(<ModelLineageGraph {...props()} />)
    await waitFor(() =>
      expect(within(screen.getByRole("link", { name: "orders_kpi" }).closest("li")!).getByText(/^Succeeded · /)).toBeInTheDocument(),
    )
    expect(calls().length).toBeGreaterThan(before)
    // It does not keep reloading once it has.
    const after = calls().length
    rerender(<ModelLineageGraph {...props({ running: { ...props().running } })} />)
    await new Promise((r) => setTimeout(r, 20))
    expect(calls()).toHaveLength(after)
  })

  it("says which statuses could not be loaded, keeps what its row vouches for, and retries", async () => {
    media.canvas = false
    let downStatus = 500
    serve(
      chainRoutes({
        [runsUrl("q-down")]: () =>
          downStatus === 500 ? res(500, {}) : res(200, { runs: [{ status: "succeeded", finished_at: "2026-09-16T03:05:00Z" }] }),
        [runsUrl(PRIVATE_ID)]: res(429, {}),
      }),
    )
    const user = userEvent.setup()
    render(<ModelLineageGraph {...props()} />)

    // The downstream's own schedule row names it; only its status is unknown.
    const down = await screen.findByRole("link", { name: "orders_kpi" })
    expect(within(down.closest("li")!).getByText("Status unavailable")).toBeInTheDocument()
    // A 429 is not a refusal, so it is not "can't open" — and it is no licence to print the name.
    expect(listItem("A model")).toBeInTheDocument()
    expect(screen.queryByText("A model you can't open")).not.toBeInTheDocument()
    expect(document.body.innerHTML).not.toContain(PRIVATE_NAME)
    expect(screen.getByText("Some statuses could not be loaded.")).toBeInTheDocument()
    expect(screen.getByRole("status")).toHaveTextContent("Graph loaded: 2 upstream, 1 downstream. Some statuses could not be loaded.")

    downStatus = 200
    const before = calls().filter((c) => c.url === runsUrl("q-down")).length
    await user.click(screen.getByRole("button", { name: "Retry" }))
    expect(screen.getByRole("heading", { name: "Graph" })).toHaveFocus()
    await waitFor(() => expect(within(screen.getByRole("link", { name: "orders_kpi" }).closest("li")!).getByText(/^Succeeded · /)).toBeInTheDocument())
    expect(calls().filter((c) => c.url === runsUrl("q-down")).length).toBe(before + 1)
  })

  it("keeps the last graph on screen, marked busy, while Refresh reloads it", async () => {
    media.canvas = false
    serve(chainRoutes())
    const { rerender } = render(<ModelLineageGraph {...props()} />)
    await screen.findByRole("link", { name: "orders_kpi" })

    const schedules = deferred()
    serve(chainRoutes({ "/api/v1/explorer/schedules": () => schedules.promise }))
    rerender(<ModelLineageGraph {...props({ reloadTick: 1 })} />)

    expect(screen.getByRole("link", { name: "orders_kpi" })).toBeInTheDocument()
    expect(screen.getByRole("link", { name: "orders_kpi" }).closest("[aria-busy]")).toHaveAttribute("aria-busy", "true")

    schedules.resolve(res(200, { schedules: [ROOT], count: 1 }))
    await waitFor(() => expect(screen.queryByRole("link", { name: "orders_kpi" })).not.toBeInTheDocument())
    expect(screen.getByText("2 upstream · 0 downstream")).toBeInTheDocument()
  })

  it("tells the page when it is loading", async () => {
    media.canvas = false
    serve(chainRoutes())
    const onLoadingChange = vi.fn()
    render(<ModelLineageGraph {...props({ onLoadingChange })} />)
    await screen.findByRole("link", { name: "orders_kpi" })
    expect(onLoadingChange).toHaveBeenCalledWith(true)
    // The link is drawn at commit, but the effect that reports false runs in a later scheduler task when the CPU is busy.
    await waitFor(() => expect(onLoadingChange).toHaveBeenLastCalledWith(false))
  })

  it("says the links could not be loaded, rather than that there are none, and retries", async () => {
    media.canvas = false
    let listStatus = 500
    serve(
      chainRoutes({
        "/api/v1/explorer/schedules": () =>
          listStatus === 500 ? res(500, {}) : res(200, { schedules: [ROOT, DOWN], count: 2 }),
      }),
    )
    const user = userEvent.setup()
    render(<ModelLineageGraph {...props()} />)

    expect(await screen.findByRole("alert")).toHaveTextContent("Could not load the models linked to this one (HTTP 500).")
    expect(screen.queryByText(/waits on it/)).not.toBeInTheDocument()

    listStatus = 200
    await user.click(screen.getByRole("button", { name: "Retry" }))
    // The button is gone, so focus goes to the tab's heading rather than the page.
    expect(screen.getByRole("heading", { name: "Graph" })).toHaveFocus()
    expect(await screen.findByRole("link", { name: "orders_kpi" })).toBeInTheDocument()
  })

  it("says so when the server cannot be reached or answers with no list", async () => {
    media.canvas = false
    ;(authFetch as Mock).mockRejectedValue(new TypeError("Failed to fetch"))
    render(<ModelLineageGraph {...props()} />)
    expect(await screen.findByText("Could not reach the server to load the graph.")).toBeInTheDocument()
    cleanup()

    serve(chainRoutes({ "/api/v1/explorer/schedules": res(200, { schedules: null }) }))
    render(<ModelLineageGraph {...props()} />)
    expect(await screen.findByText("Could not read the models linked to this one.")).toBeInTheDocument()
  })

  it("explains an unlinked model by how it is scheduled, and who can link it", async () => {
    serve({
      "/api/v1/explorer/schedules": res(200, { schedules: [], count: 0 }),
      [runsUrl("q-1")]: res(200, { runs: [] }),
    })

    render(<ModelLineageGraph {...props({ rootSchedule: null })} />)
    expect(await screen.findByText("This query has no schedule, and no model you can see waits on it.")).toBeInTheDocument()
    expect(screen.getByText(/To link it, set a schedule to “After a pipeline or model runs”/)).toBeInTheDocument()
    cleanup()

    render(<ModelLineageGraph {...props({ rootSchedule: sched("q-1"), canSchedule: false })} />)
    expect(
      await screen.findByText("No model or pipeline feeds this model, and no model you can see waits on it. It runs on its own schedule."),
    ).toBeInTheDocument()
    expect(screen.getByText("A workspace admin can make it run after another model.")).toBeInTheDocument()
    cleanup()

    render(<ModelLineageGraph {...props({ rootSchedule: after("q-1", []) })} />)
    expect(
      await screen.findByText("No model or pipeline is chosen to trigger this model, and no model you can see waits on it."),
    ).toBeInTheDocument()
    // One node is not a chain: no canvas, no toggle.
    expect(screen.queryByRole("group", { name: "Show the chain as" })).not.toBeInTheDocument()
  })

  it("hedges when the schedule list came back full", async () => {
    serve({
      "/api/v1/explorer/schedules": res(200, { schedules: [sched("q-1")], count: 500 }),
      [runsUrl("q-1")]: res(200, { runs: [] }),
    })
    render(<ModelLineageGraph {...props({ rootSchedule: sched("q-1") })} />)
    expect(
      await screen.findByText(
        "As far as this page can see, no model or pipeline feeds this model, and no model you can see waits on it. It runs on its own schedule.",
      ),
    ).toBeInTheDocument()
    expect(screen.getByText(/This workspace has 500 or more schedules/)).toBeInTheDocument()
  })

  it("draws the nearest 49 of a large chain, says how many are left out, and asks six at a time", async () => {
    media.canvas = false
    const root = sched("q-1")
    const downs = Array.from({ length: 60 }, (_, i) => after(`d-${i}`, [{ kind: "model", id: "q-1" }]))
    let inFlight = 0
    let most = 0
    ;(authFetch as Mock).mockImplementation(async (url: string) => {
      if (url === "/api/v1/explorer/schedules") return res(200, { schedules: [root, ...downs], count: 61 })
      inFlight++
      most = Math.max(most, inFlight)
      await new Promise((r) => setTimeout(r, 1))
      inFlight--
      return res(200, { runs: [] })
    })
    render(<ModelLineageGraph {...props({ rootSchedule: root })} />)

    expect(
      // Fifty lookups, six at a time: on a loaded CI runner that is more than the default wait.
      await screen.findByText(/Showing 49 of 60 linked models and pipelines, nearest first\.\s+11 more are not drawn\./, {}, { timeout: 25_000 }),
    ).toBeInTheDocument()
    expect(screen.getByRole("heading", { name: "Downstream (49 of 60)" })).toBeInTheDocument()
    expect(screen.getByRole("heading", { name: "Upstream (0)" })).toBeInTheDocument()
    expect(calls().filter((c) => c.url.includes("/runs?")).length).toBe(50)
    expect(most).toBeLessThanOrEqual(6)
    expect(most).toBeGreaterThan(1)
  })

  it("counts a side the cap cut as drawn of found, and still draws the other side", async () => {
    media.canvas = false
    // 60 upstreams and one downstream at the same distance: the downstream still gets its place.
    const ups = Array.from({ length: 60 }, (_, i) => sched(`u-${i}`))
    const root = after("q-1", ups.map((u) => ({ kind: "model", id: u.saved_query_id })))
    ;(authFetch as Mock).mockImplementation(async (url: string) =>
      url === "/api/v1/explorer/schedules"
        ? res(200, { schedules: [root, ...ups, DOWN], count: 62 })
        : res(200, { runs: [] }),
    )
    render(<ModelLineageGraph {...props({ rootSchedule: root })} />)
    expect(await screen.findByRole("heading", { name: "Upstream (48 of 60)" }, { timeout: 25_000 })).toBeInTheDocument()
    expect(screen.getByRole("heading", { name: "Downstream (1)" })).toBeInTheDocument()
    expect(screen.getByRole("link", { name: "orders_kpi" })).toBeInTheDocument()
  })

  it("says in the list that a paused model won't be triggered", async () => {
    media.canvas = false
    serve(chainRoutes({ "/api/v1/explorer/schedules": res(200, { schedules: [ROOT, { ...DOWN, status: "paused" }], count: 2 }) }))
    render(<ModelLineageGraph {...props()} />)
    const down = await screen.findByRole("link", { name: "orders_kpi" })
    expect(within(down.closest("li")!).getByText("won't trigger (paused)")).toBeInTheDocument()
    expect(within(down.closest("li")!).getByText("Paused")).toBeInTheDocument()
    const pipeline = screen.getByRole("link", { name: "orders sync" }).closest("li")!
    expect(within(pipeline).queryByText("won't trigger (paused)")).not.toBeInTheDocument()
  })

  it("dashes a link into a paused model, and says what the dashes mean", async () => {
    serve(chainRoutes({ "/api/v1/explorer/schedules": res(200, { schedules: [ROOT, { ...DOWN, status: "paused" }], count: 2 }) }))
    const { container } = render(<ModelLineageGraph {...props()} />)
    expect(await screen.findByText("won't trigger (paused)")).toBeInTheDocument()
    await waitFor(() => expect(container.querySelectorAll(".react-flow__edge")).toHaveLength(3))
    // A card cuts its second line short, so the whole of it is kept for a pointer.
    const paused = within(container.querySelector(".react-flow")!.parentElement!).getByText("Paused")
    expect(paused).toHaveAttribute("title", "Paused")
    const dashed = [...container.querySelectorAll<SVGPathElement>(".react-flow__edge-path")].filter(
      (path) => path.style.strokeDasharray === "4 4",
    )
    expect(dashed).toHaveLength(1)
  })

  describe("the run panel and the run grid", () => {
    const at = (time: string) => `2026-09-16T${time}Z`
    const versionsUrl = (id: string) => `/api/v1/explorer/saved/${id}/versions`
    const ROOT_RUNS = [
      {
        run_id: "r-2",
        status: "failed",
        trigger_source: "triggered",
        upstream_kind: "pipeline",
        upstream_id: "pl-1",
        started_at: at("04:00:00"),
        finished_at: at("04:00:01.200"),
        duration_ms: 1200,
        error: 'relation "orders" does not exist',
      },
      { run_id: "r-1", status: "succeeded", trigger_source: "manual", started_at: at("03:00:00"), finished_at: at("03:00:00.097"), duration_ms: 97, rows_affected: 42 },
    ]
    // Woken by the model's r-1, so it sits in r-1's column.
    const DOWN_RUNS = [
      {
        run_id: "d-1",
        status: "succeeded",
        trigger_source: "triggered",
        upstream_kind: "model",
        upstream_id: "q-1",
        upstream_run_id: "r-1",
        started_at: at("03:00:05"),
        finished_at: at("03:00:08"),
        duration_ms: 3000,
      },
    ]
    // orders_kpi was edited at 03:30, after d-1 started: d-1 ran the text from before it.
    const DOWN_SQL = {
      versions: [{ version: 1, sql_text: "select 1 as before_edit", created_at: at("03:30:00") }],
      count: 1,
      current: { name: "orders_kpi", sql_text: "select 2 as after_edit" },
    }
    const ROOT_SQL = { versions: [], count: 0, current: { name: "revenue_rollup", sql_text: "select 42" } }

    function runRoutes(overrides: Record<string, Handler> = {}): Record<string, Handler> {
      return chainRoutes({
        [runsUrl("q-1")]: res(200, { runs: ROOT_RUNS }),
        [runsUrl("q-down")]: res(200, { runs: DOWN_RUNS }),
        [versionsUrl("q-1")]: res(200, ROOT_SQL),
        [versionsUrl("q-down")]: res(200, DOWN_SQL),
        ...overrides,
      })
    }

    async function canvasOf(container: HTMLElement): Promise<HTMLElement> {
      await waitFor(() => expect(container.querySelectorAll(".react-flow__node")).toHaveLength(4))
      return container.querySelector(".react-flow")!.parentElement!
    }

    // A plain click, which is what onNodeClick listens for. user-event's mousedown carries no
    // `view` in jsdom, and d3-zoom's mousedown handler reads the document from it and throws.
    const clickCard = (canvas: HTMLElement, text: string) => fireEvent.click(within(canvas).getByText(text))

    const recentRuns = (panel: HTMLElement) => within(panel).getByRole("list", { name: "Recent runs, newest first" })

    it("says how long each node's last run took", async () => {
      media.canvas = false
      serve(runRoutes())
      render(<ModelLineageGraph {...props()} />)
      const down = await findListItem("orders_kpi")
      expect(within(down).getByText(/^Succeeded in 3s · /)).toBeInTheDocument()
      expect(within(listItem("revenue_rollup")).getByText(/^Failed in 1.2s · /)).toBeInTheDocument()
    })

    it("opens a card's recent runs and the SQL the chosen run executed", async () => {
      serve(runRoutes())
      const user = userEvent.setup()
      const { container } = render(<ModelLineageGraph {...props()} />)
      const canvas = await canvasOf(container)
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument()

      clickCard(canvas, "orders_kpi")
      const panel = await screen.findByRole("dialog", { name: "orders_kpi" })
      expect(within(panel).getByRole("link", { name: "Open model" })).toHaveAttribute("href", "/explorer/schedules/q-down?tab=graph")
      // The newest run is picked, with how long it took and what woke it.
      expect(within(recentRuns(panel)).getByRole("button", { name: /^Succeeded in 3s/ })).toHaveAttribute("aria-pressed", "true")
      expect(within(panel).getByText("after a model")).toBeInTheDocument()

      // The text as it was when the run started, not as it is now.
      expect(await within(panel).findByLabelText("SQL when this run started")).toHaveTextContent(/^select 1 as before_edit$/)
      expect(within(panel).getByText("The model has been edited since.")).toBeInTheDocument()
      expect(within(panel).getByRole("button", { name: "Copy" })).toBeInTheDocument()
      await user.click(within(panel).getByRole("button", { name: "Show current SQL" }))
      expect(within(panel).getByLabelText("Current SQL")).toHaveTextContent(/^select 2 as after_edit$/)
      expect(calls().filter((c) => c.url === versionsUrl("q-down"))).toHaveLength(1)

      await user.keyboard("{Escape}")
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument()
    })

    it("opens this model's own card, shows a failed run's error, and says so when the SQL route refuses", async () => {
      serve(runRoutes({ [versionsUrl("q-1")]: res(403, { error: "forbidden" }) }))
      const user = userEvent.setup()
      const { container } = render(<ModelLineageGraph {...props()} />)
      const canvas = await canvasOf(container)

      clickCard(canvas, "revenue_rollup")
      const panel = await screen.findByRole("dialog", { name: "revenue_rollup" })
      // This model is the page: there is nothing to open.
      expect(within(panel).queryByRole("link")).not.toBeInTheDocument()
      const runs = within(recentRuns(panel)).getAllByRole("button")
      expect(runs.map((b) => b.getAttribute("aria-pressed"))).toEqual(["true", "false"])
      expect(runs[0]).toHaveAccessibleName(/^Failed in 1.2s/)
      expect(within(panel).getByText('relation "orders" does not exist')).toBeInTheDocument()
      expect(within(panel).getByText("after a pipeline")).toBeInTheDocument()
      expect(await within(panel).findByText("You can't open this model's SQL.")).toBeInTheDocument()
      expect(within(panel).queryByRole("button", { name: "Copy" })).not.toBeInTheDocument()

      await user.click(runs[1])
      expect(runs[1]).toHaveAttribute("aria-pressed", "true")
      expect(within(panel).getByText("manual")).toBeInTheDocument()
      expect(within(panel).getByText("42")).toBeInTheDocument()
      expect(within(panel).queryByText('relation "orders" does not exist')).not.toBeInTheDocument()
    })

    it("opens from the list's button, retries a SQL load that failed, and gives focus back on close", async () => {
      media.canvas = false
      let sqlStatus = 500
      serve(runRoutes({ [versionsUrl("q-down")]: () => (sqlStatus === 500 ? res(500, {}) : res(200, DOWN_SQL)) }))
      const user = userEvent.setup()
      render(<ModelLineageGraph {...props()} />)

      const open = await screen.findByRole("button", { name: "Runs and SQL: orders_kpi" })
      await user.click(open)
      const panel = await screen.findByRole("dialog", { name: "orders_kpi" })
      expect(await within(panel).findByRole("alert")).toHaveTextContent("Could not load the SQL (HTTP 500).")
      expect(within(panel).queryByRole("button", { name: "Copy" })).not.toBeInTheDocument()

      sqlStatus = 200
      await user.click(within(panel).getByRole("button", { name: "Retry" }))
      expect(await within(panel).findByLabelText("SQL when this run started")).toHaveTextContent(/^select 1 as before_edit$/)

      await user.click(within(panel).getByRole("button", { name: "Close" }))
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument()
      expect(open).toHaveFocus()
    })

    it("gives up on SQL whose body stalls, and offers Retry", async () => {
      media.canvas = false
      vi.useFakeTimers({ shouldAdvanceTime: true })
      try {
        const stalled = { ...res(200, {}), json: () => new Promise(() => {}) } as unknown as Response
        serve(runRoutes({ [versionsUrl("q-down")]: stalled }))
        const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime })
        render(<ModelLineageGraph {...props()} />)
        await user.click(await screen.findByRole("button", { name: "Runs and SQL: orders_kpi" }))
        const panel = await screen.findByRole("dialog", { name: "orders_kpi" })
        expect(within(panel).getByText("Loading the SQL…")).toBeInTheDocument()
        await vi.advanceTimersByTimeAsync(10_500)
        expect(await within(panel).findByRole("alert")).toHaveTextContent("Could not reach the server to load the SQL.")
        expect(within(panel).getByRole("button", { name: "Retry" })).toBeInTheDocument()
      } finally {
        vi.useRealTimers()
      }
    })

    it("opens a pipeline's last execution, with no SQL to ask for", async () => {
      media.canvas = false
      serve(runRoutes())
      const user = userEvent.setup()
      render(<ModelLineageGraph {...props()} />)
      await user.click(await screen.findByRole("button", { name: "Runs and SQL: orders sync" }))
      const panel = await screen.findByRole("dialog", { name: "orders sync" })
      expect(within(panel).getByRole("link", { name: "Open pipeline" })).toHaveAttribute("href", "/pipelines/pl-1")
      expect(within(panel).getByText("Last execution")).toBeInTheDocument()
      expect(calls().some((c) => c.url.endsWith("/versions"))).toBe(false)
    })

    it("opens nothing for a node the caller can't open, and never asks for its SQL", async () => {
      serve(runRoutes())
      const user = userEvent.setup()
      const { container } = render(<ModelLineageGraph {...props()} />)
      const canvas = await canvasOf(container)

      const hidden = within(canvas).getByText("A model you can't open")
      expect(hidden.closest("[title='Show runs and SQL']")).toBeNull()
      expect(within(canvas).getByText("orders_kpi").closest("[title='Show runs and SQL']")).not.toBeNull()
      fireEvent.click(hidden)
      await new Promise((r) => setTimeout(r, 20))
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument()

      await user.click(screen.getByRole("button", { name: "List" }))
      const row = await findListItem("A model you can't open")
      expect(within(row).queryByRole("button")).not.toBeInTheDocument()
      expect(
        screen
          .getAllByRole("button", { name: /^Runs and SQL: / })
          .map((b) => b.getAttribute("aria-label"))
          .sort(),
      ).toEqual(["Runs and SQL: orders sync", "Runs and SQL: orders_kpi", "Runs and SQL: revenue_rollup"])
      expect(calls().some((c) => c.url.endsWith("/versions"))).toBe(false)
      expect(document.body.innerHTML).not.toContain(PRIVATE_NAME)
      expect(document.body.innerHTML).not.toContain(PRIVATE_ID)
    })

    it("lines each run of the model up with the runs it woke, and a square opens that run", async () => {
      media.canvas = false
      serve(runRoutes())
      const user = userEvent.setup()
      render(<ModelLineageGraph {...props()} />)

      const section = await screen.findByRole("region", { name: "Runs across the chain" })
      const table = within(section).getByRole("table")
      const cells = (title: string) => {
        const header = within(table).getAllByRole("rowheader").find((th) => th.textContent?.endsWith(title))
        return [...header!.closest("tr")!.querySelectorAll<HTMLElement>("td > *")]
      }
      const label = (el: HTMLElement) => `${el.tagName === "BUTTON" ? "button" : el.getAttribute("role")} ${el.getAttribute("aria-label")}`

      // Oldest on the left: r-1, then r-2.
      expect(cells("revenue_rollup").map(label)).toEqual([
        expect.stringMatching(/^button revenue_rollup: succeeded in 97ms, started .+\. Open the run and its SQL$/),
        expect.stringMatching(/^button revenue_rollup: failed in 1\.2s, started .+\. Open the run and its SQL$/),
      ])
      // d-1 names r-1 as the run that woke it; the failed r-2 woke nothing.
      expect(cells("orders_kpi").map(label)).toEqual([
        expect.stringMatching(/^button orders_kpi: succeeded in 3s, started .+\. Open the run and its SQL$/),
        expect.stringMatching(/^img orders_kpi: /),
      ])
      expect(cells("orders sync").map(label)).toEqual([
        expect.stringMatching(/^img orders sync: /),
        "img orders sync: Woke this run; no run of its own is shown",
      ])
      expect(cells("A model you can't open").every((el) => el.getAttribute("role") === "img")).toBe(true)
      expect(document.body.innerHTML).not.toContain(PRIVATE_ID)

      await user.click(cells("orders_kpi")[0])
      const panel = await screen.findByRole("dialog", { name: "orders_kpi" })
      expect(within(recentRuns(panel)).getByRole("button", { name: /^Succeeded in 3s/ })).toHaveAttribute("aria-pressed", "true")
      await user.keyboard("{Escape}")

      // The older run of the model, not its newest.
      await user.click(cells("revenue_rollup")[0])
      const root = await screen.findByRole("dialog", { name: "revenue_rollup" })
      const picked = within(recentRuns(root))
        .getAllByRole("button")
        .filter((b) => b.getAttribute("aria-pressed") === "true")
      expect(picked).toHaveLength(1)
      expect(picked[0]).toHaveAccessibleName(/^Succeeded in 97ms/)
      expect(await within(root).findByLabelText("SQL when this run started")).toHaveTextContent(/^select 42$/)
    })

    it("draws no grid for a model with nothing linked", async () => {
      media.canvas = false
      const alone = sched("q-1", { name: "revenue_rollup" })
      serve(
        chainRoutes({
          "/api/v1/explorer/schedules": res(200, { schedules: [alone], count: 1 }),
          [runsUrl("q-1")]: res(200, { runs: ROOT_RUNS }),
        }),
      )
      render(<ModelLineageGraph {...props({ rootSchedule: alone })} />)
      await waitFor(() => expect(calls().some((c) => c.url === runsUrl("q-1"))).toBe(true))
      await new Promise((r) => setTimeout(r, 20))
      expect(screen.queryByRole("region", { name: "Runs across the chain" })).not.toBeInTheDocument()
    })
  })
})
