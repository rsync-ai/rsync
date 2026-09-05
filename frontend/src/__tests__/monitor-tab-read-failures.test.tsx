/**
 * Regression tests for the remaining two F-284 consumers on the Monitor tab.
 *
 * #82 fixed the third one (the Live events card). The other two were left, and
 * they have the same shape: a hook that computes an `error` correctly, and a
 * caller that destructures only `data`.
 *
 *   ThroughputCard   `const { data } = usePolledJson(url)` — MonitorTab.tsx:295
 *                    (was :265; the 404 fix below moved it).
 *                    `usePolledJson` sets `error` on a non-2xx and on a thrown
 *                    request (`:162`, `:172`). Dropped, so a 500 renders
 *                    "Loading throughput…" — forever, and indistinguishable
 *                    from a run that simply has not reported yet.
 *
 *   DependenciesCard fed from `usePipelineRuntime(pipelineId)` — MonitorTab.tsx:497,
 *                    destructured as `{ runtime }`. The hook sets `error` at
 *                    `usePipelineRuntime.ts:109`/`:130`. Dropped, so a failed
 *                    read shows "No dependencies reported." next to a health
 *                    dot reading "unknown" — which is exactly what a healthy
 *                    pipeline with no registered dependencies shows.
 *
 * Both are the F-242 class: an infrastructure failure borrowing the empty
 * state's pixels. The bound in each case is that the genuine empty state must
 * survive — "nothing to report" is a real and different answer from
 * "we could not ask".
 *
 * The 404 case was out of scope when this file was written and is now covered
 * at the bottom: `usePolledJson` used to short-circuit 404 into silence and
 * disable itself, which is the same defect with a longer fuse — the card showed
 * the empty state and never recovered.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, waitFor, act } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))

import { MonitorTab } from "@/components/pipeline/MonitorTab"

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

const RUNTIME_OK = {
  pipeline_id: "p1",
  execution_id: "e1",
  mode: "batch",
  phase: "syncing",
  health: "healthy",
  dependencies: [
    { kind: "mcp_source", identifier: "postgresql@v1.0.14", status: "healthy" },
  ],
  updated_at: "2026-08-05T10:00:00Z",
}

const STATS_OK = {
  summary: { mode: "batch", total_tables: 1, total_read_rows: 1200, total_inserted_rows: 1200 },
  tables: [],
  total: 0,
}

/** Route the mock by URL so each panel can be failed independently. */
function routeFetch(handler: (url: string) => Response) {
  authFetch.mockImplementation(async (url: string) => handler(String(url)))
}

beforeEach(() => {
  vi.clearAllMocks()
})

afterEach(() => {
  // Only the 404 block installs them, but a test that throws mid-way would
  // otherwise leave real timers replaced for every file that follows.
  vi.useRealTimers()
})

describe("F-284 — Throughput states a failed read instead of loading forever", () => {
  it("says the read failed when table-stats returns 500", async () => {
    routeFetch((url) => {
      if (url.includes("/runtime")) return res(200, RUNTIME_OK)
      if (url.includes("/table-stats")) return res(500, { error: "boom" })
      return res(200, { events: [] })
    })

    render(<MonitorTab pipelineId="p1" />)

    await waitFor(() =>
      expect(screen.getByText(/couldn't load throughput|could not load throughput/i)).toBeInTheDocument(),
    )
    expect(screen.queryByText(/loading throughput/i)).toBeNull()
  })

  // THE BOUND. A pipeline that has never run has no execution to scope to, and
  // that is not a failure — the card must keep saying so.
  it("still explains that throughput appears once the pipeline runs", async () => {
    routeFetch((url) => {
      if (url.includes("/runtime")) return res(200, { ...RUNTIME_OK, execution_id: undefined })
      return res(200, { events: [] })
    })

    render(<MonitorTab pipelineId="p1" />)

    await waitFor(() =>
      expect(screen.getByText(/throughput appears once this pipeline runs/i)).toBeInTheDocument(),
    )
  })

  it("still renders the numbers on a healthy read", async () => {
    routeFetch((url) => {
      if (url.includes("/runtime")) return res(200, RUNTIME_OK)
      if (url.includes("/table-stats")) return res(200, STATS_OK)
      return res(200, { events: [] })
    })

    render(<MonitorTab pipelineId="p1" />)

    // getAllBy, not getBy: the card renders read AND written, and this fixture
    // reports 1,200 for both. A getBy here fails on the healthy path for a
    // reason that has nothing to do with the defect under test.
    await waitFor(() => expect(screen.getAllByText("1,200").length).toBeGreaterThan(0))
  })
})

describe("F-284 — Dependencies states a failed read instead of 'none reported'", () => {
  it("says the read failed when the runtime endpoint returns 500", async () => {
    routeFetch((url) => {
      if (url.includes("/runtime")) return res(500, { error: "boom" })
      return res(200, { events: [] })
    })

    render(<MonitorTab pipelineId="p1" />)

    await waitFor(() =>
      expect(
        screen.getByText(/couldn't load dependencies|could not load dependencies/i),
      ).toBeInTheDocument(),
    )
    expect(screen.queryByText(/no dependencies reported/i)).toBeNull()
  })

  // THE BOUND. A successful read that genuinely returns no dependencies still
  // has to say so, and must not be dressed up as an error.
  it("still says none are reported when the read succeeds and the list is empty", async () => {
    routeFetch((url) => {
      if (url.includes("/runtime")) return res(200, { ...RUNTIME_OK, dependencies: [] })
      return res(200, { events: [] })
    })

    render(<MonitorTab pipelineId="p1" />)

    await waitFor(() =>
      expect(screen.getByText(/no dependencies reported/i)).toBeInTheDocument(),
    )
  })

  it("still lists the dependencies on a healthy read", async () => {
    routeFetch((url) => {
      if (url.includes("/runtime")) return res(200, RUNTIME_OK)
      return res(200, { events: [] })
    })

    render(<MonitorTab pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(/postgresql@v1.0.14/)).toBeInTheDocument())
  })
})

/**
 * KI-POLLEDJSON-404-STOPS-POLLING-SILENTLY.
 *
 * The premise the issue was filed on turned out to be wrong, which changes the
 * fix. It recorded the 404 branch as "deliberate for one consumer — a 404 from
 * table-stats means this pipeline has no stats yet". It does not. Both endpoints
 * answer "nothing to report" with a 200 and an empty body; each 404s only for an
 * unparseable id or from `requirePipelineWorkspaceRole`, i.e. the pipeline is
 * gone or is not in the caller's ACTIVE workspace. So the branch was wrong for
 * every consumer, and the proposed per-call `treat404As` option would have been
 * a knob with no correct second setting.
 *
 * That is also why the recovery case below is the load-bearing one. Merely
 * showing an error on 404 is one line; the actual damage was that the hook
 * latched a ref and went quiet, so the card could not come back when the
 * pipeline did.
 */
describe("a 404 is a failed read that keeps retrying, not a silent empty state", () => {
  it("says the read failed instead of showing the empty state", async () => {
    routeFetch((url) => {
      if (url.includes("/runtime")) return res(200, RUNTIME_OK)
      if (url.includes("/table-stats")) return res(404, { error: "Pipeline not found" })
      return res(200, { events: [] })
    })

    render(<MonitorTab pipelineId="p1" />)

    await waitFor(() =>
      expect(screen.getByText(/couldn't load throughput \(404\)/i)).toBeInTheDocument(),
    )
    // The two states this used to be indistinguishable from.
    expect(screen.queryByText(/loading throughput/i)).toBeNull()
    expect(screen.queryByText(/throughput appears once this pipeline runs/i)).toBeNull()
  })

  it("says so for the events feed too", async () => {
    routeFetch((url) => {
      if (url.includes("/runtime")) return res(200, RUNTIME_OK)
      if (url.includes("/table-stats")) return res(200, STATS_OK)
      return res(404, { error: "Pipeline not found" })
    })

    render(<MonitorTab pipelineId="p1" />)

    await waitFor(() =>
      expect(screen.getByText(/could not load recent events \(404\)/i)).toBeInTheDocument(),
    )
    expect(screen.queryByText(/no recent events/i)).toBeNull()
  })

  // THE CASE THAT DISCRIMINATES. Rendering an error on the first 404 passes even
  // with the old `disabledRef` still in place, because the ref only silences the
  // *later* ticks. This one advances the clock past the 5s poll and requires the
  // card to recover on its own -- which is exactly what a workspace switch or a
  // pipeline that reappears looks like from here.
  it("keeps polling after a 404 and recovers when the resource comes back", async () => {
    let missing = true
    routeFetch((url) => {
      if (url.includes("/runtime")) return res(200, RUNTIME_OK)
      if (url.includes("/table-stats")) {
        return missing ? res(404, { error: "Pipeline not found" }) : res(200, STATS_OK)
      }
      return res(200, { events: [] })
    })

    // Installed BEFORE render: the hook registers its interval on mount, and
    // switching to fake timers afterwards does not take over an already-scheduled
    // real one. `waitFor`/`findBy*` are avoided from here for the same reason.
    vi.useFakeTimers()
    render(<MonitorTab pipelineId="p1" />)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0)
    })
    expect(screen.getByText(/couldn't load throughput \(404\)/i)).toBeInTheDocument()

    missing = false
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_100)
    })

    expect(screen.getAllByText("1,200").length).toBeGreaterThan(0)
    expect(screen.queryByText(/couldn't load throughput/i)).toBeNull()
  })

  // THE BOUND, and it is the whole reason the 404 branch existed. A read that
  // succeeds and genuinely has nothing in it must still render as empty -- if
  // this fix turned every quiet pipeline into an error card it would be a worse
  // defect than the one it replaced.
  it("still renders an empty 200 as empty, not as a failure", async () => {
    routeFetch((url) => {
      if (url.includes("/runtime")) return res(200, RUNTIME_OK)
      if (url.includes("/table-stats")) return res(200, { summary: null, tables: [], total: 0 })
      return res(200, { events: [] })
    })

    render(<MonitorTab pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(/loading throughput/i)).toBeInTheDocument())
    expect(screen.getByText(/no recent events/i)).toBeInTheDocument()
    expect(screen.queryByText(/couldn't load throughput/i)).toBeNull()
    expect(screen.queryByText(/could not load recent events/i)).toBeNull()
  })
})
