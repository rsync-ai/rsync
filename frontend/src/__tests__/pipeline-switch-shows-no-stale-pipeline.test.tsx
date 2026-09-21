/**
 * Prod, 2026-09-20 (app.rsync.ai): opening a healthy CDC pipeline from another
 * pipeline's page showed, for about a second, the *other* pipeline's state --
 * the header read "Failed · last event 2d ago" before flipping to
 * "Running · Streaming · caught up", and Dependencies read
 * "Unhealthy -- no MCP server registered with orchestrator" before flipping to
 * all Healthy. Nothing was wrong with either pipeline; the page was reporting
 * the previous one.
 *
 * `/pipelines/[id]` is one route segment, so React keeps the component (and its
 * hook state) across a client navigation from A to B. `usePipelineRuntime` and
 * `usePolledJson` only ever replaced their data when a response arrived, so
 * between the navigation and B's first response both rendered A's answer as if
 * it were B's. The header and the Dependencies card read the same hook, which
 * is why one bug produced both symptoms.
 *
 * The bound: a *failed poll* must still keep what it has -- "the feed skipped a
 * beat" is not "we are now looking at something else". That is a different
 * question from the subject changing, and the last test here holds it.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))

import { MonitorTab } from "@/components/pipeline/MonitorTab"
import { PipelineHealthHeader } from "@/components/pipeline/PipelineHealthHeader"

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

const OLD_RUNTIME = {
  pipeline_id: "p-old",
  execution_id: "e-old",
  mode: "cdc",
  phase: "failed",
  health: "unhealthy",
  message: "a dependency is unhealthy",
  liveness: { stale_seconds: 172_800, pending_events: 0 },
  dependencies: [
    {
      kind: "mcp_source",
      identifier: "postgresql@v1.0.14",
      status: "unhealthy",
      last_error: "no MCP server registered with orchestrator",
    },
  ],
  updated_at: "2026-09-18T12:00:00Z",
}

// A CDC runtime asks for `?mode=cdc`, so the summary has to be a CDC one --
// otherwise the card renders its batch rows and the number never appears.
const OLD_STATS = {
  summary: {
    mode: "cdc",
    total_tables: 1,
    tables_completed: 1,
    total_cdc_events: 1200,
    total_applied_cdc_events: 1200,
  },
  tables: [],
  total: 0,
}

/** p-old answers; p-new never does, so only what the page already held can show. */
function routeOldAnswersNewHangs() {
  authFetch.mockImplementation(async (rawUrl: string) => {
    const url = String(rawUrl)
    if (url.includes("p-new")) return new Promise<Response>(() => {})
    if (url.includes("/runtime")) return res(200, OLD_RUNTIME)
    if (url.includes("/table-stats")) return res(200, OLD_STATS)
    return res(200, { events: [] })
  })
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe("switching pipelines never shows the previous pipeline's state", () => {
  it("the Dependencies card drops the old pipeline's failure", async () => {
    routeOldAnswersNewHangs()
    const { rerender } = render(<MonitorTab pipelineId="p-old" />)
    await waitFor(() =>
      expect(screen.getByText(/no MCP server registered with orchestrator/i)).toBeInTheDocument(),
    )

    rerender(<MonitorTab pipelineId="p-new" />)

    expect(screen.queryByText(/no MCP server registered with orchestrator/i)).toBeNull()
  })

  it("the Throughput card drops the old pipeline's numbers", async () => {
    routeOldAnswersNewHangs()
    const { rerender } = render(<MonitorTab pipelineId="p-old" />)
    await waitFor(() => expect(screen.getAllByText("1,200").length).toBeGreaterThan(0))

    rerender(<MonitorTab pipelineId="p-new" />)

    expect(screen.queryAllByText("1,200")).toHaveLength(0)
  })

  it("the health header drops the old pipeline's vital and says it is loading", async () => {
    routeOldAnswersNewHangs()
    const { rerender } = render(<PipelineHealthHeader pipelineId="p-old" />)
    await waitFor(() => expect(screen.getByText(/last event 2d ago/i)).toBeInTheDocument())

    rerender(<PipelineHealthHeader pipelineId="p-new" />)

    expect(screen.queryByText(/last event 2d ago/i)).toBeNull()
    expect(screen.getByText(/loading pipeline health/i)).toBeInTheDocument()
  })

  // THE BOUND. Re-rendering the same pipeline is not a change of subject, and
  // a reset keyed on anything looser than the pipeline id would blank the page
  // on every parent render.
  it("a re-render of the same pipeline keeps what it has", async () => {
    routeOldAnswersNewHangs()
    const { rerender } = render(<MonitorTab pipelineId="p-old" />)
    // Both signals, not just the first: they come from separate fetches, and the
    // assertions after the re-render are synchronous on purpose — that is what
    // makes them read "kept" rather than "eventually arrived". Waiting on only
    // the MCP message lets the throughput fetch still be in flight, which is a
    // race the test would lose on a loaded runner.
    await waitFor(() => {
      expect(screen.getByText(/no MCP server registered with orchestrator/i)).toBeInTheDocument()
      expect(screen.getAllByText("1,200").length).toBeGreaterThan(0)
    })

    rerender(<MonitorTab pipelineId="p-old" />)

    expect(screen.getByText(/no MCP server registered with orchestrator/i)).toBeInTheDocument()
    expect(screen.getAllByText("1,200").length).toBeGreaterThan(0)
  })
})
