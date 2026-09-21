/**
 * "View Logs" in a pipeline's ⋯ menu opened the run's page on per-table row
 * counts — the same numbers the pipeline's Table statistics tab shows — and no
 * log. It now opens the run's log (#logs): the run's own events plus the
 * pipeline-level alerts and self-healing decisions raised while it ran, without
 * timer ticks unless asked for.
 */
import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

import { authFetch } from "@/lib/api/auth-fetch"
import { RunLogPanel, RUN_LOG_PAGE_SIZE } from "@/components/executions/RunLogPanel"

const mockAuthFetch = vi.mocked(authFetch)

function json(body: unknown, status = 200): Response {
  return { ok: status < 400, status, json: async () => body } as unknown as Response
}

function row(over: Record<string, unknown>) {
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    event_type: "STAGE_COMPLETED",
    severity: "info",
    occurred_at: "2026-09-18T10:00:00Z",
    received_at: "2026-09-18T10:00:00Z",
    payload: {},
    ...over,
  }
}

const PAGE_ONE = [
  row({
    event_id: "fail",
    event_type: "STAGE_FAILED",
    stage_id: "executor",
    severity: "error",
    occurred_at: "2026-09-18T10:05:00Z",
    payload: { message: "sink rejected batch: column amount is NOT NULL" },
  }),
  row({
    event_id: "alert",
    execution_id: null,
    event_type: "SENTINEL_ALERT",
    severity: "warn",
    occurred_at: "2026-09-18T10:04:00Z",
    payload: { message: "Consumer lag above threshold" },
  }),
  row({ event_id: "done", stage_id: "planner", occurred_at: "2026-09-18T10:01:00Z", payload: { summary: "Plan ready" } }),
]

const CURSOR = { before_ts: "2026-09-18T10:01:00Z", before_seq: 3, before_event_id: "done" }

function urls(): URL[] {
  return mockAuthFetch.mock.calls.map(([u]) => new URL(String(u), "http://x"))
}

describe("RunLogPanel", () => {
  beforeEach(() => {
    mockAuthFetch.mockReset()
    mockAuthFetch.mockImplementation(async (u) => {
      const q = new URL(String(u), "http://x").searchParams
      if (q.get("before_event_id") === "done") {
        return json({ events: [row({ event_id: "start", event_type: "PIPELINE_STARTED", occurred_at: "2026-09-18T09:59:00Z" })], has_more: false, next_cursor: null })
      }
      return json({ events: PAGE_ONE, has_more: true, next_cursor: CURSOR })
    })
  })

  it("asks for this run's log — run scope, no timer ticks — on the #logs anchor", async () => {
    const { container } = render(<RunLogPanel pipelineId="p1" executionId="e1" />)
    await screen.findByText("sink rejected batch: column amount is NOT NULL")

    const u = urls()[0]
    expect(u.pathname).toBe("/api/v1/pipelines/p1/events")
    expect(u.searchParams.get("execution_id")).toBe("e1")
    expect(u.searchParams.get("scope")).toBe("run")
    expect(u.searchParams.get("exclude_routine")).toBe("true")
    expect(u.searchParams.get("limit")).toBe(String(RUN_LOG_PAGE_SIZE))
    expect(container.querySelector("#logs")).not.toBeNull()
  })

  it("renders each entry as severity · what happened · stage · detail, newest first", async () => {
    render(<RunLogPanel pipelineId="p1" executionId="e1" />)
    const rows = await screen.findAllByTestId("run-log-row")
    expect(rows).toHaveLength(3)

    expect(within(rows[0]).getByText("Error")).toBeInTheDocument()
    expect(within(rows[0]).getByText("Stage failed")).toBeInTheDocument()
    expect(within(rows[0]).getByText(/Executing Pipeline/)).toBeInTheDocument()

    // A pipeline-level alert raised during the run is part of its log.
    expect(within(rows[1]).getByText("Warning")).toBeInTheDocument()
    expect(within(rows[1]).getByText("Monitoring alert")).toBeInTheDocument()
    expect(within(rows[1]).getByText("Consumer lag above threshold")).toBeInTheDocument()

    expect(within(rows[2]).getByText("Info")).toBeInTheDocument()
    expect(within(rows[2]).getByText("Plan ready")).toBeInTheDocument()
    // Not table statistics.
    expect(screen.queryByText(/table statistics/i)).not.toBeInTheDocument()
  })

  it("narrows to errors and warnings, and searches", async () => {
    const user = userEvent.setup()
    render(<RunLogPanel pipelineId="p1" executionId="e1" />)
    await screen.findAllByTestId("run-log-row")

    await user.click(screen.getByRole("button", { name: /errors & warnings/i }))
    expect(screen.getAllByTestId("run-log-row")).toHaveLength(2)
    expect(screen.queryByText("Plan ready")).not.toBeInTheDocument()

    await user.click(screen.getByRole("button", { name: /^all$/i }))
    await user.type(screen.getByRole("searchbox", { name: /search the log/i }), "lag")
    const rows = screen.getAllByTestId("run-log-row")
    expect(rows).toHaveLength(1)
    expect(within(rows[0]).getByText("Monitoring alert")).toBeInTheDocument()
    expect(screen.getByText("1 of 3 loaded entries shown")).toBeInTheDocument()
  })

  it("re-reads with routine updates included when asked", async () => {
    const user = userEvent.setup()
    render(<RunLogPanel pipelineId="p1" executionId="e1" />)
    await screen.findAllByTestId("run-log-row")

    await user.click(screen.getByRole("checkbox", { name: /show routine updates/i }))
    await waitFor(() => expect(mockAuthFetch).toHaveBeenCalledTimes(2))
    expect(urls()[1].searchParams.has("exclude_routine")).toBe(false)
    expect(urls()[1].searchParams.get("scope")).toBe("run")
  })

  it("reads as loading until the re-read lands, not as the old answer", async () => {
    const user = userEvent.setup()
    render(<RunLogPanel pipelineId="p1" executionId="e1" />)
    await screen.findAllByTestId("run-log-row")
    const refresh = screen.getByRole("button", { name: /refresh/i })
    expect(refresh).toBeEnabled()

    let release!: (r: Response) => void
    mockAuthFetch.mockImplementationOnce(() => new Promise<Response>((r) => { release = r }))
    await user.click(screen.getByRole("checkbox", { name: /show routine updates/i }))
    await waitFor(() => expect(mockAuthFetch).toHaveBeenCalledTimes(2))
    expect(refresh).toBeDisabled()

    release(json({ events: [row({ event_id: "tick", event_type: "DATA_PLANE_METRICS" })], has_more: false, next_cursor: null }))
    await waitFor(() => expect(screen.getAllByTestId("run-log-row")).toHaveLength(1))
    expect(refresh).toBeEnabled()
  })

  it("pages older entries with the server's cursor, verbatim", async () => {
    const user = userEvent.setup()
    render(<RunLogPanel pipelineId="p1" executionId="e1" />)
    await screen.findAllByTestId("run-log-row")

    await user.click(screen.getByRole("button", { name: /load older/i }))
    await screen.findByText("Pipeline started")
    const q = urls()[1].searchParams
    expect(q.get("before_ts")).toBe(CURSOR.before_ts)
    expect(q.get("before_seq")).toBe(String(CURSOR.before_seq))
    expect(q.get("before_event_id")).toBe(CURSOR.before_event_id)
    expect(q.get("scope")).toBe("run")
    expect(screen.getAllByTestId("run-log-row")).toHaveLength(4)
    expect(screen.queryByRole("button", { name: /load older/i })).not.toBeInTheDocument()
  })

  it("shows the raw payload behind Details", async () => {
    const user = userEvent.setup()
    render(<RunLogPanel pipelineId="p1" executionId="e1" />)
    const rows = await screen.findAllByTestId("run-log-row")

    await user.click(within(rows[1]).getByRole("button", { name: "Details" }))
    expect(within(rows[1]).getByText("SENTINEL_ALERT")).toBeInTheDocument()
    expect(within(rows[1]).getByText("pipeline-level event")).toBeInTheDocument()
    expect(within(rows[1]).getByText(/"message": "Consumer lag above threshold"/)).toBeInTheDocument()
  })

  it("says so when the log cannot be read, and when it is empty", async () => {
    mockAuthFetch.mockResolvedValueOnce(json({}, 500))
    const { unmount } = render(<RunLogPanel pipelineId="p1" executionId="e1" />)
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not load the log (HTTP 500)")
    unmount()

    mockAuthFetch.mockResolvedValueOnce(json({ events: [], has_more: false, next_cursor: null }))
    render(<RunLogPanel pipelineId="p1" executionId="e1" />)
    expect(await screen.findByText("No log entries for this run.")).toBeInTheDocument()
  })
})
