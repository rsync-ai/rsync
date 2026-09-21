/**
 * The Overview "Recent runs" card (GET /pipelines/:id/trends).
 *
 * The rate is succeeded / finished over the same window; a run still in flight
 * counts toward neither side. Below three finished runs no percentage is shown,
 * because it would overstate what we know.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
}))

import { PipelineRecentRuns, summarizeRecentRuns, type TrendRun } from "@/components/pipeline/PipelineRecentRuns"

function run(id: string, status: string, duration_ms?: number): TrendRun {
  return { execution_id: id, status, start_time: "2026-09-18T10:00:00Z", duration_ms }
}

function respond(body: unknown, ok = true) {
  authFetch.mockResolvedValue({ ok, status: ok ? 200 : 500, json: async () => body })
}

beforeEach(() => authFetch.mockReset())
afterEach(() => cleanup())

describe("summarizeRecentRuns", () => {
  it("orders oldest first and uses the gateway's counts", () => {
    const s = summarizeRecentRuns({
      finished_runs: 3,
      succeeded_runs: 2,
      avg_duration_ms: 252_000,
      recent_executions: [run("new", "running"), run("mid", "failed", 60_000), run("old", "completed", 1000), run("older", "completed", 1000)],
    })
    expect(s.runs.map((r) => r.execution_id)).toEqual(["older", "old", "mid", "new"])
    expect(s).toMatchObject({ finished: 3, succeeded: 2, running: 1, ratePercent: 67, avgDurationMs: 252_000 })
  })

  it("derives the counts from the list for an older gateway", () => {
    const s = summarizeRecentRuns({
      success_rate: 0.1, // the old, wrong all-time denominator — must be ignored
      recent_executions: [run("a", "completed"), run("b", "completed"), run("c", "failed"), run("d", "running")],
    })
    expect(s).toMatchObject({ finished: 3, succeeded: 2, running: 1, ratePercent: 67 })
  })

  it("shows no percentage below three finished runs", () => {
    expect(summarizeRecentRuns({ recent_executions: [run("a", "completed"), run("b", "failed")] }).ratePercent).toBeNull()
  })
})

describe("PipelineRecentRuns", () => {
  it("renders one linked pill per run and the N-of-M sentence", async () => {
    respond({
      finished_runs: 10,
      succeeded_runs: 9,
      avg_duration_ms: 252_000,
      recent_executions: Array.from({ length: 10 }, (_, i) => run(`e${i}`, i === 3 ? "failed" : "completed", 250_000)),
    })
    render(<PipelineRecentRuns pipelineId="p1" />)
    expect(await screen.findByTestId("pipeline-recent-runs-summary")).toHaveTextContent(
      "9 of the last 10 finished runs succeeded · average 4m 12s"
    )
    expect(screen.getByText("90% succeeded")).toBeInTheDocument()
    const pills = screen.getAllByRole("link").filter((a) => a.getAttribute("href")?.startsWith("/executions/"))
    expect(pills).toHaveLength(10)
    expect(pills[0]).toHaveAttribute("href", "/executions/e9")
    expect(authFetch.mock.calls[0][0]).toContain("/pipelines/p1/trends?limit=10")
  })

  it("says No runs yet for a pipeline that never ran", async () => {
    respond({ finished_runs: 0, succeeded_runs: 0, recent_executions: [] })
    render(<PipelineRecentRuns pipelineId="p1" />)
    expect(await screen.findByText(/No runs yet/)).toBeInTheDocument()
    expect(screen.queryByText(/% succeeded/)).toBeNull()
  })

  it("says so when nothing has finished yet", async () => {
    respond({ finished_runs: 0, succeeded_runs: 0, recent_executions: [run("e1", "running")] })
    render(<PipelineRecentRuns pipelineId="p1" />)
    expect(await screen.findByTestId("pipeline-recent-runs-summary")).toHaveTextContent("No run has finished yet · 1 running")
  })

  it("shows an error line instead of vanishing when the request fails", async () => {
    respond({}, false)
    render(<PipelineRecentRuns pipelineId="p1" />)
    expect(await screen.findByText("Couldn't load recent runs.")).toBeInTheDocument()
  })
})
