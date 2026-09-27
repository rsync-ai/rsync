/**
 * The Overview's "Agentic pipeline stages" list and the Steps/DAG tab drew the
 * same stages with the same names and durations. Once setup is over the Overview
 * folds its list into one line that links to Steps/DAG; it keeps the full list
 * while that list is the news: a stage running, a question waiting on the user,
 * or the stage a run died in.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, cleanup } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: null, loading: false, error: null }),
}))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
  authFetchOrThrow: (...args: unknown[]) => authFetch(...args),
}))

import { PipelineLiveStatePanel, finishedSetupSummary } from "@/components/pipeline/PipelineLiveStatePanel"
import type { StageExecution } from "@/lib/pipeline/stageDefinitions"

type PlanStage = { id: string; display_name: string; status: string; actual_duration_ms?: number }

const DONE: PlanStage[] = [
  { id: "capability_resolver", display_name: "Resolving Connectors", status: "complete", actual_duration_ms: 489 },
  { id: "planner", display_name: "Creating Plan", status: "complete", actual_duration_ms: 4_200 },
  { id: "executor", display_name: "Running Pipeline", status: "complete", actual_duration_ms: 7_311 },
]

function liveState(status: string, stages: PlanStage[], over: Record<string, unknown> = {}) {
  return {
    schema_version: 1,
    pipeline_id: "p1",
    execution_id: "e1",
    status,
    message: "",
    created_at: "2026-09-25T14:58:00Z",
    updated_at: "2026-09-25T14:59:00Z",
    progress: { percent: 100, current_step: stages.length, total_steps: stages.length },
    execution_plan: { mode: "batch", stages },
    ...over,
  }
}

function jsonOk(body: unknown) {
  return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) }
}

function mountWith(state: Record<string, unknown>) {
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/state")) return jsonOk(state)
    if (u.includes("/events")) return jsonOk({ events: [] })
    return jsonOk({})
  })
  return render(<PipelineLiveStatePanel pipelineId="p1" />)
}

beforeEach(() => authFetch.mockReset())
afterEach(() => cleanup())

describe("the Overview folds a finished setup into one line", () => {
  it("replaces the stage list with a count, the summed time and a link to Steps/DAG", async () => {
    mountWith(liveState("completed", DONE))

    const line = await screen.findByTestId("setup-summary")
    // 489ms + 4.2s + 7.311s = 12,000ms, written by the formatter each stage uses.
    expect(line).toHaveTextContent("Setup completed: 3 steps, 12s in total")
    expect(screen.getByRole("link", { name: "See each step in Steps/DAG" })).toHaveAttribute(
      "href",
      "/pipelines/p1?tab=steps"
    )
    expect(screen.queryByText("Agentic pipeline stages")).not.toBeInTheDocument()
  })

  it("keeps the full list while a stage is still running", async () => {
    mountWith(
      liveState("running", [DONE[0], DONE[1], { id: "executor", display_name: "Running Pipeline", status: "running" }])
    )

    expect(await screen.findByText("Agentic pipeline stages")).toBeInTheDocument()
    expect(screen.queryByTestId("setup-summary")).not.toBeInTheDocument()
  })

  it("keeps the full list for a failed run, even when every stage it reached finished", async () => {
    mountWith(liveState("failed", DONE, { error_message: "sink rejected the batch" }))

    expect(await screen.findByText("Agentic pipeline stages")).toBeInTheDocument()
    expect(screen.queryByTestId("setup-summary")).not.toBeInTheDocument()
  })
})

function stage(status: StageExecution["status"], durationMs?: number): StageExecution {
  return { stage: "s", label: "S", description: "", icon: "", status, attempts: [], durationMs }
}

describe("finishedSetupSummary", () => {
  it("folds only when every stage completed and the run is not waiting, failed or stopped", () => {
    const done = [stage("completed", 100), stage("completed", 200)]
    expect(finishedSetupSummary(done, "completed")).toEqual({ steps: 2, durationMs: 300 })
    expect(finishedSetupSummary(done, "running")).toEqual({ steps: 2, durationMs: 300 })
    expect(finishedSetupSummary(done, "waiting_for_user")).toBeNull()
    expect(finishedSetupSummary(done, "failed")).toBeNull()
    expect(finishedSetupSummary(done, "cancelled")).toBeNull()
    expect(finishedSetupSummary([stage("completed", 100), stage("waiting")], "running")).toBeNull()
    expect(finishedSetupSummary([], "completed")).toBeNull()
  })

  it("prints no time when no stage was measured, rather than 0s", () => {
    expect(finishedSetupSummary([stage("completed"), stage("completed", 0)], "completed")).toEqual({
      steps: 2,
      durationMs: null,
    })
  })
})
