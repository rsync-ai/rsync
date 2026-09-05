/**
 * Regression tests for F-279 — one JSON key, two units, two writers.
 *
 * `actual_duration` is written in SECONDS by the temporal-adapter
 * (graph_converter.go, nl_pipeline_v2_workflow.go — the struct comment even
 * says "// Seconds") and in MILLISECONDS by the frontend's own StepsDAGTab
 * when it synthesises the infra-preflight stage. It is then read as
 * milliseconds by six consumers and as seconds by a seventh:
 *
 *   ms  — StageDetailPanel:260, DAGVisualization:694, DAGVisualizationV2:244
 *         (all via `formatDuration(ms)`), PipelineInsightsBar:41 and
 *         NodeInspector:89 (both `Math.round(x / 1000)` + "s")
 *   sec — PipelineAccordionView:1066 (`formatDurationSeconds`)
 *
 * So a stage that really took 42 s renders "42ms" in the DAG, "0s" in the
 * insights bar, and "42s" in the chat accordion — three answers, one row.
 *
 * The fix (the user's call) makes the backend emit `actual_duration_ms` and
 * routes every consumer through one helper. The legacy seconds field survives
 * because plan JSON already in the DB carries the old unit.
 */

import { describe, it, expect, vi } from "vitest"
import { render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

// StageDetailPanel calls `useRouter` for its "open the run" links; without this
// the render dies on "invariant expected app router to be mounted" and the
// assertions never run — a failure that says nothing about the unit bug.
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

import { stageDurationMs } from "@/components/pipeline/dagHelpers"
import { formatDuration, type ExecutionPlanStage } from "@/components/pipeline/DAGVisualization"
import { StageDetailPanel } from "@/components/pipeline/StageDetailPanel"

function stage(over: Partial<ExecutionPlanStage>): ExecutionPlanStage {
  return {
    id: "s1",
    display_name: "Extract",
    status: "complete",
    started_at: "2026-08-05T12:00:00Z",
    completed_at: "2026-08-05T12:00:42Z",
    ...over,
  }
}

describe("stageDurationMs — one unit, whatever the producer wrote", () => {
  it("prefers actual_duration_ms when the backend sends it", () => {
    expect(stageDurationMs(stage({ actual_duration_ms: 42500, actual_duration: 42 }))).toBe(42500)
  })

  it("converts a legacy seconds-only stage, so already-persisted plans still read right", () => {
    // This is the shape sitting in `execution_plans` today. Dropping it would
    // make every historical run show no duration at all.
    expect(stageDurationMs(stage({ actual_duration: 42 }))).toBe(42000)
  })

  it("keeps sub-second precision the seconds field could not carry", () => {
    // int(0.3s) == 0 in Go, and every consumer guards on `> 0`, so a 300 ms
    // stage used to vanish rather than read "0".
    expect(stageDurationMs(stage({ actual_duration_ms: 300 }))).toBe(300)
  })

  it("returns null — not 0 — when the stage was never timed", () => {
    // "no measurement" and "measured zero" must not be the same value, or the
    // caller cannot decide whether to render the row.
    expect(stageDurationMs(stage({}))).toBeNull()
  })
})

describe("StageDetailPanel renders the duration in the unit the backend meant", () => {
  it("THE REGRESSION: a 42-second backend stage reads as seconds, not milliseconds", () => {
    render(
      <StageDetailPanel
        stage={stage({ actual_duration: 42 })}
        allStages={[]}
        onClose={() => {}}
        onSelectStage={() => {}}
      />
    )

    // Before the fix this rendered "42ms".
    expect(screen.getByText("42.0s")).toBeInTheDocument()
    expect(screen.queryByText("42ms")).not.toBeInTheDocument()
  })

  it("a sub-second stage is shown at all", () => {
    render(
      <StageDetailPanel
        stage={stage({ actual_duration_ms: 300 })}
        allStages={[]}
        onClose={() => {}}
        onSelectStage={() => {}}
      />
    )

    expect(screen.getByText("300ms")).toBeInTheDocument()
  })

  it("positive control: an untimed stage renders no Duration row", () => {
    // Without this, the two assertions above could pass on a panel that
    // rendered a duration unconditionally.
    render(
      <StageDetailPanel
        stage={stage({ started_at: undefined, completed_at: undefined })}
        allStages={[]}
        onClose={() => {}}
        onSelectStage={() => {}}
      />
    )

    expect(screen.queryByText("Duration")).not.toBeInTheDocument()
  })
})

describe("formatDuration is unchanged — it was always a milliseconds formatter", () => {
  it("formats the boundary cases the same as before", () => {
    expect(formatDuration(300)).toBe("300ms")
    expect(formatDuration(42500)).toBe("42.5s")
    expect(formatDuration(180000)).toBe("3m")
  })
})
