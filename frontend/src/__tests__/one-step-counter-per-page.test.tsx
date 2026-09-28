/**
 * One step counter on the pipeline page (item 33).
 *
 * Bug class: the same fact rendered from two sources. The health header, above
 * every tab, printed runtime.progress ("78% · step 7/8"), whose total is a count
 * the executor hard-codes, while the Overview timeline and the Monitoring header
 * both count the run's own stages (stepInfoFromEvents → deriveStepInfo, pinned
 * by monitoring-header-matches-overview): "7/8" above "7/7" on one screen. The
 * header now keeps the percentage and leaves the step to the timeline.
 *
 * Controls: the percentage stays, and a completed run still reads 100 %.
 */
import { afterEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

import type { PipelineRuntime } from "@/lib/hooks/usePipelineRuntime"

let runtimeValue: PipelineRuntime | null = null
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: runtimeValue, loading: false, error: null }),
}))
vi.mock("@/components/pipeline/DiagnosePanel", () => ({ DiagnosePanel: () => null }))

import { PipelineHealthHeader } from "@/components/pipeline/PipelineHealthHeader"

const batch = (phase: PipelineRuntime["phase"]): PipelineRuntime => ({
  pipeline_id: "p1",
  mode: "batch",
  phase,
  health: "healthy",
  dependencies: [],
  updated_at: "2026-09-27T10:00:00Z",
  progress: { percent: 78, current_step: 7, total_steps: 8 },
})

afterEach(() => {
  cleanup()
  runtimeValue = null
})

describe("the health header leaves the step count to the timeline", () => {
  it("a running batch run shows its percentage, not a second step counter", () => {
    runtimeValue = batch("syncing")
    const { container } = render(<PipelineHealthHeader pipelineId="p1" />)
    expect(container).toHaveTextContent("78%")
    expect(container.textContent).not.toMatch(/step\s*\d+\s*\/\s*\d+/i)
  })

  it("control: a completed run reads 100 %, with no step fraction", () => {
    runtimeValue = batch("completed")
    const { container } = render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.getByText(/100%/)).toBeInTheDocument()
    expect(container.textContent).not.toMatch(/step\s*\d+\s*\/\s*\d+/i)
  })
})
