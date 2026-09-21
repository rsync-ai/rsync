/**
 * Prod retest 2026-09-19: "Comparison with previous run" for run 402b1e5e
 * compared it against 67b8ac8b, the pipeline id that a CDC pipeline's stream
 * stats carry as their execution id, and showed "— → —".
 */
import { describe, it, expect } from "vitest"
import { pickPreviousRun } from "@/components/executions/ExecutionComparison"

const PIPELINE = "67b8ac8b-0000-4000-8000-000000000001"

describe("the run a run is compared against", () => {
  it("skips the CDC stream id and runs still in flight", () => {
    const recent = [
      { execution_id: PIPELINE, status: "running" },
      { execution_id: "run-3", status: "running" },
      { execution_id: "run-2", status: "running" },
      { execution_id: "run-1", status: "completed" },
    ]
    expect(pickPreviousRun(recent, "run-3", PIPELINE)?.execution_id).toBe("run-1")
  })

  it("is older than this run, never newer", () => {
    const recent = [
      { execution_id: "run-3", status: "completed" },
      { execution_id: "run-2", status: "failed" },
      { execution_id: "run-1", status: "completed" },
    ]
    expect(pickPreviousRun(recent, "run-2", PIPELINE)?.execution_id).toBe("run-1")
  })

  it("is the newest finished run when this one is outside the window", () => {
    const recent = [
      { execution_id: PIPELINE, status: "running" },
      { execution_id: "run-9", status: "failed" },
    ]
    expect(pickPreviousRun(recent, "run-1", PIPELINE)?.execution_id).toBe("run-9")
  })

  it("control: nothing finished before it means no comparison", () => {
    const recent = [
      { execution_id: "run-2", status: "completed" },
      { execution_id: PIPELINE, status: "running" },
    ]
    expect(pickPreviousRun(recent, "run-2", PIPELINE)).toBeUndefined()
  })
})
