import { describe, expect, it } from "vitest"

import {
  createInitialExplorerSteps,
  updateExplorerStep,
  type ExplorerRun,
} from "@/components/explorer/ExplorerStepTimeline"

// A step's `error`/`recovery` describe one specific failure and are meaningless
// once the step leaves "failed". updateExplorerStep used to merge with a bare
// spread, and a spread cannot remove a key -- so `{status: "success"}` overwrote
// the status and left the old error string on the step.
//
// That is reachable on every run after the first. executeQuery only calls
// startExplorationRun (which builds fresh steps) when `currentRun` is null:
//
//   if (!currentRun) { startExplorationRun(...) }   // explorer/page.tsx
//
// so a failed run followed by a corrected, successful one reuses the very same
// step objects. The timeline then rendered the error the user had just fixed, in
// red, underneath a green "success" step.

function runWithFailedExecute(message: string): ExplorerRun {
  const run: ExplorerRun = {
    id: "run-1",
    question: "orders by status",
    steps: createInitialExplorerSteps(),
    startedAt: new Date(0),
    status: "running",
  }
  return updateExplorerStep(run, "execute", {
    status: "failed",
    error: message,
    recovery: { action: "retry", description: "Fix the SQL and run again" },
  })
}

const executeStep = (run: ExplorerRun) => {
  const step = run.steps.find((s) => s.id === "execute")
  if (!step) throw new Error("execute step missing")
  return step
}

describe("updateExplorerStep stale-failure cleanup", () => {
  it("drops the previous error when the step recovers to success", () => {
    const failed = runWithFailedExecute("Table 'orders' doesn't exist")
    expect(executeStep(failed).error).toBe("Table 'orders' doesn't exist")

    // The second run: same step objects, only the status is written.
    const recovered = updateExplorerStep(failed, "execute", {
      status: "success",
      durationMs: 42,
    })

    const step = executeStep(recovered)
    expect(step.status).toBe("success")
    expect(step.error).toBeUndefined()
    expect(step.recovery).toBeUndefined()
    expect(step.durationMs).toBe(42)
  })

  it("drops the previous error for every non-failed status, not just success", () => {
    for (const status of ["pending", "running", "skipped", "waiting_hitl"] as const) {
      const step = executeStep(
        updateExplorerStep(runWithFailedExecute("boom"), "execute", { status })
      )
      expect(step.status).toBe(status)
      expect(step.error, `error survived a move to "${status}"`).toBeUndefined()
      expect(step.recovery, `recovery survived a move to "${status}"`).toBeUndefined()
    }
  })

  it("keeps the error while the step is still failed", () => {
    // A later update that does not change status must not wipe the failure --
    // durations and outputs land on failed steps too.
    const step = executeStep(
      updateExplorerStep(runWithFailedExecute("boom"), "execute", { durationMs: 7 })
    )
    expect(step.status).toBe("failed")
    expect(step.error).toBe("boom")
    expect(step.recovery).toBeDefined()
    expect(step.durationMs).toBe(7)
  })

  it("honours a new error supplied in the same update", () => {
    const step = executeStep(
      updateExplorerStep(runWithFailedExecute("first"), "execute", {
        status: "failed",
        error: "second",
      })
    )
    expect(step.error).toBe("second")
  })

  it("leaves other steps untouched", () => {
    const failed = runWithFailedExecute("boom")
    const withSafety = updateExplorerStep(failed, "safety_check", { status: "success" })

    // Updating a different step must not clear the execute step's failure.
    expect(executeStep(withSafety).error).toBe("boom")
    expect(withSafety.steps.find((s) => s.id === "safety_check")?.status).toBe("success")
  })
})
