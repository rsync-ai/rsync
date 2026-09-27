import { describe, it, expect } from "vitest"
import {
  resolveElapsedBase,
  withInfraPreflight,
  type PipelineState,
} from "@/components/chat/PipelineAccordionView"

type Stages = NonNullable<PipelineState["execution_plan"]>["stages"]

const stages = (over: Partial<Stages[number]> = {}): Stages => [
  { id: "connection_validation", display_name: "Validate", order: 1, status: "complete" },
  { id: "executor", display_name: "Execute", order: 2, status: "pending", ...over },
]

const preflightOf = (out: Stages) => out.find((s) => s.id === "infra_preflight")!

describe("resolveElapsedBase", () => {
  it("returns null rather than now() when the stage has no start time", () => {
    // The bug: falling back to Date.now() measured time since MOUNT and the
    // caller rendered it as "<n> elapsed" for the stage.
    expect(resolveElapsedBase(undefined)).toBeNull()
    expect(resolveElapsedBase("")).toBeNull()
  })

  it("returns null for an unparseable timestamp", () => {
    expect(resolveElapsedBase("not-a-date")).toBeNull()
  })

  it("returns the real instant when one is known", () => {
    // Non-zero control: the fix must not blind the normal path.
    expect(resolveElapsedBase("2026-09-24T10:00:00.000Z")).toBe(
      Date.parse("2026-09-24T10:00:00.000Z")
    )
  })
})

describe("withInfraPreflight", () => {
  it("does not render a failed preflight as a running spinner", () => {
    // ExecutorWorker fails the task when preflight.Run errors, so the pipeline
    // is `failed` while current_stage stays pinned at infra_preflight.
    const out = withInfraPreflight(stages(), "infra_preflight", "failed", "kafka-connect never became healthy")
    expect(preflightOf(out).status).toBe("failed")
    expect(preflightOf(out).error_message).toBe("kafka-connect never became healthy")
  })

  it("still shows a live preflight as running", () => {
    // Non-zero control #1.
    expect(preflightOf(withInfraPreflight(stages(), "infra_preflight", "processing")).status).toBe("running")
  })

  it("still marks preflight complete once the executor has started", () => {
    // Non-zero control #2: the executor is only reached after preflight
    // succeeds, so this inference stays.
    expect(preflightOf(withInfraPreflight(stages(), "executor", "processing")).status).toBe("complete")
  })

  it("leaves preflight pending before it is reached", () => {
    // Non-zero control #3.
    expect(preflightOf(withInfraPreflight(stages(), "connection_validation", "processing")).status).toBe("pending")
  })
})
