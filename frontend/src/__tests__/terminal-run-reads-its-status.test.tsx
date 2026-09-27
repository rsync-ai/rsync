/**
 * A finished run must read as what it ended as, on the pipeline page and on the
 * chat card.
 *
 * Prod 2026-09-26 (batch pipeline 93e1e11d, execution 65f0c413): the run went
 * through every stage, the workflow wrote current_stage 'completed' and percent
 * 100, and the post-run check then failed it. The pipeline page read "Failed
 * during: Completed" over a "Completed" stage row; the chat card showed a green
 * "Completed" badge. A run that did complete could read "Stopped during: <stage>",
 * "Processing pipeline..." and "7/8 steps".
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

import {
  PipelineLiveStatePanel,
  buildAgenticStagesFromEvents,
  deriveStepInfo,
  terminalStageLine,
} from "@/components/pipeline/PipelineLiveStatePanel"
import { PipelineAccordionView, type PipelineState } from "@/components/chat/PipelineAccordionView"

type Ev = Parameters<typeof buildAgenticStagesFromEvents>[0][number]
type State = Parameters<typeof buildAgenticStagesFromEvents>[1]

let seq = 0
function ev(stage: string, type: string, minute: number): Ev {
  seq += 1
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    event_id: `ev-${seq}`,
    event_type: type,
    stage_id: stage,
    occurred_at: `2026-09-26T10:${String(minute).padStart(2, "0")}:00Z`,
  }
}

function done(stage: string, minute: number): Ev[] {
  return [ev(stage, "STAGE_STARTED", minute), ev(stage, "STAGE_COMPLETED", minute)]
}

// The run 65f0c413 went through: every stage finished, but the executor's
// STAGE_COMPLETED is missing (it lands after the terminal event or not at all),
// and PIPELINE_COMPLETED carries the pseudo-stage 'completed' as its stage_id.
const RAN_EVERY_STAGE: Ev[] = [
  ...done("intent", 1),
  ...done("capability_resolver", 2),
  ...done("connector_check", 3),
  ...done("connection_validation", 4),
  ...done("planner", 5),
  ...done("validator", 6),
  ...done("infra_preflight", 7),
  ev("executor", "STAGE_STARTED", 8),
  ev("completed", "PIPELINE_COMPLETED", 9),
]

function state(status: string, currentStage: string, over: Record<string, unknown> = {}): State {
  return {
    schema_version: 1,
    pipeline_id: "p1",
    execution_id: "e1",
    status,
    current_stage: currentStage,
    message: "",
    created_at: "2026-09-26T10:00:00Z",
    updated_at: "2026-09-26T10:10:00Z",
    progress: { percent: 100, stage: currentStage, current_step: 7, total_steps: 8 },
    ...over,
  } as unknown as State
}

describe("a run the post-run check failed after every stage", () => {
  const s = state("failed", "completed", { message: "silent_drop_detected: public.subscriptions" })
  const stages = buildAgenticStagesFromEvents(RAN_EVERY_STAGE, s)

  it("has no row for the pseudo-stage, and every stage it ran is done", () => {
    expect(stages.map((x) => x.stage)).not.toContain("completed")
    expect(stages.filter((x) => x.status !== "completed")).toEqual([])
  })

  it("is on its last step, and says it failed after the steps, not during 'Completed'", () => {
    expect(deriveStepInfo(stages, "completed", "failed")).toMatchObject({
      current_step: stages.length,
      total_steps: stages.length,
    })
    expect(terminalStageLine("failed", stages, "completed", "Completed")).toBe("Failed after every step finished")
  })
})

describe("a completed run", () => {
  const stages = buildAgenticStagesFromEvents(RAN_EVERY_STAGE, state("completed", "completed"))

  it("is on its last step even when current_stage still names an earlier stage", () => {
    expect(deriveStepInfo(stages, "completed", "completed")).toMatchObject({
      current_step: stages.length,
      total_steps: stages.length,
    })
    expect(deriveStepInfo(stages, "validator", "completed")?.current_step).toBe(stages.length)
  })

  it("reads Completed, never 'Stopped during'", () => {
    expect(terminalStageLine("completed", stages, "validator", "Validator")).toBe("Completed")
  })
})

describe("the line still names the stage when there is one", () => {
  it("a stage that failed", () => {
    const events = [...done("intent", 1), ...done("planner", 2), ev("validator", "STAGE_STARTED", 3), ev("validator", "STAGE_FAILED", 4)]
    const stages = buildAgenticStagesFromEvents(events, state("failed", "validator"))
    const failed = stages.find((x) => x.stage === "validator")!
    expect(failed.status).toBe("failed")
    expect(terminalStageLine("failed", stages, "validator", failed.label)).toBe(`Failed during: ${failed.label}`)
    // The pseudo-stage cannot hide a stage that really failed.
    expect(terminalStageLine("failed", stages, "completed", "Completed")).toBe(`Failed during: ${failed.label}`)
    expect(deriveStepInfo(stages, "completed", "failed")?.current_step).toBe(
      stages.findIndex((x) => x.stage === "validator") + 1
    )
  })

  it("a stopped run: the stage it stopped in, or just Stopped", () => {
    const stages = buildAgenticStagesFromEvents([...done("intent", 1), ev("planner", "STAGE_STARTED", 2)], state("cancelled", "planner"))
    expect(terminalStageLine("cancelled", stages, "planner", "Planner")).toBe("Stopped during: Planner")
    expect(terminalStageLine("cancelled", stages, "cancelled", "Cancelled")).toBe("Stopped")
  })
})

// ---------------------------------------------------------------------------
// The pipeline page, rendered.
// ---------------------------------------------------------------------------

function jsonOk(body: unknown) {
  return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) }
}

function mountPanel(st: State, events: Ev[]) {
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/state")) return jsonOk(st)
    if (u.includes("/events")) return jsonOk({ events })
    return jsonOk({})
  })
  return render(<PipelineLiveStatePanel pipelineId="p1" />)
}

beforeEach(() => authFetch.mockReset())
afterEach(() => cleanup())

describe("PipelineLiveStatePanel — a run failed by the post-run check", () => {
  it("reads 'Failed after every step finished' with the reason below, and Step N/N", async () => {
    mountPanel(state("failed", "completed", { message: "silent_drop_detected: public.subscriptions" }), RAN_EVERY_STAGE)

    expect(await screen.findByText("Failed after every step finished")).toBeInTheDocument()
    expect(screen.getAllByText("silent_drop_detected: public.subscriptions").length).toBeGreaterThan(0)
    expect(screen.queryByText(/Failed during: Completed/)).not.toBeInTheDocument()
    expect(screen.getByText(/^Step (\d+)\/\1$/)).toBeInTheDocument()
  })
})

// ---------------------------------------------------------------------------
// The chat card.
// ---------------------------------------------------------------------------

function cardState(over: Partial<PipelineState>): PipelineState {
  return {
    pipeline_id: "p1",
    status: "processing",
    message: "",
    created_at: "2026-09-26T10:00:00Z",
    updated_at: "2026-09-26T10:10:00Z",
    ...over,
  } as PipelineState
}

describe("PipelineAccordionView — the terminal status is final", () => {
  it("a run failed after current_stage 'completed' and 100% stays Failed", () => {
    render(
      <PipelineAccordionView
        state={cardState({
          status: "failed",
          current_stage: "completed",
          progress: { percent: 100, current_step: 7, total_steps: 8 },
          error_message: "silent_drop_detected: public.subscriptions",
        })}
      />
    )
    expect(screen.getAllByText("Failed").length).toBeGreaterThan(0)
    expect(screen.queryByText("Completed")).not.toBeInTheDocument()
    expect(screen.getByText("Pipeline failed")).toBeInTheDocument()
    expect(screen.getByText("silent_drop_detected: public.subscriptions")).toBeInTheDocument()
  })

  it("a completed run with no message says so, on its last step", () => {
    render(
      <PipelineAccordionView
        state={cardState({ status: "completed", current_stage: "completed", progress: { percent: 100, current_step: 7, total_steps: 8 } })}
      />
    )
    expect(screen.getByText("Completed")).toBeInTheDocument()
    expect(screen.getByText("Pipeline completed")).toBeInTheDocument()
    expect(screen.getByText("8/8 steps")).toBeInTheDocument()
    expect(screen.queryByText("Processing pipeline...")).not.toBeInTheDocument()
    expect(screen.queryByText("7/8 steps")).not.toBeInTheDocument()
  })

  // Control: the lagging-status fallback still applies to a run that has not ended.
  it("a run still reporting 'processing' at current_stage 'completed' reads Completed", () => {
    render(
      <PipelineAccordionView
        state={cardState({ status: "processing", current_stage: "completed", progress: { percent: 100, current_step: 8, total_steps: 8 } })}
      />
    )
    expect(screen.getByText("Completed")).toBeInTheDocument()
  })
})
