/**
 * Prod retest 2026-09-19 on a CDC pipeline: the Monitoring header said
 * "Step 1/1" and "trace 67b8ac8b" (the pipeline id) beside an Overview that said
 * "Step 2/2" for run 402b1e5e, whose events carry trace 402b1e5e.
 *
 * - The header read state.progress; it now reads the Overview's own timeline.
 * - The header took the newest row's trace, and the newest row of a CDC pipeline
 *   is a stream stat stamped with the pipeline id; it now takes the run's.
 */
import { describe, it, expect, vi } from "vitest"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  useSearchParams: () => new URLSearchParams(),
}))

import { deriveStepInfo, stepInfoFromEvents } from "@/components/pipeline/PipelineLiveStatePanel"
import { runTraceId } from "@/components/pipeline/PipelineMonitoringPanel"

type Ev = Parameters<typeof stepInfoFromEvents>[0][number]
type State = NonNullable<Parameters<typeof stepInfoFromEvents>[1]>

const PIPELINE = "67b8ac8b-0000-4000-8000-000000000001"
const RUN = "402b1e5e-0000-4000-8000-000000000002"

let seq = 0
function ev(stage: string, type: string, second: number, exec = RUN): Ev {
  seq += 1
  return {
    pipeline_id: PIPELINE,
    execution_id: exec,
    event_id: `ev-${seq}`,
    event_type: type,
    stage_id: stage,
    occurred_at: new Date(Date.UTC(2026, 8, 19, 12, 26, second)).toISOString(),
    payload: {},
  }
}

const runEvents: Ev[] = [
  ev("infra_preflight", "STAGE_STARTED", 10),
  ev("infra_preflight", "STAGE_COMPLETED", 20),
  ev("executor", "STAGE_STARTED", 25),
  ev("executor", "STAGE_COMPLETED", 59),
]

const state = {
  pipeline_id: PIPELINE,
  execution_id: RUN,
  status: "running",
  trace_id: undefined,
  progress: { current_step: 1, total_steps: 1, stage: "" },
} as unknown as State

describe("the Monitoring header's step", () => {
  it("counts the stages the Overview's timeline shows, not state.progress", () => {
    const info = stepInfoFromEvents(runEvents, state)
    expect(info).toMatchObject({ current_step: 2, total_steps: 2 })
  })

  it("ignores another execution's stages", () => {
    const other = [ev("planner", "STAGE_STARTED", 1, PIPELINE), ev("planner", "STAGE_COMPLETED", 2, PIPELINE)]
    expect(stepInfoFromEvents([...other, ...runEvents], state)).toMatchObject({ current_step: 2, total_steps: 2 })
  })

  it("control: no stages at all leaves the header to state.progress", () => {
    expect(stepInfoFromEvents([], { ...state, execution_plan: undefined } as State)).toBeNull()
    expect(deriveStepInfo([], "executor")).toBeNull()
  })
})

describe("the Monitoring header's trace", () => {
  const streamStat = { execution_id: PIPELINE, trace_id: PIPELINE }
  const runRow = { execution_id: RUN, trace_id: "402b1e5e9f00" }

  it("is the run's trace even when a stream stat is the newest row", () => {
    expect(runTraceId([streamStat, runRow], { execution_id: RUN }, PIPELINE)).toBe("402b1e5e9f00")
  })

  it("falls back to the state's trace, and never to the pipeline id", () => {
    expect(runTraceId([streamStat], { execution_id: RUN, trace_id: "abc" }, PIPELINE)).toBe("abc")
    expect(runTraceId([streamStat], { execution_id: RUN }, PIPELINE)).toBeUndefined()
  })
})
