/**
 * KI-LIVE-STATE-STAGES-COMPLETED-BY-POSITION — a stage is completed only on evidence.
 *
 * Bug class: status inferred from position. On a waiting or failed run,
 * inferLinearStatuses marked every stage before the active one completed at
 * 100 %, so a stage that STARTED but whose STAGE_COMPLETED was dropped painted
 * green — the panel reported success it never observed and could not show where
 * the run actually stopped.
 *
 * Pinned: that stage reads "unknown" with no progress and no invented finish
 * time. Controls: a stage with its own STAGE_COMPLETED, and one the execution
 * plan reports completed, still read completed; a completed run still completes
 * every stage (the run's success is the evidence).
 */
import { describe, it, expect, vi } from "vitest"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

import { buildAgenticStagesFromEvents } from "@/components/pipeline/PipelineLiveStatePanel"

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
    occurred_at: `2026-09-27T10:${String(minute).padStart(2, "0")}:00Z`,
  }
}
const done = (stage: string, minute: number): Ev[] => [ev(stage, "STAGE_STARTED", minute), ev(stage, "STAGE_COMPLETED", minute)]

function state(status: string, stage: string, planStages: Array<{ id: string; status: string }> = []): State {
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    status,
    current_stage: stage,
    progress: { stage },
    error_message: "boom",
    execution_plan: { stages: planStages },
    created_at: "2026-09-27T10:00:00Z",
  } as unknown as State
}

// capability_resolver started, then its STAGE_COMPLETED was lost.
const events = [
  ...done("intent", 1),
  ev("capability_resolver", "STAGE_STARTED", 2),
  ...done("connector_check", 3),
  ...done("planner", 5),
]

const byKey = (s: ReturnType<typeof buildAgenticStagesFromEvents>) => new Map(s.map((x) => [x.stage, x]))

describe.each([
  ["waiting_for_user", "waiting"],
  ["failed", "failed"],
])("%s run — no completion without evidence", (runStatus, activeStatus) => {
  it("a stage that started but never reported completing reads unknown, not green 100 %", () => {
    const stages = byKey(buildAgenticStagesFromEvents([...events, ev("validator", "STAGE_STARTED", 6)], state(runStatus, "validator")))
    const cr = stages.get("capability_resolver")!
    expect(cr.status).toBe("unknown")
    expect(cr.progress).toBeUndefined()
    expect(cr.completedAt).toBeUndefined()
    expect(stages.get("validator")!.status).toBe(activeStatus)
  })

  it("control: stages with their own STAGE_COMPLETED stay completed", () => {
    const stages = byKey(buildAgenticStagesFromEvents([...events, ev("validator", "STAGE_STARTED", 6)], state(runStatus, "validator")))
    expect(stages.get("intent")!.status).toBe("completed")
    expect(stages.get("planner")!.status).toBe("completed")
  })

  it("control: the execution plan saying completed is evidence too", () => {
    const stages = byKey(
      buildAgenticStagesFromEvents(
        [...events, ev("validator", "STAGE_STARTED", 6)],
        state(runStatus, "validator", [{ id: "capability_resolver", status: "completed" }]),
      ),
    )
    expect(stages.get("capability_resolver")!.status).toBe("completed")
  })
})

it("control: a completed run still completes every stage it went through", () => {
  const stages = byKey(buildAgenticStagesFromEvents(events, state("completed", "completed")))
  expect(stages.get("capability_resolver")!.status).toBe("completed")
})
