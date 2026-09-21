/**
 * #42 — a live CDC stream read "Step 8/10" with two stages stuck "pending" forever.
 *
 * The timeline pads the event-derived stages with a fixed 10-stage baseline so an
 * early run does not read "Step 2/2". Two of those ten never run on a normal
 * pipeline (connector_generation when the connector exists, and one of the
 * connection_validation / connection_validator aliases), so they stayed pending
 * after the run was long past them. Stages before the furthest one the run reported
 * on are skipped and must leave the list; stages after it are still to come.
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
    occurred_at: `2026-09-18T10:${String(minute).padStart(2, "0")}:00Z`,
  }
}

function done(stage: string, minute: number): Ev[] {
  return [ev(stage, "STAGE_STARTED", minute), ev(stage, "STAGE_COMPLETED", minute)]
}

const state = (status: string): State =>
  ({ pipeline_id: "p1", execution_id: "e1", status, created_at: "2026-09-18T10:00:00Z" }) as unknown as State

describe("buildAgenticStagesFromEvents — skipped baseline stages", () => {
  it("drops never-reported stages the run is past, so a streaming run is not 8 of 10", () => {
    const events = [
      ...done("intent", 1),
      ...done("capability_resolver", 2),
      ...done("connector_check", 3),
      // connector_generation: connector already existed, never ran
      ...done("connection_validation", 4),
      // connection_validator: the alias this build does not emit
      ...done("planner", 5),
      ...done("validator", 6),
      ...done("infra_preflight", 7),
      ev("executor", "STAGE_STARTED", 8),
    ]
    const stages = buildAgenticStagesFromEvents(events, state("running"))
    const keys = stages.map((s) => s.stage)

    expect(keys).not.toContain("connector_generation")
    expect(keys).not.toContain("connection_validator")
    expect(keys).toHaveLength(8)
    expect(stages.filter((s) => s.status === "pending")).toEqual([])
  })

  it("keeps the stages still to come as pending on an early run", () => {
    const events = [...done("intent", 1), ev("capability_resolver", "STAGE_STARTED", 2)]
    const stages = buildAgenticStagesFromEvents(events, state("running"))
    const keys = stages.map((s) => s.stage)

    expect(keys[0]).toBe("intent")
    expect(keys).toContain("executor")
    expect(keys).toContain("connector_generation")
    expect(stages.find((s) => s.stage === "executor")?.status).toBe("pending")
  })
})
