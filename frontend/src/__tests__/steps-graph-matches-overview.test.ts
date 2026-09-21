/**
 * #41 prod retest: the Steps graph drew 9 nodes beside the Overview's "Step 8/8".
 *
 * The graph draws the execution plan (execution_plan_builder.go: 8 stages, one of
 * them connector_generation) plus an Infra Preflight node it adds from events. The
 * Overview builds its list from events and leaves out connector_generation when the
 * connector already exists (#42). So the graph had the one extra node the run
 * passed over. Both now drop the same stages.
 */
import { describe, it, expect, vi } from "vitest"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

import { buildAgenticStagesFromEvents } from "@/components/pipeline/PipelineLiveStatePanel"
import { withoutPassedOverStages } from "@/lib/pipeline/stageDefinitions"

type Ev = Parameters<typeof buildAgenticStagesFromEvents>[0][number]
type State = Parameters<typeof buildAgenticStagesFromEvents>[1]
type PlanStage = { id: string; status: string; dependencies: string[] }

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
const done = (stage: string, minute: number): Ev[] => [
  ev(stage, "STAGE_STARTED", minute),
  ev(stage, "STAGE_COMPLETED", minute),
]
const state = { pipeline_id: "p1", execution_id: "e1", status: "running", created_at: "2026-09-18T10:00:00Z" } as unknown as State

// The plan as the adapter builds it (execution_plan_builder.go), after a run that
// found its connector, with the Infra Preflight node the Steps tab splices in
// before the executor.
function plan(status: Record<string, string>): PlanStage[] {
  const s = (id: string, deps: string[]): PlanStage => ({ id, status: status[id] ?? "pending", dependencies: deps })
  return [
    s("intent", []),
    s("capability_resolver", ["intent"]),
    s("connector_check", ["capability_resolver"]),
    s("connector_generation", ["connector_check"]),
    s("connection_validation", ["connector_check"]),
    s("planner", ["connection_validation"]),
    s("validator", ["planner"]),
    s("infra_preflight", ["validator"]),
    s("executor", ["infra_preflight"]),
  ]
}

describe("Steps graph and Overview count the same stages (#41)", () => {
  it("a streaming run: both list 8, without Generating Connectors", () => {
    const events = [
      ...done("intent", 1),
      ...done("capability_resolver", 2),
      ...done("connector_check", 3),
      ...done("connection_validation", 4),
      ...done("planner", 5),
      ...done("validator", 6),
      ...done("infra_preflight", 7),
      ev("executor", "STAGE_STARTED", 8),
    ]
    const overview = buildAgenticStagesFromEvents(events, state).map((s) => s.stage)

    const complete = Object.fromEntries(
      ["intent", "capability_resolver", "connector_check", "connection_validation", "planner", "validator", "infra_preflight"].map(
        (id) => [id, "complete"]
      )
    )
    const graph = withoutPassedOverStages(plan({ ...complete, connector_generation: "skipped", executor: "running" }))

    expect(graph.map((s) => s.id)).toEqual(overview)
    expect(graph).toHaveLength(8)
  })

  it("keeps a stage still to come, as the Overview does on an early run", () => {
    const events = [...done("intent", 1), ev("capability_resolver", "STAGE_STARTED", 2)]
    const overview = buildAgenticStagesFromEvents(events, state).map((s) => s.stage)
    const graph = withoutPassedOverStages(plan({ intent: "complete", capability_resolver: "running" })).map((s) => s.id)

    expect(graph).toContain("connector_generation")
    expect(overview).toContain("connector_generation")
  })

  it("rewires a stage that hung off a dropped one to that stage's parents", () => {
    const stages = [
      { id: "connector_check", status: "complete", dependencies: [] },
      { id: "connector_generation", status: "pending", dependencies: ["connector_check"] },
      { id: "connection_validation", status: "complete", dependencies: ["connector_generation"] },
    ]
    const out = withoutPassedOverStages(stages)

    expect(out.map((s) => s.id)).toEqual(["connector_check", "connection_validation"])
    expect(out[1].dependencies).toEqual(["connector_check"])
  })

  it("leaves data-flow nodes and an untouched plan alone", () => {
    const dag = [
      { id: "source_1", status: "pending", dependencies: [] },
      { id: "transform_1", status: "complete", dependencies: ["source_1"] },
    ]
    expect(withoutPassedOverStages(dag)).toBe(dag)
  })
})
