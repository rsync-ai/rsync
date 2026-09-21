/**
 * Prod retest 2026-09-18: one stage of one run read "Checking Capabilities · 0s"
 * on the Overview and "Resolving Connectors · 184ms" in the Steps graph.
 *
 * The graph names a stage by the plan's display_name (execution_plan_builder.go)
 * and times it by the adapter's actual_duration_ms. The Overview named it from its
 * own label map and floored an event-timestamp delta to whole seconds. It now reads
 * both from the plan when the plan has the stage, and writes durations with the
 * graph's formatter.
 */
import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, it, expect, vi } from "vitest"
import { render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

import { buildAgenticStagesFromEvents } from "@/components/pipeline/PipelineLiveStatePanel"
import { StageTimeline } from "@/components/pipeline/StageTimeline"
import { formatDuration, stageDurationMs } from "@/components/pipeline/dagHelpers"
import { stageLabel } from "@/lib/pipeline/stageDefinitions"

type Ev = Parameters<typeof buildAgenticStagesFromEvents>[0][number]
type State = NonNullable<Parameters<typeof buildAgenticStagesFromEvents>[1]>
type PlanStage = NonNullable<NonNullable<State["execution_plan"]>["stages"]>[number]

let seq = 0
function ev(stage: string, type: string, at: string): Ev {
  seq += 1
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    event_id: `ev-${seq}`,
    event_type: type,
    stage_id: stage,
    occurred_at: at,
  }
}

function stateWith(stages: PlanStage[]): State {
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    status: "running",
    created_at: "2026-09-18T10:00:00Z",
    execution_plan: { stages },
  } as unknown as State
}

describe("stage names — the frontend map says what the adapter's plan says", () => {
  // Runs with no plan yet (or a plan without the stage) fall back to stageLabel,
  // so the map itself has to agree with the builder, not just the plan path.
  const goPath = resolve(__dirname, "../../../backend-temporal-adapter/internal/workflows/execution_plan_builder.go")
  const pairs = Array.from(
    readFileSync(goPath, "utf8").matchAll(/ID:\s*"([a-z_]+)",\s*\n\s*DisplayName:\s*"([^"]+)"/g),
    (m) => [m[1], m[2]] as const
  )

  it("reads every stage out of the builder", () => {
    // A regex that stops matching must not pass as "no mismatches".
    expect(pairs.length).toBeGreaterThanOrEqual(8)
    expect(pairs.map(([id]) => id)).toContain("capability_resolver")
  })

  it.each(pairs)("%s is %s", (id, displayName) => {
    expect(stageLabel(id)).toBe(displayName)
  })
})

describe("Overview stage vs Steps graph node — same name, same duration", () => {
  // The prod case: a fast stage whose STARTED and COMPLETED carry the same
  // timestamp, so the event delta is 0 while the adapter measured 184ms.
  const planStage: PlanStage = {
    id: "capability_resolver",
    display_name: "Resolving Connectors",
    status: "complete",
    actual_duration_ms: 184,
  }
  const events = [
    ev("capability_resolver", "STAGE_STARTED", "2026-09-18T10:02:00Z"),
    ev("capability_resolver", "STAGE_COMPLETED", "2026-09-18T10:02:00Z"),
  ]

  it("takes the plan's display_name and the adapter's measured ms", () => {
    const stage = buildAgenticStagesFromEvents(events, stateWith([planStage])).find((s) => s.stage === "capability_resolver")!
    expect(stage.label).toBe(planStage.display_name)
    expect(stage.durationMs).toBe(stageDurationMs(planStage))
    expect(formatDuration(stage.durationMs!)).toBe("184ms")
  })

  it("renders 184ms on the Overview, not 0s", () => {
    const stages = buildAgenticStagesFromEvents(events, stateWith([planStage])).filter((s) => s.stage === "capability_resolver")
    render(<StageTimeline stages={stages} />)
    // The icon shares the label's span.
    expect(screen.getByText(/Resolving Connectors$/)).toBeInTheDocument()
    expect(screen.getByText("184ms")).toBeInTheDocument()
    expect(screen.queryByText("0s")).not.toBeInTheDocument()
    expect(screen.queryByText(/Checking Capabilities/)).not.toBeInTheDocument()
  })

  it("follows the plan when the plan renames a stage", () => {
    const renamed: PlanStage = { ...planStage, display_name: "Finding Connectors", description: "Looking up connectors" }
    const stage = buildAgenticStagesFromEvents(events, stateWith([renamed])).find((s) => s.stage === "capability_resolver")!
    expect(stage.label).toBe("Finding Connectors")
    expect(stage.description).toBe("Looking up connectors")
  })

  it("reads a legacy seconds-only plan the way the graph does", () => {
    const legacy: PlanStage = { id: "planner", display_name: "Creating Plan", status: "complete", actual_duration: 42 }
    const stage = buildAgenticStagesFromEvents(
      [ev("planner", "STAGE_STARTED", "2026-09-18T10:05:00Z"), ev("planner", "STAGE_COMPLETED", "2026-09-18T10:05:40Z")],
      stateWith([legacy])
    ).find((s) => s.stage === "planner")!
    expect(stage.durationMs).toBe(42_000)
  })

  it("times a stage the plan does not carry (Infra Preflight) from its events", () => {
    const stage = buildAgenticStagesFromEvents(
      [
        ev("infra_preflight", "STAGE_STARTED", "2026-09-18T10:07:00Z"),
        ev("infra_preflight", "STAGE_COMPLETED", "2026-09-18T10:07:03Z"),
      ],
      stateWith([planStage])
    ).find((s) => s.stage === "infra_preflight")!
    expect(stage.label).toBe("Infra Preflight")
    expect(stage.durationMs).toBe(3_000)
  })

  it("shows no duration for a stage that was never timed or measured zero", () => {
    const stages = buildAgenticStagesFromEvents(
      [ev("intent", "STAGE_STARTED", "2026-09-18T10:01:00Z"), ev("intent", "STAGE_COMPLETED", "2026-09-18T10:01:00Z")],
      stateWith([])
    ).filter((s) => s.stage === "intent")
    expect(stages[0].durationMs).toBe(0)
    render(<StageTimeline stages={stages} />)
    expect(screen.queryByText(/^0(ms|s|\.0s)$/)).not.toBeInTheDocument()
  })
})
