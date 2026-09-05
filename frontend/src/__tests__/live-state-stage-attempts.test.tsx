/**
 * KI-LIVESTATE-FABRICATED-STAGE-ATTEMPTS — what the timeline may claim about a
 * stage it was told nothing about.
 *
 * `stagesFromExecutionPlan` used to write a literal `currentAttempt: 1`,
 * `maxAttempts: 1` and `startedAt: startedAt ?? created_at ?? 0` onto every
 * plan-derived stage. Two things about that were worth fixing, and they are not
 * the two the issue was originally filed with:
 *
 *   - The invented `1/1` was *invisible* (StageTimeline only draws the badge
 *     above 1) but it displaced something real. `pipeline_progress` stores
 *     `stage_attempt` / `stage_max_attempts`, api-gateway serves them as the
 *     top-level `attempt` / `max_attempts` of the live state, and the builder
 *     ignored them -- so a stage genuinely on attempt 3 of 5 rendered as a first
 *     try. The badge was suppressed exactly when there was a retry to show.
 *
 *   - Those columns, and `message` / `error_message` beside them, describe the
 *     row's `current_stage` and nothing else. Attaching them to all stages
 *     would trade one wrong answer for a broader one, so this file pins the
 *     scope as hard as it pins the plumbing.
 *
 * The plan's own stage entries carry no attempt field, in the API type or in the
 * `ExecutionStage` struct behind it, so absence has to stay expressible: the
 * counters are optional and an unstarted stage keeps an unknown start time.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, waitFor, cleanup } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

// The panel's liveness probe polls on its own timer; it has nothing to do with
// stage attempts and an unmocked one would just add noise to every assertion.
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
  buildStagesFromExecutionPlan,
} from "@/components/pipeline/PipelineLiveStatePanel"

const PIPELINE_CREATED_AT = "2026-01-01T00:00:00Z"

type PlanStage = {
  id: string
  display_name?: string
  status?: string
  started_at?: string
  completed_at?: string
}

/**
 * A live-state payload shaped like the one api-gateway's PipelineState marshals:
 * top-level attempt counters (from the `stage_*` columns), and a plan whose own
 * stage entries carry none.
 */
function liveState(over: Record<string, unknown> = {}, stages: PlanStage[] = []) {
  return {
    schema_version: 1,
    pipeline_id: "p1",
    execution_id: "e1",
    status: "processing",
    message: "Working",
    created_at: PIPELINE_CREATED_AT,
    updated_at: PIPELINE_CREATED_AT,
    progress: { percent: 40, current_step: 2, total_steps: 4 },
    execution_plan: { mode: "batch", stages },
    ...over,
  }
}

/**
 * The stage label renders as "<icon> <label>" inside one span, so an exact-text
 * lookup for the label alone never matches. Matching the span's own text keeps
 * this independent of which icon the stage happens to carry.
 */
async function stageLabel(label: string): Promise<HTMLElement> {
  const found = await screen.findAllByText(
    (_content, node) => node?.tagName === "SPAN" && (node.textContent ?? "").trim().endsWith(` ${label}`)
  )
  return found[0] as HTMLElement
}

function jsonOk(body: unknown) {
  return {
    ok: true,
    status: 200,
    json: async () => body,
    text: async () => JSON.stringify(body),
  }
}

function mountWith(state: Record<string, unknown>) {
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/state")) return jsonOk(state)
    // No events: with an empty event stream the panel falls through to the
    // plan-derived builder, which is the code under test. Agentic stages built
    // from events already read the counters correctly and are not in scope here.
    if (u.includes("/events")) return jsonOk({ events: [] })
    return jsonOk({})
  })
  return render(<PipelineLiveStatePanel pipelineId="p1" />)
}

beforeEach(() => authFetch.mockReset())
afterEach(() => cleanup())

describe("a plan-derived stage reports the retry count it was given", () => {
  it("shows the real counters on the stage the backend is reporting on", async () => {
    mountWith(
      liveState({ current_stage: "transform", attempt: 3, max_attempts: 5 }, [
        { id: "extract", display_name: "Extract", status: "complete" },
        { id: "transform", display_name: "Transform", status: "running" },
      ])
    )

    // Before the fix this read "Retry 1/1" internally and therefore rendered
    // nothing at all: the badge is gated on `currentAttempt > 1`.
    expect(await screen.findByText("Retry 3/5")).toBeInTheDocument()
  })

  it("does not lend those counters to any other stage", async () => {
    mountWith(
      liveState({ current_stage: "transform", attempt: 3, max_attempts: 5 }, [
        { id: "extract", display_name: "Extract", status: "complete" },
        { id: "transform", display_name: "Transform", status: "running" },
        { id: "load", display_name: "Load", status: "pending" },
      ])
    )

    await screen.findByText("Retry 3/5")
    // One badge, on one stage. `stage_attempt` is a column of the row that names
    // `current_stage`; Extract and Load are not that stage.
    expect(screen.getAllByText(/^Retry /)).toHaveLength(1)
  })

  it("claims no retry at all when the backend reports no counters", async () => {
    mountWith(
      liveState({ current_stage: "transform" }, [
        { id: "extract", display_name: "Extract", status: "complete" },
        { id: "transform", display_name: "Transform", status: "running" },
      ])
    )

    await stageLabel("Transform")
    expect(screen.queryByText(/^Retry /)).not.toBeInTheDocument()
  })

  it("treats a zero or absent attempt as unknown rather than as attempt zero", async () => {
    // `attempt,omitempty` means the backend drops the field at 0; a 0 that does
    // arrive is likewise "never recorded", not a real attempt number.
    mountWith(
      liveState({ current_stage: "transform", attempt: 0, max_attempts: 0 }, [
        { id: "transform", display_name: "Transform", status: "running" },
      ])
    )

    await stageLabel("Transform")
    expect(screen.queryByText(/^Retry /)).not.toBeInTheDocument()
  })
})

describe("the built stage carries only what the plan and the live state knew", () => {
  // These read the builder's output directly. Nothing renders `attempts[].startedAt`,
  // so the DOM cannot tell a fabricated start time from an absent one -- a rendered
  // assertion here passed with the fabrication restored, which is why it is gone.

  it("leaves an unstarted stage's attempt with no start time at all", () => {
    const [extract, load] = buildStagesFromExecutionPlan(
      liveState({ current_stage: "extract" }, [
        { id: "extract", display_name: "Extract", status: "running", started_at: "2026-01-02T03:04:05Z" },
        { id: "load", display_name: "Load", status: "pending" },
      ]) as never
    )

    expect(extract.attempts[0].startedAt).toBe(Date.parse("2026-01-02T03:04:05Z"))
    // Was `startedAt ?? created_at ?? 0`: every not-yet-started stage claimed the
    // pipeline's creation as its own start, and a plan with no timestamps at all
    // claimed the epoch.
    expect(load.attempts[0].startedAt).toBeUndefined()
    expect(load.startedAt).toBeUndefined()
  })

  it("gives the counters to the current stage and to no other", () => {
    const [extract, transform, load] = buildStagesFromExecutionPlan(
      liveState({ current_stage: "transform", attempt: 3, max_attempts: 5 }, [
        { id: "extract", display_name: "Extract", status: "complete" },
        { id: "transform", display_name: "Transform", status: "running" },
        { id: "load", display_name: "Load", status: "pending" },
      ]) as never
    )

    expect(transform.currentAttempt).toBe(3)
    expect(transform.maxAttempts).toBe(5)
    expect(transform.attempts[0].attemptNumber).toBe(3)

    for (const other of [extract, load]) {
      expect(other.currentAttempt).toBeUndefined()
      expect(other.maxAttempts).toBeUndefined()
    }
  })

  it("reports no counters when the backend reported none", () => {
    const [only] = buildStagesFromExecutionPlan(
      liveState({ current_stage: "transform" }, [
        { id: "transform", display_name: "Transform", status: "running" },
      ]) as never
    )

    expect(only.currentAttempt).toBeUndefined()
    expect(only.maxAttempts).toBeUndefined()
    // The attempts array still needs one entry for the status text StageTimeline
    // draws from it; numbering that lone entry 1 is a fact about the array, not a
    // claim about retries.
    expect(only.attempts).toHaveLength(1)
    expect(only.attempts[0].attemptNumber).toBe(1)
  })

  it("treats a zero or negative count as never-recorded, not as attempt zero", () => {
    // `attempt,omitempty` drops the field at 0, so a 0 that does arrive came from
    // a NULL `stage_attempt` read as zero -- it is the absence of a measurement.
    // Passing it through would have the stage announce itself as attempt 0 of 0.
    for (const bad of [0, -1, Number.NaN]) {
      const [only] = buildStagesFromExecutionPlan(
        liveState({ current_stage: "transform", attempt: bad, max_attempts: bad }, [
          { id: "transform", display_name: "Transform", status: "running" },
        ]) as never
      )
      expect(only.currentAttempt).toBeUndefined()
      expect(only.maxAttempts).toBeUndefined()
      expect(only.attempts[0].attemptNumber).toBe(1)
    }
  })

  it("does not date a pending stage to the pipeline's creation", async () => {
    mountWith(
      liveState({ current_stage: "extract" }, [
        { id: "extract", display_name: "Extract", status: "running", started_at: "2026-01-02T03:04:05Z" },
        { id: "load", display_name: "Load", status: "pending" },
      ])
    )

    await stageLabel("Load")
    // The old fallback chain (`?? created_at ?? 0`) stamped every not-yet-started
    // stage with the pipeline's created_at. Rendering that date anywhere would
    // mean a stage that has not begun is claiming a start.
    const created = new Date(PIPELINE_CREATED_AT)
    for (const text of [created.toLocaleTimeString(), created.toLocaleString(), created.toLocaleDateString()]) {
      expect(screen.queryByText(new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")))).not.toBeInTheDocument()
    }
  })
})

describe("the pipeline-level error belongs to the stage it came from", () => {
  it("shows a failure message only on the stage the backend was reporting on", async () => {
    mountWith(
      liveState(
        {
          current_stage: "transform",
          status: "failed",
          error_message: "connection refused by upstream",
        },
        [
          { id: "extract", display_name: "Extract", status: "failed" },
          { id: "transform", display_name: "Transform", status: "failed" },
        ]
      )
    )

    await stageLabel("Transform")

    // Extract also failed, but the run was not on Extract when it did, so the
    // message from `pipeline_progress` is not its message. It falls back to
    // StageTimeline's own generic wording -- and that fallback appearing exactly
    // once is the discriminator: before the fix Extract borrowed the real text
    // and this count was zero.
    await waitFor(() => expect(screen.getAllByText("Stage failed")).toHaveLength(1))

    // The real message still reaches the reader: on the current stage's row, in
    // the panel's own error banner, and on the synthesized terminal marker.
    expect(screen.getAllByText(/connection refused by upstream/).length).toBeGreaterThan(0)
  })
})
