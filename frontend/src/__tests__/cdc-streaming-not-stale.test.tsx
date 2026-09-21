/**
 * Issue #7 — a healthy CDC stream with no new changes showed "Stale" and a red Stop button.
 *
 * api-gateway's /state now marks a CDC pipeline past the streaming hand-off with
 * `streaming: true` and takes its `is_stale` from stream liveness (a quiet, healthy stream
 * is not stale; a backlog that is not draining, an unhealthy dependency, or a stream that
 * never delivered still is). It never recommends cancelling a stream. The UI must:
 *
 *   - show no destructive "Stale" badge and no destructive "Stop Pipeline" /
 *     "Cancel Pipeline" in the stall banner for a stream,
 *   - still show a real stream stall's reason, so nothing is hidden,
 *   - keep the ordinary header Stop / Cancel, and
 *   - leave batch pipelines exactly as before (the controls below).
 *
 * Payloads follow api-gateway's PipelineState JSON (pipeline_state.go).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, waitFor, cleanup } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

// The panel's separate /runtime probe is not what these tests are about.
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: null, loading: false, error: null }),
}))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
  authFetchOrThrow: (...args: unknown[]) => authFetch(...args),
}))

import { PipelineLiveStatePanel } from "@/components/pipeline/PipelineLiveStatePanel"
import { PipelineAccordionView, type PipelineState } from "@/components/chat/PipelineAccordionView"

const TEN_MINUTES_AGO = () => new Date(Date.now() - 10 * 60 * 1000).toISOString()

const STREAM_STALL_REASON =
  "42 changes are waiting to be written, but nothing has reached the destination for 20m 0s. Check the pipeline's health details to see what is wrong."
const BATCH_STALL_REASON = "No update for 10m 0s (expected heartbeat every 180s)"

/** A /state body for a run sitting in the executor stage with a heartbeat that stopped. */
function stateBody(over: Record<string, unknown> = {}) {
  return {
    schema_version: 1,
    pipeline_id: "p1",
    execution_id: "e1",
    status: "processing",
    current_stage: "executor",
    stage_group: "executing",
    message: "Pipeline executed",
    last_heartbeat_at: TEN_MINUTES_AGO(),
    created_at: "2026-01-01T00:00:00Z",
    updated_at: TEN_MINUTES_AGO(),
    progress: { percent: 90, current_step: 5, total_steps: 6 },
    execution_plan: { mode: "cdc", stages: [] },
    ...over,
  }
}

function jsonOk(body: unknown) {
  return {
    ok: true,
    status: 200,
    json: async () => body,
    text: async () => JSON.stringify(body),
  }
}

function mountPanel(state: Record<string, unknown>) {
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/state")) return jsonOk(state)
    if (u.includes("/events")) return jsonOk({ events: [] })
    return jsonOk({})
  })
  return render(<PipelineLiveStatePanel pipelineId="p1" />)
}

/** The header's ordinary Stop, which appears once a processing /state has loaded. */
async function headerStop() {
  return waitFor(() => screen.getByRole("button", { name: "Stop" }))
}

beforeEach(() => authFetch.mockReset())
afterEach(() => cleanup())

describe("PipelineLiveStatePanel — streaming CDC", () => {
  it("a quiet healthy stream shows no Stale badge and no destructive Stop", async () => {
    mountPanel(stateBody({ streaming: true, is_stale: false, cancel_recommended: false }))

    // The state loaded (the ordinary stop is there) ...
    expect(await headerStop()).toBeInTheDocument()
    // ... and nothing calls the stream stuck.
    expect(screen.queryByText("Stale")).not.toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "Stop Pipeline" })).not.toBeInTheDocument()
    expect(screen.queryByText(/looks stuck|taking longer than expected|may not be delivering/i)).not.toBeInTheDocument()
  })

  it("a real stream stall still shows its reason, without the Stale badge or destructive Stop", async () => {
    mountPanel(
      stateBody({
        streaming: true,
        is_stale: true,
        stale_reason: STREAM_STALL_REASON,
        stale_elapsed_seconds: 1200,
        cancel_recommended: false,
      })
    )

    expect(await screen.findByText(new RegExp(STREAM_STALL_REASON.slice(0, 40)))).toBeInTheDocument()
    expect(screen.getByText("This stream may not be delivering changes.")).toBeInTheDocument()
    expect(screen.queryByText("Stale")).not.toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "Stop Pipeline" })).not.toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Stop" })).toBeInTheDocument()
    // The reason already says "for 20m 0s"; no raw seconds counter is appended.
    expect(screen.queryByText(/\(idle \d+s\)/)).not.toBeInTheDocument()
  })

  it("keeps the destructive Stop off a stream even if cancel_recommended arrives set", async () => {
    // The gateway never sets both today; the panel must not rely on that alone.
    mountPanel(
      stateBody({
        streaming: true,
        is_stale: true,
        stale_reason: STREAM_STALL_REASON,
        stale_elapsed_seconds: 1200,
        cancel_recommended: true,
      })
    )

    expect(await screen.findByText("This stream may not be delivering changes.")).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "Stop Pipeline" })).not.toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Stop" })).toBeInTheDocument()
  })
})

describe("PipelineLiveStatePanel — batch control (unchanged)", () => {
  it("a stale batch run shows the Stale badge and the destructive Stop", async () => {
    mountPanel(
      stateBody({
        execution_plan: { mode: "batch", stages: [] },
        is_stale: true,
        stale_reason: BATCH_STALL_REASON,
        stale_elapsed_seconds: 600,
        cancel_recommended: true,
      })
    )

    expect(await screen.findByText(new RegExp(BATCH_STALL_REASON.slice(0, 20)))).toBeInTheDocument()
    expect(screen.getByText("Stale")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Stop Pipeline" })).toBeInTheDocument()
    expect(screen.getByText("Data sync is taking longer than expected.")).toBeInTheDocument()
    expect(screen.getByText(/\(idle 600s\)/)).toBeInTheDocument()
  })

  // Whether a run is a stream is the gateway's call (`streaming`), not the plan's mode: a
  // CDC pipeline still in its initial load has a CDC plan, a heartbeat, and a real stall.
  it.each([
    ["streaming omitted", {}],
    ["streaming false", { streaming: false }],
  ])("a CDC-plan run in its initial load (%s) keeps the Stale badge and destructive Stop", async (_label, flag) => {
    mountPanel(
      stateBody({
        execution_plan: { mode: "cdc", stages: [] },
        ...flag,
        is_stale: true,
        stale_reason: BATCH_STALL_REASON,
        stale_elapsed_seconds: 600,
        cancel_recommended: true,
      })
    )

    expect(await screen.findByText(new RegExp(BATCH_STALL_REASON.slice(0, 20)))).toBeInTheDocument()
    expect(screen.getByText("Stale")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Stop Pipeline" })).toBeInTheDocument()
    expect(screen.getByText("Data sync is taking longer than expected.")).toBeInTheDocument()
    expect(screen.queryByText("This stream may not be delivering changes.")).not.toBeInTheDocument()
  })
})

function accordionState(over: Partial<PipelineState> = {}): PipelineState {
  return {
    pipeline_id: "p1",
    status: "processing",
    current_stage: "executor",
    last_heartbeat_at: TEN_MINUTES_AGO(),
    execution_plan: { stages: [{ id: "executor", display_name: "Executor", status: "running" }] },
    progress: { percent: 90 },
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    ...over,
  }
}

describe("PipelineAccordionView — streaming CDC", () => {
  it("a stream stall shows its reason but offers no Cancel Pipeline in the banner", () => {
    const onCancel = vi.fn()
    render(
      <PipelineAccordionView
        state={accordionState({ streaming: true, is_stale: true, stale_reason: STREAM_STALL_REASON })}
        onCancel={onCancel}
        onRefresh={vi.fn()}
      />
    )

    expect(screen.getByText(STREAM_STALL_REASON)).toBeInTheDocument()
    expect(screen.getByText("Stream May Not Be Delivering Changes")).toBeInTheDocument()
    expect(screen.queryByText("Pipeline May Be Stuck")).not.toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "Cancel Pipeline" })).not.toBeInTheDocument()
    // The ordinary header Cancel stays.
    expect(screen.getByRole("button", { name: "Cancel" })).toBeInTheDocument()
  })

  it("keeps Cancel Pipeline out of a stream's banner even if cancel_recommended arrives set", () => {
    render(
      <PipelineAccordionView
        state={accordionState({
          streaming: true,
          is_stale: true,
          stale_reason: STREAM_STALL_REASON,
          cancel_recommended: true,
          metadata: { sync_mode: "cdc" },
        })}
        onCancel={vi.fn()}
      />
    )

    expect(screen.getByText("Stream May Not Be Delivering Changes")).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "Cancel Pipeline" })).not.toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Cancel" })).toBeInTheDocument()
  })

  it("a quiet healthy stream shows no stall banner at all", () => {
    render(
      <PipelineAccordionView
        state={accordionState({ streaming: true, is_stale: false, metadata: { sync_mode: "cdc" } })}
        onCancel={vi.fn()}
      />
    )

    expect(screen.getByRole("button", { name: "Cancel" })).toBeInTheDocument()
    expect(screen.queryByText(/May Not Be Delivering Changes|May Be Stuck/)).not.toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "Cancel Pipeline" })).not.toBeInTheDocument()
  })
})

describe("PipelineAccordionView — batch control (unchanged)", () => {
  it("a stale batch run keeps the banner's Cancel Pipeline", () => {
    render(
      <PipelineAccordionView
        state={accordionState({ is_stale: true, stale_reason: BATCH_STALL_REASON, cancel_recommended: true })}
        onCancel={vi.fn()}
      />
    )

    expect(screen.getByText("Pipeline May Be Stuck")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Cancel Pipeline" })).toBeInTheDocument()
  })

  it("a batch run stale for under 5 minutes still offers Cancel Pipeline while processing", () => {
    render(
      <PipelineAccordionView
        state={accordionState({ is_stale: true, stale_reason: BATCH_STALL_REASON, cancel_recommended: false })}
        onCancel={vi.fn()}
      />
    )

    expect(screen.getByText("Pipeline May Be Stuck")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Cancel Pipeline" })).toBeInTheDocument()
  })

  // A CDC pipeline still in its initial load looks like CDC (metadata, stage names) but is
  // not a stream until the gateway says so; its stall keeps the batch wording and action.
  it.each([
    ["streaming omitted", {}],
    ["streaming false", { streaming: false }],
  ])("a CDC run in its initial load (%s) keeps Pipeline May Be Stuck and Cancel Pipeline", (_label, flag) => {
    render(
      <PipelineAccordionView
        state={accordionState({
          ...flag,
          metadata: { sync_mode: "cdc" },
          is_stale: true,
          stale_reason: BATCH_STALL_REASON,
          cancel_recommended: true,
        })}
        onCancel={vi.fn()}
      />
    )

    expect(screen.getByText("Pipeline May Be Stuck")).toBeInTheDocument()
    expect(screen.queryByText("Stream May Not Be Delivering Changes")).not.toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Cancel Pipeline" })).toBeInTheDocument()
  })
})
