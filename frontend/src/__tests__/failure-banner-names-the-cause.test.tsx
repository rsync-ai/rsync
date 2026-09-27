/**
 * A failed run's banner must not blame a dependency that is healthy.
 *
 * Prod 2026-09-26 (batch pipeline 93e1e11d, execution 65f0c413): the run failed its
 * post-run check, and the card read "Pipeline is not running — a required dependency is
 * unreachable … auto-restart within ~60 seconds" while every dependency showed Healthy.
 * /runtime reports phase "failed" for any failed run (and forces health to "unhealthy"
 * with it), so only an unhealthy dependency row may produce the dependency banner.
 */

import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, cleanup } from "@testing-library/react"
import "@testing-library/jest-dom"
import type { PipelineRuntime, RuntimeDep } from "@/lib/hooks/usePipelineRuntime"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

let runtime: PipelineRuntime | null = null
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime, loading: false, error: null }),
}))

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async () => ({ ok: true, status: 200, json: async () => ({}), text: async () => "{}" })),
  authFetchOrThrow: vi.fn(async () => ({ ok: true, status: 200, json: async () => ({}), text: async () => "{}" })),
}))

import { PipelineAccordionView, type PipelineState } from "@/components/chat/PipelineAccordionView"

const DEPENDENCY_BANNER = /a required dependency is unreachable/i
const POSTFLIGHT_ERROR =
  "silent_drop_detected: public.subscriptions: selected but never reported (no stats row)"

function dep(status: RuntimeDep["status"]): RuntimeDep {
  return { kind: "mcp_dest", identifier: "gcs@v1.0.0", status }
}

function failedRuntime(deps: RuntimeDep[]): PipelineRuntime {
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    mode: "batch",
    phase: "failed",
    health: "unhealthy", // the gateway forces this for every failed run
    dependencies: deps,
    updated_at: new Date().toISOString(),
  }
}

function failedState(): PipelineState {
  return {
    pipeline_id: "p1",
    status: "failed",
    current_stage: "executor",
    error_message: POSTFLIGHT_ERROR,
    execution_plan: { stages: [{ id: "executor", display_name: "Executor", status: "complete" }] },
    progress: { percent: 100 },
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }
}

afterEach(() => {
  cleanup()
  runtime = null
})

describe("PipelineAccordionView — failure banner", () => {
  it("a failed run with healthy dependencies shows its own reason, not a dependency outage", () => {
    runtime = failedRuntime([dep("healthy"), dep("healthy")])
    render(<PipelineAccordionView state={failedState()} />)

    expect(screen.queryByText(DEPENDENCY_BANNER)).not.toBeInTheDocument()
    expect(screen.getByText("Pipeline Failed")).toBeInTheDocument()
    expect(screen.getByText(POSTFLIGHT_ERROR)).toBeInTheDocument()
  })

  it("a failed run with no dependency rows does not claim a dependency is down", () => {
    runtime = failedRuntime([])
    render(<PipelineAccordionView state={failedState()} />)

    expect(screen.queryByText(DEPENDENCY_BANNER)).not.toBeInTheDocument()
  })

  // Control: the banner still fires for what it was built for.
  it("a failed run with an unhealthy dependency keeps the dependency banner", () => {
    runtime = failedRuntime([dep("healthy"), dep("unhealthy")])
    render(<PipelineAccordionView state={failedState()} />)

    expect(screen.getByText(DEPENDENCY_BANNER)).toBeInTheDocument()
  })
})
