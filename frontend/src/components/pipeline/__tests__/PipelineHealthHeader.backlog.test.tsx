/**
 * The header's "changes waiting" segment (liveness.pending_events).
 *
 * pending_events is captured-minus-applied (pipeline_runtime.go loadCDCLiveness).
 * It is the one number that tells a quiet CDC stream (nothing waiting) from a
 * stuck one (changes waiting, nothing written). These tests pin each state,
 * including the two where the header must say nothing: a batch run, and a zero
 * reported while health is not "healthy" (a degraded source freezes both
 * counters, so that zero can be false).
 */

import { afterEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

import type { PipelineRuntime } from "@/lib/hooks/usePipelineRuntime"

let runtimeValue: PipelineRuntime | null = null
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: runtimeValue, loading: false, error: null }),
}))
vi.mock("@/components/pipeline/DiagnosePanel", () => ({ DiagnosePanel: () => null }))

import { PipelineHealthHeader, backlogVital } from "@/components/pipeline/PipelineHealthHeader"

function cdc(over: Partial<PipelineRuntime> = {}, liveness?: PipelineRuntime["liveness"]): PipelineRuntime {
  return {
    pipeline_id: "p1",
    mode: "cdc",
    phase: "streaming",
    health: "healthy",
    dependencies: [],
    updated_at: "2026-09-18T10:00:00Z",
    liveness,
    ...over,
  }
}

afterEach(() => {
  cleanup()
  runtimeValue = null
})

describe("backlogVital", () => {
  it("healthy and nothing waiting reads caught up", () => {
    expect(backlogVital(cdc({}, { stale_seconds: 900, pending_events: 0 }))).toMatchObject({
      text: "caught up",
      tone: "ok",
    })
  })

  it("a small backlog on a live stream is neutral, with the exact count in the title", () => {
    const v = backlogVital(cdc({}, { stale_seconds: 20, pending_events: 1204 }))
    expect(v).toMatchObject({ text: "1.2K changes waiting", tone: "muted" })
    expect(v?.title).toContain("1,204 changes")
  })

  it("a backlog with no write for over 5 minutes is amber", () => {
    expect(backlogVital(cdc({ phase: "idle" }, { stale_seconds: 301, pending_events: 7 }))?.tone).toBe("warn")
  })

  it("uses the singular for one change", () => {
    expect(backlogVital(cdc({}, { stale_seconds: 5, pending_events: 1 }))?.text).toBe("1 change waiting")
  })

  it("says nothing for a zero while health is not healthy (the zero may be frozen)", () => {
    for (const health of ["degraded", "unhealthy", "unknown"] as const) {
      expect(backlogVital(cdc({ health }, { stale_seconds: 900, pending_events: 0 }))).toBeNull()
    }
  })

  it("still reports a real backlog while degraded", () => {
    expect(backlogVital(cdc({ health: "degraded" }, { stale_seconds: 900, pending_events: 3 }))?.tone).toBe("warn")
  })

  it("says nothing for batch, missing liveness, or an older gateway without the field", () => {
    expect(backlogVital(cdc({ mode: "batch" }, { pending_events: 5 }))).toBeNull()
    expect(backlogVital(cdc({}, undefined))).toBeNull()
    expect(backlogVital(cdc({}, { stale_seconds: 12 }))).toBeNull()
  })
})

describe("PipelineHealthHeader backlog segment", () => {
  it("renders next to the last-event vital", () => {
    runtimeValue = cdc({}, { stale_seconds: 12, pending_events: 37 })
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.getByText("last event 12s ago")).toBeInTheDocument()
    expect(screen.getByTestId("pipeline-backlog")).toHaveTextContent("37 changes waiting")
  })

  it("renders caught up for a quiet healthy stream", () => {
    runtimeValue = cdc({}, { stale_seconds: 720, pending_events: 0 })
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.getByText("last event 12m ago")).toBeInTheDocument()
    expect(screen.getByTestId("pipeline-backlog")).toHaveTextContent("caught up")
  })

  it("control: no segment when there is nothing honest to say", () => {
    runtimeValue = cdc({ health: "degraded" }, { stale_seconds: 720, pending_events: 0 })
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.getByText("last event 12m ago")).toBeInTheDocument()
    expect(screen.queryByTestId("pipeline-backlog")).toBeNull()
  })
})
