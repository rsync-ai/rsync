/**
 * Issues #19 and #20 from the 2026-09-16 MongoDB→GCS run.
 *
 *  #19 — the CDC chip said "Setting up connector…" while the run was still planning and
 *        while it waited on table selection. No connector exists in either phase.
 *  #20 — the chip said "Status unavailable" while the connector was RUNNING, because the
 *        orchestrator returned the Kafka Connect payload under result.status and never
 *        set result.connector_state.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, cleanup } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

let runtimeValue: Record<string, unknown> | null = null
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: runtimeValue, loading: false, error: null }),
}))

const getPipelineCDCStatus = vi.fn()
vi.mock("@/lib/api/pipelines", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api/pipelines")>()
  return {
    ...actual,
    getPipelineCDCStatus: (...args: unknown[]) => getPipelineCDCStatus(...args),
    restartPipelineCDC: vi.fn(),
  }
})

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async () => ({ ok: true, status: 200, json: async () => ({}), text: async () => "{}" })),
  authFetchOrThrow: vi.fn(async () => ({ ok: true, status: 200, json: async () => ({}), text: async () => "{}" })),
}))

import { PipelineAccordionView, type PipelineState } from "@/components/chat/PipelineAccordionView"
import { cdcConnectorState, cdcPreProvisionLabel } from "@/components/chat/cdcChipStatus"

const NOT_FOUND = { success: false, connector_name: "cdc-aaa0ded3", error: "not_found" }

function cdcState(over: Partial<PipelineState> = {}): PipelineState {
  return {
    pipeline_id: "aaa0ded3-0000-0000-0000-000000000001",
    status: "processing",
    current_stage: "planner",
    metadata: { sync_mode: "cdc" },
    execution_plan: { stages: [{ id: "planner", display_name: "Planner", status: "running" }] },
    progress: { percent: 10 },
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    ...over,
  } as PipelineState
}

describe("cdcPreProvisionLabel", () => {
  it("names table selection while the run waits on it", () => {
    expect(cdcPreProvisionLabel({ status: "waiting_for_user", blockingType: "table_selection", currentStage: "execution" }))
      .toBe("Waiting for table selection")
  })
  it("names any other HITL generically", () => {
    expect(cdcPreProvisionLabel({ status: "waiting_for_user", blockingType: "approval" })).toBe("Waiting for your input")
  })
  it("says planning during the planning stages", () => {
    expect(cdcPreProvisionLabel({ status: "processing", currentStage: "planner" })).toBe("Planning…")
    expect(cdcPreProvisionLabel({ status: "running", currentStage: "validator" })).toBe("Planning…")
    expect(cdcPreProvisionLabel({ status: "processing", stageGroup: "planning", currentStage: "x" })).toBe("Planning…")
  })
  it("steps aside once the executor runs, so a starting connector still reads as setting up", () => {
    expect(cdcPreProvisionLabel({ status: "processing", currentStage: "executor" })).toBeNull()
  })
  it("ignores a leftover blocker on a run that is no longer waiting", () => {
    expect(cdcPreProvisionLabel({ status: "completed", blockingType: "table_selection", currentStage: "planner" })).toBeNull()
    expect(cdcPreProvisionLabel({ status: "processing", blockingType: "table_selection", currentStage: "executor" })).toBeNull()
  })
})

describe("cdcConnectorState", () => {
  it("reads the orchestrator's flattened field", () => {
    expect(cdcConnectorState({ connector_state: "RUNNING" })).toBe("RUNNING")
  })
  it("falls back to the raw Kafka Connect payload from an older orchestrator", () => {
    expect(cdcConnectorState({ status: { name: "cdc-aaa0ded3", connector: { state: "RUNNING" }, tasks: [] } })).toBe("RUNNING")
  })
  it("is empty when there is nothing to read", () => {
    expect(cdcConnectorState(undefined)).toBe("")
    expect(cdcConnectorState({})).toBe("")
  })
})

describe("PipelineAccordionView CDC chip", () => {
  beforeEach(() => {
    getPipelineCDCStatus.mockReset()
    runtimeValue = null
  })
  afterEach(() => cleanup())

  it("#19: waiting on table selection does not claim a connector is being set up", async () => {
    getPipelineCDCStatus.mockResolvedValue(NOT_FOUND)
    render(
      <PipelineAccordionView
        state={cdcState({
          status: "waiting_for_user",
          current_stage: "execution",
          blocking_reason: { type: "table_selection", description: "Select tables to continue" },
        })}
      />
    )
    expect(await screen.findByTestId("cdc-chip-pre-provision")).toHaveTextContent("Waiting for table selection")
    await vi.waitFor(() => expect(getPipelineCDCStatus).toHaveBeenCalled())
    expect(screen.queryByText("Setting up connector…")).not.toBeInTheDocument()
  })

  it("#19: planning does not claim a connector is being set up", async () => {
    getPipelineCDCStatus.mockResolvedValue(NOT_FOUND)
    render(<PipelineAccordionView state={cdcState()} />)
    expect(await screen.findByTestId("cdc-chip-pre-provision")).toHaveTextContent("Planning…")
    expect(screen.queryByText("Setting up connector…")).not.toBeInTheDocument()
  })

  it("still says setting up once the executor is provisioning", async () => {
    getPipelineCDCStatus.mockResolvedValue(NOT_FOUND)
    render(
      <PipelineAccordionView
        state={cdcState({
          current_stage: "executor",
          execution_plan: { stages: [{ id: "executor", display_name: "Executor", status: "running" }] },
        })}
      />
    )
    expect(await screen.findByText("Setting up connector…")).toBeInTheDocument()
  })

  it("#20: a RUNNING connector shows its state, not 'Status unavailable'", async () => {
    getPipelineCDCStatus.mockResolvedValue({
      success: true,
      connector_name: "cdc-aaa0ded3",
      result: {
        connector_name: "cdc-aaa0ded3",
        connector_state: "RUNNING",
        healthy: true,
        tasks: [{ id: 0, state: "RUNNING" }],
        status: { connector: { state: "RUNNING" }, tasks: [{ id: 0, state: "RUNNING" }] },
      },
    })
    render(
      <PipelineAccordionView
        state={cdcState({
          status: "completed",
          current_stage: "executor",
          execution_plan: { stages: [{ id: "executor", display_name: "Executor", status: "complete" }] },
        })}
      />
    )
    expect(await screen.findByText(/RUNNING\s*\(healthy\)/)).toBeInTheDocument()
    expect(screen.queryByText("Status unavailable")).not.toBeInTheDocument()
  })
})

// /runtime reports waiting_for_data once a stream has been handed off and nothing has
// reached the destination past the grace period. The chat card did not know the phase:
// it hid a missing connector as "Setting up connector…", showed no banner and kept the
// "LIVE (streaming)" badge.
describe("PipelineAccordionView waiting for first data", () => {
  const WAITING_MESSAGE = "Streaming is set up, but no data has reached the destination yet"
  const RUNNING = {
    success: true,
    connector_name: "cdc-aaa0ded3",
    result: { connector_name: "cdc-aaa0ded3", connector_state: "RUNNING", healthy: true },
  }

  function runtime(phase: string) {
    return {
      pipeline_id: "aaa0ded3-0000-0000-0000-000000000001",
      execution_id: "e1",
      mode: "cdc",
      phase,
      health: "healthy",
      message: phase === "waiting_for_data" ? WAITING_MESSAGE : "Streaming pipeline active",
      dependencies: [],
      updated_at: "2026-09-16T10:00:00Z",
    }
  }

  // Past the handoff: /state says running and the stream started.
  function streamingState(over: Partial<PipelineState> = {}) {
    return cdcState({
      status: "running",
      current_stage: "executor",
      message: "Streaming pipeline started",
      execution_plan: { stages: [{ id: "executor", display_name: "Executor", status: "complete" }] },
      progress: { percent: 95 },
      ...over,
    })
  }

  beforeEach(() => {
    getPipelineCDCStatus.mockReset()
    runtimeValue = null
  })
  afterEach(() => cleanup())

  it("a missing connector is shown as an error, not as still being set up", async () => {
    runtimeValue = runtime("waiting_for_data")
    getPipelineCDCStatus.mockResolvedValue(NOT_FOUND)
    render(<PipelineAccordionView state={streamingState()} />)

    expect(await screen.findByText("not_found")).toBeInTheDocument()
    expect(screen.queryByText("Setting up connector…")).not.toBeInTheDocument()
  })

  it("control: a run still syncing its snapshot says setting up for the same not_found", async () => {
    runtimeValue = runtime("syncing")
    getPipelineCDCStatus.mockResolvedValue(NOT_FOUND)
    render(<PipelineAccordionView state={streamingState()} />)

    expect(await screen.findByText("Setting up connector…")).toBeInTheDocument()
    expect(screen.queryByText("not_found")).not.toBeInTheDocument()
  })

  it.each([
    ["running", {}],
    ["completed", { status: "completed", progress: { percent: 100 } }],
  ])("a %s run says Waiting for first data with the runtime's message, not LIVE", async (_label, over) => {
    runtimeValue = runtime("waiting_for_data")
    getPipelineCDCStatus.mockResolvedValue(RUNNING)
    render(<PipelineAccordionView state={streamingState(over as Partial<PipelineState>)} />)

    const banner = await screen.findByTestId("cdc-waiting-for-data-banner")
    expect(banner).toHaveTextContent("Waiting for first data")
    expect(banner).toHaveTextContent(`${WAITING_MESSAGE}.`)
    expect(screen.getAllByText("Waiting for first data")).toHaveLength(2) // badge + banner
    expect(screen.queryByText("LIVE (streaming)")).not.toBeInTheDocument()
    expect(screen.queryByText("CDC stream is live")).not.toBeInTheDocument()
  })

  it("control: a streaming phase keeps the live banner and badge", async () => {
    runtimeValue = runtime("streaming")
    getPipelineCDCStatus.mockResolvedValue(RUNNING)
    render(<PipelineAccordionView state={streamingState()} />)

    expect(await screen.findByText("CDC stream is live")).toBeInTheDocument()
    expect(screen.getByText("LIVE (streaming)")).toBeInTheDocument()
    expect(screen.queryByTestId("cdc-waiting-for-data-banner")).not.toBeInTheDocument()
    expect(screen.queryByText("Waiting for first data")).not.toBeInTheDocument()
  })

  it.each(["paused", "failed", "waiting_for_user"])(
    "a %s run is not relabelled by a waiting_for_data answer",
    async (status) => {
      runtimeValue = runtime("waiting_for_data")
      getPipelineCDCStatus.mockResolvedValue(RUNNING)
      render(<PipelineAccordionView state={streamingState({ status, progress: { percent: 60 } })} />)

      await vi.waitFor(() => expect(getPipelineCDCStatus).toHaveBeenCalled())
      expect(screen.queryByTestId("cdc-waiting-for-data-banner")).not.toBeInTheDocument()
      expect(screen.queryByText("Waiting for first data")).not.toBeInTheDocument()
    }
  )
})
