/**
 * RuntimePhase now includes waiting_for_data (issue #20 follow-up).
 *
 * The gateway has sent phase "waiting_for_data" since cdcLivenessPhase was added
 * (api-gateway pipeline_runtime.go), but the client union did not list it, so the
 * health header compared against it through a cast. Two things are pinned here:
 *
 *  1. Types — the phase is a RuntimePhase, the shared constant is assignable to it,
 *     and an invented phase is not. These lines are checked by `tsc --noEmit`
 *     (tsconfig includes test files); vitest strips types and does not.
 *
 *  2. Behaviour — waiting_for_data is not terminal. A CDC stream in it can start
 *     delivering on the next poll, so the hook must keep polling; a phase that
 *     stopped polling would leave the header on "Waiting for first data" forever.
 *     Controls: "completed" does stop, and so does a failed batch run.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))
// The header's diagnose panel is closed by default; keep its module out anyway.
vi.mock("@/components/pipeline/DiagnosePanel", () => ({ DiagnosePanel: () => null }))

import {
  RUNTIME_PHASE_POLLING,
  runtimePhaseEndsPolling,
  usePipelineRuntime,
  type PhasePolling,
  type RuntimePhase,
} from "@/lib/hooks/usePipelineRuntime"
import { PipelineHealthHeader } from "@/components/pipeline/PipelineHealthHeader"
import { RUNTIME_PHASE_WAITING_FOR_DATA, WAITING_FOR_FIRST_DATA_LABEL } from "@/lib/pipeline/statusNormalization"

// ---------------------------------------------------------------------------
// Type-level checks (tsc).
// ---------------------------------------------------------------------------
const wirePhase: RuntimePhase = "waiting_for_data"
const sharedConstant: RuntimePhase = RUNTIME_PHASE_WAITING_FOR_DATA
// @ts-expect-error — not a phase the gateway sends; the union must stay closed.
const invented: RuntimePhase = "waiting_for_godot"

// Every phase exactly once. Dropping a member from the union turns its key here
// into an excess-property error; adding one without a key is a missing-property error.
const EVERY_PHASE: Record<RuntimePhase, true> = {
  initializing: true,
  planning: true,
  validating: true,
  syncing: true,
  streaming: true,
  waiting_for_data: true,
  idle: true,
  completed: true,
  failed: true,
  paused: true,
}
const ALL_PHASES = Object.keys(EVERY_PHASE) as RuntimePhase[]

// The hook's own polling table must name a rule for every phase, and only for
// phases. Loosening its annotation leaves every runtime value the same, so only
// these two type checks notice:
//  - Partial<...>: an optional key is not assignable to a required one.
//  - Record<string, ...> or an index signature: that IS assignable to the Record
//    below (the index signature "has" every key), so the key set is compared too.
const POLLING_RULE_FOR_EVERY_PHASE: Record<RuntimePhase, PhasePolling> = RUNTIME_PHASE_POLLING
type SameKeys<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false
const POLLING_KEYS_ARE_EXACTLY_THE_PHASES: SameKeys<keyof typeof RUNTIME_PHASE_POLLING, RuntimePhase> = true

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

function runtime(phase: string, mode: "batch" | "cdc" = "cdc", over: Record<string, unknown> = {}) {
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    mode,
    phase,
    health: "healthy",
    message: phase === "waiting_for_data" ? "Streaming is set up, but no data has reached the destination yet" : "",
    dependencies: [],
    updated_at: "2026-09-16T10:00:00Z",
    ...over,
  }
}

function PhaseProbe({ pollMs }: { pollMs: number }) {
  const { runtime: r } = usePipelineRuntime("p1", { pollMs })
  return <span data-testid="phase">{r?.phase ?? "none"}</span>
}

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.useFakeTimers()
})
afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

describe("RuntimePhase type", () => {
  it("lists waiting_for_data and the shared constant is that wire value", () => {
    expect(wirePhase).toBe("waiting_for_data")
    expect(sharedConstant).toBe(wirePhase)
    expect(invented).toBe("waiting_for_godot")
    expect(ALL_PHASES).toContain("waiting_for_data")
    expect(ALL_PHASES).toHaveLength(10)
  })

  it("the hook's polling table has a rule for exactly the phases in the union", () => {
    expect(POLLING_KEYS_ARE_EXACTLY_THE_PHASES).toBe(true) // the real check is tsc, above
    expect(Object.keys(POLLING_RULE_FOR_EVERY_PHASE).sort()).toEqual([...ALL_PHASES].sort())
    expect(POLLING_RULE_FOR_EVERY_PHASE.waiting_for_data).toBe("keep")
  })
})

describe("runtimePhaseEndsPolling", () => {
  it("keeps polling on waiting_for_data in either mode", () => {
    expect(runtimePhaseEndsPolling("waiting_for_data", "cdc")).toBe(false)
    expect(runtimePhaseEndsPolling("waiting_for_data", "batch")).toBe(false)
  })

  it("stops only on completed, and on failed outside CDC (controls)", () => {
    const stopsCdc = ALL_PHASES.filter((p) => runtimePhaseEndsPolling(p, "cdc"))
    const stopsBatch = ALL_PHASES.filter((p) => runtimePhaseEndsPolling(p, "batch"))
    // Non-empty, or "never stops" would pass the waiting_for_data assertions above.
    expect(stopsCdc).toEqual(["completed"])
    expect(stopsBatch).toEqual(["completed", "failed"])
  })

  it("keeps polling on a phase this client does not know", () => {
    expect(runtimePhaseEndsPolling("some_future_phase", "batch")).toBe(false)
    // Inherited object keys are not phases.
    expect(runtimePhaseEndsPolling("constructor", "batch")).toBe(false)
    expect(runtimePhaseEndsPolling("toString", "cdc")).toBe(false)
  })
})

describe("usePipelineRuntime — polling across phases", () => {
  it("keeps asking while the stream waits for its first data", async () => {
    authFetch.mockResolvedValue(res(200, runtime("waiting_for_data")))
    render(<PhaseProbe pollMs={1000} />)
    await tick(0)
    expect(screen.getByTestId("phase")).toHaveTextContent("waiting_for_data")

    await tick(3000)
    expect(authFetch.mock.calls.length).toBeGreaterThanOrEqual(4)
  })

  it("control: stops asking once the run is completed", async () => {
    authFetch.mockResolvedValue(res(200, runtime("completed", "batch")))
    render(<PhaseProbe pollMs={1000} />)
    await tick(0)
    expect(screen.getByTestId("phase")).toHaveTextContent("completed")

    await tick(3000)
    expect(authFetch).toHaveBeenCalledTimes(1)
  })

  // The hook must pass the runtime's own mode through. For CDC, "failed" means a
  // dependency is unhealthy right now; if the hook stopped polling on it, the
  // header would stay on Failed after the stream recovered.
  it("keeps asking after a CDC stream reports failed, and shows the recovery", async () => {
    authFetch
      .mockResolvedValueOnce(res(200, runtime("failed", "cdc", { health: "unhealthy" })))
      .mockResolvedValue(res(200, runtime("streaming", "cdc")))
    render(<PhaseProbe pollMs={1000} />)
    await tick(0)
    expect(screen.getByTestId("phase")).toHaveTextContent("failed")

    await tick(3000)
    expect(authFetch.mock.calls.length).toBeGreaterThanOrEqual(2)
    expect(screen.getByTestId("phase")).toHaveTextContent("streaming")
  })

  it("control: a failed batch run stops asking", async () => {
    authFetch
      .mockResolvedValueOnce(res(200, runtime("failed", "batch", { health: "unhealthy" })))
      .mockResolvedValue(res(200, runtime("streaming", "batch")))
    render(<PhaseProbe pollMs={1000} />)
    await tick(0)
    expect(screen.getByTestId("phase")).toHaveTextContent("failed")

    await tick(3000)
    expect(authFetch).toHaveBeenCalledTimes(1)
    expect(screen.getByTestId("phase")).toHaveTextContent("failed")
  })
})

describe("PipelineHealthHeader — waiting_for_data end to end", () => {
  it("shows Waiting for first data, then Streaming when rows start arriving", async () => {
    authFetch
      .mockResolvedValueOnce(res(200, runtime("waiting_for_data")))
      .mockResolvedValue(
        res(200, runtime("streaming", "cdc", { liveness: { stale_seconds: 4 }, message: "Streaming pipeline active" })),
      )

    render(<PipelineHealthHeader pipelineId="p1" />)
    await tick(0)
    expect(screen.getByText(WAITING_FOR_FIRST_DATA_LABEL)).toBeInTheDocument()
    expect(screen.getByText(/no data has reached the destination yet/)).toBeInTheDocument()

    // Default poll interval is 5s.
    await tick(5000)
    expect(screen.getByText("Streaming")).toBeInTheDocument()
    expect(screen.queryByText(WAITING_FOR_FIRST_DATA_LABEL)).toBeNull()
    expect(screen.getByText("last event 4s ago")).toBeInTheDocument()
  })

  // The comparison that used to go through a cast decides this line when the
  // gateway sends no message.
  it("says no data has arrived when the gateway sends no message", async () => {
    authFetch.mockResolvedValue(res(200, runtime("waiting_for_data", "cdc", { message: "" })))
    render(<PipelineHealthHeader pipelineId="p1" />)
    await tick(0)
    expect(screen.getByText("no data has reached the destination yet")).toBeInTheDocument()
  })

  it("control: a streaming phase with no message still reads streaming", async () => {
    authFetch.mockResolvedValue(res(200, runtime("streaming", "cdc", { message: "" })))
    render(<PipelineHealthHeader pipelineId="p1" />)
    await tick(0)
    expect(screen.getByText("streaming")).toBeInTheDocument()
    expect(screen.queryByText("no data has reached the destination yet")).toBeNull()
  })
})
