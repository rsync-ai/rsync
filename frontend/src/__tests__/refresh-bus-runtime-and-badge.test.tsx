/**
 * #13: Pause / Resume / Edit tables / Re-snapshot announce themselves on the
 * pipeline refresh bus (`emitPipelineRefresh`). The status pill and the
 * runtime hook must re-read on it, or a paused pipeline keeps reading
 * "Running" / "Streaming" until the next poll — or forever, once a terminal
 * phase has stopped the runtime poll.
 *
 * Pinned here:
 *  - the badge re-reads /state (and the hook /runtime) on THIS pipeline's
 *    refresh, without waiting for a poll; another pipeline's refresh is ignored;
 *  - a refresh re-arms a runtime poll that a terminal phase stopped;
 *  - but not one a 404 stopped (the endpoint does not exist on that backend).
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))

import { PipelineExecutionStatusBadge } from "@/components/pipeline/PipelineExecutionStatusBadge"
import { usePipelineRuntime } from "@/lib/hooks/usePipelineRuntime"
import { emitPipelineRefresh } from "@/lib/events/pipelineRefresh"

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

function runtime(phase: string, mode: "batch" | "cdc" = "cdc") {
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    mode,
    phase,
    health: "healthy",
    message: "",
    dependencies: [],
    updated_at: "2026-09-24T10:00:00Z",
  }
}

const gets = (suffix: string) => authFetch.mock.calls.filter(([u]) => String(u).endsWith(`/pipelines/p1${suffix}`)).length

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
})
afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

describe("status badge on the refresh bus", () => {
  it("flips to Paused on this pipeline's refresh, before any poll, and re-reads /runtime too", async () => {
    let state = "running"
    authFetch.mockImplementation(async (url: string) => {
      if (url.endsWith("/runtime")) return res(200, runtime("streaming"))
      if (url.endsWith("/state")) return res(200, { status: state })
      return res(404, {})
    })
    render(<PipelineExecutionStatusBadge pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText("Running")).toBeInTheDocument())
    await waitFor(() => expect(gets("/runtime")).toBe(1))
    const stateBefore = gets("/state")
    const runtimeBefore = gets("/runtime")

    state = "paused"

    // Control: another pipeline's refresh changes nothing.
    act(() => emitPipelineRefresh("p2"))
    expect(gets("/state")).toBe(stateBefore)
    expect(gets("/runtime")).toBe(runtimeBefore)
    expect(screen.getByText("Running")).toBeInTheDocument()

    act(() => emitPipelineRefresh("p1"))
    // Well inside the 4 s badge poll and the 5 s runtime poll: only the
    // refresh can have caused these reads.
    await waitFor(() => expect(screen.getByText("Paused")).toBeInTheDocument(), { timeout: 1000 })
    expect(gets("/state")).toBe(stateBefore + 1)
    expect(gets("/runtime")).toBe(runtimeBefore + 1)
  })
})

describe("runtime hook re-arms on refresh", () => {
  it("re-arms a poll a terminal phase stopped", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    let phase = "completed"
    authFetch.mockImplementation(async () => res(200, runtime(phase, "batch")))
    render(<PhaseProbe pollMs={5000} />)
    await tick(0)
    expect(screen.getByTestId("phase")).toHaveTextContent("completed")
    const stopped = gets("/runtime")

    // "completed" ends the poll: two more intervals read nothing.
    await tick(10_000)
    expect(gets("/runtime")).toBe(stopped)

    // A new run starts (Resume / Re-snapshot) and announces itself.
    phase = "syncing"
    act(() => emitPipelineRefresh("p1"))
    await tick(0)
    expect(screen.getByTestId("phase")).toHaveTextContent("syncing")
    expect(gets("/runtime")).toBe(stopped + 1)

    // …and the poll is live again, not just that one read.
    await tick(5_000)
    expect(gets("/runtime")).toBe(stopped + 2)
  })

  it("control: a poll a 404 stopped stays stopped, refresh or not", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    authFetch.mockImplementation(async () => res(404, {}))
    render(<PhaseProbe pollMs={5000} />)
    await tick(0)
    expect(gets("/runtime")).toBe(1)

    act(() => emitPipelineRefresh("p1"))
    await tick(10_000)
    expect(gets("/runtime")).toBe(1)
    expect(screen.getByTestId("phase")).toHaveTextContent("none")
  })
})
