/**
 * The page follows a status change in the database within one /state read, not
 * a 30 s poll later (item 35: the UI trailed the DB by ~20 s).
 *
 * Bug class: each panel keeps its own slow timer and nothing tells it the
 * pipeline moved. The shared /state poller sees the change first; it now
 * announces it on the refresh bus, so /runtime, the live-state and monitoring
 * panels and the Overview tab re-read at once. A paused or idle pipeline (which
 * a schedule or another tab can move on) is read every 10 s, not 30.
 *
 * Controls: no announcement while the status holds, the announcement does not
 * make the poller read itself again, and a finished pipeline stays at 30 s.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render } from "@testing-library/react"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))

import { SETTLED_POLL_MS, statePollMs, usePipelineStatePoll } from "@/lib/hooks/usePipelineStatePoll"
import { onPipelineRefresh, emitPipelineRefresh } from "@/lib/events/pipelineRefresh"
import { MonitoringOverviewTab } from "@/components/pipeline/MonitoringOverviewTab"

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

let stateNow = "processing"
const count = (suffix: string) => authFetch.mock.calls.filter(([u]) => String(u).includes(suffix)).length

function Reader({ id }: { id: string }) {
  usePipelineStatePoll(id)
  return null
}

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.useFakeTimers()
  stateNow = "processing"
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.endsWith("/state")) return res(200, { status: stateNow })
    if (u.includes("/runtime"))
      return res(200, { pipeline_id: "p35", mode: "cdc", phase: "streaming", health: "healthy", dependencies: [], updated_at: new Date().toISOString() })
    return res(200, {})
  })
})
afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

describe("a status change is announced to every panel", () => {
  it("the /state poller announces it once, and does not re-read itself for it", async () => {
    const heard: string[] = []
    const off = onPipelineRefresh((pid) => heard.push(pid))
    render(<Reader id="p35" />)
    await tick(0)
    await tick(4000)
    expect(heard).toEqual([]) // control: no change, no announcement
    const before = count("/p35/state")
    stateNow = "completed"
    await tick(4000)
    expect(heard).toEqual(["p35"])
    await tick(100)
    expect(count("/p35/state")).toBe(before + 1)
    off()
  })
})

describe("cadence of a settled pipeline", () => {
  it("paused and idle are read every 10 s; a finished one stays at 30 s (control)", () => {
    expect(statePollMs("paused")).toBe(10_000)
    expect(statePollMs("idle")).toBe(10_000)
    expect(statePollMs("completed")).toBe(SETTLED_POLL_MS)
    expect(SETTLED_POLL_MS).toBe(30_000)
  })
})

describe("the Overview tab listens to the refresh bus", () => {
  it("re-reads /runtime at once on a refresh event for its pipeline, not for another", async () => {
    render(<MonitoringOverviewTab pipelineId="p35" />)
    await tick(0)
    const first = count("/p35/runtime")
    expect(first).toBe(1)
    act(() => emitPipelineRefresh("other"))
    await tick(0)
    expect(count("/p35/runtime")).toBe(first) // control
    act(() => emitPipelineRefresh("p35"))
    await tick(0)
    expect(count("/p35/runtime")).toBe(first + 1)
  })
})
