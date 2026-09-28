/**
 * A streaming run's panels keep reading /state, and re-read on the refresh bus (U-18).
 *
 * Bug class: a poll allowlist that leaves out the steady state. Both run panels
 * polled /state only while processing / waiting_for_user / pending. A streaming
 * CDC pipeline reads "running", so after Stop → Resume the Live Execution panel
 * kept the previous execution id and its "Failed" verdict until a hard reload:
 * it did not listen to the refresh bus Resume emits, and its poll was off.
 *
 * Pinned for both panels: "running" is read on a 10 s tick, and a refresh event
 * reads at once. Control: a completed run still stops polling.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render } from "@testing-library/react"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  useSearchParams: () => new URLSearchParams(),
  usePathname: () => "/pipelines/p1",
}))
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: null, loading: false, error: null }),
}))
vi.mock("@/components/pipeline/MonitoringOverviewTab", () => ({ MonitoringOverviewTab: () => null }))
vi.mock("@/components/pipeline/TableStatisticsPanel", () => ({ TableStatisticsPanel: () => null }))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
  authFetchOrThrow: (...args: unknown[]) => authFetch(...args),
}))

import { PipelineLiveStatePanel } from "@/components/pipeline/PipelineLiveStatePanel"
import { PipelineMonitoringPanel } from "@/components/pipeline/PipelineMonitoringPanel"
import { emitPipelineRefresh } from "@/lib/events/pipelineRefresh"

function jsonOk(body: unknown) {
  return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) }
}

function reads() {
  return authFetch.mock.calls.filter(([u]) => String(u).endsWith("/pipelines/p1/state")).length
}

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

let status = "running"

beforeEach(() => {
  authFetch.mockReset()
  vi.useFakeTimers()
  status = "running"
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => "visible" })
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.endsWith("/state")) {
      return jsonOk({ schema_version: 1, pipeline_id: "p1", execution_id: "e1", status, current_stage: "executor" })
    }
    if (u.includes("/events")) return jsonOk({ events: [] })
    return jsonOk({})
  })
})
afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

const PANELS = [
  ["PipelineLiveStatePanel", () => <PipelineLiveStatePanel pipelineId="p1" />],
  ["PipelineMonitoringPanel", () => <PipelineMonitoringPanel pipelineId="p1" />],
] as const

describe.each(PANELS)("%s — a running (streaming) pipeline", (_name, panel) => {
  it("keeps reading /state on a 10 s tick", async () => {
    render(panel())
    await tick(0)
    const first = reads()
    expect(first).toBeGreaterThan(0)
    await tick(30_000)
    expect(reads()).toBe(first + 3)
  })

  it("reads /state at once when the refresh bus fires (Resume, Stop, Reload emit it)", async () => {
    render(panel())
    await tick(0)
    const first = reads()
    await act(async () => {
      emitPipelineRefresh("p1")
    })
    await tick(0)
    expect(reads()).toBeGreaterThan(first)
  })

  it("control: a completed run does not poll", async () => {
    status = "completed"
    render(panel())
    await tick(0)
    const first = reads()
    await tick(60_000)
    expect(reads()).toBe(first)
  })
})
