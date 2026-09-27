/**
 * The pipeline page's run panels skip their /state reads while the tab is hidden
 * (#13).
 *
 * The live-state panel read /state every 2.5 s while a run was processing and
 * the monitoring panel did the same, in a background tab too. Both already stop
 * once the run has ended.
 *
 * Pinned here: a hidden tab reads nothing; back in view, the next tick reads.
 * Control: a visible tab reads every 2.5 s.
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

let visibility: DocumentVisibilityState = "visible"

beforeEach(() => {
  authFetch.mockReset()
  vi.useFakeTimers()
  visibility = "visible"
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility })
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.endsWith("/state")) {
      return jsonOk({ schema_version: 1, pipeline_id: "p1", execution_id: "e1", status: "processing", current_stage: "executor" })
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

describe.each(PANELS)("%s — /state poll while a run is processing", (_name, panel) => {
  it("control: a visible tab reads every 2.5 s", async () => {
    render(panel())
    await tick(0)
    const first = reads()
    expect(first).toBeGreaterThan(0)
    await tick(10_000)
    expect(reads()).toBe(first + 4)
  })

  it("a hidden tab reads nothing, and the next tick in view reads", async () => {
    render(panel())
    await tick(0)
    const first = reads()
    visibility = "hidden"
    await tick(30_000)
    expect(reads()).toBe(first)
    visibility = "visible"
    await tick(2_500)
    expect(reads()).toBe(first + 1)
  })
})
