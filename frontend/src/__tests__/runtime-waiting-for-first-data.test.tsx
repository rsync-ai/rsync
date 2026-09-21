/**
 * Issue #20 — "Running · Streaming pipeline active" with nothing moved for 15+ minutes.
 *
 * At the CDC handoff the snapshot execution closes and pipeline_progress is set to
 * running / "Streaming pipeline active", so /state says a stream is live whether or
 * not a single row ever reaches the destination. api-gateway's /runtime now reports
 * phase waiting_for_data when nothing has been delivered past a grace period
 * (pipeline_runtime.go cdcLivenessPhase). These tests render the three detail-page
 * surfaces that repeated /state's claim and pin that they say "Waiting for first
 * data" instead — and, as controls, that a streaming phase still renders LIVE /
 * running and a paused /state is never relabelled by a stale runtime poll.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  useSearchParams: () => new URLSearchParams(),
  usePathname: () => "/pipelines/p1",
}))

let runtimeValue: Record<string, unknown> | null = null
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: runtimeValue, loading: false, error: null }),
}))

// The monitoring panel's tabs are not what is under test and pull in their own fetches.
vi.mock("@/components/pipeline/MonitoringOverviewTab", () => ({ MonitoringOverviewTab: () => null }))
vi.mock("@/components/pipeline/TableStatisticsPanel", () => ({ TableStatisticsPanel: () => null }))
vi.mock("@/components/pipeline/ReasoningTimeline", () => ({ ReasoningTimeline: () => null }))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
  authFetchOrThrow: (...args: unknown[]) => authFetch(...args),
}))

import { PipelineHealthHeader } from "@/components/pipeline/PipelineHealthHeader"
import { PipelineLiveStatePanel } from "@/components/pipeline/PipelineLiveStatePanel"
import { PipelineMonitoringPanel } from "@/components/pipeline/PipelineMonitoringPanel"

const WAITING_MESSAGE = "Streaming is set up, but no data has reached the destination yet"

function runtime(phase: string, over: Record<string, unknown> = {}) {
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    mode: "cdc",
    phase,
    health: "healthy",
    message: phase === "waiting_for_data" ? WAITING_MESSAGE : "Streaming pipeline active",
    dependencies: [],
    updated_at: "2026-09-16T10:00:00Z",
    ...over,
  }
}

function jsonOk(body: unknown) {
  return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) }
}

// The /state a CDC handoff leaves behind (pipeline_status_activity.go "streaming_active").
function handoffState(status = "running") {
  return {
    schema_version: 1,
    pipeline_id: "p1",
    execution_id: "e1",
    status,
    current_stage: "streaming",
    message: "Streaming pipeline active",
    progress: { percent: 100 },
    created_at: "2026-09-16T09:00:00Z",
    updated_at: "2026-09-16T09:05:00Z",
  }
}

function routeWith(state: Record<string, unknown>) {
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/state")) return jsonOk(state)
    if (u.includes("/events")) return jsonOk({ events: [] })
    if (/\/pipelines\/p1(\?|$)/.test(u)) return jsonOk({ id: "p1", name: "orders", sync_mode: "cdc", cdc_mode: "streaming_only" })
    return jsonOk({})
  })
}

beforeEach(() => {
  authFetch.mockReset()
  runtimeValue = null
})
afterEach(() => cleanup())

describe("PipelineHealthHeader", () => {
  it("says Waiting for first data, with the runtime's message, instead of Streaming", () => {
    runtimeValue = runtime("waiting_for_data")
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.getByText("Waiting for first data")).toBeInTheDocument()
    expect(screen.getByText(WAITING_MESSAGE)).toBeInTheDocument()
    expect(screen.queryByText("Streaming")).toBeNull()
    expect(screen.queryByText(/waiting_for_data/i)).toBeNull()
  })

  it("control: a streaming phase still reads Streaming", () => {
    runtimeValue = runtime("streaming", { liveness: { stale_seconds: 12 } })
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.getByText("Streaming")).toBeInTheDocument()
    expect(screen.getByText("last event 12s ago")).toBeInTheDocument()
    expect(screen.queryByText("Waiting for first data")).toBeNull()
  })
})

describe("PipelineLiveStatePanel", () => {
  it("replaces running / LIVE / 'Streaming pipeline active' with Waiting for first data", async () => {
    runtimeValue = runtime("waiting_for_data")
    routeWith(handoffState())
    render(<PipelineLiveStatePanel pipelineId="p1" />)
    expect(await screen.findByText("Waiting for first data")).toBeInTheDocument()
    expect(screen.getByText(WAITING_MESSAGE)).toBeInTheDocument()
    expect(screen.getByText("Current stage: Streaming (CDC), no data yet")).toBeInTheDocument()
    expect(screen.queryByText("LIVE")).toBeNull()
    expect(screen.queryByText("100%")).toBeNull()
    expect(screen.queryByText("Streaming pipeline active")).toBeNull()
    expect(screen.queryByText("running")).toBeNull()
  })

  it("control: the same /state with a streaming phase still renders LIVE and running", async () => {
    runtimeValue = runtime("streaming")
    routeWith(handoffState())
    render(<PipelineLiveStatePanel pipelineId="p1" />)
    expect(await screen.findByText("LIVE")).toBeInTheDocument()
    expect(screen.getByText("running")).toBeInTheDocument()
    expect(screen.getByText("Streaming pipeline active")).toBeInTheDocument()
    expect(screen.queryByText("Waiting for first data")).toBeNull()
  })

  it("a paused /state is not relabelled by a waiting_for_data poll from before the pause", async () => {
    runtimeValue = runtime("waiting_for_data")
    routeWith(handoffState("paused"))
    render(<PipelineLiveStatePanel pipelineId="p1" />)
    expect(await screen.findByText("paused")).toBeInTheDocument()
    expect(screen.queryByText("Waiting for first data")).toBeNull()
  })
})

describe("PipelineMonitoringPanel", () => {
  it("shows Waiting for first data and the runtime message instead of running", async () => {
    runtimeValue = runtime("waiting_for_data")
    routeWith({ ...handoffState(), summary: "Streaming pipeline active" })
    render(<PipelineMonitoringPanel pipelineId="p1" />)
    expect(await screen.findByText("Waiting for first data")).toBeInTheDocument()
    expect(screen.getByText(WAITING_MESSAGE)).toBeInTheDocument()
    expect(screen.queryByText("running")).toBeNull()
    expect(screen.queryByText("Streaming pipeline active")).toBeNull()
  })

  it("control: a streaming phase keeps the running badge and the /state summary", async () => {
    runtimeValue = runtime("streaming")
    routeWith({ ...handoffState(), summary: "Streaming pipeline active" })
    render(<PipelineMonitoringPanel pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText("running")).toBeInTheDocument())
    expect(screen.getByText("Streaming pipeline active")).toBeInTheDocument()
    expect(screen.queryByText("Waiting for first data")).toBeNull()
  })
})
