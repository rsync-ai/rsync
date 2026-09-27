/**
 * The chat's /state poll, once the run has ended (#13).
 *
 * It read /state every 2 s for as long as the chat stayed open, finished run or
 * not, background tab or not, and dispatched the run's end again on every read:
 * a completed run re-dispatched PIPELINE_COMPLETED every 2 s, forever.
 *
 * Pinned here:
 *  - a completed run is read every 30 s, and its end is dispatched once;
 *  - a hidden tab reads nothing, and reads once on return;
 *  - controls: a run still processing is read every 2 s, and a new run of the
 *    same pipeline that completes is dispatched again.
 */
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { act, fireEvent, render, screen } from "@testing-library/react"
import type { Mock } from "vitest"

const seen = vi.hoisted(() => ({ actions: [] as string[] }))
vi.mock("@/lib/pipeline/pipelineStateReducer", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/pipeline/pipelineStateReducer")>()
  return {
    ...actual,
    pipelineReducer: (s: Parameters<typeof actual.pipelineReducer>[0], a: Parameters<typeof actual.pipelineReducer>[1]) => {
      seen.actions.push(a.type)
      return actual.pipelineReducer(s, a)
    },
  }
})
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("@/lib/api/chat", () => ({
  sendChatMessage: vi.fn(),
  resetSessionId: vi.fn(() => "session-reset"),
}))
vi.mock("@/lib/api/pipelines", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api/pipelines")>()
  return { ...actual, getPipeline: vi.fn(async () => ({})) }
})
vi.mock("@/contexts/WebSocketContext", () => ({
  useWebSocket: () => ({ subscribe: () => () => {} }),
}))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/chat",
  useSearchParams: () => new URLSearchParams(),
}))
vi.mock("@/components/chat/AgenticPipelineHome", () => ({
  AgenticPipelineHome: ({ onSubmit }: { onSubmit: (p: string) => void }) => (
    <button type="button" onClick={() => onSubmit("Move my orders table to cloud storage")}>
      Start from home
    </button>
  ),
}))
vi.mock("@/components/chat/ChatMessageItem", () => ({ ChatMessageItem: () => null }))
vi.mock("@/components/chat/PipelineAccordionView", () => ({ PipelineAccordionView: () => null }))
vi.mock("@/components/chat/ChatRightPanel", () => ({ ChatRightPanel: () => null }))
vi.mock("@/components/chat/ActivePipelinesList", () => ({ ActivePipelinesList: () => null }))
vi.mock("@/components/chat/SuggestionsReviewDialog", () => ({ SuggestionsReviewDialog: () => null }))
vi.mock("@/components/pipeline/PipelineMonitoringPanel", () => ({ PipelineMonitoringPanel: () => null }))
vi.mock("@/components/pipeline/PipelineConnectionSelector", () => ({ PipelineConnectionSelector: () => null }))
vi.mock("@/components/pipeline/PipelineTableSelector", () => ({ PipelineTableSelector: () => null }))

import { AgenticChatInterfaceV2 } from "@/components/chat/AgenticChatInterfaceV2"
import { authFetch } from "@/lib/api/auth-fetch"
import { sendChatMessage } from "@/lib/api/chat"

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

const PIPELINE_ID = "3c9f1d2e-0000-4000-8000-000000000013"

// What /state says about the run.
let run = { status: "completed", execution_id: "exec-1" }

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } })
}

function stateReads() {
  return (authFetch as Mock).mock.calls.filter(([u]) => String(u).endsWith(`/pipelines/${PIPELINE_ID}/state`)).length
}

function completions() {
  return seen.actions.filter((t) => t === "PIPELINE_COMPLETED").length
}

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

let visibility: DocumentVisibilityState = "visible"
function setVisibility(v: DocumentVisibilityState) {
  visibility = v
  act(() => {
    document.dispatchEvent(new Event("visibilitychange"))
  })
}

async function startPipeline() {
  ;(sendChatMessage as Mock).mockResolvedValue({
    type: "pipeline_started",
    message: "Setting up your pipeline.",
    data: { pipeline_id: PIPELINE_ID, execution_id: run.execution_id },
    metadata: {},
  })
  render(<AgenticChatInterfaceV2 />)
  fireEvent.click(screen.getByRole("button", { name: "Start from home" }))
  // Let the send settle and the poll's first read, and the re-reads each dispatch
  // triggers, land.
  await tick(0)
  await tick(100)
}

beforeEach(() => {
  vi.clearAllMocks()
  seen.actions = []
  run = { status: "completed", execution_id: "exec-1" }
  try {
    window.localStorage.clear()
  } catch {
    // storage unavailable
  }
  visibility = "visible"
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility })
  vi.useFakeTimers()
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    if (String(url).endsWith(`/pipelines/${PIPELINE_ID}/state`)) {
      return json(200, { pipeline_id: PIPELINE_ID, current_stage: "completed", ...run })
    }
    return json(404, { error: "not found" })
  })
})
afterEach(() => {
  vi.useRealTimers()
})

describe("chat /state poll — a run that has ended", () => {
  it("is read every 30 s, and its end is dispatched once", async () => {
    await startPipeline()
    expect(completions()).toBeGreaterThan(0)
    const reads = stateReads()
    const done = completions()

    await tick(28_000)
    expect(stateReads()).toBe(reads)
    await tick(2_000)
    expect(stateReads()).toBe(reads + 1)

    await tick(60_000)
    expect(stateReads()).toBe(reads + 3)
    expect(completions()).toBe(done)
  })

  it("control: a new run of the same pipeline that completes is dispatched again", async () => {
    await startPipeline()
    const done = completions()
    run = { status: "completed", execution_id: "exec-2" }
    await tick(30_000)
    expect(completions()).toBeGreaterThan(done)
  })

  it("control: a run still processing is read every 2 s", async () => {
    run = { status: "processing", execution_id: "exec-1" }
    await startPipeline()
    const reads = stateReads()
    await tick(2_000)
    expect(stateReads()).toBe(reads + 1)
    await tick(2_000)
    expect(stateReads()).toBe(reads + 2)
    expect(completions()).toBe(0)
  })
})

describe("chat /state poll — hidden tab", () => {
  it("reads nothing while hidden, and once on return", async () => {
    run = { status: "processing", execution_id: "exec-1" }
    await startPipeline()
    const reads = stateReads()
    setVisibility("hidden")
    await tick(60_000)
    expect(stateReads()).toBe(reads)
    setVisibility("visible")
    await tick(0)
    expect(stateReads()).toBe(reads + 1)
  })
})
