/**
 * The chat's right panel stops reading /state once the run has ended (#13).
 *
 * It stopped only on "completed" or "failed". /state answers "stopped" for a
 * stopped run, so an open panel on one read it every 3 s for as long as it stayed
 * open, in a background tab too.
 *
 * Pinned here: a stopped run is read once and no more; a hidden tab reads
 * nothing. Control: a run still processing is read every 3 s.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, render } from "@testing-library/react"
import type { Mock } from "vitest"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), refresh: vi.fn() }),
}))
vi.mock("@/components/pipeline/DAGVisualization", () => ({
  DAGVisualization: () => null,
  LinearTimeline: () => null,
}))
vi.mock("@/components/pipeline/NodeInspector", () => ({ NodeInspector: () => null }))

import { ChatRightPanel } from "@/components/chat/ChatRightPanel"
import { authFetch } from "@/lib/api/auth-fetch"

const PIPELINE_ID = "p-panel"
let status = "stopped"

function reads() {
  return (authFetch as Mock).mock.calls.filter(([u]) => String(u).endsWith(`/pipelines/${PIPELINE_ID}/state`)).length
}

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

let visibility: DocumentVisibilityState = "visible"

beforeEach(() => {
  vi.clearAllMocks()
  vi.useFakeTimers()
  visibility = "visible"
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility })
  ;(authFetch as Mock).mockImplementation(async () => ({
    ok: true,
    status: 200,
    json: async () => ({ pipeline_id: PIPELINE_ID, status, current_stage: "executor" }),
  }))
})
afterEach(() => {
  vi.useRealTimers()
})

describe("ChatRightPanel — /state poll", () => {
  it("reads a stopped run once, and no more", async () => {
    status = "stopped"
    render(<ChatRightPanel pipelineId={PIPELINE_ID} isOpen onClose={vi.fn()} />)
    await tick(0)
    expect(reads()).toBe(1)
    await tick(30_000)
    expect(reads()).toBe(1)
  })

  it("control: a run still processing is read every 3 s", async () => {
    status = "processing"
    render(<ChatRightPanel pipelineId={PIPELINE_ID} isOpen onClose={vi.fn()} />)
    await tick(0)
    expect(reads()).toBe(1)
    await tick(9_000)
    expect(reads()).toBe(4)
  })

  it("a hidden tab reads nothing", async () => {
    status = "processing"
    render(<ChatRightPanel pipelineId={PIPELINE_ID} isOpen onClose={vi.fn()} />)
    await tick(0)
    visibility = "hidden"
    await tick(30_000)
    expect(reads()).toBe(1)
  })
})
