/**
 * One /state poller per pipeline, slowed on a settled status, paused while the
 * tab is hidden (#13).
 *
 * The pipeline page header read /state from up to four places (status badge,
 * run/pause/stop actions, CDC actions, overflow menu), each on its own 4 s
 * interval that never slowed or stopped: an open page sent two (batch) or three
 * (CDC) identical GETs every 4 s for as long as it stayed open, finished
 * pipeline or not, background tab or not.
 *
 * Pinned here:
 *  - the whole header sends one /state request per tick;
 *  - a finished pipeline is read every 30 s, a paused or idle one every 10 s, a live one every 4 s;
 *  - a hidden tab sends nothing on its ticks and reads once on return;
 *  - the refresh bus reads at once, without leaving a second timer behind;
 *  - a 404 stops the poll for good; the last reader leaving stops it;
 *  - controls: another pipeline gets its own poll, and the overflow menu's
 *    optimistic "Stop hides itself" lasts until the next read, not forever.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/pipelines/p1",
  useSearchParams: () => new URLSearchParams(""),
}))
// /runtime has its own shared poller (runtime-shared-poll.test.tsx); keep it out
// of the request counts here.
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: null, loading: false, error: null }),
}))
vi.mock("@/lib/api/pipelines", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api/pipelines")>()
  return {
    ...actual,
    getPipelineCDCStatus: vi.fn(async () => ({ recovery_enabled: false })),
    executePipelineWithRunMode: vi.fn(),
    recoverPipelineCDC: vi.fn(),
    restartPipelineCDC: vi.fn(),
  }
})

import {
  ACTIVE_POLL_MS,
  PAUSED_POLL_MS,
  SETTLED_POLL_MS,
  statePollMs,
  usePipelineStatePoll,
} from "@/lib/hooks/usePipelineStatePoll"
import { emitPipelineRefresh } from "@/lib/events/pipelineRefresh"
import { PipelineExecutionStatusBadge } from "@/components/pipeline/PipelineExecutionStatusBadge"
import { PipelineActions } from "@/components/pipeline/PipelineActions"
import { CDCPipelineActions } from "@/components/pipeline/CDCPipelineActions"
import { PipelineHeaderOverflowMenu } from "@/components/pipeline/PipelineHeaderOverflowMenu"

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

// What /state says, per pipeline; a number is an HTTP error status.
let stateOf: Record<string, string | number> = {}

function stateCalls(pipelineId = "p1") {
  return authFetch.mock.calls.filter(([url]) => String(url).endsWith(`/pipelines/${pipelineId}/state`)).length
}

function Reader({ id, testId = "s" }: { id: string; testId?: string }) {
  const s = usePipelineStatePoll(id)
  return <span data-testid={testId}>{s.status ?? "none"}</span>
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

beforeEach(() => {
  vi.clearAllMocks()
  visibility = "visible"
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility })
  stateOf = { p1: "processing", p2: "processing" }
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.endsWith("/state")) {
      const id = u.split("/pipelines/")[1]?.split("/")[0] ?? "?"
      const s = stateOf[id] ?? "processing"
      return typeof s === "number" ? res(s, {}) : res(200, { status: s })
    }
    return res(200, {})
  })
})
afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

describe("statePollMs", () => {
  it("4 s while live or unknown, 10 s paused or idle, 30 s once finished", () => {
    for (const s of [null, "running", "waiting_for_user", "unknown"] as const) expect(statePollMs(s)).toBe(ACTIVE_POLL_MS)
    for (const s of ["idle", "paused"] as const) expect(statePollMs(s)).toBe(PAUSED_POLL_MS)
    for (const s of ["completed", "failed", "cancelled"] as const)
      expect(statePollMs(s)).toBe(SETTLED_POLL_MS)
  })
})

describe("the pipeline page header", () => {
  it("sends one /state request per tick, however many header parts read it", async () => {
    vi.useFakeTimers()
    render(
      <>
        <PipelineExecutionStatusBadge pipelineId="p1" />
        <PipelineActions pipelineId="p1" status="running" />
        <CDCPipelineActions pipelineId="p1" pipelineName="P1" status="running" />
        <PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="P1" pipelineType="cdc" status="running" />
      </>,
    )
    await tick(0)
    expect(stateCalls()).toBe(1)
    await tick(ACTIVE_POLL_MS)
    expect(stateCalls()).toBe(2)
    await tick(ACTIVE_POLL_MS)
    expect(stateCalls()).toBe(3)
  })
})

describe("usePipelineStatePoll — cadence", () => {
  it("a finished pipeline is read every 30 s, not every 4 s", async () => {
    vi.useFakeTimers()
    stateOf.p1 = "completed"
    render(<Reader id="p1" />)
    await tick(0)
    expect(screen.getByTestId("s")).toHaveTextContent("completed")
    expect(stateCalls()).toBe(1)
    await tick(SETTLED_POLL_MS - 1000)
    expect(stateCalls()).toBe(1)
    await tick(1000)
    expect(stateCalls()).toBe(2)
  })

  it("control: a running one is read every 4 s, and slows once it finishes", async () => {
    vi.useFakeTimers()
    render(<Reader id="p1" />)
    await tick(0)
    expect(stateCalls()).toBe(1)
    await tick(ACTIVE_POLL_MS)
    expect(stateCalls()).toBe(2)
    stateOf.p1 = "failed"
    await tick(ACTIVE_POLL_MS)
    expect(stateCalls()).toBe(3)
    expect(screen.getByTestId("s")).toHaveTextContent("failed")
    await tick(SETTLED_POLL_MS - 1000)
    expect(stateCalls()).toBe(3)
    await tick(1000)
    expect(stateCalls()).toBe(4)
  })
})

describe("usePipelineStatePoll — hidden tab", () => {
  it("sends nothing while hidden, and reads once as soon as it is visible", async () => {
    vi.useFakeTimers()
    render(<Reader id="p1" />)
    await tick(0)
    expect(stateCalls()).toBe(1)
    setVisibility("hidden")
    await tick(60_000)
    expect(stateCalls()).toBe(1)
    setVisibility("visible")
    await tick(0)
    expect(stateCalls()).toBe(2)
    // Back on the ordinary schedule, with one timer, not two.
    await tick(ACTIVE_POLL_MS - 1)
    expect(stateCalls()).toBe(2)
    await tick(1)
    expect(stateCalls()).toBe(3)
  })
})

describe("usePipelineStatePoll — refresh bus and 404", () => {
  it("a refresh reads at once, and the next read is a full interval after it", async () => {
    vi.useFakeTimers()
    stateOf.p1 = "completed"
    render(<Reader id="p1" />)
    await tick(0)
    await tick(20_000)
    expect(stateCalls()).toBe(1)
    // Another 30 s status, so the old timer and the new one are due at different times.
    stateOf.p1 = "failed"
    act(() => emitPipelineRefresh("p1"))
    await tick(0)
    expect(stateCalls()).toBe(2)
    expect(screen.getByTestId("s")).toHaveTextContent("failed")
    // t=30 s, when the timer set before the refresh was due: it is gone.
    await tick(10_000)
    expect(stateCalls()).toBe(2)
    // t=50 s, a full interval after the refresh.
    await tick(20_000)
    expect(stateCalls()).toBe(3)
  })

  it("another pipeline's refresh is ignored", async () => {
    vi.useFakeTimers()
    stateOf.p1 = "completed"
    render(<Reader id="p1" />)
    await tick(0)
    act(() => emitPipelineRefresh("p2"))
    await tick(0)
    expect(stateCalls()).toBe(1)
  })

  it("a 404 stops the poll for good, refresh or not", async () => {
    vi.useFakeTimers()
    stateOf.p1 = 404
    render(<Reader id="p1" />)
    await tick(0)
    expect(stateCalls()).toBe(1)
    await tick(60_000)
    act(() => emitPipelineRefresh("p1"))
    await tick(0)
    expect(stateCalls()).toBe(1)
    expect(screen.getByTestId("s")).toHaveTextContent("none")
  })

  it("control: a 500 keeps polling", async () => {
    vi.useFakeTimers()
    stateOf.p1 = 500
    render(<Reader id="p1" />)
    await tick(0)
    await tick(ACTIVE_POLL_MS)
    expect(stateCalls()).toBe(2)
  })
})

describe("usePipelineStatePoll — readers", () => {
  it("the last reader leaving stops the poll", async () => {
    vi.useFakeTimers()
    const { unmount } = render(
      <>
        <Reader id="p1" testId="a" />
        <Reader id="p1" testId="b" />
      </>,
    )
    await tick(0)
    expect(stateCalls()).toBe(1)
    unmount()
    await tick(60_000)
    expect(stateCalls()).toBe(1)
  })

  it("control: two pipelines are polled separately", async () => {
    vi.useFakeTimers()
    stateOf.p2 = "paused"
    render(
      <>
        <Reader id="p1" testId="a" />
        <Reader id="p2" testId="b" />
      </>,
    )
    await tick(0)
    expect(stateCalls("p1")).toBe(1)
    expect(stateCalls("p2")).toBe(1)
    expect(screen.getByTestId("a")).toHaveTextContent("running")
    expect(screen.getByTestId("b")).toHaveTextContent("paused")
    await tick(ACTIVE_POLL_MS)
    expect(stateCalls("p1")).toBe(2)
    expect(stateCalls("p2")).toBe(1)
  })
})

describe("overflow menu — Stop hides itself until the next read", () => {
  it("hides Stop the moment it lands, then follows /state", async () => {
    // /state still says running after the stop POST, and the read the refresh
    // asks for is held until the test releases it.
    stateOf.p1 = "running"
    let release: (() => void) | null = null
    let holdNext = false
    authFetch.mockImplementation(async (url: string) => {
      const u = String(url)
      if (u.endsWith("/stop")) {
        holdNext = true
        return res(200, {})
      }
      if (u.endsWith("/state")) {
        if (holdNext) {
          holdNext = false
          await new Promise<void>((r) => (release = r))
        }
        return res(200, { status: stateOf.p1 })
      }
      return res(200, {})
    })
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="P1" pipelineType="cdc" status="running" />)
    await waitFor(() => expect(stateCalls()).toBe(1))

    // Stop closes the menu (U-16), so reopen it to see the item gone, then back.
    await user.click(screen.getByRole("button", { name: /open pipeline menu/i }))
    await user.click(screen.getByRole("menuitem", { name: /stop pipeline/i }))
    await waitFor(() => expect(release).not.toBeNull())
    await user.click(screen.getByRole("button", { name: /open pipeline menu/i }))
    await screen.findByRole("menu")
    expect(screen.queryByRole("menuitem", { name: /stop pipeline/i })).not.toBeInTheDocument()

    await act(async () => {
      release?.()
    })
    expect(await screen.findByRole("menuitem", { name: /stop pipeline/i })).toBeInTheDocument()
  })
})
