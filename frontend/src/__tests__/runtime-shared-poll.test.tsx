/**
 * One /runtime poller per pipeline, paused while the tab is hidden.
 *
 * The pipeline detail page mounts usePipelineRuntime from up to six places at
 * once (header, status badge, overflow menu, CDC actions, Data flow tab, live
 * stream strip). Each used to run its own 5 s interval, so one open page sent a
 * burst of identical GETs every tick, and kept sending them from a background
 * tab nobody was looking at.
 *
 * Pinned here:
 *  - two readers of the same pipeline share one request per tick, and both see
 *    the answer;
 *  - a hidden tab sends nothing on its ticks, and asks once as soon as it is
 *    visible again, so the page is never a full interval stale on return;
 *  - the last reader unmounting stops the poll;
 *  - controls: a different pipeline still gets its own request, and a 404 still
 *    stops the poll for every reader.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))

import { usePipelineRuntime } from "@/lib/hooks/usePipelineRuntime"

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

function runtime(pipelineId: string, phase = "streaming") {
  return {
    pipeline_id: pipelineId,
    mode: "cdc",
    phase,
    health: "healthy",
    dependencies: [],
    updated_at: "2026-09-25T10:00:00Z",
  }
}

function Reader({ id, testId }: { id: string; testId: string }) {
  const { runtime: r } = usePipelineRuntime(id)
  return <span data-testid={testId}>{r?.phase ?? "none"}</span>
}

function runtimeCalls(pipelineId?: string) {
  return authFetch.mock.calls.filter(([url]) =>
    String(url).includes("/runtime") && (!pipelineId || String(url).includes(`/${pipelineId}/`)),
  ).length
}

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

let visibility: DocumentVisibilityState = "visible"
function setVisibility(v: DocumentVisibilityState) {
  visibility = v
  document.dispatchEvent(new Event("visibilitychange"))
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.useFakeTimers()
  visibility = "visible"
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility })
  authFetch.mockImplementation(async (url: string) => {
    const id = String(url).split("/pipelines/")[1]?.split("/")[0] ?? "?"
    return res(200, runtime(id))
  })
})
afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

describe("usePipelineRuntime — one poller per pipeline", () => {
  it("two readers of one pipeline share one request per tick", async () => {
    render(
      <>
        <Reader id="p1" testId="a" />
        <Reader id="p1" testId="b" />
      </>,
    )
    await tick(0)
    expect(screen.getByTestId("a")).toHaveTextContent("streaming")
    expect(screen.getByTestId("b")).toHaveTextContent("streaming")
    expect(runtimeCalls()).toBe(1)

    await tick(5000)
    expect(runtimeCalls()).toBe(2)
    await tick(5000)
    expect(runtimeCalls()).toBe(3)
  })

  it("control: a different pipeline still gets its own request", async () => {
    render(
      <>
        <Reader id="p1" testId="a" />
        <Reader id="p2" testId="b" />
      </>,
    )
    await tick(0)
    expect(runtimeCalls("p1")).toBe(1)
    expect(runtimeCalls("p2")).toBe(1)
  })

  it("a reader that mounts later reads the shared answer without a new request", async () => {
    const { rerender } = render(<Reader id="p1" testId="a" />)
    await tick(0)
    expect(runtimeCalls()).toBe(1)

    rerender(
      <>
        <Reader id="p1" testId="a" />
        <Reader id="p1" testId="b" />
      </>,
    )
    await tick(0)
    expect(screen.getByTestId("b")).toHaveTextContent("streaming")
    expect(runtimeCalls()).toBe(1)
  })

  it("stops polling once the last reader unmounts", async () => {
    const { unmount } = render(<Reader id="p1" testId="a" />)
    await tick(0)
    unmount()
    await tick(20_000)
    expect(runtimeCalls()).toBe(1)
  })

  it("control: a 404 still stops the poll for every reader", async () => {
    authFetch.mockResolvedValue(res(404, {}))
    render(
      <>
        <Reader id="p1" testId="a" />
        <Reader id="p1" testId="b" />
      </>,
    )
    await tick(0)
    await tick(20_000)
    expect(runtimeCalls()).toBe(1)
    expect(screen.getByTestId("a")).toHaveTextContent("none")
  })
})

describe("usePipelineRuntime — hidden tab", () => {
  it("sends nothing while hidden and asks at once when visible again", async () => {
    render(<Reader id="p1" testId="a" />)
    await tick(0)
    expect(runtimeCalls()).toBe(1)

    act(() => setVisibility("hidden"))
    await tick(30_000)
    expect(runtimeCalls()).toBe(1)

    act(() => setVisibility("visible"))
    await tick(0)
    expect(runtimeCalls()).toBe(2)

    // And the interval resumes.
    await tick(5000)
    expect(runtimeCalls()).toBe(3)
  })

  it("control: a visible tab keeps polling on every tick", async () => {
    render(<Reader id="p1" testId="a" />)
    await tick(0)
    await tick(15_000)
    expect(runtimeCalls()).toBe(4)
  })
})
