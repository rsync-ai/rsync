/**
 * A /runtime answer slower than the poll interval must still land (U-18).
 *
 * Bug class: a fixed-interval poller that aborts the previous request on every
 * tick. When /runtime took >= 5 s, each tick cancelled the request before it
 * answered, the abort was swallowed, and the shared store kept its last answer —
 * a transient CDC "failed" — for as long as the gateway stayed slow. The header,
 * status pill and Restart CDC button all read that store, so the page said
 * "Failed / replication stopped" for 50 s while /state said running.
 *
 * Pinned: a tick never cancels a request still in flight; that request's answer
 * is shown when it lands; and every request carries a timeout, so a hung one
 * cannot hold the poll forever.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))

import { usePipelineRuntime } from "@/lib/hooks/usePipelineRuntime"

const runtime = (phase: string) => ({
  pipeline_id: "p1",
  mode: "cdc",
  phase,
  health: phase === "failed" ? "unhealthy" : "healthy",
  dependencies: [],
  updated_at: "2026-09-27T10:00:00Z",
})
const ok = (body: unknown) => ({ ok: true, status: 200, json: async () => body }) as unknown as Response

function Reader() {
  const { runtime: r } = usePipelineRuntime("p1")
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
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => "visible" })
})
afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

describe("usePipelineRuntime — a slow /runtime still lands", () => {
  it("a 6 s answer replaces a stale 'failed' instead of being aborted by the next tick", async () => {
    let call = 0
    authFetch.mockImplementation((_url: string, init: { signal?: AbortSignal }) => {
      call += 1
      if (call === 1) return Promise.resolve(ok(runtime("failed")))
      return new Promise((resolve, reject) => {
        const t = setTimeout(() => resolve(ok(runtime("streaming"))), 6000)
        init?.signal?.addEventListener("abort", () => {
          clearTimeout(t)
          reject(Object.assign(new Error("aborted"), { name: "AbortError" }))
        })
      })
    })
    render(<Reader />)
    await tick(0)
    expect(screen.getByTestId("phase")).toHaveTextContent("failed")

    await tick(30_000)
    expect(screen.getByTestId("phase")).toHaveTextContent("streaming")
  })

  it("every /runtime request carries a timeout, so a hung one cannot hold the poll", async () => {
    authFetch.mockResolvedValue(ok(runtime("streaming")))
    render(<Reader />)
    await tick(0)
    const init = authFetch.mock.calls[0][1] as { timeoutMs?: number }
    expect(init.timeoutMs).toBeGreaterThan(0)
  })
})
