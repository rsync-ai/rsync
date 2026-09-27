import { describe, expect, it, vi } from "vitest"
import { act, renderHook } from "@testing-library/react"

import { useGraphLayout } from "@/lib/hooks/useGraphLayout"

/** A layout the test finishes by hand, one deferred answer per call. */
function controlledRun() {
  const calls: { input: string; resolve: (v: string) => void; reject: (e: Error) => void }[] = []
  const run = vi.fn(
    (input: string) =>
      new Promise<string>((resolve, reject) => {
        calls.push({ input, resolve, reject })
      }),
  )
  return { run, calls }
}

describe("useGraphLayout", () => {
  it("lays out once per key, whatever the input's identity", async () => {
    const { run, calls } = controlledRun()
    const { result, rerender } = renderHook(({ k, input }) => useGraphLayout(k, input, run), {
      initialProps: { k: "a", input: "cards A" },
    })
    expect(result.current).toEqual({ settled: null, pending: true, error: null })
    await act(async () => calls[0].resolve("layout A"))
    expect(result.current).toEqual({ settled: { key: "a", input: "cards A", layout: "layout A" }, pending: false, error: null })

    // A refresh that rebuilds the same drawing is not laid out again.
    rerender({ k: "a", input: "cards A, refetched" })
    expect(run).toHaveBeenCalledTimes(1)
  })

  it("keeps the last drawing whole until the next one is ready, and draws the input it was laid out for", async () => {
    const { run, calls } = controlledRun()
    const { result, rerender } = renderHook(({ k, input }) => useGraphLayout(k, input, run), {
      initialProps: { k: "a", input: "cards A" },
    })
    await act(async () => calls[0].resolve("layout A"))
    rerender({ k: "b", input: "cards B" })
    expect(result.current.pending).toBe(true)
    expect(result.current.settled).toEqual({ key: "a", input: "cards A", layout: "layout A" })

    // The input changes again under the same key while B runs: B still answers with what it was given.
    rerender({ k: "b", input: "cards B2" })
    await act(async () => calls[1].resolve("layout B"))
    expect(calls[1].input).toBe("cards B")
    expect(result.current).toEqual({ settled: { key: "b", input: "cards B", layout: "layout B" }, pending: false, error: null })
  })

  it("drops an answer for a key that has since changed", async () => {
    const { run, calls } = controlledRun()
    const { result, rerender } = renderHook(({ k }) => useGraphLayout(k, k, run), { initialProps: { k: "a" } })
    rerender({ k: "b" })
    await act(async () => calls[1].resolve("layout B"))
    await act(async () => calls[0].resolve("layout A"))
    expect(result.current.settled?.layout).toBe("layout B")
    await act(async () => calls[0].reject(new Error("late")))
    expect(result.current.error).toBeNull()
  })

  it("reports a failure for the current key only", async () => {
    const { run, calls } = controlledRun()
    const { result, rerender } = renderHook(({ k }) => useGraphLayout(k, k, run), { initialProps: { k: "a" } })
    await act(async () => calls[0].reject(new Error("engine broke")))
    expect(result.current.error?.message).toBe("engine broke")
    expect(result.current.pending).toBe(false)

    rerender({ k: "b" })
    expect(result.current.error).toBeNull()
    expect(result.current.pending).toBe(true)
    await act(async () => calls[1].resolve("layout B"))
    expect(result.current.settled?.key).toBe("b")
  })
})
