/**
 * "Loading strategy…" stayed forever after a run completed: the card fetched the
 * pipeline once, with no timeout (authFetch has none by default) and no retry.
 * The fetch is now bounded and retried before the card gives up.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, act } from "@testing-library/react"
import "@testing-library/jest-dom"

const getPipeline = vi.fn()
vi.mock("@/lib/api/pipelines", () => ({
  getPipeline: (...a: unknown[]) => getPipeline(...a),
}))

import {
  DataLoadingStrategyCard,
  STRATEGY_FETCH_MAX_RETRIES,
  STRATEGY_FETCH_RETRY_DELAY_MS,
  STRATEGY_FETCH_TIMEOUT_MS,
} from "@/components/pipeline/DataLoadingStrategyCard"

async function flush() {
  await act(async () => {
    await Promise.resolve()
    await Promise.resolve()
  })
}

describe("DataLoadingStrategyCard fetch", () => {
  beforeEach(() => {
    vi.useFakeTimers()
    getPipeline.mockReset()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it("passes a timeout and recovers from a failed first attempt", async () => {
    getPipeline
      .mockRejectedValueOnce(new Error("Request timed out"))
      .mockResolvedValueOnce({ data_loading_strategy: { mode: "batch", selected_tables: ["datingapp.matches"] } })

    render(<DataLoadingStrategyCard pipelineId="p1" />)
    await flush()
    expect(getPipeline).toHaveBeenCalledWith("p1", { timeoutMs: STRATEGY_FETCH_TIMEOUT_MS })
    // A transient failure keeps the loading label rather than flashing an error.
    expect(screen.queryByText("Request timed out")).not.toBeInTheDocument()

    await act(async () => {
      vi.advanceTimersByTime(STRATEGY_FETCH_RETRY_DELAY_MS)
    })
    await flush()
    expect(getPipeline).toHaveBeenCalledTimes(2)
    expect(screen.queryAllByText("Loading strategy…")).toHaveLength(0)
  })

  it("stops loading and shows the error once retries are exhausted", async () => {
    getPipeline.mockRejectedValue(new Error("Request timed out"))

    render(<DataLoadingStrategyCard pipelineId="p1" />)
    await flush()
    for (let i = 0; i < STRATEGY_FETCH_MAX_RETRIES; i++) {
      await act(async () => {
        vi.advanceTimersByTime(STRATEGY_FETCH_RETRY_DELAY_MS)
      })
      await flush()
    }
    expect(getPipeline).toHaveBeenCalledTimes(STRATEGY_FETCH_MAX_RETRIES + 1)
    expect(screen.getByText("Request timed out")).toBeInTheDocument()
    expect(screen.queryAllByText("Loading strategy…")).toHaveLength(0)
  })
})
