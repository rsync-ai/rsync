import { describe, it, expect } from "vitest"
import {
  summarizeRunDuration,
  type ExplorerStep,
} from "@/components/explorer/ExplorerStepTimeline"

const step = (over: Partial<ExplorerStep>): ExplorerStep => ({
  id: "s",
  type: "execute",
  status: "success",
  title: "Step",
  ...over,
})

describe("summarizeRunDuration", () => {
  it("does not print 0ms total when nothing was timed", () => {
    // The bug: `sum + (s.durationMs || 0)` turns 'we have no timings' into the
    // number 0 and the header typesets it as a measured total.
    const out = summarizeRunDuration([
      step({ id: "a", status: "success" }),
      step({ id: "b", status: "failed" }),
    ])
    expect(out.label).not.toMatch(/\b0ms\b/)
    expect(out.label).toBe("timing unavailable")
  })

  it("marks a partial total as a floor rather than a total", () => {
    const out = summarizeRunDuration([
      step({ id: "a", durationMs: 120 }),
      step({ id: "b" }),
    ])
    expect(out.label).toBe("≥ 120ms")
    expect(out.detail).toContain("1 of 2")
  })

  it("still reports a real total when every completed step is timed", () => {
    // Non-zero control: the fix must not degrade the good case.
    const out = summarizeRunDuration([
      step({ id: "a", durationMs: 120 }),
      step({ id: "b", durationMs: 380, status: "failed" }),
      step({ id: "c", status: "pending" }),
      step({ id: "d", status: "skipped" }),
    ])
    expect(out.label).toBe("500ms total")
  })

  it("says nothing has finished rather than claiming 0ms", () => {
    const out = summarizeRunDuration([
      step({ id: "a", status: "pending" }),
      step({ id: "b", status: "running" }),
    ])
    expect(out.label).toBe("not timed yet")
  })
})
