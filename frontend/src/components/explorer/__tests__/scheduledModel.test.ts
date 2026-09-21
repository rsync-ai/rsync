import { describe, expect, it } from "vitest"
import { describeUpstreamNextRun, formatDuration } from "@/components/explorer/scheduledModel"

describe("formatDuration", () => {
  // Seen in a browser on 2026-09-16: stg_orders' runs took 14 to 181 ms, and every one of
  // them read "0s" in the table and under the chart's pointer.
  it("counts milliseconds while a run is under a second", () => {
    expect(formatDuration(0)).toBe("0ms")
    expect(formatDuration(16)).toBe("16ms")
    expect(formatDuration(48)).toBe("48ms")
    expect(formatDuration(420)).toBe("420ms")
    expect(formatDuration(999)).toBe("999ms")
    // Rounds up into the next unit rather than printing "1000ms".
    expect(formatDuration(999.6)).toBe("1s")
  })

  it("keeps a tenth of a second from a second up to a minute", () => {
    expect(formatDuration(1_000)).toBe("1s")
    expect(formatDuration(1_240)).toBe("1.2s")
    expect(formatDuration(59_940)).toBe("59.9s")
  })

  // The zero padding this used to assert ("1m 00s", "1h 02m") was this panel's
  // alone: the pipeline pages, the DAG and the run tables all wrote "1m 5s".
  // One of the two spellings had to go, and padding a unit that is often zero
  // costs a column of width to say nothing. A unit that is zero is now dropped
  // outright, so exactly a minute reads "1m".
  it("switches to minutes and seconds at a minute", () => {
    expect(formatDuration(60_000)).toBe("1m")
    expect(formatDuration(65_000)).toBe("1m 5s")
    expect(formatDuration(3_599_000)).toBe("59m 59s")
  })

  it("switches to hours and minutes at an hour", () => {
    expect(formatDuration(3_600_000)).toBe("1h")
    expect(formatDuration(3_720_000)).toBe("1h 2m")
  })

  it("does not print a number for a duration that is not one", () => {
    expect(formatDuration(-1)).toBe("—")
    expect(formatDuration(Number.NaN)).toBe("—")
  })
})

describe("describeUpstreamNextRun", () => {
  const two = [
    { kind: "model", id: "m-1", name: "q_two" },
    { kind: "model", id: "m-2", name: "stg_orders" },
  ]

  it("does not say the next landing wakes a fan-in that waits on all of them", () => {
    expect(describeUpstreamNextRun({ upstreams: two, upstream_policy: "all" })).toBe(
      "When all its upstreams have run",
    )
  })

  it("says the next landing wakes a fan-in on any, or one sent without a policy", () => {
    expect(describeUpstreamNextRun({ upstreams: two, upstream_policy: "any" })).toBe("When an upstream runs")
    expect(describeUpstreamNextRun({ upstreams: two })).toBe("When an upstream runs")
  })

  // One upstream is all of them: the policy cannot change when it runs, and the cadence
  // sentence (describeUpstreamSet) ignores it there too.
  it("ignores the policy with a single upstream", () => {
    expect(describeUpstreamNextRun({ upstreams: two.slice(0, 1), upstream_policy: "all" })).toBe(
      "When an upstream runs",
    )
  })
})
