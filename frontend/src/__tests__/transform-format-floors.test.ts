import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { formatAge, formatDuration } from "@/lib/transform-format"

// #56: the transform panel's freshness rounded ("16h ago") while the pipeline
// and execution pages floored the same moment ("15h ago"). #37's class too:
// a rounded remainder printed "2m 60s".
describe("transform-format floors like the rest of the app", () => {
  const NOW = Date.parse("2026-09-18T12:00:00Z")
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(NOW)
  })
  afterEach(() => vi.useRealTimers())

  const ago = (ms: number) => new Date(NOW - ms).toISOString()

  it("15h40m is 15h ago, not 16h", () => {
    expect(formatAge(ago(15 * 3_600_000 + 40 * 60_000))).toBe("15h ago")
  })
  it("59m40s is 59m ago, not 60m", () => {
    expect(formatAge(ago(59 * 60_000 + 40_000))).toBe("59m ago")
  })
  it("1d20h is 1d ago, not 2d", () => {
    expect(formatAge(ago(44 * 3_600_000))).toBe("1d ago")
  })
  it("control: 50s is 1m ago and 10s is just now", () => {
    expect(formatAge(ago(50_000))).toBe("1m ago")
    expect(formatAge(ago(10_000))).toBe("just now")
  })
  it("durations never print 60 in the remainder", () => {
    expect(formatDuration(2 * 60_000 + 59_600)).toBe("2m 59s")
    expect(formatDuration(3_600_000 + 59 * 60_000 + 40_000)).toBe("1h 59m")
    expect(formatDuration(4 * 60_000 + 45_000)).toBe("4m 45s")
  })
})
