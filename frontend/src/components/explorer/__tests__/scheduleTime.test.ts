import { describe, it, expect, vi, afterEach } from "vitest"
import { formatAbsoluteTime, formatNextRun } from "@/components/explorer/scheduleTime"

// These strings are what a user reads to decide whether to wait for the next
// scheduled rebuild or force one now. The failure mode is not a crash — it is a
// confident, wrong sentence, so each case below is one way of being wrong.

afterEach(() => {
  vi.useRealTimers()
})

function at(iso: string) {
  vi.useFakeTimers()
  vi.setSystemTime(new Date(iso))
}

describe("formatNextRun", () => {
  it("counts forward, unlike utils.formatRelativeTime which only counts back", () => {
    at("2026-08-15T10:00:00Z")
    // The bug this guards: a past-only formatter floors every future instant into
    // its "Just now" fallback, so a run 3 hours out reads as one happening now.
    expect(formatNextRun("2026-08-15T13:00:00Z")).toBe("in 3h")
    expect(formatNextRun("2026-08-15T10:04:00Z")).toBe("in 4m")
    expect(formatNextRun("2026-08-17T10:00:00Z")).toBe("in 2d")
  })

  it("rounds a sub-minute wait down to a bound, never to zero", () => {
    at("2026-08-15T10:00:00Z")
    // "in 0m" reads as "not scheduled".
    expect(formatNextRun("2026-08-15T10:00:30Z")).toBe("in <1m")
  })

  it("says a time that has already passed is due, not overdue", () => {
    at("2026-08-15T10:00:00Z")
    // next_run_at is legitimately in the past between a tick firing and the list
    // refetching. "3s ago" there reads as a run that was missed.
    expect(formatNextRun("2026-08-15T09:59:57Z")).toBe("due now")
    expect(formatNextRun("2026-08-15T10:00:00Z")).toBe("due now")
  })

  it("returns empty rather than NaN for an unparseable timestamp", () => {
    expect(formatNextRun("not a date")).toBe("")
  })
})

describe("formatAbsoluteTime", () => {
  it("returns empty rather than 'Invalid Date' for an unparseable timestamp", () => {
    expect(formatAbsoluteTime("not a date")).toBe("")
  })

  it("renders something for a valid timestamp", () => {
    // Deliberately not pinning the exact string: it is locale- and zone-dependent,
    // and asserting one would only pin the CI runner's environment.
    expect(formatAbsoluteTime("2026-08-15T10:00:00Z")).not.toBe("")
  })
})
