import { describe, it, expect, vi, afterEach } from "vitest"
import {
  NEXT_RUN_REFETCH_GRACE_MS,
  NEXT_RUN_REFETCH_MAX_DELAY_MS,
  formatNextRunOrDue,
  nextRunRefetchDelay,
} from "@/components/explorer/scheduleTime"

// The bug these guard: a schedules list fetched once kept showing "due now" after the
// tick fired, because nothing ever asked the server for the following next_run_at.
// nextRunRefetchDelay decides when a list re-asks; formatNextRunOrDue decides what the
// row says until it has.

const NOW = new Date("2026-08-15T10:00:00Z").getTime()
const SECOND = 1000
const MINUTE = 60 * SECOND

function iso(ms: number) {
  return new Date(ms).toISOString()
}

afterEach(() => {
  vi.useRealTimers()
})

describe("nextRunRefetchDelay", () => {
  it("refetches a grace period after the EARLIEST upcoming run, whatever the order", () => {
    const later = iso(NOW + 60 * MINUTE)
    const earliest = iso(NOW + 4 * MINUTE)
    const middle = iso(NOW + 10 * MINUTE)
    expect(nextRunRefetchDelay([later, earliest, middle], NOW)).toBe(
      4 * MINUTE + NEXT_RUN_REFETCH_GRACE_MS,
    )
    // Order must not matter: the first or last element winning is the easy mistake.
    expect(nextRunRefetchDelay([earliest, middle, later], NOW)).toBe(
      4 * MINUTE + NEXT_RUN_REFETCH_GRACE_MS,
    )
  })

  it("never refetches at the due instant itself, when the server would still report it", () => {
    expect(nextRunRefetchDelay([iso(NOW + 1)], NOW)).toBe(1 + NEXT_RUN_REFETCH_GRACE_MS)
  })

  it("retries a past time after the grace, then backs off as it stays overdue", () => {
    // Just overdue: the tick fired a moment ago and the list has not caught up.
    expect(nextRunRefetchDelay([iso(NOW - 1 * SECOND)], NOW)).toBe(NEXT_RUN_REFETCH_GRACE_MS)
    expect(nextRunRefetchDelay([iso(NOW)], NOW)).toBe(NEXT_RUN_REFETCH_GRACE_MS)
    // Still overdue a minute later (a server that keeps reporting it): wait as long
    // again, so a stuck value costs a handful of requests, not one every 5s forever.
    expect(nextRunRefetchDelay([iso(NOW - 1 * MINUTE)], NOW)).toBe(1 * MINUTE)
  })

  it("lets an upcoming run that comes due sooner win over a long-overdue one", () => {
    expect(nextRunRefetchDelay([iso(NOW - 30 * MINUTE), iso(NOW + 2 * MINUTE)], NOW)).toBe(
      2 * MINUTE + NEXT_RUN_REFETCH_GRACE_MS,
    )
  })

  it("ignores null, undefined, empty and unparseable values", () => {
    expect(nextRunRefetchDelay([], NOW)).toBeNull()
    expect(nextRunRefetchDelay([null, undefined, "", "not a date"], NOW)).toBeNull()
    // And they do not poison the pick among valid ones.
    expect(nextRunRefetchDelay([null, "garbage", iso(NOW + 3 * MINUTE), undefined], NOW)).toBe(
      3 * MINUTE + NEXT_RUN_REFETCH_GRACE_MS,
    )
  })

  it("clamps far-future runs below setTimeout's 2^31-1 ms overflow", () => {
    // A monthly schedule is ~30 days out, past the int32 bound where setTimeout fires
    // immediately — which would refetch in a tight loop.
    const monthly = nextRunRefetchDelay([iso(NOW + 30 * 24 * 60 * MINUTE)], NOW)
    expect(monthly).toBe(NEXT_RUN_REFETCH_MAX_DELAY_MS)
    expect(monthly!).toBeLessThanOrEqual(2 ** 31 - 1)
    // Overdue for days backs off to the same ceiling, not past it.
    expect(nextRunRefetchDelay([iso(NOW - 40 * 24 * 60 * MINUTE)], NOW)).toBe(
      NEXT_RUN_REFETCH_MAX_DELAY_MS,
    )
    expect(NEXT_RUN_REFETCH_MAX_DELAY_MS).toBeLessThanOrEqual(2 ** 31 - 1)
  })

  it("defaults now to the clock", () => {
    vi.useFakeTimers()
    vi.setSystemTime(NOW)
    expect(nextRunRefetchDelay([iso(NOW + 4 * MINUTE)])).toBe(4 * MINUTE + NEXT_RUN_REFETCH_GRACE_MS)
  })
})

describe("formatNextRunOrDue", () => {
  const absolute = (s: string) => `ABS(${s})`

  it("reads an upcoming run the way formatNextRun does", () => {
    vi.useFakeTimers()
    vi.setSystemTime(NOW)
    expect(formatNextRunOrDue(iso(NOW + 4 * MINUTE), absolute)).toBe("in 4m")
  })

  it("names the time an overdue run was due instead of only 'due now'", () => {
    vi.useFakeTimers()
    vi.setSystemTime(NOW)
    const due = iso(NOW - 3 * SECOND)
    expect(formatNextRunOrDue(due, absolute)).toBe(`due ABS(${due})`)
    expect(formatNextRunOrDue(iso(NOW), absolute, NOW)).toBe(`due ABS(${iso(NOW)})`)
  })

  it("falls back to 'due now' when no absolute time can be formatted", () => {
    expect(formatNextRunOrDue(iso(NOW - MINUTE), () => "", NOW)).toBe("due now")
  })

  it("says nothing for an unparseable time", () => {
    expect(formatNextRunOrDue("not a date", absolute, NOW)).toBe("")
  })
})
