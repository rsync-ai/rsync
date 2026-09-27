import { describe, it, expect } from "vitest"
import {
  describeFeedOutage,
  feedFailed,
  feedSucceeded,
  healthyFeed,
  FEED_STALE_AFTER_FAILURES,
} from "@/lib/polling/feedHealth"

const failNTimes = (n: number, from = healthyFeed) => {
  let h = from
  for (let i = 0; i < n; i++) h = feedFailed(h)
  return h
}

describe("feed health", () => {
  it("says nothing while the feed is healthy", () => {
    // Non-zero control: the normal case must stay untouched.
    expect(describeFeedOutage(healthyFeed)).toBeNull()
    expect(describeFeedOutage(feedSucceeded())).toBeNull()
  })

  it("tolerates a blip without crying outage", () => {
    // Non-zero control: polls race navigation and sleep; one miss is not an
    // outage, and warning on it would train the user to ignore the warning.
    for (let n = 1; n < FEED_STALE_AFTER_FAILURES; n++) {
      expect(describeFeedOutage(failNTimes(n))).toBeNull()
    }
  })

  it("announces a run of failures instead of leaving the last state on screen", () => {
    // The bug: nothing downstream ever learned that contact was lost, so a
    // stage stayed "running" with a ticking clock indefinitely.
    const now = Date.now()
    const health = failNTimes(FEED_STALE_AFTER_FAILURES, feedSucceeded(now - 42_000))
    const msg = describeFeedOutage(health, now)
    expect(msg).toContain("42s ago")
    expect(msg).toContain("not the run's current state")
  })

  it("does not claim the run stopped, only that the updates did", () => {
    // Losing contact says nothing about whether the pipeline is still going.
    const msg = describeFeedOutage(failNTimes(FEED_STALE_AFTER_FAILURES, feedSucceeded()), Date.now())!
    expect(msg).not.toMatch(/pipeline (has )?(failed|stopped)/i)
  })

  it("handles never having reached the server at all", () => {
    expect(describeFeedOutage(failNTimes(FEED_STALE_AFTER_FAILURES))).toContain(
      "have not started"
    )
  })

  it("a success clears the outage", () => {
    // Non-zero control: recovery must actually recover.
    const recovered = feedSucceeded()
    expect(failNTimes(FEED_STALE_AFTER_FAILURES).consecutiveFailures).toBe(FEED_STALE_AFTER_FAILURES)
    expect(recovered.consecutiveFailures).toBe(0)
    expect(describeFeedOutage(recovered)).toBeNull()
  })

  it("spells the silence with the shared duration formatter", () => {
    // Not a private unit ladder: `@/lib/duration` is the one place that renders
    // a span, and a ninth copy here is exactly what its guard test exists to
    // stop. These are its renderings, not invented ones.
    const now = Date.now()
    const at = (agoMs: number) =>
      describeFeedOutage(failNTimes(FEED_STALE_AFTER_FAILURES, feedSucceeded(now - agoMs)), now)!
    expect(at(90_000)).toContain("1m 30s ago")
    expect(at(7_200_000)).toContain("2h ago")
    // Never a measured-looking zero, however fresh the last success was.
    expect(at(0)).not.toContain("0ms")
  })
})
