/**
 * Whether a polled live view is still in contact with the server.
 *
 * The pipeline views are driven entirely by a short-interval poll of
 * `/pipelines/:id/state`. Both polls used to answer every failure the same way:
 * a bare `return` on a non-ok status and an empty catch. Nothing downstream ever
 * learned that contact had been lost, so the last state received stayed on
 * screen -- a stage still marked "running", its clock still ticking -- for as
 * long as the tab stayed open. The screen kept asserting live progress about a
 * pipeline it had stopped hearing from, which is the one thing it must not do.
 *
 * A single miss is not an outage: polls race navigation, sleep, and ordinary
 * blips. Only a run of consecutive failures is, and until then the view is left
 * alone.
 */

import { formatDuration } from "@/lib/duration"

export const FEED_STALE_AFTER_FAILURES = 3

export interface FeedHealth {
  /** Consecutive failed polls. Reset to 0 by every success. */
  consecutiveFailures: number
  /** Epoch ms of the last poll that returned usable state, or null. */
  lastSuccessAt: number | null
}

export const healthyFeed: FeedHealth = { consecutiveFailures: 0, lastSuccessAt: null }

export function feedSucceeded(now: number = Date.now()): FeedHealth {
  return { consecutiveFailures: 0, lastSuccessAt: now }
}

export function feedFailed(prev: FeedHealth): FeedHealth {
  return { ...prev, consecutiveFailures: prev.consecutiveFailures + 1 }
}

/**
 * A sentence for the user, or `null` while the feed is healthy.
 *
 * Deliberately phrased as a statement about THIS VIEW, not about the pipeline:
 * losing contact says nothing about whether the run is still going, and
 * claiming otherwise would repeat the original mistake in the other direction.
 */
export function describeFeedOutage(health: FeedHealth, now: number = Date.now()): string | null {
  if (health.consecutiveFailures < FEED_STALE_AFTER_FAILURES) return null

  if (health.lastSuccessAt === null) {
    return "Live updates have not started. Nothing below has been confirmed by the server."
  }

  // Through the shared formatter, so the silence is spelled the way every other
  // span in the app is. Floored at a second because the notice only appears
  // after three failed polls -- a sub-second gap is not reachable in practice,
  // and "stopped 0ms ago" would read as a glitch rather than as an outage.
  const ago = formatDuration(Math.max(1000, now - health.lastSuccessAt))
  return `Live updates stopped ${ago} ago. What you see below is the last state the server sent, not the run's current state.`
}
