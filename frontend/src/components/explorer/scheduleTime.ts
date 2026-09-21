// utils.formatRelativeTime is past-only: it subtracts in one direction and floors,
// so every future timestamp lands in its `return "Just now"` fallback. A next-run
// time is always in the future, which is exactly the case it cannot express — hence
// a forward-looking formatter rather than a call to that one.
//
// next_run_at is computed server-side (api-gateway nextScheduleRun) rather than in
// the browser, so these helpers only format an instant they are handed. They never
// derive one from a cron string: two implementations of the same schedule arithmetic
// is precisely how a UI starts disagreeing with the scheduler that actually fires.

/**
 * "in 4m" / "in 3h" / "in 2d" for a future instant.
 *
 * Returns "due now" rather than a negative or a past-tense phrase for a time that
 * has already passed: between a tick firing and the list refetching, next_run_at is
 * legitimately a moment in the past, and "3s ago" there reads as a missed run.
 */
export function formatNextRun(iso: string): string {
  const ms = new Date(iso).getTime() - Date.now()
  if (Number.isNaN(ms)) return ""
  if (ms <= 0) return "due now"

  const minutes = Math.floor(ms / 60000)
  if (minutes < 1) return "in <1m"
  if (minutes < 60) return `in ${minutes}m`

  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `in ${hours}h`
  return `in ${Math.floor(hours / 24)}d`
}

/**
 * The absolute local time, for the title attribute. A relative string is easier to
 * read at a glance but ambiguous when it matters — someone deciding whether to wait
 * for the next rebuild or force one needs the wall-clock time, in their own zone.
 *
 * Defined in @/lib/utils so pages outside the explorer share it; re-exported here
 * for the existing imports.
 */
export { formatAbsoluteTime } from "@/lib/utils"

// ---------------------------------------------------------------------------
// Refetching when a next run comes due
// ---------------------------------------------------------------------------
// A list fetched once shows next_run_at as it was at fetch time. When that instant
// passes, the tick has fired and the server already knows the following one, but
// nothing re-asks — so the row sits on "due now" indefinitely. These helpers tell a
// list when to re-ask, and what to say in the gap before it does.

/**
 * How long after a next run comes due to refetch. The server recomputes next_run_at
 * from its own clock, so asking at the exact instant would often get the same
 * (now past) time back; a few seconds' grace absorbs the tick and ordinary skew.
 */
export const NEXT_RUN_REFETCH_GRACE_MS = 5_000

/**
 * The longest a refetch is ever put off. setTimeout stores its delay as a signed
 * 32-bit int, and anything above 2^31-1 ms (~24.8 days) fires immediately instead —
 * a monthly schedule would otherwise refetch in a tight loop. An hour is far inside
 * that bound, and a timer that re-arms hourly costs nothing.
 */
export const NEXT_RUN_REFETCH_MAX_DELAY_MS = 60 * 60 * 1000

/**
 * Milliseconds until a list showing these next_run_at values should refetch, or
 * null when nothing needs it (every value is missing or unparseable).
 *
 * An upcoming time is refetched NEXT_RUN_REFETCH_GRACE_MS after it; the earliest one
 * wins. A time already past — the browser clock ahead of the server's, or a refetch
 * that came back before the server moved on — is retried after as long as it has
 * been overdue (never less than the grace). That backs off on its own, so a value
 * the server keeps reporting in the past costs a handful of requests, not a loop.
 */
export function nextRunRefetchDelay(
  nextRunAts: ReadonlyArray<string | null | undefined>,
  now: number = Date.now(),
): number | null {
  let delay: number | null = null
  for (const iso of nextRunAts) {
    if (!iso) continue
    const at = new Date(iso).getTime()
    if (Number.isNaN(at)) continue
    const d =
      at > now
        ? at - now + NEXT_RUN_REFETCH_GRACE_MS
        : Math.max(NEXT_RUN_REFETCH_GRACE_MS, now - at)
    if (delay === null || d < delay) delay = d
  }
  if (delay === null) return null
  return Math.min(delay, NEXT_RUN_REFETCH_MAX_DELAY_MS)
}

/**
 * The next-run label for a time that may already have passed. An upcoming time reads
 * as formatNextRun does ("in 4m"). A past one reads as the wall-clock time it was
 * due ("due Aug 15, 10:00 AM UTC") rather than only "due now", which after a missed
 * refetch would say nothing about when.
 *
 * The absolute formatter is passed in (callers hand it formatAbsoluteTime) rather than
 * named here, so this helper does not depend on where that function is defined. `now`
 * decides past versus upcoming; the upcoming wording itself is formatNextRun's, which
 * reads the real clock.
 */
export function formatNextRunOrDue(
  iso: string,
  formatAbsolute: (iso: string) => string,
  now: number = Date.now(),
): string {
  const at = new Date(iso).getTime()
  if (Number.isNaN(at)) return ""
  if (at > now) return formatNextRun(iso)
  const absolute = formatAbsolute(iso)
  return absolute ? `due ${absolute}` : "due now"
}
