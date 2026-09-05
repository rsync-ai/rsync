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
 */
export function formatAbsoluteTime(iso: string): string {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return ""
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
  })
}
