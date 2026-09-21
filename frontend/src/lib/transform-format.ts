// Shared formatting for the transform-monitoring surface (rollup, config history,
// execution logs) so the same figure never renders two different ways across the
// panels — one source of truth for counts, ages and durations.

const numberFmt = new Intl.NumberFormat()

/** Locale-grouped integer, e.g. 100588 -> "100,588". */
export function formatCount(n: number | null | undefined): string {
  return numberFmt.format(Number(n ?? 0))
}

/** Coarse "how long ago" from an ISO timestamp. Guards NaN and clock skew (a
 *  future timestamp clamps to "just now" rather than rendering a negative age).
 *  Floored like every other age in the app: rounding made 15h40m read "16h ago"
 *  here while the pipeline and execution pages said "15h ago" (#56). */
export function formatAge(iso?: string | null): string {
  if (!iso) return "—"
  const t = new Date(iso).getTime()
  if (Number.isNaN(t)) return "—"
  const ms = Math.max(0, Date.now() - t)
  if (ms < 45_000) return "just now"
  const mins = Math.floor(ms / 60_000)
  if (mins < 60) return `${Math.max(1, mins)}m ago`
  const hrs = Math.floor(ms / 3_600_000)
  if (hrs < 24) return `${hrs}h ago`
  return `${Math.floor(ms / 86_400_000)}d ago`
}

/** Human duration from milliseconds, with an hours branch so a long run reads
 *  "1h 35m" instead of "95m 0s". This implementation moved to `@/lib/duration`,
 *  which every panel now shares; re-exported so its importers keep working. */
export { formatDuration } from "@/lib/duration"

/** Absolute timestamp for a screen-reader-reachable secondary line / title,
 *  e.g. "Jul 17, 11:46 AM". Empty string when the input is missing/invalid. */
export function formatAbsoluteTime(iso?: string | null): string {
  if (!iso) return ""
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return ""
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  })
}
