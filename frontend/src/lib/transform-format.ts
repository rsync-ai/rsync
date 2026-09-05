// Shared formatting for the transform-monitoring surface (rollup, config history,
// execution logs) so the same figure never renders two different ways across the
// panels — one source of truth for counts, ages and durations.

const numberFmt = new Intl.NumberFormat()

/** Locale-grouped integer, e.g. 100588 -> "100,588". */
export function formatCount(n: number | null | undefined): string {
  return numberFmt.format(Number(n ?? 0))
}

/** Coarse "how long ago" from an ISO timestamp. Guards NaN and clock skew (a
 *  future timestamp clamps to "just now" rather than rendering a negative age). */
export function formatAge(iso?: string | null): string {
  if (!iso) return "—"
  const t = new Date(iso).getTime()
  if (Number.isNaN(t)) return "—"
  const ms = Math.max(0, Date.now() - t)
  if (ms < 45_000) return "just now"
  const mins = Math.round(ms / 60_000)
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.round(ms / 3_600_000)
  if (hrs < 24) return `${hrs}h ago`
  return `${Math.round(ms / 86_400_000)}d ago`
}

/** Human duration from milliseconds, with an hours branch so a long run reads
 *  "1h 35m" instead of "95m 0s". */
export function formatDuration(ms: number | null | undefined): string {
  const v = Number(ms ?? 0)
  if (!v || v < 0) return "0ms"
  if (v < 1000) return `${Math.round(v)}ms`
  if (v < 60_000) return `${(v / 1000).toFixed(1)}s`
  if (v < 3_600_000) {
    const m = Math.floor(v / 60_000)
    const s = Math.round((v % 60_000) / 1000)
    return s ? `${m}m ${s}s` : `${m}m`
  }
  const h = Math.floor(v / 3_600_000)
  const m = Math.round((v % 3_600_000) / 60_000)
  return m ? `${h}h ${m}m` : `${h}h`
}

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
