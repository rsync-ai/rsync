/**
 * Per-table row-count helpers for the pipeline monitoring panel.
 */

/**
 * sameCounts reports whether two per-table row-count maps carry the same numbers.
 *
 * The monitoring panel refreshes these maps on a timer so a streaming pipeline's
 * "Expected rows" stops reporting the count it read when the page loaded. The
 * effect that consumes them queries the SOURCE database, and it re-runs on object
 * identity — so a refresh that found nothing new must hand back the PREVIOUS
 * object rather than an equal-but-new one, or every tick would cost a source query
 * for no new information.
 */
export function sameCounts(a: Record<string, number>, b: Record<string, number>): boolean {
  const keysA = Object.keys(a)
  if (keysA.length !== Object.keys(b).length) return false
  for (const k of keysA) {
    // Object.is, not ===, so a NaN that slipped through compares equal to itself
    // and does not force an endless "changed!" refresh.
    if (!Object.is(a[k], b[k])) return false
  }
  return true
}
