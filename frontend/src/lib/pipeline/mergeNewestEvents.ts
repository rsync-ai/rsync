/**
 * Folding a freshly polled newest page of `GET /pipelines/:id/events` into the
 * rows already on screen.
 *
 * The endpoint pages newest-first with an opaque cursor, and the Activity list
 * may hold several pages the user pulled in with "Load more". A poll re-reads
 * only the newest page, so:
 *
 *   - overlap (some fresh row is already shown): prepend what is new and keep
 *     everything older, so the loaded pages and their cursor stay valid;
 *   - no overlap: either more rows arrived than one page holds (a gap between
 *     the fresh page and what is shown), or the page is short and the shown
 *     rows no longer exist on the server. Stitching would present a hole, or
 *     deleted rows, as history, so the fresh page replaces the list and the
 *     caller re-seats the cursor from this response. Nothing shown yet is the
 *     same case.
 *
 * `replaced` tells the caller which of these happened.
 */
export function mergeNewestPage<T extends { event_id: string }>(
  prev: T[],
  fresh: T[],
): { events: T[]; replaced: boolean } {
  const shown = new Set(prev.map((e) => e.event_id))
  if (!fresh.some((e) => shown.has(e.event_id))) return { events: fresh, replaced: true }
  const freshIds = new Set(fresh.map((e) => e.event_id))
  return { events: [...fresh, ...prev.filter((e) => !freshIds.has(e.event_id))], replaced: false }
}
