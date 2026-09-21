/**
 * Words for why a model ran, why it did not, and what its trigger waits for.
 *
 * Shared by the Schedules page and the model dialog so the two cannot describe the same
 * trigger differently. Everything here reads fields the gateway joins or stores
 * (migration 104); nothing resolves an id itself.
 */

export type UpstreamPolicy = "any" | "all"

/** The provenance half of a saved_query_runs row, as GET /explorer/saved/:id/runs returns it. */
export interface RunProvenanceFields {
  trigger_source: string
  upstream_kind?: string
  upstream_id?: string
  upstream_name?: string
  upstream_run_id?: string
  origin_execution_id?: string
  trigger_depth?: number
  coalesced_count?: number
  skip_reason?: string
}

/**
 * "After orders_sync runs", "After any of a, b runs" or "After all of a, b run".
 *
 * The id stands in for a name the join could not find: it is what the trigger still
 * points at when the producer behind it was deleted, or is a model the caller cannot see.
 */
export function describeUpstreamSet(
  upstreams: { id: string; name?: string }[] | undefined,
  policy?: string,
): string {
  const names = (upstreams ?? []).map((u) => u.name?.trim() || u.id).filter(Boolean)
  if (names.length === 0) return "After an upstream runs"
  if (names.length === 1) return `After ${names[0]} runs`
  // "any of" / "all of", never a bare comma list: a plain list reads as whichever of the
  // two the reader already assumed, and they rebuild at very different moments.
  if (policy === "all") return `After all of ${names.join(", ")} run`
  return `After any of ${names.join(", ")} runs`
}

/** "manual", "scheduled", or "after orders_daily (depth 2, 3 coalesced)". */
export function describeRunOrigin(r: RunProvenanceFields): string {
  if (r.trigger_source !== "triggered") return r.trigger_source
  if (!r.upstream_kind) return "triggered"
  // A missing name is a producer deleted since, or a private model of someone else's.
  const who = r.upstream_name?.trim() || (r.upstream_kind === "pipeline" ? "a pipeline" : "a model")
  const details: string[] = []
  // Depth 1 is the ordinary case and says nothing; deeper tells the reader this run is
  // the tail of a chain, and which row to walk back to.
  if ((r.trigger_depth ?? 0) > 1) details.push(`depth ${r.trigger_depth}`)
  if ((r.coalesced_count ?? 0) > 1) details.push(`${r.coalesced_count} coalesced`)
  return details.length ? `after ${who} (${details.join(", ")})` : `after ${who}`
}

const SKIP_REASON_TEXT: Record<string, string> = {
  upstream_failed: "not rebuilt: its upstream failed",
  upstream_skipped: "not rebuilt: a model above it did not rebuild",
  chain_depth_exceeded: "not rebuilt: the rebuild chain reached its hop limit",
  waiting_on_upstreams: "not rebuilt yet: waiting for every upstream",
}

/**
 * The sentence for a deliberate skip, or null for a row that is not one. An unknown
 * reason from a newer gateway is shown verbatim rather than dropped: a skip with no
 * explanation is exactly what these rows exist to stop.
 */
export function describeSkipReason(r: { skip_reason?: string }): string | null {
  if (!r.skip_reason) return null
  return SKIP_REASON_TEXT[r.skip_reason] ?? `not rebuilt: ${r.skip_reason}`
}
