/**
 * Load and save a pipeline's whole transform plan — GET/POST
 * /api/v1/transforms/pipeline/:pipeline_id.
 *
 * This is what the Transform Builder page's "Save Plan" button was missing: it
 * had no `onClick` at all (transforms/page.tsx), so a plan built there could be
 * previewed and then only thrown away.
 *
 * SAVE IS A REPLACE, NOT AN APPEND. SavePipelineTransforms
 * (transforms.go:432-462) opens a transaction, runs
 * `DELETE FROM transform_definitions WHERE pipeline_id = $1`, and re-inserts
 * whatever the request carried. So a save from an empty builder DELETES every
 * transform the pipeline had — including the `mask_pii` rows the NL gate
 * materialized, whose absence makes the next run copy those columns to the
 * destination in the clear (nl_transforms_gate.go:584 only fails closed on
 * rows that exist and are enabled).
 *
 * That is why this module leads with `loadPipelineTransformPlan`: the builder
 * has to show what is already there before it can be allowed to overwrite it,
 * and `describeReplace` has to say — in the confirm dialog, in numbers — what
 * the save is about to remove.
 */

import { authFetch } from "@/lib/api/auth-fetch"

/** A builder row. Structurally the page's TransformRule, minus its operation union. */
export type PlanRule = {
  id: string
  order: number
  type: "producer" | "consumer"
  operation: string
  enabled: boolean
  config: Record<string, unknown>
  description?: string
}

/** A row as the API stores it. */
export type TransformDefinition = {
  id: string
  pipeline_id?: string
  transform_type: string
  transform_order: number
  transform_config: Record<string, unknown>
  enabled: boolean
}

export type PipelineTransformsResponse = {
  pipeline_id?: string
  producer_transforms?: TransformDefinition[] | null
  consumer_transforms?: TransformDefinition[] | null
}

export function transformPlanUrl(pipelineId: string): string {
  return `/api/v1/transforms/pipeline/${encodeURIComponent(pipelineId)}`
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

/**
 * Builder rows → stored rows.
 *
 * `transform_config` carries `operation` alongside the operation's own fields,
 * because that is where every reader looks for it (`operationOf` in
 * PipelineTransformsTab.tsx:106 reads `transform_config.operation`). An id that
 * is not a UUID is dropped rather than sent: the column is `uuid`, so a
 * client-minted non-UUID would fail the INSERT and roll the whole save back,
 * whereas an empty id makes the handler mint one (transforms.go:443).
 */
export function toDefinitions(rules: PlanRule[]): TransformDefinition[] {
  return rules.map((r, i) => ({
    id: UUID_RE.test(r.id) ? r.id : "",
    transform_type: r.type,
    // The handler overwrites this with the array index anyway; sending the same
    // number keeps the request readable rather than pretending to disagree.
    transform_order: i,
    transform_config: {
      operation: r.operation,
      ...(r.description ? { description: r.description } : {}),
      ...(r.config || {}),
    },
    enabled: r.enabled,
  }))
}

/**
 * Stored rows → builder rows, producers first then consumers, each in
 * `transform_order`. The two arrays come back already split by the handler
 * (transforms.go:381-385); the builder keeps them in one list and re-splits by
 * `type` for display.
 */
export function fromDefinitions(resp: PipelineTransformsResponse | null): PlanRule[] {
  const all = [...(resp?.producer_transforms || []), ...(resp?.consumer_transforms || [])]
  all.sort((a, b) => (a.transform_order ?? 0) - (b.transform_order ?? 0))
  return all.map((t, i) => {
    const cfg = { ...(t.transform_config || {}) }
    const operation = String(cfg.operation || "")
    const description = typeof cfg.description === "string" ? cfg.description : undefined
    delete cfg.operation
    delete cfg.description
    return {
      id: t.id,
      order: i,
      type: t.transform_type === "consumer" ? "consumer" : "producer",
      operation,
      enabled: Boolean(t.enabled),
      config: cfg,
      description,
    }
  })
}

/**
 * Rows whose whole purpose is keeping values out of the destination.
 * `hash` counts: the Hash Column card is mask_pii with mask_type=hash
 * (transforms/validate.go normalizeType), so dropping one uncounted would
 * under-report exactly the rules describeReplace exists to warn about.
 */
const MASKING_OPS = new Set(["mask", "mask_pii", "hash"])

export function isMaskingOp(operation: unknown): boolean {
  return MASKING_OPS.has(String(operation || "").trim().toLowerCase())
}

export function countMasks(rules: PlanRule[]): number {
  return rules.filter((r) => isMaskingOp(r.operation)).length
}

/**
 * The sentence the confirm dialog shows. Written in counts, because "this will
 * replace your transforms" is exactly the phrasing people click through: the
 * number of rows about to be deleted is what makes it land.
 */
export function describeReplace(existing: PlanRule[], next: PlanRule[]): string {
  if (existing.length === 0) {
    return `This pipeline has no transforms yet. Saving writes ${next.length} ${
      next.length === 1 ? "rule" : "rules"
    }.`
  }

  const masks = countMasks(existing)
  const keptIds = new Set(next.map((r) => r.id))
  const droppedMasks = existing.filter((r) => isMaskingOp(r.operation) && !keptIds.has(r.id))

  const base =
    `Saving REPLACES every transform on this pipeline: the ${existing.length} existing ` +
    `${existing.length === 1 ? "rule is" : "rules are"} deleted and ${next.length} ` +
    `${next.length === 1 ? "rule is" : "rules are"} written in their place.`

  if (droppedMasks.length === 0) {
    return masks > 0
      ? `${base} All ${masks} masking ${masks === 1 ? "rule is" : "rules are"} still in the builder and will be kept.`
      : base
  }

  const cols = droppedMasks
    .map((r) => String(r.config?.column || r.config?.columns || r.operation))
    .join(", ")
  return (
    `${base} ${droppedMasks.length} masking ${droppedMasks.length === 1 ? "rule" : "rules"} ` +
    `(${cols}) ${droppedMasks.length === 1 ? "is" : "are"} NOT in the builder and will be deleted — ` +
    `the next run copies those columns to the destination UNMASKED.`
  )
}

export type PlanResult<T> = { ok: true; data: T } | { ok: false; error: string }

async function readError(res: Response): Promise<string> {
  const data = (await res.json().catch(() => null)) as { error?: string; message?: string } | null
  if (res.status === 404) {
    return data?.message || "Pipeline not found in this workspace."
  }
  return data?.error || data?.message || `HTTP ${res.status}`
}

export async function loadPipelineTransformPlan(pipelineId: string): Promise<PlanResult<PlanRule[]>> {
  let res: Response
  try {
    res = await authFetch(transformPlanUrl(pipelineId), { cache: "no-store" })
  } catch {
    return { ok: false, error: "The API is unreachable." }
  }
  if (!res.ok) return { ok: false, error: await readError(res) }
  const data = (await res.json().catch(() => null)) as PipelineTransformsResponse | null
  return { ok: true, data: fromDefinitions(data) }
}

export async function savePipelineTransformPlan(
  pipelineId: string,
  rules: PlanRule[]
): Promise<PlanResult<{ count: number }>> {
  let res: Response
  try {
    res = await authFetch(transformPlanUrl(pipelineId), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ transforms: toDefinitions(rules) }),
    })
  } catch {
    return { ok: false, error: "The API is unreachable — nothing was saved." }
  }
  if (!res.ok) return { ok: false, error: await readError(res) }
  const data = (await res.json().catch(() => null)) as { count?: number } | null
  return { ok: true, data: { count: data?.count ?? rules.length } }
}

/**
 * A stable fingerprint of a plan's *content*, for answering "is there unsaved
 * work here".
 *
 * The builder held a plan entirely in React state with nothing watching it. A
 * reload, a back button, or picking a different pipeline from the selector
 * replaced that state without a word, and everything built since the last save
 * was gone — including a plan the NL generator had just produced, which can
 * take a minute to regenerate and does not come back identical.
 *
 * Content, not identity: row `id` is deliberately excluded, because the server
 * mints ids on save and a re-read would otherwise always look like a change.
 * Order is included by position rather than by the `order` field, since that is
 * what the save actually writes.
 */
export function planFingerprint(rules: PlanRule[]): string {
  return JSON.stringify(
    rules.map((r) => [
      r.type,
      r.operation,
      r.enabled,
      r.description ?? "",
      // Keys sorted so a config rebuilt in a different order is not a change.
      Object.keys(r.config ?? {})
        .sort()
        .map((k) => [k, (r.config ?? {})[k]]),
    ])
  )
}

/** True when the builder holds work that the last load/save does not account for. */
export function isPlanDirty(savedFingerprint: string, rules: PlanRule[]): boolean {
  return planFingerprint(rules) !== savedFingerprint
}
