/**
 * Client for PUT/DELETE /api/v1/transforms/:id — the two routes registered at
 * api-gateway/internal/handlers/transforms.go:82-83 that had no caller, leaving
 * the Transforms tab read-only.
 *
 * TWO IDs, NOT ONE. The creation flow persists every transform twice with a
 * byte-identical config — once as a `producer` row (batch) and once as a
 * `consumer` row (CDC) — and `mergeTransforms` in PipelineTransformsTab
 * collapses that pair into a single logical entry that records BOTH ids. A
 * mutation that hit only `ids.batch` would leave the CDC copy running: for a
 * mask, that is the difference between "masking is off" and "masking is off on
 * one lane and the operator believes it is off on both". Every function here
 * therefore fans out over every id the logical transform carries and reports a
 * partial failure as a failure.
 *
 * WHY DISABLING A MASK IS GUARDED. `enabled` is not decorative. The Mongo CDC
 * fail-closed check in backend-orchestrator/internal/agents/executor/
 * nl_transforms_gate.go:584 loads masks `WHERE ... enabled = TRUE`, and
 * :530 folds the aliases `mask` and `mask_pii` together. So turning a mask off
 * removes it both from runtime application AND from the check that would have
 * refused the run — the next execution copies those columns to the destination
 * in the clear. That is a decision worth a typed confirmation, not a toggle.
 */

import { authFetch } from "@/lib/api/auth-fetch"

/** The ids a logical transform was merged from. */
export type TransformIds = { batch?: string; cdc?: string }

export function transformUrl(id: string): string {
  return `/api/v1/transforms/${encodeURIComponent(id)}`
}

/** Every id the logical row stands for, in a stable order. */
export function idsOf(ids: TransformIds): string[] {
  return [ids.batch, ids.cdc].filter((x): x is string => Boolean(x))
}

/**
 * Operations whose whole purpose is to keep values out of the destination.
 * Exactly the aliases the NL gate folds at nl_transforms_gate.go — the frontend
 * normalizes to `mask_pii`, but a row written directly through the API can
 * still carry a short form, so all three are listed. `hash` is one of them: the
 * Hash Column card is mask_pii with mask_type=hash, and a rule this set does
 * not recognize is a rule the operator gets to delete with no warning.
 */
const DE_IDENTIFYING_OPS = new Set(["mask", "mask_pii", "hash"])

export function isDeIdentifying(operation: string): boolean {
  return DE_IDENTIFYING_OPS.has(String(operation || "").trim().toLowerCase())
}

/**
 * The sentence shown before a de-identifying transform is turned off or
 * deleted. Deliberately concrete about the consequence and about WHEN it
 * happens — "the next run", not "may affect data".
 */
export function deIdentifyWarning(action: "disable" | "delete", sides: Array<"batch" | "cdc">): string {
  const where =
    sides.length === 2
      ? "on both the batch and the CDC lane"
      : sides[0] === "cdc"
        ? "on the CDC lane"
        : "on the batch lane"
  const verb = action === "delete" ? "Deleting" : "Disabling"
  return (
    `${verb} this masking rule ${where} means the next run copies these columns to the destination UNMASKED. ` +
    `It also removes them from the pre-run check that would otherwise have refused to start.`
  )
}

async function readError(res: Response): Promise<string> {
  const data = (await res.json().catch(() => null)) as { error?: string; message?: string } | null
  return data?.error || data?.message || `HTTP ${res.status}`
}

/**
 * Apply one mutation to every id behind a logical transform.
 *
 * Runs them sequentially rather than in parallel: the failure mode that matters
 * is "one lane changed and the other did not", and sequential means the
 * reported error names the first id that refused rather than an arbitrary
 * winner of a race. `applied` lets the caller say how far it got.
 */
async function fanOut(
  ids: TransformIds,
  apply: (id: string) => Promise<Response>
): Promise<{ ok: true } | { ok: false; error: string; applied: number }> {
  const list = idsOf(ids)
  if (list.length === 0) {
    return { ok: false, error: "This transform has no id to act on.", applied: 0 }
  }
  let applied = 0
  for (const id of list) {
    let res: Response
    try {
      res = await apply(id)
    } catch {
      return { ok: false, error: "The API is unreachable.", applied }
    }
    if (!res.ok) {
      return { ok: false, error: await readError(res), applied }
    }
    applied++
  }
  return { ok: true }
}

/**
 * Flip `enabled` on every row behind a logical transform.
 * UpdateTransform writes only the non-nil fields, so this sends `enabled`
 * alone and leaves `transform_config` / `transform_order` untouched.
 */
export function setTransformEnabled(ids: TransformIds, enabled: boolean) {
  return fanOut(ids, (id) =>
    authFetch(transformUrl(id), {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled }),
    })
  )
}

/** Remove every row behind a logical transform. Not recoverable from the UI. */
export function deleteTransform(ids: TransformIds) {
  return fanOut(ids, (id) => authFetch(transformUrl(id), { method: "DELETE" }))
}

/**
 * Message for a fan-out that changed one lane and then failed on the other.
 * This state is the reason the functions above report `applied`: silence here
 * would leave batch and CDC disagreeing with nothing on screen saying so.
 */
export function partialFailureMessage(applied: number, total: number, error: string): string {
  if (applied === 0 || applied >= total) return error
  return `${error} — ${applied} of ${total} rows changed, so the batch and CDC copies now disagree. Reload and retry.`
}
