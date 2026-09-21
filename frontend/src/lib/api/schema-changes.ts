/**
 * Schema-change (drift) approval API.
 *
 * Backs the approve-UX page at /pipelines/:id/schema-changes — the deep-link the
 * healer emits for CategorySchemaDrift (backend-orchestrator healer.go). The
 * api-gateway is the source of truth (schema_evolution.go):
 *   - LIST   GET  /pipelines/:id/schema-changes        -> { schema_changes }
 *   - APPROVE POST /pipelines/:id/schema-changes/:cid/approve
 *   - REJECT  POST /pipelines/:id/schema-changes/:cid/reject
 *   - POLICY GET/PUT /pipelines/:id/schema-drift-policy -> { schema_drift_policy, detector_enabled }
 * APPROVE/REJECT are owner-gated and only act on a `pending` row; a non-pending
 * row returns 404, which callers treat as a soft "already actioned" signal.
 */

import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { extractErrorMessage } from "@/lib/utils/error-handling"

/** One schema-drift change as returned by the LIST endpoint. */
export interface SchemaChange {
  id: string
  pipeline_id: string
  change_type: string
  table_name: string
  ddl: string
  reasoning: string
  risks: string
  user_message: string
  status: "pending" | "approved" | "rejected" | "applied" | "failed"
  /**
   * Whether the healer will actually run the DDL after approval. False when the
   * DDL trips the healer's DROP/TRUNCATE guard (applyMigration refuses it even
   * post-approval) — approving then only records the decision and the DDL must
   * be run manually on the destination. Optional for older gateways that
   * predate the field.
   */
  auto_applicable?: boolean
  reviewed_by: string | null
  reviewed_at: string | null
  applied_at: string | null
  error_message: string | null
  created_at: string
  updated_at: string
}

/**
 * Thrown when approve/reject targets a change that is no longer `pending`
 * (the backend returns 404). The caller should treat this as "already actioned"
 * — re-fetch the list rather than surfacing a hard error.
 */
export class AlreadyActionedError extends Error {
  constructor(message = "This change was already actioned") {
    super(message)
    this.name = "AlreadyActionedError"
  }
}

/** List all schema-drift changes (pending + resolved) for a pipeline. */
export async function listSchemaChanges(pipelineId: string): Promise<SchemaChange[]> {
  const res = await authFetch(API_ENDPOINTS.PIPELINES.SCHEMA_CHANGES(pipelineId), {
    cache: "no-store",
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Failed to load schema changes" }))
    throw new Error(extractErrorMessage(err) || "Failed to load schema changes")
  }
  const data = await res.json().catch(() => null)
  return (data?.schema_changes ?? []) as SchemaChange[]
}

async function postAction(url: string): Promise<Record<string, unknown> | null> {
  const res = await authFetch(url, { method: "POST" })
  if (res.ok) return (await res.json().catch(() => null)) as Record<string, unknown> | null
  if (res.status === 404) {
    throw new AlreadyActionedError()
  }
  const err = await res.json().catch(() => ({ error: "Action failed" }))
  throw new Error(extractErrorMessage(err) || "Action failed")
}

/**
 * Approve a pending change. Resolves to what approval actually did: `true` when
 * the healer was asked to apply the DDL, `false` when approval only recorded the
 * decision (destructive DDL the healer refuses — run it manually). Comes from the
 * response rather than a second copy of the predicate here, so the message the
 * user reads is the one the server acted on. Older gateways omit the field; we
 * fall back to the caller's own read of `auto_applicable`.
 */
export async function approveSchemaChange(pipelineId: string, changeId: string): Promise<boolean | null> {
  const body = await postAction(API_ENDPOINTS.PIPELINES.SCHEMA_CHANGE_APPROVE(pipelineId, changeId))
  return typeof body?.auto_applicable === "boolean" ? (body.auto_applicable as boolean) : null
}

/** Reject a pending change. */
export async function rejectSchemaChange(pipelineId: string, changeId: string): Promise<void> {
  await postAction(API_ENDPOINTS.PIPELINES.SCHEMA_CHANGE_REJECT(pipelineId, changeId))
}

/**
 * Which schema changes the batch detector files for review
 * (pipelines.config.schema_drift_policy; read by the orchestrator's
 * schema_drift.go filterChangesByPolicy). Type changes are not listed: they are
 * always reported, because one can silently break writes to the destination.
 */
export interface SchemaDriftPolicy {
  enabled: boolean
  notify_on_add: boolean
  notify_on_drop: boolean
}

export interface SchemaDriftPolicyState {
  policy: SchemaDriftPolicy
  /**
   * Whether this installation runs the detector at all (RSYNC_SCHEMA_DRIFT_ENABLED).
   * Null for an older gateway that predates the field, so the card says nothing
   * rather than guessing.
   */
  detectorEnabled: boolean | null
}

function toPolicyState(data: unknown): SchemaDriftPolicyState {
  const d = (data ?? {}) as { schema_drift_policy?: Partial<SchemaDriftPolicy>; detector_enabled?: unknown }
  const p = d.schema_drift_policy ?? {}
  // Absent fields read as true, the same default the gateway and orchestrator use.
  return {
    policy: {
      enabled: p.enabled !== false,
      notify_on_add: p.notify_on_add !== false,
      notify_on_drop: p.notify_on_drop !== false,
    },
    detectorEnabled: typeof d.detector_enabled === "boolean" ? d.detector_enabled : null,
  }
}

export async function getSchemaDriftPolicy(pipelineId: string): Promise<SchemaDriftPolicyState> {
  const res = await authFetch(API_ENDPOINTS.PIPELINES.SCHEMA_DRIFT_POLICY(pipelineId), { cache: "no-store" })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Failed to load alert settings" }))
    throw new Error(extractErrorMessage(err) || "Failed to load alert settings")
  }
  return toPolicyState(await res.json().catch(() => null))
}

/**
 * Save the whole policy. Always sends all three fields: the gateway resets an
 * absent field to true, so a partial body would silently undo an opt-out.
 */
export async function updateSchemaDriftPolicy(
  pipelineId: string,
  policy: SchemaDriftPolicy
): Promise<SchemaDriftPolicyState> {
  const res = await authFetch(API_ENDPOINTS.PIPELINES.SCHEMA_DRIFT_POLICY(pipelineId), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      enabled: policy.enabled,
      notify_on_add: policy.notify_on_add,
      notify_on_drop: policy.notify_on_drop,
    }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Failed to save alert settings" }))
    throw new Error(extractErrorMessage(err) || "Failed to save alert settings")
  }
  return toPolicyState(await res.json().catch(() => null))
}
