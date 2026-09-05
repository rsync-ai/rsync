/**
 * Pipeline API functions
 */

import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { extractErrorMessage } from "@/lib/utils/error-handling"
import type { DataLoadingStrategy } from "@/lib/pipeline/dataLoadingStrategy"

export interface PipelineNode {
  id: string
  type: "source" | "transform" | "destination"
  name: string
  connector: string
  parameters?: Record<string, unknown>
  configured?: boolean
}

export interface Pipeline {
  id: string
  name: string
  description: string
  config: {
    nodes: PipelineNode[]
  }
  status:
    | "draft"
    | "active"
    | "pending"
    | "running"
    | "waiting_for_user"
    | "paused"
    | "stopped"
    | "completed"
    | "failed"
    | "archived"
  sync_mode?: "batch" | "cdc"
  cdc_mode?: "initial" | "streaming_only"
  dataset?: string
  default_run_mode?: "resume" | "reload"
  selected_tables?: string[]
  destination_namespace?: string
  destination_config?: DestinationConfig
  data_loading_strategy?: DataLoadingStrategy
  source_connection_id?: string
  destination_connection_id?: string
  // Lightweight connection summaries the GET endpoint embeds; used to label the
  // destination engine (e.g. postgresql/mysql) when config.nodes isn't populated
  // yet (chat-created pipelines).
  source_connection?: { connector_type?: string }
  destination_connection?: { connector_type?: string }
  user_id?: string
  created_at: string
  updated_at: string
}

export interface CreatePipelineRequest {
  name: string
  description: string
  config: {
    nodes: PipelineNode[]
  }
  status?: "draft" | "active"
}

/**
 * Get a pipeline by ID
 */
export async function getPipeline(id: string): Promise<Pipeline> {
  const response = await authFetch(API_ENDPOINTS.PIPELINES.GET(id))

  if (!response.ok) {
    throw new Error(extractErrorMessage(await response.text().catch(() => "")) || "Pipeline not found")
  }

  return response.json()
}

export type CDCBackfillMode = "incremental" | "blocking"

export interface CDCBackfillSummary {
  triggered?: boolean
  mode?: CDCBackfillMode
  tables?: string[]
  message?: string
}

export async function updatePipelineCDCTables(
  pipelineId: string,
  tables: string[],
  opts?: { backfill_newly_added?: boolean; backfill_mode?: CDCBackfillMode },
): Promise<{ success: boolean; new_tables?: string[]; backfill?: CDCBackfillSummary }> {
  const response = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/cdc/tables`, {
    method: "POST",
    body: JSON.stringify({ tables, ...(opts || {}) }),
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: "Failed to update CDC tables" }))
    throw new Error(extractErrorMessage(error) || "Failed to update CDC tables")
  }

  return response.json()
}

/**
 * Persist batch pipeline table selection at the pipeline level.
 * This affects future runs (and is used to skip table-selection HITL once configured).
 */
export async function updatePipelineTables(pipelineId: string, tables: string[]): Promise<{ success: boolean }> {
  const response = await authFetch(API_ENDPOINTS.PIPELINES.TABLES(pipelineId), {
    method: "POST",
    body: JSON.stringify({ tables }),
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: "Failed to update tables" }))
    throw new Error(extractErrorMessage(error) || "Failed to update tables")
  }

  return response.json()
}

/**
 * Update a pipeline
 */
export async function updatePipeline(id: string, data: Partial<CreatePipelineRequest>): Promise<Pipeline> {
  const response = await authFetch(API_ENDPOINTS.PIPELINES.UPDATE(id), {
    method: "PUT",
    body: JSON.stringify(data),
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: "Failed to update pipeline" }))
    throw new Error(extractErrorMessage(error) || "Failed to update pipeline")
  }

  return response.json()
}

/**
 * Delete a pipeline
 */
export async function deletePipeline(id: string): Promise<void> {
  const response = await authFetch(API_ENDPOINTS.PIPELINES.DELETE(id), {
    method: "DELETE",
  })

  if (!response.ok) {
    throw new Error(extractErrorMessage(await response.text().catch(() => "")) || "Failed to delete pipeline")
  }
}

/**
 * Execute a pipeline immediately
 */
export async function executePipeline(id: string): Promise<{ execution_id: string }> {
  const response = await authFetch(API_ENDPOINTS.PIPELINES.RUN(id), {
    method: "POST",
  })

  if (!response.ok) {
    const body = await response.json().catch(() => ({ error: "Execution failed" }))
    if (response.status === 402) {
      const errCode = (body as PlanLimitPayload).error
      if (errCode === "pipeline_limit_reached" || errCode === "trial_expired" || errCode === "gb_limit_reached") {
        throw new PlanLimitError(body as PlanLimitPayload)
      }
    }
    throw new Error(extractErrorMessage(body) || "Execution failed")
  }

  return response.json()
}

/**
 * Pre-migration assessment payload. Mirrors the api-gateway's
 * `AssessmentReport` Go struct. Surfaces structured warnings the user
 * must acknowledge before the run starts — PK-less tables, JSON
 * collapse, sinks without auto-create, etc.
 */
export type AssessmentSeverity = "error" | "warning" | "info"

export interface AssessmentFinding {
  code: string
  severity: AssessmentSeverity
  message: string
  details?: Record<string, unknown>
}

export interface AssessmentTable {
  name: string
  schema?: string
  findings: AssessmentFinding[]
  primary_keys: string[]
  primary_key_source: "declared" | "synthetic" | "nominated"
  column_count: number
  json_column_count: number
  mode: "upsert" | "upsert_synthetic"
  // Source column names — populated for keyless tables so the pre-migration
  // modal can render a key-column picker (PR-D nomination).
  columns?: string[]
  // The user-nominated key columns applied to this table (PR-D), echoed back.
  nominated_keys?: string[]
}

export interface AssessmentReport {
  blocking: boolean
  summary: string
  tables: AssessmentTable[]
  generated_at: string
  source_connector_type?: string
  destination_connector_type?: string
  destination_supports_ddl: boolean
}

/**
 * Per-pipeline destination mapping (PR-C). Persisted under
 * pipelines.config.destination_config and edited in the destination-mapping
 * HITL. `namespace_kind` labels the field per destination engine
 * (schema/database/dataset/prefix/path).
 */
export interface DestinationConfig {
  namespace: string
  namespace_kind?: string
  create_if_not_exists?: boolean
  // schema_mode is an explicit per-pipeline override for how source schemas map
  // to the destination: "preserve"/"mirror" mirrors each source schema at the
  // destination, "flatten" collapses them into `namespace`. Sent (with an empty
  // namespace) when a multi-schema selection leaves the destination namespace
  // blank, so mirroring holds regardless of the seeded namespace. Omitted ⇒ the
  // backend auto-decides from the namespace + source schema count.
  schema_mode?: "preserve" | "mirror" | "flatten"
}

export interface PlanLimitPayload {
  error: "pipeline_limit_reached" | "trial_expired" | "gb_limit_reached"
  message: string
  plan?: string
  limit?: number
  used?: number
}

/**
 * Thrown when /pipelines/:id/run or /pipelines (create) returns 402.
 * The UI catches this and renders the UpgradeModal.
 */
export class PlanLimitError extends Error {
  readonly payload: PlanLimitPayload
  constructor(payload: PlanLimitPayload) {
    super(payload.message)
    this.name = "PlanLimitError"
    this.payload = payload
  }
}

/**
 * Thrown when /pipelines/:id/run returns 422 with an assessment
 * payload. The UI catches this, renders the modal, and re-issues the
 * run with `ack_warnings: true` once the user proceeds.
 */
export class AssessmentRequiredError extends Error {
  readonly report: AssessmentReport
  constructor(report: AssessmentReport) {
    super(report.summary || "Pre-migration assessment requires acknowledgement")
    this.name = "AssessmentRequiredError"
    this.report = report
  }
}

/**
 * Fetch the pre-migration assessment for a pipeline without starting
 * a run. Used to render the modal on demand from a "Review" button.
 */
export async function assessPipeline(id: string): Promise<AssessmentReport> {
  const response = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(id)}/assess`, {
    method: "POST",
  })
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: "Assessment failed" }))
    throw new Error(extractErrorMessage(error) || "Assessment failed")
  }
  return response.json()
}

/**
 * Execute a pipeline with an explicit run_mode override.
 * - resume: continue from checkpoints (default)
 * - reload: rebuild from scratch (best-effort destination cleanup, etc.)
 *
 * When the server returns 422 with a pre-migration assessment, throws
 * AssessmentRequiredError carrying the report so the caller can render
 * the modal and retry with `ackWarnings: true`.
 */
export async function executePipelineWithRunMode(
  id: string,
  runMode: "resume" | "reload",
  options?: { ackWarnings?: boolean; nominatedKeys?: Record<string, string[]> }
): Promise<{ execution_id: string }> {
  // PR-D: user-nominated key columns for keyless/GIPK tables, shape
  // { "<table>": ["col1", "col2"] }. Persisted server-side before assessment so
  // the run upserts on these columns instead of the content-hash surrogate.
  const nominated = options?.nominatedKeys
    ? Object.fromEntries(
        Object.entries(options.nominatedKeys).filter(([, cols]) => cols.length > 0),
      )
    : undefined
  const response = await authFetch(API_ENDPOINTS.PIPELINES.RUN(id), {
    method: "POST",
    body: JSON.stringify({
      run_mode: runMode,
      ack_warnings: !!options?.ackWarnings,
      ...(nominated && Object.keys(nominated).length > 0
        ? { nominated_keys: nominated }
        : {}),
    }),
  })

  if (!response.ok) {
    const body = await response.json().catch(() => ({ error: "Execution failed" }))
    if (response.status === 402) {
      const errCode = (body as PlanLimitPayload).error
      if (errCode === "pipeline_limit_reached" || errCode === "trial_expired" || errCode === "gb_limit_reached") {
        throw new PlanLimitError(body as PlanLimitPayload)
      }
    }
    if (response.status === 422) {
      if (body?.error === "pre_migration_assessment" && body?.assessment) {
        throw new AssessmentRequiredError(body.assessment as AssessmentReport)
      }
    }
    throw new Error(extractErrorMessage(body) || "Execution failed")
  }

  return response.json()
}

/**
 * HITL: Resume a pipeline after connection configuration.
 * Signals the Temporal workflow via API Gateway.
 */
export async function resumePipelineConnections(
  pipelineId: string,
  body: {
    execution_id?: string
    source_connection_id?: string
    destination_connection_id?: string
    direction?: "source" | "destination"
    connection_id?: string
    metadata?: Record<string, unknown>
  },
  opts?: { signal?: AbortSignal }
): Promise<{ success: boolean; pipeline_id: string; execution_id: string; signal: string }> {
  const response = await authFetch(API_ENDPOINTS.PIPELINES.HITL_CONNECTIONS(pipelineId), {
    method: "POST",
    body: JSON.stringify(body),
    signal: opts?.signal,
  })

  if (!response.ok) {
    const errorText = await response.text().catch(() => "")
    throw new Error(extractErrorMessage(errorText) || "Failed to resume pipeline connections")
  }

  return response.json()
}

/**
 * HITL: Resume a pipeline after connector generation.
 */
export async function resumePipelineConnectors(
  pipelineId: string,
  body: { execution_id?: string; connectors?: string[]; metadata?: Record<string, unknown> }
): Promise<{ success: boolean; pipeline_id: string; execution_id: string; signal: string }> {
  const response = await authFetch(API_ENDPOINTS.PIPELINES.HITL_CONNECTORS(pipelineId), {
    method: "POST",
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    const errorText = await response.text().catch(() => "")
    throw new Error(extractErrorMessage(errorText) || "Failed to resume pipeline connectors")
  }

  return response.json()
}

/**
 * HITL: Resume a pipeline after table/resource selection.
 */
export async function resumePipelineTables(
  pipelineId: string,
  body: {
    execution_id?: string
    selected_tables: string[]
    metadata?: Record<string, unknown>
    // Confirmed destination namespace mapping (PR-C). Collected in the same
    // first-run HITL as table selection and persisted before the workflow resumes.
    destination_config?: DestinationConfig
  }
): Promise<{ success: boolean; pipeline_id: string; execution_id: string; signal: string }> {
  const response = await authFetch(API_ENDPOINTS.PIPELINES.HITL_TABLES(pipelineId), {
    method: "POST",
    body: JSON.stringify(body),
    // Bound this HITL-resume signal POST: it normally returns in a few seconds, so
    // a stalled socket must surface a timeout error the modal can recover from
    // rather than pinning its spinner indefinitely.
    timeoutMs: 30000,
  })

  if (!response.ok) {
    const errorText = await response.text().catch(() => "")
    let msg = extractErrorMessage(errorText) || "Failed to resume pipeline tables"
    // If backend returned structured invalid_tables details, surface them to the user.
    try {
      const parsed: unknown = errorText ? JSON.parse(errorText) : null
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        const p = parsed as { missing_tables?: unknown; ambiguous_tables?: unknown; error?: unknown; reason?: unknown }
        // Invalid destination namespace (PR-C): prefer the server's reason.
        if (p.error === "invalid_destination_namespace" && typeof p.reason === "string" && p.reason) {
          msg = p.reason
        }
        const missing = Array.isArray(p.missing_tables)
          ? p.missing_tables.map((v) => String(v))
          : []
        const ambiguous =
          p.ambiguous_tables && typeof p.ambiguous_tables === "object" && !Array.isArray(p.ambiguous_tables)
            ? (p.ambiguous_tables as Record<string, unknown>)
            : null
        if (missing.length > 0) {
          msg += `\nMissing: ${missing.slice(0, 10).join(", ")}${missing.length > 10 ? "…" : ""}`
        }
        if (ambiguous && Object.keys(ambiguous).length > 0) {
          const keys = Object.keys(ambiguous).slice(0, 5)
          msg += `\nAmbiguous: ${keys.join(", ")} (use fully-qualified names)`
        }
      }
    } catch {
      // ignore
    }
    throw new Error(msg)
  }

  return response.json()
}

/**
 * HITL (DAG): Provide node-level input to resume a blocked node.
 * Used when backend emits blocking_reason.type="node_input" (PolicyCodeNodeInputRequired).
 */
export async function resumePipelineNodeInput(
  pipelineId: string,
  body: {
    execution_id?: string
    node_id: string
    message?: string
    config_patch?: Record<string, unknown>
    metadata?: Record<string, unknown>
  }
): Promise<{ success: boolean; pipeline_id: string; execution_id: string; node_id: string; signal: string }> {
  const response = await authFetch(API_ENDPOINTS.PIPELINES.HITL_NODE_INPUT(pipelineId), {
    method: "POST",
    body: JSON.stringify(body),
    // See resumePipelineTables — bound the HITL-resume signal POST so a stalled
    // socket surfaces a timeout instead of trapping the modal spinner.
    timeoutMs: 30000,
  })

  if (!response.ok) {
    const errorText = await response.text().catch(() => "")
    throw new Error(extractErrorMessage(errorText) || "Failed to resume pipeline node input")
  }

  return response.json()
}

/**
 * Connector state reported by Debezium / Kafka Connect for a CDC pipeline.
 * Mirrors the subset of the connector status REST response that we render
 * in PipelineAccordionView.
 */
export interface CDCStatusResponse {
  success: boolean
  connector_name?: string
  result?: {
    connector_state?: string
    healthy?: boolean
    tasks?: Array<{ id?: number; state?: string; trace?: string }>
  }
  error?: string
  // True when the operator-guarded CDC recovery endpoint is enabled on this
  // deployment (CDC_RECOVERY_ENABLED). Dormant/false by default; the UI gates the
  // "Recover" affordance on it. Advertised by the orchestrator status handler.
  recovery_enabled?: boolean
}

/**
 * CDC: Get Debezium/Kafka Connect status for a pipeline's CDC connector.
 * (Best-effort: batch pipelines may return errors; callers should treat as optional.)
 */
export async function getPipelineCDCStatus(pipelineId: string): Promise<CDCStatusResponse> {
  const response = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/cdc/status`)

  if (!response.ok) {
    const errorText = await response.text().catch(() => "")
    throw new Error(extractErrorMessage(errorText) || "Failed to fetch CDC status")
  }

  return response.json()
}

/**
 * CDC: Restart the Debezium/Kafka Connect connector for a pipeline.
 */
export async function restartPipelineCDC(
  pipelineId: string,
): Promise<{ success: boolean; message?: string; connector_name?: string }> {
  const response = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/cdc/restart`, {
    method: "POST",
    body: JSON.stringify({}),
  })

  if (!response.ok) {
    const errorText = await response.text().catch(() => "")
    throw new Error(extractErrorMessage(errorText) || "Failed to restart CDC pipeline")
  }

  return response.json()
}

export interface CDCRecoverRequest {
  // Audit metadata (required by the backend): who triggered recovery and why.
  operator: string
  reason: string
  // Debezium snapshot.mode: "recovery" repositions the stream without dropping data;
  // "initial" re-snapshots; "never" streams only. Defaults server-side to "initial".
  snapshot_mode?: "initial" | "recovery" | "never"
  // Force a Kafka Connect offset reset (required to re-snapshot after WAL/binlog loss).
  reset_offsets?: boolean
  // Preview validations + planned actions without side effects.
  dry_run?: boolean
}

export interface CDCRecoverPlan {
  connector_name?: string
  connector_state?: string
  db_type?: string
  snapshot_mode?: string
  reset_offsets?: boolean
  table_count?: number
  publication_name?: string
  slot_name?: string
}

export interface CDCRecoverResponse {
  success: boolean
  dry_run?: boolean
  recovery_id?: string
  pipeline_id?: string
  connector?: string
  plan?: CDCRecoverPlan
  offsets?: { attempted?: boolean; success?: boolean }
  message?: string
  error?: string
}

/**
 * CDC: Operator-guarded recovery for a FAILED connector. Repositions/re-snapshots
 * the change stream without a full destination rebuild. Gated behind the backend
 * CDC_RECOVERY_ENABLED flag (403 when disabled). Pass { dry_run: true } first to
 * preview the plan, then again with dry_run omitted/false to execute.
 */
export async function recoverPipelineCDC(
  pipelineId: string,
  body: CDCRecoverRequest,
): Promise<CDCRecoverResponse> {
  const response = await authFetch(API_ENDPOINTS.PIPELINES.CDC_RECOVER(pipelineId), {
    method: "POST",
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    const errorText = await response.text().catch(() => "")
    throw new Error(extractErrorMessage(errorText) || "Failed to recover CDC pipeline")
  }

  return response.json()
}
