/**
 * AI Suggestions API Client
 * Handles PII detection, transform recommendations, and optimizations
 */

import { authFetch } from "./auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { getAuthHeaders } from "@/lib/auth"

export interface SuggestionsRequest {
  schema: {
    tables?: Array<{
      name: string
      columns: Array<{
        name: string
        type: string
        nullable?: boolean
        description?: string
      }>
    }>
    // Optional direct columns for non-table connectors
    columns?: Array<{
      name: string
      type: string
      nullable?: boolean
      description?: string
    }>
  }
  intent?: {
    use_case?: string
    description?: string
    source_type?: string
    destination_type?: string
  }
  discovery_result?: any
}

// NOTE: This API is currently a passthrough to llm-service SuggestionsResponse.
// llm-service returns pii_detected as boolean and pii_columns as a list of objects
// with a different schema than the frontend-native PIISuggestion we originally planned.
export interface PIIColumnSuggestion {
  column: string
  pii_types: string[]
  confidence: "high" | "medium" | "low" | string
  suggested_action: "mask" | "review" | "hash" | "redact" | string
}

export interface TransformSuggestion {
  /**
   * Transform type is the same "type" stored in DraftState.transforms[] and used by preview/runtime.
   * Keep this list small and engine-supported.
   */
  type:
    | "filter"
    | "select_columns"
    | "mask_pii"
    | "validate"
    | "rename_columns"
    | "json_flatten"
    | "array_expand"
    | string
  /**
   * Transform config payload (engine-specific).
   * Examples:
   * - filter: { condition: "status = 'active'" }
   * - select_columns: { columns: ["id","email"] }
   * - mask_pii: { column: "users.email", mask_type: "hash", hash_function: "sha256" }
   * - validate: { required_columns: ["id"] }
   * - rename_columns: { mappings: { cust_nm: "customer_name" } }
   * - json_flatten: { column: "meta_json", prefix: "meta_", separator: "_", max_depth: 2 }
   * - array_expand: { column: "tag_ids", output_prefix: "tag_", max_elements: 10 }
   */
  config: Record<string, unknown>
  /** Short human label for UI (optional). */
  summary?: string
  /** Longer explanation (optional). */
  reason?: string
  /** Backwards-compat passthrough fields (older llm-service responses). */
  source_column?: string
  target_column?: string
  transformation?: string
}

export interface OptimizationSuggestion {
  type: string
  table?: string
  // llm-service currently uses "suggestion"/"reason"; keep both shapes supported.
  description?: string
  impact?: string
  column?: string
  suggestion?: string
  reason?: string
}

export interface SuggestionsResponse {
  success?: boolean
  pii_detected: boolean
  pii_columns: PIIColumnSuggestion[]
  transforms: TransformSuggestion[]
  optimizations?: OptimizationSuggestion[]
  total_suggestions?: number
  /**
   * Human-readable error/info text. When `degraded` is true this describes
   * the partial failure (e.g. "Transform suggestions timed out, PII still
   * available") and the rest of the response is usable.
   */
  error?: string | null
  /**
   * Machine-readable error category. Set by the llm-service when something
   * went wrong, so the modal can pick a recovery path:
   *   - "ok"                — full success
   *   - "invalid_schema"    — frontend sent an empty/malformed schema
   *   - "llm_unavailable"   — OpenAI key missing / API down
   *   - "llm_timeout"       — LLM call took too long
   *   - "llm_invalid_json"  — LLM returned non-JSON output
   *   - "token_budget"      — schema too wide for token budget
   *   - "execution_timeout" — overall workflow exceeded its time budget
   *   - "internal_error"    — unexpected exception in the service
   */
  error_code?: string
  /** True when the response is partial (heuristics-only) but still usable. */
  degraded?: boolean
}

/**
 * Generate AI suggestions for a schema
 */
export async function generateSuggestions(request: SuggestionsRequest): Promise<SuggestionsResponse> {
  // llm-service expects schema.columns (flat list).
  // Convert tables → columns with table-qualified names, but also allow callers
  // to pass schema.columns directly for non-table connectors.
  const schemaAny = request.schema as unknown as Record<string, unknown>
  const directColumns = schemaAny?.["columns"]
  const flatColumns =
    (Array.isArray(directColumns)
      ? directColumns
      : request.schema?.tables?.flatMap((t) =>
          (t.columns || []).map((c) => ({
            name: `${t.name}.${c.name}`,
            type: c.type,
            nullable: Boolean(c.nullable),
            description: c.description || "",
          }))
        )) || []

  const payload = {
    schema: {
      columns: flatColumns,
    },
    intent: {
      operation: request.intent?.description || request.intent?.use_case || "sync",
      use_case: request.intent?.use_case || "sync",
      source_type: request.intent?.source_type || "unknown",
      destination_type: request.intent?.destination_type || "unknown",
    },
    discovery_result: request.discovery_result,
  }

  const response = await authFetch(`${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/suggestions/generate`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...getAuthHeaders(),
    },
    body: JSON.stringify(payload),
  })

  // The llm-service now returns 200 with a structured error_code+error
  // even for recoverable failures, so the modal can show actionable text.
  // We still defend against true 5xx (proxy unreachable, etc.) here.
  let data: SuggestionsResponse | null = null
  try {
    data = await response.json()
  } catch {
    data = null
  }

  // An error status whose body happens to parse used to slip through every
  // guard here: `!response.ok && !data` did not fire because the body parsed,
  // and the `success === false` guard did not fire because an upstream error
  // body ({"detail": ...} from FastAPI, {"error": ...} from the gateway) has no
  // `success` field. The cast at the end then handed the caller an object whose
  // pii_columns and transforms were `undefined`, and the modal rendered
  // "0 PII columns, 0 transforms" -- telling the user the AI had scanned their
  // schema and found no PII in it. An error status is an error, body or not.
  if (!response.ok) {
    const detail = (data as Record<string, unknown> | null)?.error
      ?? (data as Record<string, unknown> | null)?.detail
      ?? (data as Record<string, unknown> | null)?.message
    const detailText = typeof detail === "string" && detail.trim() ? detail.trim() : null
    throw new Error(
      response.status === 504 || response.status === 502
        ? "AI suggestions service is unavailable right now. You can continue without transforms."
        : detailText
          ? `Failed to generate suggestions: ${detailText}`
          : `Failed to generate suggestions (HTTP ${response.status})`
    )
  }

  // Hard failure (no usable payload): surface the structured error.
  if (data && data.success === false && (!data.pii_columns?.length && !data.transforms?.length && !data.optimizations?.length)) {
    throw new Error(data.error || "Failed to generate suggestions")
  }

  // A 200 whose body is not a suggestions payload at all. The discriminator is
  // present-and-empty vs absent: a genuine "nothing to suggest" answer carries
  // `pii_columns: []` and `transforms: []`, so it passes. A body with neither
  // array is some other message being cast into this shape.
  if (!data || (!Array.isArray(data.pii_columns) && !Array.isArray(data.transforms))) {
    throw new Error(
      (data as Record<string, unknown> | null)?.error as string
        || "The suggestions service returned a response with no suggestions in it"
    )
  }

  return data as SuggestionsResponse
}
