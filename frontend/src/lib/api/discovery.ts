/**
 * Tool-generator discovery API client.
 *
 * Two-phase generation flow:
 *   1. POST /v1/discover         — open a session, get a contract + questions
 *   2. POST /v1/discover/{id}/confirm — submit answers, re-evaluate
 *   3. POST /v1/generate          — when contract.can_generate=true
 *
 * VERSION: 1.0.0
 */

import { authFetch } from "./auth-fetch"
import { API_GATEWAY_URL } from "@/lib/config/api"

// All tool-generator calls now go through the api-gateway proxy so:
//   - CORS is solved exactly once at the gateway edge
//   - tool-generator stays internal (no browser-reachable port 5010)
//   - auth / rate-limit / observability paths apply uniformly
//
// Gateway exposes /api/v1/tool-generator/<subpath> which forwards to
// the underlying /v1/<subpath> on tool-generator. The NEXT_PUBLIC_…
// env is kept as an override for tests / standalone-dev that talks
// directly to the service.
const TOOLGEN_BASE =
  process.env.NEXT_PUBLIC_TOOL_GENERATOR_URL ||
  `${API_GATEWAY_URL}/api/v1/tool-generator`

// ---------------------------------------------------------------------------
// Wire types — match schemas/contract.py
// ---------------------------------------------------------------------------

export type Dimension =
  | "vendor"
  | "api_variant"
  | "protocol"
  | "base_url"
  | "auth_type"
  | "auth_header"
  | "operations"
  | "pagination"
  | "runtime_fields"

export type FactSource =
  | "user_provided"
  | "vendor_yaml"
  | "learned_api"
  | "introspection"
  | "openapi_spec"
  | "doc_parse"
  | "llm_inference"
  | "heuristic"
  | "default"
  | "missing"

export interface Fact {
  dimension: Dimension
  value: unknown
  confidence: number
  source: FactSource
  evidence: string
}

export interface Question {
  dimension: Dimension
  prompt: string
  kind: "free_text" | "choice" | "multi_choice" | "bool"
  choices?: Array<{ value: string; label: string }>
  default?: unknown
  required: boolean
  help_text?: string
}

export interface RuntimeField {
  name: string
  required: boolean
  secret: boolean
  default: unknown
  description: string
  validation_regex: string | null
}

export interface GenerationContract {
  session_id: string
  api_name: string
  created_at: string
  updated_at: string
  facts: Record<Dimension, Fact>
  questions: Question[]
  can_generate: boolean
  refusal_reason: string | null
  metadata: {
    docs_url?: string | null
    available_variants?: Array<{
      id: string
      name: string
      protocol: string
      recommended?: boolean
      reason?: string
    }>
    operations_summary?: {
      names?: string[]
      queries?: string[]
      mutations?: string[]
      total?: number
      custom_scalars?: string[]
      enums?: string[]
    }
    protocol_evidence?: Array<{
      signal: string
      protocol: string
      weight: number
      detail: string
    }>
    // P0 — credential-free reachability + auth-confirmation probe result.
    // Set by the backend discovery probe; absent when no base URL was known.
    reachability_probe?: {
      reachable: boolean
      status?: number | null
      auth_confirmed?: string | false | null
      detail?: string
      www_authenticate?: string | null
    }
    [key: string]: unknown
  }
}

export interface DiscoverResponse {
  session_id: string
  contract: GenerationContract
}

export interface VendorApi {
  id: string
  name: string
  protocol: string
  endpoint_template: string
  docs_url: string | null
  // Direct OpenAPI spec URL — set for Tier-3 directory (APIs.guru) variants.
  // When present, the flow generates straight from this spec.
  openapi_spec_url?: string | null
  auth_preset: string | null
  recommended: boolean
  reason: string | null
  runtime_config_fields: RuntimeField[]
}

export interface Vendor {
  id: string
  display_name: string
  category: string
  source_tier: number
  apis: VendorApi[]
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

class DiscoveryError extends Error {
  status: number
  constructor(message: string, status: number) {
    super(message)
    this.status = status
  }
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const url = `${TOOLGEN_BASE}${path}`
  const res = await authFetch(url, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  })
  if (!res.ok) {
    let detail = res.statusText
    try {
      const body = await res.json()
      detail = body?.detail || body?.error || detail
    } catch {
      // ignore
    }
    throw new DiscoveryError(detail || "request failed", res.status)
  }
  return (await res.json()) as T
}

// Vendor registry
export async function listVendors(query?: string): Promise<Vendor[]> {
  const qs = query ? `?q=${encodeURIComponent(query)}` : ""
  const res = await call<{ vendors: Vendor[]; total: number }>(`/v1/vendors${qs}`)
  return res.vendors
}

export async function getVendor(id: string): Promise<Vendor> {
  const res = await call<{ vendor: Vendor }>(`/v1/vendors/${encodeURIComponent(id)}`)
  return res.vendor
}

// Discovery session
export interface DiscoverInput {
  api_name: string
  docs_url?: string | null
  protocol_hint?: "rest" | "graphql" | "openapi" | "auto" | null
  base_url_hint?: string | null
  vendor_id?: string | null
  api_variant_id?: string | null
  // Spec-first deterministic inputs. When provided, discovery builds the
  // contract straight from the spec instead of LLM-scraping docs.
  openapi_spec?: string | null
  openapi_spec_url?: string | null
  graphql_schema?: string | null
  graphql_endpoint?: string | null
  // P0 — a pasted curl command; the backend converts it to a minimal OpenAPI
  // spec (deterministic, no LLM) and feeds the same spec-first path.
  curl_example?: string | null
}

export async function startDiscovery(input: DiscoverInput): Promise<DiscoverResponse> {
  return call<DiscoverResponse>("/v1/discover", {
    method: "POST",
    body: JSON.stringify(input),
  })
}

export async function getDiscovery(sessionId: string): Promise<DiscoverResponse> {
  return call<DiscoverResponse>(`/v1/discover/${encodeURIComponent(sessionId)}`)
}

export async function confirmDiscovery(
  sessionId: string,
  answers: Partial<Record<Dimension, unknown>>,
): Promise<DiscoverResponse> {
  return call<DiscoverResponse>(
    `/v1/discover/${encodeURIComponent(sessionId)}/confirm`,
    { method: "POST", body: JSON.stringify({ answers }) },
  )
}

export async function deleteDiscovery(sessionId: string): Promise<void> {
  await call(`/v1/discover/${encodeURIComponent(sessionId)}`, { method: "DELETE" })
}

// ---------------------------------------------------------------------------
// Phase 13a — Generate from a confirmed discovery session
// ---------------------------------------------------------------------------

export interface GenerateFromSessionResponse {
  success: boolean
  status: string
  connector_name?: string
  protocol?: string
  operation_count?: number
  quality_tier?: string
  total_time_ms?: number
  workflow_stages?: Array<{ name: string; status: string }>
  error_message?: string
  error_stage?: string
}

/**
 * Generate a connector from a confirmed discovery contract.
 *
 * Goes through the api-gateway's existing /api/v1/connectors/generate endpoint,
 * which proxies session_id through to tool-generator's fast path. The fast path
 * skips the agentic LLM pipeline and renders directly from the contract —
 * deterministic, no hallucination.
 */
export async function generateFromSession(
  apiName: string,
  sessionId: string,
  options: {
    description?: string
    forceRegenerate?: boolean
    apiCategoryHint?: string
  } = {},
): Promise<GenerateFromSessionResponse> {
  const url = `${API_GATEWAY_URL}/api/v1/connectors/generate`
  const res = await authFetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      api_name: apiName,
      session_id: sessionId,
      description: options.description ?? "",
      force_regenerate: options.forceRegenerate ?? false,
      // Pass detect-category result through so the architect picks the
      // right templates + capability defaults (api_saas vs database vs
      // storage etc.). Empty string is safe — backend treats it as unset.
      api_category_hint: options.apiCategoryHint ?? "",
    }),
  })
  if (!res.ok) {
    let detail: string = res.statusText
    try {
      const body = await res.json()
      detail = body?.error || body?.detail || detail
    } catch {
      // ignore
    }
    throw new DiscoveryError(detail || "generation failed", res.status)
  }
  return (await res.json()) as GenerateFromSessionResponse
}

// ---------------------------------------------------------------------------
// UI helpers
// ---------------------------------------------------------------------------

export type FactStatus = "green" | "yellow" | "red"

export function factStatus(fact: Fact | undefined): FactStatus {
  if (!fact || fact.source === "missing" || fact.value == null) return "red"
  if (fact.confidence >= 0.85) return "green"
  if (fact.confidence >= 0.5) return "yellow"
  return "red"
}

export function isCriticalDimension(dim: Dimension): boolean {
  return ["protocol", "base_url", "auth_type", "operations"].includes(dim)
}

export { DiscoveryError, TOOLGEN_BASE }
