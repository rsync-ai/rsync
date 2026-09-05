/**
 * MCP Connectors API Client
 * Single source of truth for all MCP connector API calls
 * 
 * All MCP connector endpoints go through API Gateway:
 * - GET  /api/v1/connectors              - List all connectors
 * - GET  /api/v1/connectors/:name        - Get connector details
 * - GET  /api/v1/connectors/:name/logo   - Get connector logo (SVG preferred)
 * - POST /api/v1/connectors/generate     - Generate new connector
 * - POST /api/v1/connectors/validate     - Validate connector
 */

import { MCPConnector, MCPConnectorsResponse } from "@/lib/types/mcp-connector"
import { tracedFetch, tracedJsonFetch } from "@/lib/api/traced-client"
import { API_ENDPOINTS } from "@/lib/config/api"
import { getAuthHeaders } from "@/lib/auth"
import { extractErrorMessage } from "@/lib/utils/error-handling"
import { APIRequestError } from "@/lib/errors/api-errors"

/**
 * Fetch all MCP connectors
 * Uses centralized endpoint from API_ENDPOINTS
 * @param category Optional category filter (e.g., "database", "cloud_storage")
 */
export async function fetchMCPConnectors(category?: string): Promise<MCPConnectorsResponse> {
  try {
    const url = category 
      ? `${API_ENDPOINTS.CONNECTORS.LIST}?category=${encodeURIComponent(category)}`
      : API_ENDPOINTS.CONNECTORS.LIST
    return await tracedJsonFetch<MCPConnectorsResponse>(url, { headers: { ...getAuthHeaders() } })
  } catch (error) {
    throw new Error(`Failed to fetch connectors: ${extractErrorMessage(error)}`)
  }
}

/**
 * Fetch a single MCP connector by name
 */
export async function fetchMCPConnector(name: string): Promise<MCPConnector> {
  const candidates = connectorIdCandidates(name)
  let lastError: unknown = null

  for (const candidate of candidates) {
  try {
      return await tracedJsonFetch<MCPConnector>(API_ENDPOINTS.CONNECTORS.GET(candidate), {
        headers: { ...getAuthHeaders() },
      })
  } catch (error) {
      lastError = error
      // Keep trying aliases (e.g. aws-s3 -> aws_s3) before failing.
    }
  }

  console.error(`Failed to fetch connector ${name} (candidates: ${candidates.join(", ")}):`, lastError)
  throw new Error(`Failed to fetch connector: ${name}`)
}

/**
 * Get connector logo URL
 * API Gateway serves SVG first, falls back to PNG
 */
export function getConnectorLogoUrl(connectorName: string): string {
  // API Gateway connector IDs are typically folder-safe (often underscores),
  // while the pipeline/orchestrator/DB may emit display names or hyphenated IDs
  // (e.g. "AWS S3" / "aws-s3"). Prefer an underscore alias when available.
  const candidates = connectorIdCandidates(connectorName)
  const preferred =
    candidates.find((c) => c.includes("_")) || candidates[0] || connectorName
  return API_ENDPOINTS.CONNECTORS.LOGO(preferred)
}

function connectorIdCandidates(name: string): string[] {
  const raw = String(name || "").trim()
  if (!raw) return []

  // Normalize display names ("AWS S3") into safe ids ("aws-s3") first
  const base = raw.toLowerCase().replace(/\s+/g, "-")
  const candidates = new Set<string>()

  // Keep the original in normalized casing
  candidates.add(base)

  // Common separator swaps
  candidates.add(base.replace(/-/g, "_"))
  candidates.add(base.replace(/_/g, "-"))

  // Collapse any accidental double separators
  const collapsed = Array.from(candidates).map((c) =>
    c.replace(/--+/g, "-").replace(/__+/g, "_")
  )

  // Cleanup: only keep non-empty unique values
  return Array.from(new Set(collapsed)).filter(Boolean)
}

/**
 * Test connection to a connector
 */
export async function testMCPConnection(
  connectorName: string,
  config: Record<string, unknown>
): Promise<{ success: boolean; message: string }> {
  try {
    const response = await tracedFetch(API_ENDPOINTS.CONNECTIONS.TEST + "?allow_draft=true", {
    method: "POST",
      headers: { "Content-Type": "application/json", ...getAuthHeaders() },
      body: JSON.stringify({
        connector_type: connectorName,
        config,
      }),
  })

  if (!response.ok) {
      const errorText = await response.text()
      let message = errorText || "Connection test failed"

      // Many endpoints return structured JSON errors; parse when possible so the UI
      // doesn't show raw JSON blobs to users.
      try {
        const parsed = errorText ? JSON.parse(errorText) : null
        if (parsed && typeof parsed === "object") {
          const p = parsed as Record<string, unknown>
          const base =
            (p["message"] as string | undefined) ||
            (p["error"] as string | undefined) ||
            (p["detail"] as string | undefined) ||
            message

          const validationErrors = Array.isArray(p["validation_errors"])
            ? (p["validation_errors"] as Array<{ field?: unknown; message?: unknown }>)
            : []

          if (validationErrors.length > 0) {
            message =
              `${base}\n` +
              validationErrors
                .map((e) => {
                  const field = e?.field ? String(e.field) : "field"
                  const msg = e?.message ? String(e.message) : "is invalid"
                  return `• ${field}: ${msg}`
                })
                .join("\n")
          } else {
            message = String(base)
          }
        }
      } catch {
        // ignore parse errors, fall back to raw text
      }

      return { success: false, message }
  }

    const result = await response.json()
    return {
      success: result.success ?? true,
      // Prioritize error over message when failed - error has the actual details
      message: !result.success && result.error 
        ? result.error 
        : (result.message || result.error || "Connection successful"),
    }
  } catch (error) {
    console.error("Connection test error:", error)
    return { 
      success: false, 
      message: extractErrorMessage(error) || "Connection test failed",
    }
  }
}

/**
 * Save a new connection
 */
export async function saveConnection(
  connectionData: Record<string, unknown>
): Promise<{ id: string; success: boolean; message?: string }> {
  const traceId = (connectionData.trace_id as string) || `trace-${Date.now()}`
  
  const payload = {
    name: connectionData.name,
    connection_type: connectionData.connection_type,
    connector_type: connectionData.connector_type,
    sync_mode: connectionData.sync_mode,
    config: connectionData.config,
    description: connectionData.description,
  }
  
  try {
    return await tracedJsonFetch<{ id: string; success: boolean; message?: string }>(
      API_ENDPOINTS.CONNECTIONS.CREATE,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Trace-ID": traceId,
          ...getAuthHeaders(),
        },
        body: JSON.stringify(payload),
      }
    )
  } catch (error) {
    // Preserve structured APIRequestError so UI can focus fields (e.g. duplicate name → connection_name).
    if (error instanceof APIRequestError) throw error
    throw new Error(extractErrorMessage(error) || "Failed to save connection")
  }
}
