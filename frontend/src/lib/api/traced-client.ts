/**
 * Trace-aware API client for distributed tracing support
 * 
 * This module provides:
 * - Trace ID generation and management
 * - Automatic injection of trace headers (X-Trace-ID, traceparent)
 * - Session-based trace correlation
 * - Debug logging in development mode
 * - User-friendly error parsing
 */

import { APIRequestError } from "@/lib/errors/api-errors"
// Single source of truth for service base URLs. These are self-correcting:
// on a real (non-localhost) page origin a mis-baked localhost value is rebased
// onto the current origin, so a browser request can never leak to localhost.
import { API_GATEWAY_URL, ORCHESTRATOR_URL, LLM_SERVICE_URL } from "@/lib/config/api"
import { getActiveWorkspaceId } from "@/lib/workspace/active-workspace"

function getCookie(name: string): string | null {
  if (typeof document === "undefined") return null
  const match = document.cookie.match(new RegExp(`(?:^|;\\s*)${name}=([^;]+)`))
  return match ? decodeURIComponent(match[1]) : null
}

function isUnsafeMethod(method?: string): boolean {
  const m = (method || "GET").toUpperCase()
  return m !== "GET" && m !== "HEAD" && m !== "OPTIONS"
}

// Generate a random 32-character hex string (OTel trace ID format)
function generateTraceId(): string {
  const array = new Uint8Array(16)
  crypto.getRandomValues(array)
  return Array.from(array, (byte) => byte.toString(16).padStart(2, '0')).join('')
}

// Generate a random 16-character hex string (OTel span ID format)
function generateSpanId(): string {
  const array = new Uint8Array(8)
  crypto.getRandomValues(array)
  return Array.from(array, (byte) => byte.toString(16).padStart(2, '0')).join('')
}

// Store current trace ID for session correlation
let currentTraceId: string | null = null
let traceStartTime: number | null = null

// Trace ID TTL in milliseconds (5 minutes)
const TRACE_TTL = 5 * 60 * 1000

/**
 * Get the current trace ID, generating a new one if needed
 * Trace IDs expire after TRACE_TTL to prevent stale traces
 */
export function getTraceId(): string {
  const now = Date.now()
  
  // Generate new trace if none exists or if expired
  if (!currentTraceId || !traceStartTime || (now - traceStartTime) > TRACE_TTL) {
    currentTraceId = generateTraceId()
    traceStartTime = now
  }
  
  return currentTraceId
}

/**
 * Force reset the trace ID (use when starting a new user action)
 */
export function resetTraceId(): string {
  currentTraceId = generateTraceId()
  traceStartTime = Date.now()
  return currentTraceId
}

/**
 * Get trace headers for HTTP requests
 */
export function getTraceHeaders(): Record<string, string> {
  const traceId = getTraceId()
  const spanId = generateSpanId()
  
  return {
    'X-Trace-ID': traceId,
    // W3C Trace Context format: version-traceid-spanid-flags
    // 00 = version, 01 = sampled flag
    'traceparent': `00-${traceId}-${spanId}-01`,
  }
}

/**
 * Trace-aware fetch wrapper
 * Automatically adds trace headers to all requests
 */
export async function tracedFetch(
  url: string,
  options: RequestInit = {}
): Promise<Response> {
  const traceHeaders = getTraceHeaders()
  const csrfToken = getCookie("csrf_token")
  
  // Merge headers
  const headers = new Headers(options.headers)
  // Respect caller-provided trace headers (useful for correlating multi-call flows).
  // Only inject defaults when missing.
  Object.entries(traceHeaders).forEach(([key, value]) => {
    if (!headers.has(key)) headers.set(key, value)
  })

  if (csrfToken && isUnsafeMethod(options.method)) {
    headers.set("X-CSRF-Token", csrfToken)
  }

  // Scope to the active workspace (parity with authFetch); a caller-supplied
  // header wins. Without this, connection-USE calls made via tracedFetch (e.g.
  // the connection test) would fall back to the personal workspace and 404 on a
  // shared connection.
  const activeWorkspaceId = getActiveWorkspaceId()
  if (activeWorkspaceId && !headers.has("X-Workspace-ID")) {
    headers.set("X-Workspace-ID", activeWorkspaceId)
  }

  const traceId = headers.get("X-Trace-ID") || traceHeaders["X-Trace-ID"]
  
  const startTime = performance.now()
  
  try {
    const response = await fetch(url, {
      ...options,
      // Cookie-based auth
      credentials: options.credentials ?? "include",
      headers,
    })

    // If we get a 401 from the API Gateway, the session cookie is stale/expired.
    // Redirect to /logout to clear the cookie and avoid "stuck in app but everything 401s".
    if (response.status === 401 && typeof window !== "undefined") {
      try {
        const reqUrl = new URL(url, window.location.origin)
        const gwUrl = new URL(API_GATEWAY_URL, window.location.origin)
        if (reqUrl.origin === gwUrl.origin && !reqUrl.pathname.startsWith("/api/v1/auth/")) {
          const next = `${window.location.pathname}${window.location.search}`
          window.location.href = `/logout?next=${encodeURIComponent(next)}`
        }
      } catch {
        // ignore URL parsing errors
      }
    }
    
    const duration = Math.round(performance.now() - startTime)
    
    // Debug logging in development
    if (process.env.NODE_ENV === 'development') {
      const method = options.method || 'GET'
      const status = response.status
      const statusEmoji = status >= 200 && status < 300 ? '✅' : status >= 400 ? '❌' : '⚠️'
      console.debug(
        `${statusEmoji} [Trace: ${traceId.slice(0, 8)}...] ${method} ${url} -> ${status} (${duration}ms)`
      )
    }
    
    return response
  } catch (error) {
    const duration = Math.round(performance.now() - startTime)
    
    if (process.env.NODE_ENV === 'development') {
      console.error(
        `❌ [Trace: ${traceId.slice(0, 8)}...] ${options.method || 'GET'} ${url} -> FAILED (${duration}ms)`,
        error
      )
    }
    
    throw error
  }
}

/**
 * Traced JSON fetch helper with user-friendly error parsing
 */
export async function tracedJsonFetch<T>(
  url: string,
  options: RequestInit = {}
): Promise<T> {
  const headers = new Headers(options.headers)
  headers.set('Content-Type', 'application/json')
  headers.set('Accept', 'application/json')
  
  const response = await tracedFetch(url, {
    ...options,
    headers,
  })
  
  if (!response.ok) {
    const errorText = await response.text()
    // Throw APIRequestError which provides user-friendly messages
    throw new APIRequestError(`HTTP ${response.status}: ${errorText}`)
  }
  
  return response.json()
}

/**
 * Create a traced API client for a base URL
 */
export function createTracedClient(baseUrl: string) {
  const normalizedBaseUrl = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl
  
  return {
    get: <T>(path: string, options?: RequestInit) =>
      tracedJsonFetch<T>(`${normalizedBaseUrl}${path}`, { ...options, method: 'GET' }),
    
    post: <T>(path: string, data?: unknown, options?: RequestInit) =>
      tracedJsonFetch<T>(`${normalizedBaseUrl}${path}`, {
        ...options,
        method: 'POST',
        body: data ? JSON.stringify(data) : undefined,
      }),
    
    put: <T>(path: string, data?: unknown, options?: RequestInit) =>
      tracedJsonFetch<T>(`${normalizedBaseUrl}${path}`, {
        ...options,
        method: 'PUT',
        body: data ? JSON.stringify(data) : undefined,
      }),
    
    patch: <T>(path: string, data?: unknown, options?: RequestInit) =>
      tracedJsonFetch<T>(`${normalizedBaseUrl}${path}`, {
        ...options,
        method: 'PATCH',
        body: data ? JSON.stringify(data) : undefined,
      }),
    
    delete: <T>(path: string, options?: RequestInit) =>
      tracedJsonFetch<T>(`${normalizedBaseUrl}${path}`, { ...options, method: 'DELETE' }),
    
    // Get current trace ID for debugging
    getTraceId,
    
    // Reset trace for new user action
    resetTrace: resetTraceId,
  }
}

// Export singleton clients for each service. Base URLs come from the canonical
// self-correcting config (imported above) — this file no longer re-derives them
// from raw env, which previously bypassed the origin guard and could ship a
// localhost base URL to the browser on a mis-baked bundle.
// Re-exported for back-compat with existing importers of these names.
export { API_GATEWAY_URL, ORCHESTRATOR_URL, LLM_SERVICE_URL }

export const tracedGateway = createTracedClient(API_GATEWAY_URL)
export const tracedOrchestrator = createTracedClient(ORCHESTRATOR_URL)
export const tracedLlm = createTracedClient(LLM_SERVICE_URL)
