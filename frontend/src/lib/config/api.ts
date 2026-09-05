/**
 * Centralized API Configuration
 * Single source of truth for all API endpoints
 * 
 * Internal URLs: Used for server-side fetches (Next.js server components) within Docker network
 * Public URLs: Used for client-side fetches (browser) via localhost
 */

// Base URLs - Server-side (internal Docker network)
// Defaults target host-exposed ports so `next dev` outside Docker works without env config.
// In Docker, compose explicitly sets API_GATEWAY_INTERNAL_URL=http://api-gateway:8080
// (and the equivalent for the orchestrator), so the fallback only kicks in for local dev.
export const API_GATEWAY_URL_INTERNAL = process.env.API_GATEWAY_INTERNAL_URL || "http://localhost:5001"
export const ORCHESTRATOR_URL_INTERNAL = process.env.ORCHESTRATOR_INTERNAL_URL || "http://localhost:8081"

// Base URLs - Client-side (browser access)
//
// Resolution is self-correcting to survive a mis-baked NEXT_PUBLIC_API_URL.
// In every reverse-proxied deployment (prod behind Traefik, local prod-mirror
// on :8080) the api-gateway is SAME-ORIGIN with the frontend — Traefik routes
// `/api/*` to the gateway on the same host that serves the app. So whenever the
// browser is on a real (non-localhost) origin, we use that origin and ignore any
// `http://localhost:5001` default that leaked into the bundle at build time.
// The explicit env var is only honored for cross-origin local dev
// (`next dev` on :3000 talking to the gateway on :5001).
// Shared loopback-host matcher used by the self-correcting resolvers below.
const LOCALHOST_HOST_RE = /^(localhost|127\.0\.0\.1|\[::1\])$/

declare global {
  interface Window {
    __RSYNC_RUNTIME__?: { apiUrl?: string; wsUrl?: string }
  }
}

/**
 * Configuration from the container that is actually running, read from a script
 * the root layout inlines into <head> (see @/lib/config/runtime-env).
 *
 * This exists because `NEXT_PUBLIC_*` is substituted at build time -- the
 * published images carry whatever CI passed, so on a server install the correct
 * values compose sets at runtime never reach this code. Empty whenever the
 * script has not been served (dev, tests, or a deployment that sets nothing),
 * which is why every caller keeps its previous fallback.
 *
 * The injection has to be an INLINE script and this file is the reason why:
 * API_GATEWAY_URL below is resolved at MODULE SCOPE, so the global must already
 * exist when the first app chunk runs. Anything deferred -- next/script, or an
 * external <script src> racing 14 async chunk tags -- loses that race and the
 * app silently falls back to its own origin.
 */
function runtimeConfig(key: "apiUrl" | "wsUrl"): string {
  if (typeof window === "undefined") return ""
  const value = window.__RSYNC_RUNTIME__?.[key]
  return typeof value === "string" ? value.trim() : ""
}

function resolveClientApiBase(): string {
  // Server-side render: use the configured public URL (or dev default).
  // SSR data fetches should use API_GATEWAY_URL_INTERNAL instead.
  if (typeof window === "undefined") {
    return process.env.NEXT_PUBLIC_API_URL || "http://localhost:5001"
  }
  // A published image cannot know its own address, so when the operator has
  // told the container one, it outranks anything inferred here. Still passed
  // through the leaked-localhost guard: an operator who sets PUBLIC_HOST to
  // localhost and then browses by IP should land on the origin, not on a
  // loopback address that resolves to the wrong machine.
  const runtimeBase = runtimeConfig("apiUrl")
  if (runtimeBase) {
    return rebaseLeakedLocalhost(runtimeBase)
  }
  const origin = window.location.origin
  if (!LOCALHOST_HOST_RE.test(window.location.hostname)) {
    // Real domain → api-gateway is same-origin behind the proxy. Always trust
    // the current origin, never a localhost value baked into the bundle.
    return origin
  }
  // localhost origin → `next dev` (:3000→:5001) needs the explicit override;
  // local prod-mirror (:8080) has it pointing at the same origin anyway.
  return process.env.NEXT_PUBLIC_API_URL || origin
}

// Rebase a build-time-baked service URL so a leaked localhost value can never
// escape to the browser on a real deployment. Unlike the bare same-origin
// api-gateway, the orchestrator / LLM / websocket services are reverse-proxied
// under a PATH PREFIX on the app's origin (e.g. https://host/orchestrator,
// https://host/llm, wss://host/ws), so we swap only the ORIGIN and preserve the
// path. A correct prod bake (real domain) passes through untouched; a localhost
// PAGE origin (next dev / local prod-mirror) keeps the explicit value so
// cross-origin local dev still works.
function rebaseLeakedLocalhost(baked: string): string {
  if (typeof window === "undefined") return baked
  if (LOCALHOST_HOST_RE.test(window.location.hostname)) return baked
  let parsed: URL
  try {
    parsed = new URL(baked, window.location.origin)
  } catch {
    return baked
  }
  if (!LOCALHOST_HOST_RE.test(parsed.hostname)) return baked
  // Leaked localhost on a real origin → rebase onto the current origin,
  // preserving the service's path prefix and matching ws/wss to the page scheme.
  const isWs = parsed.protocol === "ws:" || parsed.protocol === "wss:"
  const scheme = isWs
    ? window.location.protocol === "https:"
      ? "wss:"
      : "ws:"
    : window.location.protocol
  const path = parsed.pathname === "/" ? "" : parsed.pathname
  return `${scheme}//${window.location.host}${path}`
}

export const API_GATEWAY_URL = resolveClientApiBase()
export const ORCHESTRATOR_URL = rebaseLeakedLocalhost(
  process.env.NEXT_PUBLIC_BACKEND_ORCHESTRATOR_URL || "http://localhost:8081"
)
// Note: in `docker compose`, the Planner service is exposed on host port 5011.
export const LLM_SERVICE_URL = rebaseLeakedLocalhost(
  process.env.NEXT_PUBLIC_LLM_SERVICE_URL || "http://localhost:5011"
)

// True when `url` points at a loopback host while the current page is on a real
// (non-localhost) origin — i.e. a build-time-baked localhost value that would be
// a dead/incorrect link if shown to the browser. Use this to HIDE links to
// SEPARATE hosts (SigNoz, a connector's Docker health port, Superset, …) that
// legitimately cannot be same-origin rebased. Returns false on the server and on
// a localhost page origin, so SSR output and local dev are unaffected.
export function isLeakedLocalhostUrl(url: string | null | undefined): boolean {
  if (!url) return false
  if (typeof window === "undefined") return false
  if (LOCALHOST_HOST_RE.test(window.location.hostname)) return false
  try {
    return LOCALHOST_HOST_RE.test(new URL(url, window.location.origin).hostname)
  } catch {
    return false
  }
}

/**
 * Get the appropriate API URL based on execution context
 * @param clientUrl - URL for client-side requests (browser)
 * @param serverUrl - URL for server-side requests (Docker network)
 * @returns The appropriate URL based on whether code is running on server or client
 */
export function getApiUrl(clientUrl: string, serverUrl: string): string {
  // Check if we're running on the server (Next.js server components)
  const isServer = typeof window === 'undefined'
  return isServer ? serverUrl : clientUrl
}

// API Gateway Endpoints (port 5001)
export const API_ENDPOINTS = {
  API_GATEWAY_URL, // Export for use in API clients
  // Auth
  AUTH: {
    LOGIN: `${API_GATEWAY_URL}/api/v1/auth/login`,
    REGISTER: `${API_GATEWAY_URL}/api/v1/auth/register`,
    LOGOUT: `${API_GATEWAY_URL}/api/v1/auth/logout`,
    ME: `${API_GATEWAY_URL}/api/v1/auth/me`,
    UPDATE_ME: `${API_GATEWAY_URL}/api/v1/auth/me`,
    CHANGE_PASSWORD: `${API_GATEWAY_URL}/api/v1/auth/password`,
  },
  
  // Workspaces
  WORKSPACES: {
    LIST: `${API_GATEWAY_URL}/api/v1/workspaces`,
    CREATE: `${API_GATEWAY_URL}/api/v1/workspaces`,
    GET: (id: string) => `${API_GATEWAY_URL}/api/v1/workspaces/${id}`,
    UPDATE: (id: string) => `${API_GATEWAY_URL}/api/v1/workspaces/${id}`,
    DELETE: (id: string) => `${API_GATEWAY_URL}/api/v1/workspaces/${id}`,
    MEMBERS: (id: string) => `${API_GATEWAY_URL}/api/v1/workspaces/${id}/members`,
    MEMBER_ROLE: (id: string, userId: string) =>
      `${API_GATEWAY_URL}/api/v1/workspaces/${id}/members/${userId}/role`,
    MEMBER_DELETE: (id: string, userId: string) =>
      `${API_GATEWAY_URL}/api/v1/workspaces/${id}/members/${userId}`,
    INVITES: (id: string) => `${API_GATEWAY_URL}/api/v1/workspaces/${id}/invites`,
    INVITE_BY_ID: (id: string, inviteId: string) =>
      `${API_GATEWAY_URL}/api/v1/workspaces/${id}/invites/${inviteId}`,
    // Token-addressed invite endpoints (preview is public; accept is authed).
    INVITE_PREVIEW: (token: string) =>
      `${API_GATEWAY_URL}/api/v1/workspace-invites/${token}`,
    INVITE_ACCEPT: (token: string) =>
      `${API_GATEWAY_URL}/api/v1/workspace-invites/${token}/accept`,
  },

  // Notifications (header-bell inbox — populated by the notifier consumer)
  NOTIFICATIONS: {
    LIST: `${API_GATEWAY_URL}/api/v1/notifications`,
    UNREAD_COUNT: `${API_GATEWAY_URL}/api/v1/notifications/unread-count`,
    MARK_READ: `${API_GATEWAY_URL}/api/v1/notifications/mark-read`,
    MARK_ALL_READ: `${API_GATEWAY_URL}/api/v1/notifications/mark-all-read`,
  },

  // Chat (multi-turn conversation with slot-filling)
  CHAT: {
    MESSAGE: `${API_GATEWAY_URL}/api/v1/chat/message`,
  },
  
  // Connections (via API Gateway)
  CONNECTIONS: {
    LIST: `${API_GATEWAY_URL}/api/v1/connections`,
    LIST_INTERNAL: `${API_GATEWAY_URL_INTERNAL}/api/v1/connections`,
    CREATE: `${API_GATEWAY_URL}/api/v1/connections`,
    GET: (id: string) => `${API_GATEWAY_URL}/api/v1/connections/${id}`,
    UPDATE: (id: string) => `${API_GATEWAY_URL}/api/v1/connections/${id}`,
    DELETE: (id: string) => `${API_GATEWAY_URL}/api/v1/connections/${id}`,
    TEST: `${API_GATEWAY_URL}/api/v1/connections/test`,
    TEST_BY_ID: (id: string) => `${API_GATEWAY_URL}/api/v1/connections/${id}/test`,
  },
  
  // MCP Connectors (unified endpoints)
  CONNECTORS: {
    LIST: `${API_GATEWAY_URL}/api/v1/connectors`,
    GET: (name: string) => `${API_GATEWAY_URL}/api/v1/connectors/${name}`,
    LOGO: (name: string) => `${API_GATEWAY_URL}/api/v1/connectors/${name}/logo`,
    GENERATE: `${API_GATEWAY_URL}/api/v1/connectors/generate`,
    VALIDATE: `${API_GATEWAY_URL}/api/v1/connectors/validate`,
  },

  // OAuth (via API Gateway)
  OAUTH: {
    PROVIDERS: `${API_GATEWAY_URL}/api/v1/oauth/providers`,
    AUTHORIZE: (provider: string) => `${API_GATEWAY_URL}/api/v1/oauth/${provider}/authorize`,
    CALLBACK: (provider: string) => `${API_GATEWAY_URL}/oauth/callback/${provider}`,
    TOKENS: `${API_GATEWAY_URL}/api/v1/oauth/tokens`,
    REFRESH_TOKEN: (tokenId: string) => `${API_GATEWAY_URL}/api/v1/oauth/tokens/${tokenId}/refresh`,
    REVOKE_TOKEN: (tokenId: string) => `${API_GATEWAY_URL}/api/v1/oauth/tokens/${tokenId}`,
    // Per-user "bring your own" OAuth apps (client_id/secret) for providers
    // without an operator-set env app (e.g. spec-generated connectors).
    APPS: `${API_GATEWAY_URL}/api/v1/oauth/apps`,
    APP: (provider: string) => `${API_GATEWAY_URL}/api/v1/oauth/apps/${provider}`,
  },
  
  // Pipelines
  PIPELINES: {
    LIST: `${API_GATEWAY_URL}/api/v1/pipelines`,
    LIST_INTERNAL: `${API_GATEWAY_URL_INTERNAL}/api/v1/pipelines`,
    CREATE: `${API_GATEWAY_URL}/api/v1/pipelines`,
    STATS: `${API_GATEWAY_URL}/api/v1/pipelines/stats`,
    COMPARE: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/compare`,
    CDC_RECOVER: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/cdc/recover`,
    CDC_BACKFILL: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/cdc/backfill`,
    GET: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}`,
    GET_INTERNAL: (id: string) => `${API_GATEWAY_URL_INTERNAL}/api/v1/pipelines/${id}`,
    UPDATE: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}`,
    DELETE: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}`,
    RUN: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/run`,
    RUN_INTERNAL: (id: string) => `${API_GATEWAY_URL_INTERNAL}/api/v1/pipelines/${id}/run`,
    PAUSE: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/pause`,
    RESUME: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/resume`,
    STOP: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/stop`,
    CDC_PAUSE: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/cdc/pause`,
    CDC_RESUME: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/cdc/resume`,
    HITL_CONNECTIONS: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/hitl/connections`,
    HITL_CONNECTORS: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/hitl/connectors`,
    HITL_TABLES: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/hitl/tables`,
    HITL_NODE_INPUT: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/hitl/node-input`,
    // Persisted pipeline configuration helpers
    TABLES: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/tables`,
    MONITORING_OVERVIEW: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/monitoring/overview`,
    // Canonical runtime view — single source of truth for "what is this pipeline
    // doing right now". Replaces UI-side state derivation.
    RUNTIME: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/runtime`,
    DIAGNOSE: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/diagnose`,
    TABLE_STATS: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/table-stats`,
    EVENTS: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/events`,
    SCHEMA_CHANGES: (id: string) => `${API_GATEWAY_URL}/api/v1/pipelines/${id}/schema-changes`,
    SCHEMA_CHANGE_APPROVE: (id: string, changeId: string) =>
      `${API_GATEWAY_URL}/api/v1/pipelines/${id}/schema-changes/${changeId}/approve`,
    SCHEMA_CHANGE_REJECT: (id: string, changeId: string) =>
      `${API_GATEWAY_URL}/api/v1/pipelines/${id}/schema-changes/${changeId}/reject`,
  },
  
  // Monitoring
  MONITORING: {
    SENTINEL_HEALTH: `${API_GATEWAY_URL}/api/v1/monitoring/sentinel/health`,
    SENTINEL_ISSUES: `${API_GATEWAY_URL}/api/v1/monitoring/sentinel/issues`,
    FEATURES: `${API_GATEWAY_URL}/api/v1/features`,
  },
  
  // Executions
  EXECUTIONS: {
    LIST: `${API_GATEWAY_URL}/api/v1/executions`,
    LIST_INTERNAL: `${API_GATEWAY_URL_INTERNAL}/api/v1/executions`,
    GET: (id: string) => `${API_GATEWAY_URL}/api/v1/executions/${id}`,
    GET_INTERNAL: (id: string) => `${API_GATEWAY_URL_INTERNAL}/api/v1/executions/${id}`,
    TRANSFORMS: (id: string) => `${API_GATEWAY_URL}/api/v1/executions/${id}/transforms`,
    TRANSFORMS_INTERNAL: (id: string) => `${API_GATEWAY_URL_INTERNAL}/api/v1/executions/${id}/transforms`,
    CANCEL: (id: string) => `${API_GATEWAY_URL}/api/v1/executions/${id}/cancel`,
    DIAGNOSE: (id: string) => `${API_GATEWAY_URL}/api/v1/executions/${id}/diagnose`,
    DIAGNOSE_INTERNAL: (id: string) => `${API_GATEWAY_URL_INTERNAL}/api/v1/executions/${id}/diagnose`,
  },

  // Transforms — read-only monitoring + versioning views over transform_execution_logs.
  TRANSFORMS: {
    PIPELINE: (pipelineId: string) => `${API_GATEWAY_URL}/api/v1/transforms/pipeline/${pipelineId}`,
    // Per-execution GROUP BY rollup (in/out totals, honest rows-dropped, freshness).
    ROLLUP: (pipelineId: string) => `${API_GATEWAY_URL}/api/v1/transforms/pipeline/${pipelineId}/rollup`,
    // config_snapshot revision timeline per transform slot (deduped to real changes).
    CONFIG_HISTORY: (pipelineId: string) => `${API_GATEWAY_URL}/api/v1/transforms/pipeline/${pipelineId}/config-history`,
  },
}

// Orchestrator Endpoints (port 8081) - New Architecture
export const ORCHESTRATOR_ENDPOINTS = {
  // New Architecture - Control Plane + Workers
  PIPELINES: {
    LIST: `${ORCHESTRATOR_URL}/api/v1/pipelines`,
    LIST_INTERNAL: `${ORCHESTRATOR_URL_INTERNAL}/api/v1/pipelines`,
    CREATE: `${ORCHESTRATOR_URL}/api/v1/pipelines`,
    CREATE_INTERNAL: `${ORCHESTRATOR_URL_INTERNAL}/api/v1/pipelines`,
    GET: (id: string) => `${ORCHESTRATOR_URL}/api/v1/pipelines/${id}`,
    GET_INTERNAL: (id: string) => `${ORCHESTRATOR_URL_INTERNAL}/api/v1/pipelines/${id}`,
    CANCEL: (id: string) => `${ORCHESTRATOR_URL}/api/v1/pipelines/${id}/cancel`,
    RESUME: (id: string) => `${ORCHESTRATOR_URL}/api/v1/pipelines/${id}/resume`,
    EVENTS: (id: string) => `${ORCHESTRATOR_URL}/api/v1/pipelines/${id}/events`,
    EVENTS_INTERNAL: (id: string) => `${ORCHESTRATOR_URL_INTERNAL}/api/v1/pipelines/${id}/events`,
    TELEMETRY: (id: string) => `${ORCHESTRATOR_URL}/api/v1/pipelines/${id}/telemetry`,
    TELEMETRY_INTERNAL: (id: string) => `${ORCHESTRATOR_URL_INTERNAL}/api/v1/pipelines/${id}/telemetry`,
    TEST: `${ORCHESTRATOR_URL}/api/v1/test/pipeline`, // Test endpoint
  },
  
  // Direct orchestrator calls (if needed)
  CONNECTIONS: {
    LIST: `${ORCHESTRATOR_URL}/api/v1/connections`,
    CREATE: `${ORCHESTRATOR_URL}/api/v1/connections`,
    GET: (id: string) => `${ORCHESTRATOR_URL}/api/v1/connections/${id}`,
    DELETE: (id: string) => `${ORCHESTRATOR_URL}/api/v1/connections/${id}`,
    TEST: `${ORCHESTRATOR_URL}/api/v1/connections/test`,
  },
  
  // Status & Health
  HEALTH: `${ORCHESTRATOR_URL}/health`,
  WORKERS: `${ORCHESTRATOR_URL}/workers`,
  
  // CDC Pipelines (Legacy)
  CDC_PIPELINES: {
    LIST: `${ORCHESTRATOR_URL}/api/v1/cdc/data-pipelines`,
    LIST_INTERNAL: `${ORCHESTRATOR_URL_INTERNAL}/api/v1/cdc/data-pipelines`,
    GET: (id: string) => `${ORCHESTRATOR_URL}/api/v1/cdc/data-pipelines/${id}`,
  },
  
  // Agentic endpoints
  AGENTIC: {
    CONNECTIONS: `${ORCHESTRATOR_URL}/api/v1/connections`,
  },
  
  // NEW: HITL Decision Management
  DECISIONS: {
    LIST_BY_PIPELINE: (pipelineId: string) => `${ORCHESTRATOR_URL}/api/v1/pipelines/${pipelineId}/decisions`,
    GET: (decisionId: string) => `${ORCHESTRATOR_URL}/api/v1/decisions/${decisionId}`,
    RESPOND: (decisionId: string) => `${ORCHESTRATOR_URL}/api/v1/decisions/${decisionId}/respond`,
    CANCEL: (decisionId: string) => `${ORCHESTRATOR_URL}/api/v1/decisions/${decisionId}`,
  },
}

// WebSocket URLs - Generic (uses environment variables)
// Self-correcting: a mis-baked localhost value is rebased onto the current page
// origin (ws→wss under https) so it can never leak to the browser on a real
// deployment. Correct real-domain bakes pass through untouched.
export const WS_ENDPOINTS = {
  ORCHESTRATOR: rebaseLeakedLocalhost(process.env.NEXT_PUBLIC_WS_ORCHESTRATOR_URL || `ws://localhost:8081/ws`),
  // Same build-time-vs-runtime problem as the HTTP base above: on a published
  // image NEXT_PUBLIC_WS_URL is whatever CI baked, so the running container's
  // value has to win. Without this the live pipeline feed silently opens a
  // socket against the UI's own origin, which serves no /ws.
  API_GATEWAY: rebaseLeakedLocalhost(
    runtimeConfig("wsUrl") || process.env.NEXT_PUBLIC_WS_URL || `ws://localhost:5001/ws`
  ),
}

// Helper to get the right base URL for a service
export function getServiceURL(service: 'gateway' | 'orchestrator' | 'llm'): string {
  switch (service) {
    case 'gateway':
      return API_GATEWAY_URL
    case 'orchestrator':
      return ORCHESTRATOR_URL
    case 'llm':
      return LLM_SERVICE_URL
    default:
      return API_GATEWAY_URL
  }
}
