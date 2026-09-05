/**
 * Client-side fetch helper
 * Trace + auth aware fetch helper (client-side).
 *
 * NOTE:
 * - Uses `NEXT_PUBLIC_API_URL` as base (browser access).
 * - Automatically attaches auth headers from `@/lib/auth` unless skipAuth=true.
 */

import { extractErrorMessage, httpErrorFromResponse } from "@/lib/utils/error-handling"
import { getAuthHeaders } from "@/lib/auth"
import { getActiveWorkspaceId } from "@/lib/workspace/active-workspace"
import { API_GATEWAY_URL } from "@/lib/config/api"

// Self-correcting base URL (see @/lib/config/api): on a real (non-localhost)
// page origin a mis-baked localhost value is ignored in favor of the current
// origin, so authFetch can never issue a browser request to localhost on a real
// deployment. On the server this resolves to the same NEXT_PUBLIC_API_URL / dev
// default as before, so SSR behavior is unchanged.
const API_URL = API_GATEWAY_URL

export interface FetchOptions extends RequestInit {
  skipAuth?: boolean
  /**
   * Abort the request after this many milliseconds. Omit for no timeout — the
   * historical default, preserved for every existing caller. Set it on
   * interactive / HITL-resume calls so a hung socket surfaces an error the UI
   * can recover from instead of pinning a spinner forever.
   */
  timeoutMs?: number
}

function getCookie(name: string): string | null {
  if (typeof document === "undefined") return null
  const match = document.cookie.match(new RegExp(`(?:^|;\\s*)${name}=([^;]+)`))
  return match ? decodeURIComponent(match[1]) : null
}

function isUnsafeMethod(method?: string): boolean {
  const m = (method || "GET").toUpperCase()
  return m !== "GET" && m !== "HEAD" && m !== "OPTIONS"
}

function randomHex(bytes: number): string {
  // Prefer WebCrypto when available (browser + modern Node runtimes).
  const cryptoObj = (globalThis as unknown as { crypto?: Crypto }).crypto
  if (cryptoObj?.getRandomValues) {
    const arr = new Uint8Array(bytes)
    cryptoObj.getRandomValues(arr)
    return Array.from(arr, (b) => b.toString(16).padStart(2, "0")).join("")
  }

  // Fallback: randomUUID → hex (good enough for dev)
  const uuid = (globalThis as unknown as { crypto?: { randomUUID?: () => string } }).crypto?.randomUUID?.() || ""
  const clean = uuid.replace(/-/g, "")
  if (clean.length >= bytes * 2) return clean.slice(0, bytes * 2)
  // Last resort: pad with Math.random
  return Array.from({ length: bytes * 2 }, () => Math.floor(Math.random() * 16).toString(16)).join("")
}

function getTraceHeaders(): Record<string, string> {
  let traceId = randomHex(16) // 32 hex chars
  // Guard against the invalid all-zero trace id (rare, but possible with fallbacks).
  if (traceId === "0".repeat(32)) traceId = randomHex(16)

  let spanId = randomHex(8) // 16 hex chars
  if (spanId === "0".repeat(16)) spanId = randomHex(8)
  return {
    "X-Trace-ID": traceId,
    traceparent: `00-${traceId}-${spanId}-01`,
  }
}

/**
 * Get access token (returns null - no auth required)
 */
export async function getAccessToken(): Promise<string | null> {
  return null
}

/**
 * Fetch wrapper for client-side API calls
 */
export async function authFetch(
  endpoint: string,
  options: FetchOptions = {}
): Promise<Response> {
  const { skipAuth = false, headers = {}, timeoutMs, signal: callerSignal, ...restOptions } = options

  const url = endpoint.startsWith("http") ? endpoint : `${API_URL}${endpoint}`
  
  const traceHeaders = getTraceHeaders()
  const authHeaders = skipAuth ? {} : getAuthHeaders()
  const csrfToken = getCookie("csrf_token")
  const requestHeaders: HeadersInit = {
    "Content-Type": "application/json",
    Accept: "application/json",
    ...traceHeaders,
    ...authHeaders,
    ...headers,
  }

  // CSRF double-submit: attach header for state-changing requests when cookie is present.
  if (csrfToken && isUnsafeMethod(restOptions.method)) {
    ;(requestHeaders as Record<string, string>)["X-CSRF-Token"] = csrfToken
  }

  // Scope the request to the active workspace (backend reads X-Workspace-ID as the
  // primary selector). A caller-supplied header wins so callers can target a
  // specific workspace explicitly.
  const activeWorkspaceId = getActiveWorkspaceId()
  if (activeWorkspaceId && !(requestHeaders as Record<string, string>)["X-Workspace-ID"]) {
    ;(requestHeaders as Record<string, string>)["X-Workspace-ID"] = activeWorkspaceId
  }
  
  // Optional client-side timeout. Compose it with any caller-supplied AbortSignal so
  // either the timeout OR an explicit abort can cancel the request. Without timeoutMs,
  // `signal` is exactly the caller's (or undefined) — behaviour is unchanged.
  let timeoutId: ReturnType<typeof setTimeout> | undefined
  let removeCallerAbortListener: (() => void) | undefined
  let signal: AbortSignal | undefined = callerSignal ?? undefined
  if (typeof timeoutMs === "number" && timeoutMs > 0) {
    const controller = new AbortController()
    timeoutId = setTimeout(
      () => controller.abort(new DOMException("The request timed out. Please try again.", "TimeoutError")),
      timeoutMs
    )
    if (callerSignal) {
      if (callerSignal.aborted) {
        controller.abort(callerSignal.reason)
      } else {
        const onCallerAbort = () => controller.abort(callerSignal.reason)
        callerSignal.addEventListener("abort", onCallerAbort, { once: true })
        // Detached in finally so a reused caller signal doesn't accumulate listeners.
        removeCallerAbortListener = () => callerSignal.removeEventListener("abort", onCallerAbort)
      }
    }
    signal = controller.signal
  }

  try {
    const response = await fetch(url, {
      ...restOptions,
      // Cookie-based auth
      credentials: restOptions.credentials ?? "include",
      headers: requestHeaders,
      signal,
    })

    // Global session-expired handling:
    // If the API says the token is invalid/expired, clear the cookie via /logout
    // so the frontend middleware stops treating the user as authenticated.
    if (
      !skipAuth &&
      response.status === 401 &&
      typeof window !== "undefined" &&
      !String(endpoint).includes("/api/v1/auth/")
    ) {
      const next = `${window.location.pathname}${window.location.search}`
      window.location.href = `/logout?next=${encodeURIComponent(next)}`
    }

    return response
  } finally {
    if (timeoutId) clearTimeout(timeoutId)
    removeCallerAbortListener?.()
  }
}

/**
 * `authFetch` that refuses to let a non-2xx look like a success.
 *
 * Use this wherever the caller's only reaction to failure would otherwise be a
 * bare `catch` or an unchecked `await` — an action button, or a read whose
 * empty result is rendered as a real answer. A 403 from the workspace-role gate
 * and a 200 are pixel-identical when nobody checks `.ok`, and "the pipeline
 * stopped" vs "you were not allowed to stop it" is not a distinction the UI may
 * silently drop.
 *
 * Deliberately a SIBLING of `authFetch` rather than a change to it: 200 call
 * sites use `authFetch` today, 92 of them check `.ok` themselves and 15 branch
 * on a specific status (a 404 that means "this pipeline has no CDC" is not an
 * error). Making the shared helper throw would turn those branches into dead
 * code and route them into `catch` blocks written for network faults.
 *
 * The thrown Error is encoded by `httpErrorFromResponse` as
 * `HTTP <status>: <body>`, which is the form `parseApiError` / `classifyError`
 * already understand — so a call site can do
 * `catch (e) { const c = classifyError(e, "pipeline.stop"); toast.error(c.title, ...) }`
 * and get the status-aware message for free.
 *
 * On success the Response is returned with its body still unread, so existing
 * `await res.json()` / `res.blob()` at the call site keep working.
 */
export async function authFetchOrThrow(
  endpoint: string,
  options: FetchOptions = {}
): Promise<Response> {
  const response = await authFetch(endpoint, options)
  if (!response.ok) {
    throw await httpErrorFromResponse(response, `Request failed (HTTP ${response.status})`)
  }
  return response
}

/**
 * GET request
 */
export async function authGet<T>(
  endpoint: string,
  options: FetchOptions = {}
): Promise<T> {
  const response = await authFetch(endpoint, {
    ...options,
    method: "GET",
  })
  
  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: "Request failed" }))
    throw new Error(extractErrorMessage(error) || `HTTP ${response.status}`)
  }
  
  return response.json()
}

/**
 * POST request
 */
export async function authPost<T>(
  endpoint: string,
  data?: unknown,
  options: FetchOptions = {}
): Promise<T> {
  const response = await authFetch(endpoint, {
    ...options,
    method: "POST",
    body: data ? JSON.stringify(data) : undefined,
  })
  
  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: "Request failed" }))
    throw new Error(extractErrorMessage(error) || `HTTP ${response.status}`)
  }
  
  return response.json()
}

/**
 * PUT request
 */
export async function authPut<T>(
  endpoint: string,
  data?: unknown,
  options: FetchOptions = {}
): Promise<T> {
  const response = await authFetch(endpoint, {
    ...options,
    method: "PUT",
    body: data ? JSON.stringify(data) : undefined,
  })
  
  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: "Request failed" }))
    throw new Error(extractErrorMessage(error) || `HTTP ${response.status}`)
  }
  
  return response.json()
}

/**
 * DELETE request
 */
export async function authDelete<T>(
  endpoint: string,
  options: FetchOptions = {}
): Promise<T> {
  const response = await authFetch(endpoint, {
    ...options,
    method: "DELETE",
  })
  
  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: "Request failed" }))
    throw new Error(extractErrorMessage(error) || `HTTP ${response.status}`)
  }
  
  return response.json()
}

export interface CurrentUser {
  id: string
  email: string
  name: string
  role: string
  status: string
  // Plan fields — added by backend PR 103. Optional: absent on older deployments.
  plan?: "trial" | "free" | "starter" | "pro"
  trial_ends_at?: string | null
  pipelines_used?: number
  pipelines_limit?: number | null
}

/**
 * Get the current authenticated user.
 *
 * Server-side (Next.js server components / route handlers): forwards the
 * `auth_token` cookie from the incoming request to the API gateway.
 * Client-side: relies on the browser to attach cookies via `credentials: "include"`.
 *
 * Returns `null` when no user is logged in or the session is invalid.
 */
export async function getCurrentUser(): Promise<CurrentUser | null> {
  const isServer = typeof window === "undefined"

  let url: string
  let cookieHeader = ""
  if (isServer) {
    const internalBase =
      process.env.API_GATEWAY_INTERNAL_URL || "http://api-gateway:8080"
    url = `${internalBase}/api/v1/auth/me`
    try {
      const { cookies } = await import("next/headers")
      const store = await cookies()
      const token = store.get("auth_token")?.value
      if (token) {
        cookieHeader = `auth_token=${token}`
      }
    } catch {
      // Outside a request scope (e.g. build) — no cookies available.
    }
  } else {
    url = `${API_URL}/api/v1/auth/me`
  }

  try {
    const response = await fetch(url, {
      method: "GET",
      credentials: "include",
      headers: {
        Accept: "application/json",
        ...(cookieHeader ? { Cookie: cookieHeader } : {}),
      },
      cache: "no-store",
    })

    if (!response.ok) {
      return null
    }

    const data = await response.json()
    if (!data?.user_id) {
      return null
    }

    return {
      id: data.user_id,
      email: data.email,
      name: data.name || "",
      role: data.role || "viewer",
      status: data.status || "active",
      plan: data.plan,
      trial_ends_at: data.trial_ends_at,
      pipelines_used: data.pipelines_used,
      pipelines_limit: data.pipelines_limit,
    }
  } catch {
    return null
  }
}
