// Authentication utilities

import { API_GATEWAY_URL } from "@/lib/config/api"
import { clearAllWorkspaceScopes } from "@/lib/workspace/scoped-storage"

export function getAuthToken(): string | null {
  // Tokens are stored in an HttpOnly cookie set by the API gateway.
  // HttpOnly cookies are intentionally not readable from JS.
  return null
}

// Return the currently logged-in user's ID, or null if not set.
// No hard-coded or synthetic user IDs – this must come from a real login flow.
export function getUserId(): string | null {
  return null
}

export function getUserEmail(): string | null {
  return null
}

export function isAuthenticated(): boolean {
  // Client-side cannot reliably determine auth when using HttpOnly cookies.
  // Route protection is enforced by `middleware.ts` (server-side).
  return true
}

export function logout() {
  if (typeof window === "undefined") return

  // Best-effort: ask API gateway to clear the HttpOnly cookie + session.
  // (Even if this fails, we still clear local UI state.)
  // Self-correcting base URL (see @/lib/config/api): on a real origin a
  // mis-baked localhost value is ignored in favor of the current origin.
  fetch(`${API_GATEWAY_URL}/api/v1/auth/logout`, {
    method: "POST",
    credentials: "include",
  }).catch(() => {})

  // Back-compat cleanup: older builds stored user metadata here.
  localStorage.removeItem("user_id")
  localStorage.removeItem("user_email")
  localStorage.removeItem("user_role")
  // Chat state is workspace-scoped, so clearing one key would strand every OTHER
  // workspace's conversation in the browser. Logging out means keep nothing.
  clearAllWorkspaceScopes("rsync_chat_session_id")
  clearAllWorkspaceScopes("rsync_chat_ui_v2_state")
  clearAllWorkspaceScopes("rsync_active_pipelines")

  window.location.href = "/login"
}

export function getAuthHeaders(): Record<string, string> {
  // Cookie-based auth: the browser will attach cookies automatically when
  // requests use `credentials: "include"` (see `authFetch`/`tracedFetch`).
  return {}
}
