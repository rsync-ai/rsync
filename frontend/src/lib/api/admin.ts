import { authGet, authPost, authFetch } from "./auth-fetch"

// ============================================================================
// Types
// ============================================================================

export interface AdminUser {
  id: string
  email: string
  name: string
  role: string
  status: string
  created_at: string
  last_login?: string | null
}

export interface AdminUserDetail {
  user: AdminUser
  audit_logs: AdminAuditLog[]
}

export interface AdminInvitation {
  id: string
  token: string
  email_hint: string
  role: string
  status: "pending" | "used" | "expired"
  created_by_email: string
  expires_at: string
  used_by_email?: string | null
  used_at?: string | null
  created_at: string
}

export interface AdminAuditLog {
  id: string
  user_id?: string | null
  user_email?: string | null
  action: string
  resource_type: string
  resource_id?: string | null
  details?: Record<string, unknown> | null
  ip_address?: string | null
  created_at: string
}

export interface ServiceHealth {
  service: string
  status: "up" | "down" | "degraded"
  latency_ms: number
  error?: string
}

export interface PaginatedResponse<T> {
  data: T[]
  total: number
  limit: number
  offset: number
}

export interface InviteValidation {
  valid: boolean
  reason?: string
  email_hint?: string
  role?: string
  expires_at?: string
}

// ============================================================================
// Users
// ============================================================================

export async function adminListUsers(params?: {
  q?: string
  role?: string
  status?: string
  limit?: number
  offset?: number
}): Promise<PaginatedResponse<AdminUser>> {
  const searchParams = new URLSearchParams()
  if (params?.q) searchParams.set("q", params.q)
  if (params?.role) searchParams.set("role", params.role)
  if (params?.status) searchParams.set("status", params.status)
  if (params?.limit) searchParams.set("limit", String(params.limit))
  if (params?.offset) searchParams.set("offset", String(params.offset))
  const qs = searchParams.toString()
  return authGet(`/api/v1/admin/users${qs ? `?${qs}` : ""}`)
}

export async function adminGetUser(id: string): Promise<AdminUserDetail> {
  return authGet(`/api/v1/admin/users/${id}`)
}

export async function adminUpdateUserRole(id: string, role: string): Promise<{ message: string; role: string }> {
  const res = await authFetch(`/api/v1/admin/users/${id}/role`, {
    method: "PATCH",
    body: JSON.stringify({ role }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Request failed" }))
    throw new Error(err.error || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function adminUpdateUserStatus(id: string, status: string): Promise<{ message: string; status: string }> {
  const res = await authFetch(`/api/v1/admin/users/${id}/status`, {
    method: "PATCH",
    body: JSON.stringify({ status }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Request failed" }))
    throw new Error(err.error || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function adminDeleteUser(id: string): Promise<{ message: string }> {
  const res = await authFetch(`/api/v1/admin/users/${id}`, { method: "DELETE" })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Request failed" }))
    throw new Error(err.error || `HTTP ${res.status}`)
  }
  return res.json()
}

// ============================================================================
// Invitations
// ============================================================================

export async function adminCreateInvitation(body: {
  email_hint?: string
  role?: string
}): Promise<{ id: string; token: string; url: string; expires_at: string }> {
  return authPost("/api/v1/admin/invitations", body)
}

export async function adminListInvitations(params?: {
  limit?: number
  offset?: number
}): Promise<PaginatedResponse<AdminInvitation>> {
  const searchParams = new URLSearchParams()
  if (params?.limit) searchParams.set("limit", String(params.limit))
  if (params?.offset) searchParams.set("offset", String(params.offset))
  const qs = searchParams.toString()
  return authGet(`/api/v1/admin/invitations${qs ? `?${qs}` : ""}`)
}

export async function adminRevokeInvitation(id: string): Promise<{ message: string }> {
  const res = await authFetch(`/api/v1/admin/invitations/${id}`, { method: "DELETE" })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Request failed" }))
    throw new Error(err.error || `HTTP ${res.status}`)
  }
  return res.json()
}

// ============================================================================
// Audit Logs
// ============================================================================

export async function adminListAuditLogs(params?: {
  user_id?: string
  action?: string
  resource_type?: string
  from?: string
  to?: string
  q?: string
  limit?: number
  offset?: number
}): Promise<PaginatedResponse<AdminAuditLog>> {
  const searchParams = new URLSearchParams()
  if (params?.user_id) searchParams.set("user_id", params.user_id)
  if (params?.action) searchParams.set("action", params.action)
  if (params?.resource_type) searchParams.set("resource_type", params.resource_type)
  if (params?.from) searchParams.set("from", params.from)
  if (params?.to) searchParams.set("to", params.to)
  if (params?.q) searchParams.set("q", params.q)
  if (params?.limit) searchParams.set("limit", String(params.limit))
  if (params?.offset) searchParams.set("offset", String(params.offset))
  const qs = searchParams.toString()
  return authGet(`/api/v1/admin/audit-logs${qs ? `?${qs}` : ""}`)
}

// ============================================================================
// Settings
// ============================================================================

export async function adminGetSettings(): Promise<{ settings: Record<string, string> }> {
  return authGet("/api/v1/admin/settings")
}

export async function adminUpdateSettings(settings: Record<string, string>): Promise<{ message: string }> {
  const res = await authFetch("/api/v1/admin/settings", {
    method: "PATCH",
    body: JSON.stringify(settings),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Request failed" }))
    throw new Error(err.error || `HTTP ${res.status}`)
  }
  return res.json()
}

// ============================================================================
// Health
// ============================================================================

export async function adminGetHealth(): Promise<{ services: ServiceHealth[] }> {
  return authGet("/api/v1/admin/health")
}

// ============================================================================
// Public (no admin required)
// ============================================================================

export async function validateInvite(token: string): Promise<InviteValidation> {
  return authGet(`/api/v1/auth/invite/${token}`, { skipAuth: true })
}
