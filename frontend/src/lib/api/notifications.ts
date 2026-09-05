/**
 * Notifications API — the header-bell inbox.
 *
 * Backs the bell in components/layout/Header.tsx. The api-gateway is the source
 * of truth (handlers/notifications.go); rows are written by the notifier
 * consumer (api-gateway/internal/notifier) from healer/executor Kafka events
 * and are scoped to the pipeline owner (user_id):
 *   - LIST         GET  /notifications              -> { notifications, unread_count }
 *   - UNREAD_COUNT GET  /notifications/unread-count -> { unread_count }
 *   - MARK_READ    POST /notifications/mark-read    { id }
 *   - MARK_ALL     POST /notifications/mark-all-read
 *
 * `action_url` is stored RELATIVE (e.g. /pipelines/{id}/schema-changes) — the
 * bell deep-links to it same-origin. Only external Slack/email delivery needs
 * the APP_BASE_URL host prefix.
 */

import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { extractErrorMessage } from "@/lib/utils/error-handling"

/**
 * One notification row as returned by the LIST endpoint.
 *
 * `title`, `impact` and `action_label` are resolved server-side from the
 * notifier's copy catalog (api-gateway/internal/notifier/catalog.go) — the
 * frontend renders them verbatim and holds no copy of its own, so adding a new
 * notification is a one-entry backend change with no UI edit.
 */
export interface Notification {
  id: string
  pipeline_id: string
  type: string
  /** `error` still arrives on rows written before the vocabulary was unified. */
  severity: "info" | "warning" | "critical" | "error" | string
  title: string
  message: string
  /** Display name of the owning pipeline — which of my pipelines is this about. */
  pipeline_name: string
  /** "Is my data still moving, and must I act?" — empty when not applicable. */
  impact: string
  /** Verb for the deep-link button ("Review change", "Reconnect"). */
  action_label: string
  /** Stable error code, for support. Never rendered as the headline. */
  reference_code: string
  action_url: string
  read: boolean
  read_at: string | null
  created_at: string
}

/** LIST response envelope: recent notifications + the total unread count. */
export interface NotificationList {
  notifications: Notification[]
  unread_count: number
}

/** Fetch the caller's recent notifications plus the authoritative unread count. */
export async function listNotifications(): Promise<NotificationList> {
  const res = await authFetch(API_ENDPOINTS.NOTIFICATIONS.LIST, { cache: "no-store" })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Failed to load notifications" }))
    throw new Error(extractErrorMessage(err) || "Failed to load notifications")
  }
  const data = await res.json().catch(() => null)
  return {
    notifications: (data?.notifications ?? []) as Notification[],
    unread_count: Number(data?.unread_count ?? 0),
  }
}

/** Fetch just the unread count — the cheap call the bell polls on an interval. */
export async function getUnreadCount(): Promise<number> {
  const res = await authFetch(API_ENDPOINTS.NOTIFICATIONS.UNREAD_COUNT, { cache: "no-store" })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Failed to load unread count" }))
    throw new Error(extractErrorMessage(err) || "Failed to load unread count")
  }
  const data = await res.json().catch(() => null)
  return Number(data?.unread_count ?? 0)
}

/** Mark a single notification read. Idempotent server-side. */
export async function markNotificationRead(id: string): Promise<void> {
  const res = await authFetch(API_ENDPOINTS.NOTIFICATIONS.MARK_READ, {
    method: "POST",
    body: JSON.stringify({ id }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Failed to mark notification read" }))
    throw new Error(extractErrorMessage(err) || "Failed to mark notification read")
  }
}

/** Mark every unread notification for the caller read. */
export async function markAllNotificationsRead(): Promise<void> {
  const res = await authFetch(API_ENDPOINTS.NOTIFICATIONS.MARK_ALL_READ, { method: "POST" })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Failed to mark notifications read" }))
    throw new Error(extractErrorMessage(err) || "Failed to mark notifications read")
  }
}
