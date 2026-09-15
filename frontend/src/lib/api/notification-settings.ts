/**
 * Notification delivery settings — where alerts go outside the bell.
 *
 * Two audiences, one module:
 *   - Admin (instance-wide): Slack webhook + SMTP server, which alert
 *     categories reach Slack and email at all, and extra addresses that get
 *     every emailed alert. handlers/notification_channels.go
 *       GET  /admin/notifications/channels -> NotificationChannels
 *       PUT  /admin/notifications/channels  NotificationChannelsUpdate
 *       POST /admin/notifications/test      { channel }
 *   - Every user: whether they get email at all, and which categories — within
 *     what the admin allows (email_blocked_categories).
 *       GET  /notifications/preferences -> NotificationPreferences
 *       PUT  /notifications/preferences  { email_enabled, email_categories }
 *
 * Secrets never come back from the API. The view says only whether a webhook /
 * SMTP password is configured; on update, omitting the field (or sending null)
 * keeps the stored secret and "" clears it.
 */

import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { extractErrorMessage } from "@/lib/utils/error-handling"

/** One mutable group of alerts. Labels and descriptions are server-owned. */
export interface NotificationCategory {
  id: string
  label: string
  description: string
}

/** Category id -> delivered (true) or muted (false). */
export type CategoryToggles = Record<string, boolean>

export type SmtpTlsMode = "starttls" | "tls" | "none"

export interface NotificationChannels {
  /** "environment" = nothing saved yet; the SMTP_* / NOTIFIER_* env vars apply. */
  source: "database" | "environment"
  updated_at: string | null
  slack: {
    enabled: boolean
    webhook_configured: boolean
    categories: CategoryToggles
  }
  email: {
    enabled: boolean
    smtp_host: string
    smtp_port: number
    smtp_username: string
    password_configured: boolean
    from: string
    /** "opportunistic" only appears in environment mode. */
    tls_mode: SmtpTlsMode | "opportunistic" | string
    /** Categories emailed at all. Off = no owner and no alert-list address gets it. */
    categories: CategoryToggles
    /** Addresses that receive every emailed alert, whatever the owner chose. */
    extra_recipients: string[]
  }
  categories: NotificationCategory[]
}

export interface NotificationChannelsUpdate {
  slack: {
    enabled: boolean
    /** Omit/null keeps the saved webhook; "" clears it. */
    webhook_url?: string | null
    categories?: CategoryToggles
  }
  email: {
    enabled: boolean
    smtp_host: string
    smtp_port: number
    smtp_username: string
    /** Omit/null keeps the saved password; "" clears it. */
    smtp_password?: string | null
    from: string
    tls_mode: SmtpTlsMode
    categories?: CategoryToggles
    /** Omit keeps the saved list; [] clears it. */
    extra_recipients?: string[]
  }
}

export interface TestNotificationResult {
  success: boolean
  channel: "slack" | "email"
  /** The admin's own address, for an email test. */
  sent_to?: string
}

export interface NotificationPreferences {
  email_enabled: boolean
  email_categories: CategoryToggles
  /** Categories the admin turned off for email; the user cannot turn them on. */
  email_blocked_categories: string[]
  categories: NotificationCategory[]
  /** Which channels the instance has switched on. */
  channels: { email: boolean; slack: boolean }
}

export interface NotificationPreferencesUpdate {
  email_enabled: boolean
  email_categories?: CategoryToggles
}

/** An HTTP failure that keeps its status, so pages can tell 403 from 500. */
export class NotificationSettingsError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message)
    this.name = "NotificationSettingsError"
  }
}

async function readJSON<T>(res: Response, fallback: string): Promise<T> {
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: fallback }))
    // A failed test send carries the SMTP / Slack reply in `detail`; that is
    // the part the admin needs to fix the setup.
    const base = extractErrorMessage(err) || fallback
    const detail = typeof err?.detail === "string" && err.detail ? `: ${err.detail}` : ""
    throw new NotificationSettingsError(base + detail, res.status)
  }
  return res.json() as Promise<T>
}

// A 200 whose body is not the view (a proxy page, a stale gateway) must fail
// like any other read. Defaulting the missing fields would render "email off"
// or blank SMTP fields, and saving that would overwrite the real settings.
function channelsView(res: Response, data: NotificationChannels): NotificationChannels {
  if (typeof data?.slack?.enabled !== "boolean" || typeof data?.email?.enabled !== "boolean") {
    throw new NotificationSettingsError("Unexpected notification channels response", res.status)
  }
  return {
    ...data,
    slack: { ...data.slack, categories: data.slack.categories ?? {} },
    email: {
      ...data.email,
      categories: data.email.categories ?? {},
      extra_recipients: Array.isArray(data.email.extra_recipients) ? data.email.extra_recipients : [],
    },
    categories: Array.isArray(data.categories) ? data.categories : [],
  }
}

function preferencesView(res: Response, data: NotificationPreferences): NotificationPreferences {
  if (typeof data?.email_enabled !== "boolean") {
    throw new NotificationSettingsError("Unexpected notification preferences response", res.status)
  }
  return {
    ...data,
    email_categories: data.email_categories ?? {},
    email_blocked_categories: Array.isArray(data.email_blocked_categories) ? data.email_blocked_categories : [],
    categories: Array.isArray(data.categories) ? data.categories : [],
    channels: { email: data.channels?.email === true, slack: data.channels?.slack === true },
  }
}

export async function getNotificationChannels(): Promise<NotificationChannels> {
  const res = await authFetch(API_ENDPOINTS.NOTIFICATIONS.ADMIN_CHANNELS, { cache: "no-store" })
  return channelsView(res, await readJSON(res, "Failed to load notification channels"))
}

export async function updateNotificationChannels(
  update: NotificationChannelsUpdate,
): Promise<NotificationChannels> {
  const res = await authFetch(API_ENDPOINTS.NOTIFICATIONS.ADMIN_CHANNELS, {
    method: "PUT",
    body: JSON.stringify(update),
  })
  return channelsView(res, await readJSON(res, "Failed to save notification channels"))
}

export async function sendTestNotification(channel: "slack" | "email"): Promise<TestNotificationResult> {
  const res = await authFetch(API_ENDPOINTS.NOTIFICATIONS.ADMIN_TEST, {
    method: "POST",
    body: JSON.stringify({ channel }),
  })
  return readJSON(res, "Test notification failed")
}

export async function getNotificationPreferences(): Promise<NotificationPreferences> {
  const res = await authFetch(API_ENDPOINTS.NOTIFICATIONS.PREFERENCES, { cache: "no-store" })
  return preferencesView(res, await readJSON(res, "Failed to load notification preferences"))
}

export async function updateNotificationPreferences(
  update: NotificationPreferencesUpdate,
): Promise<NotificationPreferences> {
  const res = await authFetch(API_ENDPOINTS.NOTIFICATIONS.PREFERENCES, {
    method: "PUT",
    body: JSON.stringify(update),
  })
  return preferencesView(res, await readJSON(res, "Failed to save notification preferences"))
}
