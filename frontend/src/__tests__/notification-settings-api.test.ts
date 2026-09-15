import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"

import { authFetch } from "@/lib/api/auth-fetch"
import {
  getNotificationChannels,
  getNotificationPreferences,
  NotificationSettingsError,
  sendTestNotification,
  updateNotificationChannels,
  updateNotificationPreferences,
} from "@/lib/api/notification-settings"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

const mockFetch = authFetch as Mock

afterEach(() => vi.clearAllMocks())

function jsonResponse(body: unknown, ok = true, status = 200) {
  return { ok, status, json: async () => body }
}

const CHANNELS = {
  source: "database",
  updated_at: "2026-09-15T10:00:00Z",
  slack: { enabled: true, webhook_configured: true, categories: { health: false } },
  email: {
    enabled: true,
    smtp_host: "smtp.example.com",
    smtp_port: 587,
    smtp_username: "mailer",
    password_configured: true,
    from: "alerts@example.com",
    tls_mode: "starttls",
  },
  categories: [{ id: "health", label: "Health & capacity", description: "…" }],
}

describe("admin notification channels API", () => {
  it("reads the channels view without caching", async () => {
    mockFetch.mockResolvedValue(jsonResponse(CHANNELS))
    const view = await getNotificationChannels()

    expect(view.slack.categories).toEqual({ health: false })
    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain("/api/v1/admin/notifications/channels")
    expect(opts?.cache).toBe("no-store")
  })

  it("PUTs the update body verbatim, keeping null secrets as null", async () => {
    mockFetch.mockResolvedValue(jsonResponse(CHANNELS))
    await updateNotificationChannels({
      slack: { enabled: true, webhook_url: null, categories: { health: false } },
      email: {
        enabled: true,
        smtp_host: "smtp.example.com",
        smtp_port: 587,
        smtp_username: "mailer",
        smtp_password: null,
        from: "alerts@example.com",
        tls_mode: "starttls",
      },
    })

    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain("/api/v1/admin/notifications/channels")
    expect(opts.method).toBe("PUT")
    const body = JSON.parse(opts.body)
    // null (not "") is what keeps the stored secret on the server.
    expect(body.slack).toHaveProperty("webhook_url", null)
    expect(body.email).toHaveProperty("smtp_password", null)
  })

  it("defaults the email category map and alert list when a gateway omits them", async () => {
    mockFetch.mockResolvedValue(jsonResponse(CHANNELS))
    const view = await getNotificationChannels()

    expect(view.email.categories).toEqual({})
    expect(view.email.extra_recipients).toEqual([])
  })

  it("rejects a 200 whose body is not the channels view", async () => {
    mockFetch.mockResolvedValue(jsonResponse({ settings: {} }))
    await expect(getNotificationChannels()).rejects.toBeInstanceOf(NotificationSettingsError)
  })

  it("keeps the HTTP status on failure so the page can tell 403 from 500", async () => {
    mockFetch.mockResolvedValue(jsonResponse({ error: "Admin access required" }, false, 403))
    const err = await getNotificationChannels().catch((e) => e)

    expect(err).toBeInstanceOf(NotificationSettingsError)
    expect(err.status).toBe(403)
    expect(err.message).toContain("Admin access required")
  })

  it("sends a test for one channel and surfaces the SMTP reply on 502", async () => {
    mockFetch.mockResolvedValue(
      jsonResponse({ error: "Test notification failed", detail: "535 authentication failed" }, false, 502),
    )
    const err = await sendTestNotification("email").catch((e) => e)

    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain("/api/v1/admin/notifications/test")
    expect(opts.method).toBe("POST")
    expect(JSON.parse(opts.body)).toEqual({ channel: "email" })
    expect(err.message).toBe("Test notification failed: 535 authentication failed")
  })
})

describe("notification preferences API", () => {
  const PREFS = {
    email_enabled: true,
    email_categories: { data_loss: true, other: false },
    categories: [],
    channels: { email: true, slack: false },
  }

  it("reads the caller's preferences", async () => {
    mockFetch.mockResolvedValue(jsonResponse(PREFS))
    const prefs = await getNotificationPreferences()

    expect(prefs.email_categories.other).toBe(false)
    expect(String(mockFetch.mock.calls[0][0])).toContain("/api/v1/notifications/preferences")
  })

  it("defaults the admin's blocked categories to none", async () => {
    mockFetch.mockResolvedValue(jsonResponse(PREFS))
    expect((await getNotificationPreferences()).email_blocked_categories).toEqual([])

    mockFetch.mockResolvedValue(jsonResponse({ ...PREFS, email_blocked_categories: ["health"] }))
    expect((await getNotificationPreferences()).email_blocked_categories).toEqual(["health"])
  })

  it("rejects a body with no email_enabled instead of defaulting it", async () => {
    mockFetch.mockResolvedValue(jsonResponse({ name: "Ada", email: "ada@example.com" }))
    await expect(getNotificationPreferences()).rejects.toThrow(/unexpected/i)
  })

  it("PUTs email_enabled and the category map", async () => {
    mockFetch.mockResolvedValue(jsonResponse(PREFS))
    await updateNotificationPreferences({ email_enabled: false, email_categories: { health: false } })

    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain("/api/v1/notifications/preferences")
    expect(opts.method).toBe("PUT")
    expect(JSON.parse(opts.body)).toEqual({ email_enabled: false, email_categories: { health: false } })
  })
})
