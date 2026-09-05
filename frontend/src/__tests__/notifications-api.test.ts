import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"

import { authFetch } from "@/lib/api/auth-fetch"
import {
  listNotifications,
  getUnreadCount,
  markNotificationRead,
  markAllNotificationsRead,
} from "@/lib/api/notifications"

// The module is a thin wrapper over authFetch — mock it and assert each call
// hits the right endpoint/method and unwraps the response envelope correctly.
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

const mockFetch = authFetch as Mock

afterEach(() => vi.clearAllMocks())

function jsonResponse(body: unknown, ok = true, status = 200) {
  return { ok, status, json: async () => body }
}

describe("notifications API", () => {
  it("listNotifications unwraps notifications + unread_count", async () => {
    mockFetch.mockResolvedValue(
      jsonResponse({ notifications: [{ id: "n1", read: false }], unread_count: 4 }),
    )
    const result = await listNotifications()

    expect(result.unread_count).toBe(4)
    expect(result.notifications).toHaveLength(1)
    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain("/api/v1/notifications")
    expect(opts?.cache).toBe("no-store")
  })

  it("listNotifications defaults to [] + 0 when the body omits fields", async () => {
    mockFetch.mockResolvedValue(jsonResponse({}))
    const result = await listNotifications()
    expect(result.notifications).toEqual([])
    expect(result.unread_count).toBe(0)
  })

  it("getUnreadCount returns the numeric count from the cheap endpoint", async () => {
    mockFetch.mockResolvedValue(jsonResponse({ unread_count: 7 }))
    expect(await getUnreadCount()).toBe(7)
    expect(String(mockFetch.mock.calls[0][0])).toContain("/notifications/unread-count")
  })

  it("markNotificationRead POSTs the id in the body", async () => {
    mockFetch.mockResolvedValue(jsonResponse({ status: "ok" }))
    await markNotificationRead("abc-123")

    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain("/notifications/mark-read")
    expect(opts?.method).toBe("POST")
    expect(JSON.parse(String(opts?.body))).toEqual({ id: "abc-123" })
  })

  it("markAllNotificationsRead POSTs to the mark-all endpoint", async () => {
    mockFetch.mockResolvedValue(jsonResponse({ status: "ok", marked: 3 }))
    await markAllNotificationsRead()

    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain("/notifications/mark-all-read")
    expect(opts?.method).toBe("POST")
  })

  it("throws when the response is not ok", async () => {
    mockFetch.mockResolvedValue(jsonResponse({ error: "boom" }, false, 500))
    await expect(listNotifications()).rejects.toThrow()
  })
})
