import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { fireEvent, render, screen, waitFor } from "@testing-library/react"

import AdminNotificationsPage from "@/app/(dashboard)/admin/notifications/page"
import { authFetch } from "@/lib/api/auth-fetch"
import { toast } from "sonner"

// AdminNav needs the app router.
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/admin/notifications",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const mockFetch = authFetch as unknown as Mock

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

function view(overrides: Record<string, unknown> = {}) {
  return {
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
      categories: { health: false },
      extra_recipients: ["oncall@example.com"],
    },
    categories: [
      { id: "data_loss", label: "Data loss & integrity", description: "Rows may be missing." },
      { id: "health", label: "Health & capacity", description: "Out of space." },
    ],
    ...overrides,
  }
}

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

beforeEach(() => {
  mockFetch.mockReset()
  vi.mocked(toast.success).mockReset()
  vi.mocked(toast.error).mockReset()
})

const SAVE = /save changes/i

async function renderLoaded(body = view()) {
  mockFetch.mockResolvedValueOnce(res(200, body))
  render(<AdminNotificationsPage />)
  await waitFor(() => expect(screen.getByLabelText("SMTP host")).toBeInTheDocument())
}

function lastCall(method: string) {
  const call = [...mockFetch.mock.calls].reverse().find(([, opts]) => opts?.method === method)
  return call ? { url: String(call[0]), body: JSON.parse(call[1].body) } : null
}

describe("Admin Notifications — fail closed", () => {
  it("shows no form and no Save when the read returns 500", async () => {
    mockFetch.mockResolvedValue(res(500, { error: "Failed to load notification channels" }))
    render(<AdminNotificationsPage />)

    await waitFor(() => expect(screen.getByText(/could not load notification channels/i)).toBeInTheDocument())
    expect(screen.queryByLabelText("SMTP host")).not.toBeInTheDocument()
    expect(screen.queryByRole("button", { name: SAVE })).not.toBeInTheDocument()
  })

  it("treats a 200 that is not the view as a failed read", async () => {
    mockFetch.mockResolvedValue(res(200, { settings: {} }))
    render(<AdminNotificationsPage />)

    await waitFor(() => expect(screen.getByText(/could not load notification channels/i)).toBeInTheDocument())
    expect(screen.queryByRole("button", { name: SAVE })).not.toBeInTheDocument()
  })

  it("retries and then shows the saved settings", async () => {
    mockFetch.mockResolvedValueOnce(res(500, { error: "boom" })).mockResolvedValueOnce(res(200, view()))
    render(<AdminNotificationsPage />)

    await waitFor(() => expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument())
    fireEvent.click(screen.getByRole("button", { name: /retry/i }))
    await waitFor(() => expect(screen.getByLabelText("SMTP host")).toHaveValue("smtp.example.com"))
  })

  it("shows Access denied on 403, not the form", async () => {
    mockFetch.mockResolvedValue(res(403, { error: "forbidden" }))
    render(<AdminNotificationsPage />)

    await waitFor(() => expect(mockFetch).toHaveBeenCalled())
    await waitFor(() => expect(screen.queryByText(/loading/i)).not.toBeInTheDocument())
    expect(screen.queryByLabelText("SMTP host")).not.toBeInTheDocument()
    expect(screen.queryByText(/could not load notification channels/i)).not.toBeInTheDocument()
  })
})

describe("Admin Notifications — editing and saving", () => {
  it("renders the saved view without echoing secrets", async () => {
    await renderLoaded()

    expect(screen.getByLabelText("Incoming webhook URL")).toHaveValue("")
    expect(screen.getByLabelText("Incoming webhook URL")).toHaveAttribute("placeholder", "Saved — leave blank to keep it")
    expect(screen.getByLabelText("Password")).toHaveValue("")
    expect(screen.getByRole("switch", { name: "Slack: Health & capacity" })).toHaveAttribute("aria-checked", "false")
    expect(screen.getByRole("switch", { name: "Slack: Data loss & integrity" })).toHaveAttribute("aria-checked", "true")
    expect(screen.getByRole("button", { name: SAVE })).toBeDisabled()
  })

  it("saves with null secrets so the stored webhook and password are kept", async () => {
    await renderLoaded()
    fireEvent.change(screen.getByLabelText("SMTP host"), { target: { value: "mail.example.org" } })
    fireEvent.click(screen.getByRole("switch", { name: "Slack: Data loss & integrity" }))

    mockFetch.mockResolvedValueOnce(res(200, view()))
    fireEvent.click(screen.getByRole("button", { name: SAVE }))

    await waitFor(() => expect(toast.success).toHaveBeenCalled())
    const put = lastCall("PUT")!
    expect(put.url).toContain("/api/v1/admin/notifications/channels")
    expect(put.body.slack).toEqual({ enabled: true, webhook_url: null, categories: { health: false, data_loss: false } })
    expect(put.body.email).toMatchObject({ smtp_host: "mail.example.org", smtp_port: 587, smtp_password: null, tls_mode: "starttls" })
  })

  it("shows and saves which alerts are emailed and the alert list", async () => {
    await renderLoaded()

    expect(screen.getByRole("switch", { name: "Email: Health & capacity" })).toHaveAttribute("aria-checked", "false")
    expect(screen.getByRole("switch", { name: "Email: Data loss & integrity" })).toHaveAttribute("aria-checked", "true")
    expect(screen.getByLabelText("Also send to")).toHaveValue("oncall@example.com")

    fireEvent.click(screen.getByRole("switch", { name: "Email: Data loss & integrity" }))
    fireEvent.change(screen.getByLabelText("Also send to"), {
      target: { value: "oncall@example.com\n  team@example.com, \n\nlead@example.com" },
    })
    mockFetch.mockResolvedValueOnce(res(200, view()))
    fireEvent.click(screen.getByRole("button", { name: SAVE }))

    await waitFor(() => expect(lastCall("PUT")).not.toBeNull())
    const put = lastCall("PUT")!
    expect(put.body.email.categories).toEqual({ health: false, data_loss: false })
    expect(put.body.email.extra_recipients).toEqual(["oncall@example.com", "team@example.com", "lead@example.com"])
  })

  it("sends an empty string only when the admin removes a saved secret", async () => {
    await renderLoaded()
    const removes = screen.getAllByRole("button", { name: "Remove it" })
    removes.forEach((b) => fireEvent.click(b))

    mockFetch.mockResolvedValueOnce(res(200, view()))
    fireEvent.click(screen.getByRole("button", { name: SAVE }))

    await waitFor(() => expect(lastCall("PUT")).not.toBeNull())
    const put = lastCall("PUT")!
    expect(put.body.slack.webhook_url).toBe("")
    expect(put.body.email.smtp_password).toBe("")
  })

  it("shows the server's validation error and keeps the edits", async () => {
    await renderLoaded()
    fireEvent.change(screen.getByLabelText("From address"), { target: { value: "" } })
    mockFetch.mockResolvedValueOnce(res(400, { error: "email.from is required when email is enabled" }))
    fireEvent.click(screen.getByRole("button", { name: SAVE }))

    await waitFor(() => expect(toast.error).toHaveBeenCalledWith("email.from is required when email is enabled"))
    expect(screen.getByLabelText("From address")).toHaveValue("")
    expect(screen.getByRole("button", { name: SAVE })).toBeEnabled()
  })

  it("explains the environment fallback when nothing is saved", async () => {
    await renderLoaded(
      view({ source: "environment", updated_at: null, email: { ...view().email, tls_mode: "opportunistic" } }),
    )
    expect(screen.getByText(/nothing saved yet/i)).toBeInTheDocument()
    expect(screen.getByText(/upgrades to TLS only when the server offers it/i)).toBeInTheDocument()
  })
})

describe("Admin Notifications — test sends", () => {
  it("sends a test email and names the recipient", async () => {
    await renderLoaded()
    mockFetch.mockResolvedValueOnce(res(200, { success: true, channel: "email", sent_to: "admin@example.com" }))
    fireEvent.click(screen.getByRole("button", { name: /send test email to me/i }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith("Test email sent to admin@example.com"))
    expect(lastCall("POST")).toEqual({ url: expect.stringContaining("/api/v1/admin/notifications/test"), body: { channel: "email" } })
  })

  it("shows the Slack failure detail", async () => {
    await renderLoaded()
    mockFetch.mockResolvedValueOnce(res(502, { error: "Test notification failed", detail: "slack: 404 no_service" }))
    fireEvent.click(screen.getByRole("button", { name: /send test message/i }))

    await waitFor(() => expect(toast.error).toHaveBeenCalledWith("Test notification failed: slack: 404 no_service"))
  })

  it("disables tests while there are unsaved edits, because tests use the saved settings", async () => {
    await renderLoaded()
    fireEvent.change(screen.getByLabelText("SMTP host"), { target: { value: "other.example.com" } })

    expect(screen.getByRole("button", { name: /send test email to me/i })).toBeDisabled()
    expect(screen.getByRole("button", { name: /send test message/i })).toBeDisabled()
  })

  it("disables the test for a channel that is saved as off", async () => {
    await renderLoaded(view({ slack: { enabled: false, webhook_configured: false, categories: {} } }))

    expect(screen.getByRole("button", { name: /send test message/i })).toBeDisabled()
    expect(screen.getByRole("button", { name: /send test email to me/i })).toBeEnabled()
  })
})
