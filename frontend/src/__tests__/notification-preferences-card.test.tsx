import { beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { fireEvent, render, screen, waitFor } from "@testing-library/react"

import { NotificationPreferencesCard } from "@/components/settings/NotificationPreferencesCard"
import { authFetch } from "@/lib/api/auth-fetch"
import { toast } from "sonner"

// The old card had two switches wired to useState and nothing else: flipping
// "Pipeline Executions" off saved nothing and changed no delivery. These pin
// the replacement to the real preferences API.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const mockFetch = authFetch as unknown as Mock

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

function prefs(overrides: Record<string, unknown> = {}) {
  return {
    email_enabled: true,
    email_categories: { data_loss: true, schema_drift: false },
    categories: [
      { id: "data_loss", label: "Data loss & integrity", description: "Rows may be missing." },
      { id: "schema_drift", label: "Schema changes", description: "The source schema changed." },
    ],
    channels: { email: true, slack: false },
    ...overrides,
  }
}

beforeEach(() => {
  mockFetch.mockReset()
  vi.mocked(toast.error).mockReset()
})

const sw = (name: string) => screen.getByRole("switch", { name })

async function renderLoaded(body = prefs(), props: { email?: string; isAdmin?: boolean } = {}) {
  mockFetch.mockResolvedValueOnce(res(200, body))
  render(<NotificationPreferencesCard {...props} />)
  await waitFor(() => expect(sw("Email notifications")).toBeInTheDocument())
}

describe("NotificationPreferencesCard", () => {
  it("shows the saved preferences", async () => {
    await renderLoaded()

    expect(sw("Email notifications")).toHaveAttribute("aria-checked", "true")
    expect(sw("Email: Data loss & integrity")).toHaveAttribute("aria-checked", "true")
    expect(sw("Email: Schema changes")).toHaveAttribute("aria-checked", "false")
    expect(screen.queryByText(/not set up on this instance/i)).not.toBeInTheDocument()
  })

  it("saves a category toggle through PUT", async () => {
    await renderLoaded()
    mockFetch.mockResolvedValueOnce(
      res(200, prefs({ email_categories: { data_loss: false, schema_drift: false } })),
    )
    fireEvent.click(sw("Email: Data loss & integrity"))

    await waitFor(() => expect(mockFetch).toHaveBeenCalledTimes(2))
    const [url, opts] = mockFetch.mock.calls[1]
    expect(String(url)).toContain("/api/v1/notifications/preferences")
    expect(opts.method).toBe("PUT")
    expect(JSON.parse(opts.body)).toEqual({
      email_enabled: true,
      email_categories: { data_loss: false, schema_drift: false },
    })
    await waitFor(() => expect(sw("Email: Data loss & integrity")).toHaveAttribute("aria-checked", "false"))
  })

  it("reverts the switch and says so when the save fails", async () => {
    await renderLoaded()
    mockFetch.mockResolvedValueOnce(res(500, { error: "Failed to save notification preferences" }))
    fireEvent.click(sw("Email notifications"))

    await waitFor(() => expect(toast.error).toHaveBeenCalledWith("Failed to save notification preferences"))
    expect(sw("Email notifications")).toHaveAttribute("aria-checked", "true")
  })

  it("greys out the categories while email is off", async () => {
    await renderLoaded(prefs({ email_enabled: false }))

    expect(sw("Email: Data loss & integrity")).toBeDisabled()
    expect(sw("Email: Data loss & integrity")).toHaveAttribute("aria-checked", "false")
  })

  it("locks a category the admin turned off for email", async () => {
    await renderLoaded(prefs({ email_categories: { data_loss: true, schema_drift: true }, email_blocked_categories: ["data_loss"] }))

    expect(sw("Email: Data loss & integrity")).toBeDisabled()
    expect(sw("Email: Data loss & integrity")).toHaveAttribute("aria-checked", "false")
    expect(screen.getByText(/turned off for email by your admin/i)).toBeInTheDocument()
    expect(sw("Email: Schema changes")).toBeEnabled()
    expect(sw("Email: Schema changes")).toHaveAttribute("aria-checked", "true")
  })

  it("says it is only about the user's own email, and names the address", async () => {
    await renderLoaded(prefs(), { email: "ada@example.com" })

    expect(screen.getByText("My email alerts")).toBeInTheDocument()
    expect(screen.getByText(/only affects emails sent to you/i)).toBeInTheDocument()
    expect(screen.getByText("Send alerts about your pipelines to ada@example.com")).toBeInTheDocument()
  })

  it("points admins, and only admins, at the delivery settings", async () => {
    await renderLoaded(prefs({ channels: { email: false, slack: false } }), { isAdmin: true })
    const links = screen.getAllByRole("link", { name: /admin → notifications/i })
    expect(links.length).toBe(2)
    links.forEach((l) => expect(l).toHaveAttribute("href", "/admin/notifications"))
  })

  it("shows non-admins no admin link", async () => {
    await renderLoaded(prefs({ channels: { email: false, slack: false } }))
    expect(screen.queryByRole("link", { name: /admin → notifications/i })).not.toBeInTheDocument()
  })

  it("says email is not set up when the instance has no SMTP", async () => {
    await renderLoaded(prefs({ channels: { email: false, slack: true } }))

    expect(screen.getByText(/not set up on this instance/i)).toBeInTheDocument()
    expect(screen.getByText(/shared Slack channel/i)).toBeInTheDocument()
  })

  it("shows no switches when the read fails, and recovers on retry", async () => {
    mockFetch.mockResolvedValueOnce(res(500, { error: "boom" })).mockResolvedValueOnce(res(200, prefs()))
    render(<NotificationPreferencesCard />)

    await waitFor(() => expect(screen.getByText(/could not load your notification preferences/i)).toBeInTheDocument())
    expect(screen.queryByRole("switch")).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole("button", { name: /retry/i }))
    await waitFor(() => expect(sw("Email notifications")).toBeInTheDocument())
  })
})
