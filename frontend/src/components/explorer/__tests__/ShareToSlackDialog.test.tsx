/**
 * The "Share to Slack" control on the Data Explorer
 * (POST /api/v1/explorer/share/slack).
 *
 * Three things are pinned here because getting any of them wrong is invisible
 * in review:
 *
 *  1. The trigger renders while the dialog is CLOSED. This repo's Dialog does
 *     `if (!open) return null` (ui/dialog.tsx:84), so a trigger nested inside
 *     it would never be on screen — the control would ship looking present in
 *     the source and absent in the browser.
 *  2. The default message carries no row values. Rows are opt-in.
 *  3. The webhook is dropped when the dialog closes — it is a bearer
 *     credential for a channel, not a preference.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
}))

const toastError = vi.fn()
const toastSuccess = vi.fn()
vi.mock("sonner", () => ({
  toast: {
    error: (...a: unknown[]) => toastError(...a),
    success: (...a: unknown[]) => toastSuccess(...a),
  },
}))

import { ShareToSlackDialog } from "@/components/explorer/ShareToSlackDialog"

const WEBHOOK = "https://hooks.slack.com/services/T000/B000/xxxx"

const PROPS = {
  question: "How many orders shipped last week?",
  sql: "SELECT count(*) FROM orders WHERE shipped_at > now() - interval '7 days'",
  columns: ["count"],
  rows: [{ count: 412 }],
  rowCount: 1,
  executionMs: 42,
}

function json(status: number, body: unknown) {
  return { ok: status < 400, status, json: async () => body }
}

function postBody() {
  const call = authFetch.mock.calls.at(-1)
  return JSON.parse(String((call?.[1] as RequestInit).body))
}

beforeEach(() => {
  authFetch.mockReset()
  authFetch.mockResolvedValue(json(200, { success: true }))
  toastError.mockReset()
  toastSuccess.mockReset()
})
afterEach(() => cleanup())

describe("ShareToSlackDialog", () => {
  it("renders its trigger while the dialog is closed", () => {
    render(<ShareToSlackDialog {...PROPS} />)
    // The regression this guards: a trigger inside <Dialog> renders nothing.
    expect(screen.getByRole("button", { name: /share to slack/i })).toBeInTheDocument()
    expect(screen.queryByLabelText(/incoming webhook url/i)).not.toBeInTheDocument()
  })

  it("opens on the trigger and shows what will be posted", async () => {
    render(<ShareToSlackDialog {...PROPS} />)
    await userEvent.setup().click(screen.getByRole("button", { name: /share to slack/i }))

    expect(screen.getByLabelText(/incoming webhook url/i)).toBeInTheDocument()
    // The preview is the message, not a description of it.
    expect(screen.getByText(/How many orders shipped last week\? — 1 row/)).toBeInTheDocument()
  })

  it("refuses a URL that only looks like Slack's, before any request", async () => {
    render(<ShareToSlackDialog {...PROPS} />)
    const user = userEvent.setup()
    await user.click(screen.getByRole("button", { name: /share to slack/i }))
    await user.type(screen.getByLabelText(/incoming webhook url/i), "https://hooks.slack.com.evil.example/x")
    await user.click(screen.getByRole("button", { name: /send to slack/i }))

    await waitFor(() => expect(toastError).toHaveBeenCalled())
    expect(String(toastError.mock.calls[0][0])).toContain("https://hooks.slack.com/")
    expect(authFetch).not.toHaveBeenCalled()
  })

  it("posts metadata only by default — no row values on the wire", async () => {
    render(<ShareToSlackDialog {...PROPS} />)
    const user = userEvent.setup()
    await user.click(screen.getByRole("button", { name: /share to slack/i }))
    await user.type(screen.getByLabelText(/incoming webhook url/i), WEBHOOK)
    await user.click(screen.getByRole("button", { name: /send to slack/i }))

    await waitFor(() => expect(authFetch).toHaveBeenCalled())
    const body = postBody()
    expect(body.webhook_url).toBe(WEBHOOK)
    expect(JSON.stringify(body.blocks)).toContain("SELECT count(*)")
    // 412 is the one value in the result set; the default share must not carry it.
    expect(JSON.stringify(body.blocks)).not.toContain("412")
    await waitFor(() => expect(toastSuccess).toHaveBeenCalledWith("Sent to Slack."))
  })

  it("carries the rows only after the checkbox is ticked", async () => {
    render(<ShareToSlackDialog {...PROPS} />)
    const user = userEvent.setup()
    await user.click(screen.getByRole("button", { name: /share to slack/i }))
    await user.type(screen.getByLabelText(/incoming webhook url/i), WEBHOOK)
    await user.click(screen.getByLabelText(/include the first rows/i))
    await user.click(screen.getByRole("button", { name: /send to slack/i }))

    await waitFor(() => expect(authFetch).toHaveBeenCalled())
    expect(JSON.stringify(postBody().blocks)).toContain("412")
  })

  it("names a revoked webhook rather than reporting a bare 502", async () => {
    authFetch.mockResolvedValue(json(502, { error: "slack rejected", details: "no_service" }))
    render(<ShareToSlackDialog {...PROPS} />)
    const user = userEvent.setup()
    await user.click(screen.getByRole("button", { name: /share to slack/i }))
    await user.type(screen.getByLabelText(/incoming webhook url/i), WEBHOOK)
    await user.click(screen.getByRole("button", { name: /send to slack/i }))

    await waitFor(() => expect(toastError).toHaveBeenCalled())
    expect(String(toastError.mock.calls[0][0])).toContain("revoked or deleted")
  })

  it("forgets the webhook when the dialog is closed", async () => {
    render(<ShareToSlackDialog {...PROPS} />)
    const user = userEvent.setup()
    await user.click(screen.getByRole("button", { name: /share to slack/i }))
    await user.type(screen.getByLabelText(/incoming webhook url/i), WEBHOOK)
    await user.click(screen.getByRole("button", { name: /cancel/i }))

    await user.click(screen.getByRole("button", { name: /share to slack/i }))
    expect(screen.getByLabelText(/incoming webhook url/i)).toHaveValue("")
  })
})
