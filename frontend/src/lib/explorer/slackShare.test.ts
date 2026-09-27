import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"

import { authFetch } from "@/lib/api/auth-fetch"
import {
  buildSlackMessage,
  formatRowPreview,
  shareToSlack,
  validateWebhookUrl,
} from "@/lib/explorer/slackShare"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

const mockFetch = authFetch as Mock
afterEach(() => vi.clearAllMocks())

const COLUMNS = ["id", "email"]
const ROWS = [
  { id: 1, email: "alice@example.com" },
  { id: 2, email: "bob@example.com" },
]

describe("validateWebhookUrl", () => {
  it("accepts a real Slack incoming webhook", () => {
    expect(validateWebhookUrl("https://hooks.slack.com/services/T0/B0/xyz")).toBeNull()
  })

  it("rejects a host that only starts like Slack's", () => {
    // The prefix is the one thing between this endpoint and an arbitrary
    // server-side POST, so it has to include the trailing slash.
    expect(validateWebhookUrl("https://hooks.slack.com.evil.example/x")).toMatch(/starts with/)
  })

  it("rejects http and an empty box", () => {
    expect(validateWebhookUrl("http://hooks.slack.com/services/x")).toMatch(/starts with/)
    expect(validateWebhookUrl("   ")).toMatch(/Paste the Slack/)
  })
})

describe("formatRowPreview", () => {
  it("aligns columns so a result set is readable in a code fence", () => {
    const out = formatRowPreview(COLUMNS, ROWS).split("\n")
    // Each column is padded to its widest cell (min 3) and joined by two
    // spaces, so the values line up under their headers.
    expect(out[0]).toBe("id   email")
    expect(out[1]).toBe("---  -----------------")
    expect(out[2]).toBe("1    alice@example.com")
    expect(out[3]).toBe("2    bob@example.com")
  })

  it("says how much it left out rather than silently truncating", () => {
    const out = formatRowPreview(COLUMNS, ROWS, 1)
    expect(out).toContain("alice@example.com")
    expect(out).not.toContain("bob@example.com")
    expect(out).toContain("… and 1 more rows")
  })

  it("renders a missing value as NULL, not as blank", () => {
    expect(formatRowPreview(["a"], [{ a: null }])).toContain("NULL")
  })
})

describe("buildSlackMessage", () => {
  const base = {
    question: "revenue by region",
    sql: "SELECT region, sum(total) FROM orders GROUP BY 1",
    rowCount: 2,
    columns: COLUMNS,
    executionMs: 42,
    rows: ROWS,
  }

  it("posts no values when rows are not opted in", () => {
    const msg = buildSlackMessage({ ...base, includeRows: false })
    const wire = JSON.stringify(msg)
    // The default message is metadata: question, SQL, count, column names.
    expect(wire).not.toContain("alice@example.com")
    expect(wire).toContain("revenue by region")
    expect(wire).toContain("SELECT region")
    expect(wire).toContain("columns: id, email")
  })

  it("posts values only when they are opted in", () => {
    const msg = buildSlackMessage({ ...base, includeRows: true })
    expect(JSON.stringify(msg)).toContain("alice@example.com")
  })

  it("carries the headline in the notification fallback", () => {
    expect(buildSlackMessage({ ...base, includeRows: false }).text).toBe("revenue by region — 2 rows")
  })

  it("says so when the result was cut short", () => {
    const msg = buildSlackMessage({ ...base, truncated: true, includeRows: false })
    expect(JSON.stringify(msg)).toContain("result was truncated")
  })

  it("falls back to a title for a run with no question", () => {
    expect(buildSlackMessage({ ...base, question: "  ", rowCount: 1, includeRows: false }).text).toBe(
      "SQL query result — 1 row"
    )
  })
})

describe("shareToSlack", () => {
  const message = { text: "t", blocks: [] }

  it("fails a bad URL locally, without posting anywhere", async () => {
    const out = await shareToSlack({ webhookUrl: "https://example.com/hook", message })
    expect(mockFetch).not.toHaveBeenCalled()
    expect(out.ok).toBe(false)
  })

  it("explains a revoked webhook instead of echoing no_service", async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 502,
      json: async () => ({ error: "Slack returned error", status: 404, details: "no_service" }),
    })
    const out = await shareToSlack({ webhookUrl: "https://hooks.slack.com/services/x", message })

    expect(out).toEqual({
      ok: false,
      error:
        "Slack rejected the webhook (no_service) — it has been revoked or deleted. Create a new incoming webhook.",
    })
  })

  it("sends webhook_url, text and blocks", async () => {
    mockFetch.mockResolvedValue({ ok: true, status: 200, json: async () => ({ success: true }) })
    const out = await shareToSlack({ webhookUrl: " https://hooks.slack.com/services/x ", message })

    expect(out).toEqual({ ok: true })
    const body = JSON.parse(mockFetch.mock.calls[0][1].body)
    expect(body.webhook_url).toBe("https://hooks.slack.com/services/x")
    expect(body).toHaveProperty("text")
    expect(body).toHaveProperty("blocks")
  })
})
