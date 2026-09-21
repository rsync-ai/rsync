import { describe, it, expect, vi } from "vitest"

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(),
}))

import { metadataErrorMessage, metadataErrorMessageFromBody } from "@/lib/api/connections"

// The table dialog (PipelineTableSelector) renders this message as
// "<message>. Enter table name(s) manually below." A connection that cannot be
// reached must show the connector's reason, not "HTTP 422" or "No tables found".
const DNS_REASON = "The DNS query name does not exist: _mongodb._tcp.cluster0.example.net."

describe("metadataErrorMessage (table dialog error text)", () => {
  it("shows the connector's reason from the gateway's discovery-failure body", async () => {
    const res = new Response(
      JSON.stringify({ error: "Schema discovery failed", details: DNS_REASON }),
      { status: 422, headers: { "Content-Type": "application/json" } },
    )
    expect(await metadataErrorMessage(res)).toBe(DNS_REASON)
  })

  it("prefers details, then message, then error", () => {
    expect(metadataErrorMessageFromBody(422, JSON.stringify({ error: "e", message: "m", details: "d" }))).toBe("d")
    expect(metadataErrorMessageFromBody(422, JSON.stringify({ error: "e", message: "m" }))).toBe("m")
    expect(metadataErrorMessageFromBody(422, JSON.stringify({ error: "e", details: "  " }))).toBe("e")
  })

  it("ignores a non-string details field", () => {
    expect(metadataErrorMessageFromBody(400, JSON.stringify({ error: "Bad request", details: { field: "x" } }))).toBe(
      "Bad request",
    )
  })

  it("falls back to plain text, then to the status", () => {
    expect(metadataErrorMessageFromBody(502, "upstream timed out\n")).toBe("upstream timed out")
    expect(metadataErrorMessageFromBody(503, "")).toBe("Failed to load connection metadata (HTTP 503)")
    expect(metadataErrorMessageFromBody(500, "{}")).toBe("Failed to load connection metadata (HTTP 500)")
    expect(metadataErrorMessageFromBody(500, "null")).toBe("Failed to load connection metadata (HTTP 500)")
  })
})
