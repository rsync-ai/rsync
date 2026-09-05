import { describe, it, expect, vi, beforeEach } from "vitest"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

import { authFetch } from "@/lib/api/auth-fetch"
import { updateSavedQuery } from "@/components/explorer/savedQueryUpdate"

// This module exists for exactly one distinction: a 200 that changed the SQL versus a
// 200 that parked it. Every test here is about not collapsing those two.

const mockFetch = authFetch as unknown as ReturnType<typeof vi.fn>

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

beforeEach(() => vi.clearAllMocks())

describe("updateSavedQuery", () => {
  it("PATCHes the id with the patch body verbatim", async () => {
    mockFetch.mockResolvedValue(res(200, { id: "q1" }))

    const out = await updateSavedQuery("q1", { name: "New name", sql_text: "SELECT 2" })

    expect(out).toEqual({ kind: "applied" })
    const [url, init] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/v1/explorer/saved/q1")
    expect(init.method).toBe("PATCH")
    expect(JSON.parse(init.body)).toEqual({ name: "New name", sql_text: "SELECT 2" })
  })

  it("reports a parked SQL edit as proposed, never as applied", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        pending_approval: {
          statement_class: "write",
          reason: "This query is on a schedule.",
        },
      }),
    )

    const out = await updateSavedQuery("q1", { sql_text: "DELETE FROM t" })

    expect(out).toEqual({
      kind: "proposed",
      statementClass: "write",
      reason: "This query is on a schedule.",
    })
  })

  it("detects the gate by the object, not by the wording of its message", async () => {
    // The reason string is copy and will be reworded. If detection keyed off it, the
    // first rewording would silently turn every proposal into a reported success.
    mockFetch.mockResolvedValue(res(200, { pending_approval: {} }))

    const out = await updateSavedQuery("q1", { sql_text: "SELECT 2" })

    expect(out.kind).toBe("proposed")
    // Still says something useful rather than an empty toast.
    expect(out.kind === "proposed" && out.reason).toMatch(/approval/i)
  })

  it("surfaces the server's own refusal message", async () => {
    mockFetch.mockResolvedValue(res(403, { error: "only the creator or an admin can edit this" }))

    const out = await updateSavedQuery("q1", { sql_text: "SELECT 2" })

    expect(out).toEqual({
      kind: "error",
      message: "only the creator or an admin can edit this",
    })
  })

  it("does not read a failure as a success just because the body is unparseable", async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 502,
      json: async () => {
        throw new Error("not json")
      },
    } as unknown as Response)

    const out = await updateSavedQuery("q1", { sql_text: "SELECT 2" })

    expect(out.kind).toBe("error")
  })

  it("turns a network failure into an error result rather than throwing", async () => {
    // A caller that forgot a try/catch would otherwise get an unhandled rejection
    // where it expected a verdict — and the edit dialog's `finally` would clear its
    // spinner having reported nothing at all.
    mockFetch.mockRejectedValue(new TypeError("Failed to fetch"))

    const out = await updateSavedQuery("q1", { sql_text: "SELECT 2" })

    expect(out.kind).toBe("error")
    expect(out.kind === "error" && out.message).toMatch(/could not reach the server/i)
  })
})
