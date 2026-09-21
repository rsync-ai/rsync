import { describe, expect, it } from "vitest"

import { deleteWarningToast, readDeleteWarnings } from "@/lib/utils/delete-warnings"

/**
 * A pipeline DELETE returns 200 even when teardown left something behind; the
 * detail is in an optional `warnings` array. Both delete call sites used to
 * answer every 200 with "Pipeline deleted", so a leaked replication slot or a
 * Kafka topic that outlived its pipeline was visible only in the orchestrator
 * logs. These tests pin the reader that makes it visible in the UI.
 */
function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  })
}

describe("readDeleteWarnings", () => {
  it("returns the warnings a partial teardown reported", async () => {
    const res = jsonResponse({
      message: "Pipeline deleted successfully",
      warnings: ["cdc cleanup: drop replication slot failed", "kafka teardown: 2 topic(s) left behind"],
    })
    await expect(readDeleteWarnings(res)).resolves.toEqual([
      "cdc cleanup: drop replication slot failed",
      "kafka teardown: 2 topic(s) left behind",
    ])
  })

  it("returns nothing for a clean delete", async () => {
    await expect(readDeleteWarnings(jsonResponse({ message: "Pipeline deleted successfully" }))).resolves.toEqual([])
  })

  it("drops blank entries rather than toasting an empty bullet", async () => {
    await expect(readDeleteWarnings(jsonResponse({ warnings: ["", "   ", "real warning"] }))).resolves.toEqual([
      "real warning",
    ])
  })

  // Totality: a success path must never throw. Each of these used to be a
  // plausible crash if the reader assumed a well-formed body.
  it("treats an unparseable or wrongly-shaped body as no warnings", async () => {
    const notJson = new Response("<html>gateway</html>", { status: 200 })
    await expect(readDeleteWarnings(notJson)).resolves.toEqual([])
    await expect(readDeleteWarnings(jsonResponse(null))).resolves.toEqual([])
    await expect(readDeleteWarnings(jsonResponse(["a"]))).resolves.toEqual([])
    await expect(readDeleteWarnings(jsonResponse({ warnings: "slot leaked" }))).resolves.toEqual([])
  })
})

describe("deleteWarningToast", () => {
  it("says something was left behind in the title, not only the description", () => {
    const one = deleteWarningToast(["slot leaked"])
    expect(one.title).toMatch(/did not finish/i)
    expect(one.title).not.toMatch(/^Pipeline deleted$/)
    expect(one.description).toBe("slot leaked")

    const many = deleteWarningToast(["a", "b", "c"])
    expect(many.title).toContain("3")
    expect(many.description).toBe("a · b · c")
  })
})
