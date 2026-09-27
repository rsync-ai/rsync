import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"

import { runModelNow } from "../runModelNow"
import { authFetch } from "@/lib/api/auth-fetch"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

function res(status: number, body: unknown) {
  return { ok: status >= 200 && status < 300, status, json: async () => body } as unknown as Response
}

const serve = (r: Response) => (authFetch as Mock).mockResolvedValueOnce(r)

describe("runModelNow", () => {
  afterEach(() => {
    vi.clearAllMocks()
  })

  it("posts to the model's run route", async () => {
    serve(res(200, {}))
    await runModelNow("q-1", "table")
    expect(authFetch).toHaveBeenCalledWith("/api/v1/explorer/saved/q-1/run", { method: "POST" })
  })

  it("names the table the server rebuilt, then the one the caller knows", async () => {
    serve(res(200, { target_table: "analytics.orders" }))
    expect(await runModelNow("q-1", "table", "stale.name")).toEqual({ ok: true, message: "Rebuilt analytics.orders" })
    serve(res(200, {}))
    expect(await runModelNow("q-1", "table", "analytics.orders")).toEqual({ ok: true, message: "Rebuilt analytics.orders" })
    serve(res(200, {}))
    expect(await runModelNow("q-1", "table")).toEqual({ ok: true, message: "Rebuilt the target table" })
  })

  it("reports rows for a statement, never a rebuilt table", async () => {
    serve(res(200, { rows_affected: 3, target_table: "ignored" }))
    expect((await runModelNow("q-1", "statement")).message).toBe("Statement ran — 3 rows affected")
    serve(res(200, { rows_affected: 1 }))
    expect((await runModelNow("q-1", "statement")).message).toBe("Statement ran — 1 row affected")
    serve(res(200, {}))
    expect((await runModelNow("q-1", "statement")).message).toBe("Statement ran")
  })

  it("passes the server's refusal through as a failure", async () => {
    serve(res(422, { error: "syntax error at or near FROM" }))
    expect(await runModelNow("q-1", "table")).toEqual({ ok: false, message: "syntax error at or near FROM" })
    serve(res(500, {}))
    expect(await runModelNow("q-1", "table")).toEqual({ ok: false, message: "The run did not complete" })
  })

  it("says it could not run when the server cannot be reached", async () => {
    ;(authFetch as Mock).mockRejectedValueOnce(new Error("offline"))
    expect(await runModelNow("q-1", "table")).toEqual({ ok: false, message: "Could not run the model" })
  })
})
