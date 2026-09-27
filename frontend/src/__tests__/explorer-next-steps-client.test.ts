/**
 * `fetchNextStepSuggestions` — the caller that POST /explorer/nl/next-steps
 * never had.
 *
 * The explorer used to print three fixed strings ("Create Metabase Dashboard /
 * Download CSV / Share via Slack") for every run, identical whether the query
 * returned 0 rows or 40 columns, while the endpoint that computes real ones
 * shipped with no caller at all.
 *
 * The assertion that matters most here is the PRIVACY one. CLAUDE.md's "LLM
 * data privacy — metadata only" rule says row values never reach an LLM, and
 * llm-service pins it with `extra="forbid"` on ResultProfile. That is a
 * server-side backstop; this test is the client-side one, and it is written to
 * FAIL if someone adds a `sample_rows` or `preview` field to the body for
 * "better suggestions". A 422 from the server would catch it in production —
 * this catches it in CI, before the request is ever made.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"

import { fetchNextStepSuggestions } from "@/lib/explorer/nextSteps"

const mockFetch = vi.fn()
vi.stubGlobal("fetch", mockFetch)

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    text: async () => JSON.stringify(body),
    json: async () => body,
  } as unknown as Response
}

/** The JSON body of the single fetch the helper issued. */
function sentBody(): Record<string, unknown> {
  expect(mockFetch).toHaveBeenCalledTimes(1)
  const init = mockFetch.mock.calls[0][1] as RequestInit
  return JSON.parse(String(init.body))
}

beforeEach(() => {
  mockFetch.mockReset()
})

describe("fetchNextStepSuggestions", () => {
  it("sends ONLY metadata — a row count and column names, never a row", async () => {
    mockFetch.mockResolvedValue(res(200, { suggestions: [{ title: "Chart revenue by month" }] }))

    await fetchNextStepSuggestions({
      question: "revenue by month",
      sql: "SELECT month, revenue FROM sales",
      rowCount: 12,
      columns: ["month", "revenue"],
    })

    const body = sentBody()
    // The whole contract, exactly: three keys, and result_profile carries two.
    expect(Object.keys(body).sort()).toEqual(["question", "result_profile", "sql"])
    expect(body.result_profile).toEqual({ row_count: 12, columns: ["month", "revenue"] })

    // Belt and braces: no row VALUE may appear anywhere in the serialised body,
    // under any key name a future edit might invent.
    const raw = String((mockFetch.mock.calls[0][1] as RequestInit).body)
    for (const key of ["rows", "sample", "preview", "data", "results", "values"]) {
      expect(raw).not.toContain(`"${key}"`)
    }
  })

  it("substitutes a question for a raw-SQL run, because the gateway requires one", async () => {
    // explorer.go GetNextStepsRequest binds both question and sql as required;
    // an empty question is a 400, which would make every raw-SQL run show a
    // failed step. A blank/whitespace question must not reach the wire.
    mockFetch.mockResolvedValue(res(200, { suggestions: [] }))

    await fetchNextStepSuggestions({
      question: "   ",
      sql: "SELECT 1",
      rowCount: 1,
      columns: ["?column?"],
    })

    expect(sentBody().question).toBe("Results of this SQL query")
  })

  it("keeps a real question verbatim (control for the substitution above)", async () => {
    mockFetch.mockResolvedValue(res(200, { suggestions: [] }))

    await fetchNextStepSuggestions({
      question: "which customers churned",
      sql: "SELECT 1",
      rowCount: 0,
      columns: [],
    })

    expect(sentBody().question).toBe("which customers churned")
  })

  it("returns the suggestion titles, dropping blanks", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        suggestions: [{ title: "Download as CSV" }, { title: "   " }, { title: "Open in Metabase" }, {}],
      })
    )

    const out = await fetchNextStepSuggestions({
      question: "q",
      sql: "SELECT 1",
      rowCount: 3,
      columns: ["a"],
    })

    expect(out).toEqual(["Download as CSV", "Open in Metabase"])
  })

  it("surfaces the server's own message on a 503 — 'no LLM configured' must reach the user", async () => {
    // relayLLMNotConfigured passes the upstream 503 through with its
    // explanation. Swallowing it would leave the step silently empty on every
    // install that has not configured an LLM, which reads as a broken feature
    // rather than an unconfigured one.
    mockFetch.mockResolvedValue(res(503, { error: "LLM service is not configured" }))

    await expect(
      fetchNextStepSuggestions({ question: "q", sql: "SELECT 1", rowCount: 0, columns: [] })
    ).rejects.toThrow(/not configured/i)
  })

  it("throws on a non-ok with an unparseable body rather than reporting zero suggestions", async () => {
    // An empty list and a failed call are different claims: the first says the
    // server had nothing to suggest, the second that nobody asked.
    mockFetch.mockResolvedValue({
      ok: false,
      status: 502,
      statusText: "502",
      json: async () => {
        throw new Error("not json")
      },
      text: async () => "<html>bad gateway</html>",
    } as unknown as Response)

    await expect(
      fetchNextStepSuggestions({ question: "q", sql: "SELECT 1", rowCount: 0, columns: [] })
    ).rejects.toThrow(/502/)
  })

  it("posts to the gateway's next-steps route", async () => {
    mockFetch.mockResolvedValue(res(200, { suggestions: [] }))

    await fetchNextStepSuggestions({ question: "q", sql: "SELECT 1", rowCount: 0, columns: [] })

    expect(String(mockFetch.mock.calls[0][0])).toContain("/api/v1/explorer/nl/next-steps")
    expect((mockFetch.mock.calls[0][1] as RequestInit).method).toBe("POST")
  })
})
