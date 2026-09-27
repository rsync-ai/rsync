/**
 * Client for POST /api/v1/explorer/nl/next-steps.
 *
 * Lives here rather than inside the explorer page so it can be tested without
 * mounting 2,000 lines of editor.
 */

import { authFetch } from "@/lib/api/auth-fetch"

/**
 * Ask the server what to do with a result set.
 *
 * PRIVACY (CLAUDE.md "LLM data privacy — metadata only"): the body carries the
 * result PROFILE — a row count and column NAMES — and never a row. llm-service
 * pins that contract with `extra="forbid"` on ResultProfile
 * (llm-service/src/agents/explorer/api.py), so a caller that attached sample
 * data would get a 422 instead of silently leaking it. Do not widen this body.
 *
 * Returns the suggestion titles. Everything else the endpoint sends back
 * (action_type, required_inputs, cta) describes actions this page cannot
 * execute yet, so it is dropped rather than rendered as a dead button.
 */
export async function fetchNextStepSuggestions(args: {
  question: string
  sql: string
  rowCount: number
  columns: string[]
  signal?: AbortSignal
}): Promise<string[]> {
  const res = await authFetch("/api/v1/explorer/nl/next-steps", {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({
      // `question` and `sql` are both binding:"required" on the gateway
      // (explorer.go GetNextStepsRequest), so a raw-SQL run with no
      // natural-language question still has to send something non-empty.
      question: args.question.trim() || "Results of this SQL query",
      sql: args.sql,
      result_profile: { row_count: args.rowCount, columns: args.columns },
    }),
    signal: args.signal,
  })

  const data = (await res.json().catch(() => null)) as
    | { suggestions?: Array<{ title?: string }>; error?: string; message?: string }
    | null

  if (!res.ok) {
    // 503 here is the "no LLM configured" relay; its message already says so.
    throw new Error(data?.error || data?.message || `HTTP ${res.status}`)
  }

  return (data?.suggestions || []).map((x) => String(x?.title || "").trim()).filter(Boolean)
}
