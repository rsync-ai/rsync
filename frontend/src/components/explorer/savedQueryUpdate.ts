import { authFetch } from "@/lib/api/auth-fetch"

// One place that turns a PATCH /explorer/saved/:id response into "what actually
// happened", because there are now two different successes and telling them apart is
// not optional.
//
// Editing the SQL of a SCHEDULED query does not change it (migration 092). The server
// applies the name/description/visibility, parks the SQL as a proposal for a workspace
// admin, and answers 200 with a `pending_approval` object. A caller that checks only
// `res.ok` reports "Updated" for a change that did not happen — and the person who
// wrote it walks away believing the schedule now runs their new SQL. It does not, and
// will not until somebody approves it.
//
// Two call sites need this exact distinction — saving an edit, and restoring an older
// version — so it lives here as a discriminated union rather than as an `if` repeated
// in each. The point of the union is that "applied" and "proposed" cannot be confused
// by accident: there is no shared success branch to fall into.

export type SavedQueryUpdateResult =
  | { kind: "applied" }
  | {
      kind: "proposed"
      /** The class the server derived from the PROPOSED text, for the "needs review" note. */
      statementClass: string
      /** The server's own explanation of why this is pending. Shown as-is. */
      reason: string
    }
  | { kind: "error"; message: string }

/** The subset of fields PATCH accepts. Omitted keys are left alone server-side. */
export interface SavedQueryPatch {
  name?: string
  description?: string
  sql_text?: string
  visibility?: string
  /** Why the SQL should change. Only read when the edit becomes a proposal. */
  note?: string
}

const FALLBACK_REASON =
  "This query is on a schedule, so the SQL change needs a workspace admin's approval before it takes effect."

/**
 * PATCH a saved query and say which of the three things happened.
 *
 * Never throws for an HTTP or network failure — those come back as `kind: "error"` so
 * a caller cannot accidentally treat a rejected write as a successful one by omitting
 * a catch.
 */
export async function updateSavedQuery(
  savedQueryId: string,
  patch: SavedQueryPatch,
): Promise<SavedQueryUpdateResult> {
  let res: Response
  try {
    res = await authFetch(`/api/v1/explorer/saved/${savedQueryId}`, {
      method: "PATCH",
      body: JSON.stringify(patch),
    })
  } catch {
    return { kind: "error", message: "Could not reach the server to save your changes." }
  }

  const data = (await res.json().catch(() => ({}))) as {
    error?: string
    pending_approval?: { statement_class?: string; reason?: string }
  }

  if (!res.ok) {
    return { kind: "error", message: data?.error || "Could not save your changes." }
  }

  // Presence of the object is the signal, not its contents: an approval gate that
  // announced itself only through a message string would stop being detectable the
  // first time somebody reworded the message.
  if (data.pending_approval) {
    return {
      kind: "proposed",
      statementClass: String(data.pending_approval.statement_class ?? ""),
      reason: String(data.pending_approval.reason || FALLBACK_REASON),
    }
  }

  return { kind: "applied" }
}
