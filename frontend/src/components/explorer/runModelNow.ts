import { authFetch } from "@/lib/api/auth-fetch"

// Run a saved model once, now, and say what happened. Two buttons do this — Run now in
// the model editor and on the schedule page — and they were two copies of one request
// and one message. The message is where copies drift: "Rebuilt …" is wrong for a
// statement model, which has no target table and reports a row count instead.

export interface RunNowResult {
  ok: boolean
  /** What to tell the person, success or failure, ready for a toast. */
  message: string
}

/**
 * POST /explorer/saved/:id/run. Never throws: a refused run and an unreachable server
 * both come back as `ok: false` with the reason.
 *
 * `knownTarget` names the table when the server's answer does not.
 */
export async function runModelNow(
  savedQueryId: string,
  materialization: string | undefined,
  knownTarget?: string,
): Promise<RunNowResult> {
  let res: Response
  try {
    res = await authFetch(`/api/v1/explorer/saved/${encodeURIComponent(savedQueryId)}/run`, { method: "POST" })
  } catch {
    return { ok: false, message: "Could not run the model" }
  }
  const data = (await res.json().catch(() => ({}))) as {
    error?: string
    rows_affected?: unknown
    target_table?: string
  }
  if (!res.ok) {
    // 400 = rsync.ai refused (wrong class for the mode, no target, unsupported connector);
    // 422 = the engine rejected the statement. Both carry a usable message, and neither
    // is a server fault worth a generic "something went wrong".
    return { ok: false, message: data?.error || "The run did not complete" }
  }
  if (materialization === "statement") {
    // The server reports a row count only for a DML write.
    const rows = typeof data?.rows_affected === "number" ? data.rows_affected : null
    return {
      ok: true,
      message: rows === null ? "Statement ran" : `Statement ran — ${rows} row${rows === 1 ? "" : "s"} affected`,
    }
  }
  return { ok: true, message: `Rebuilt ${data?.target_table || knownTarget || "the target table"}` }
}
