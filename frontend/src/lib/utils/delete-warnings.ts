/**
 * Surfacing partial-success warnings from a DELETE.
 *
 * `DELETE /pipelines/:id` returns 200 with an optional `warnings` array naming
 * teardown work that did NOT finish — a replication slot that could not be
 * dropped, Kafka topics or consumer groups left behind, a Temporal schedule that
 * outlived its pipeline. The pipeline row is gone either way, so the status code
 * is 200 and the UI used to answer every one of those with a flat
 * "Pipeline deleted" success toast. The leak was then visible only in the
 * orchestrator's logs, which is where it stayed.
 *
 * `readDeleteWarnings` is deliberately total: a non-JSON body, a body that is not
 * an object, a `warnings` value that is not an array of strings, all collapse to
 * "no warnings" rather than throwing. Reporting a clean delete for a 200 whose
 * body we could not parse is the same behavior as before this module existed;
 * throwing out of a success path would be a regression.
 */
export async function readDeleteWarnings(res: Response): Promise<string[]> {
  let raw: unknown
  try {
    raw = await res.json()
  } catch {
    return []
  }
  if (!raw || typeof raw !== "object") return []
  const value = (raw as Record<string, unknown>)["warnings"]
  if (!Array.isArray(value)) return []
  return value.map((w) => String(w).trim()).filter((w) => w.length > 0)
}

/**
 * The toast copy for a delete that returned warnings. Kept next to the reader so
 * the pipelines list and the pipeline header cannot drift apart on wording.
 *
 * The title must not say "deleted" alone — the point is that something was left
 * behind, and a user who reads only the title should still learn that.
 */
export function deleteWarningToast(warnings: string[]): { title: string; description: string } {
  return {
    title:
      warnings.length === 1
        ? "Pipeline deleted, but one cleanup step did not finish"
        : `Pipeline deleted, but ${warnings.length} cleanup steps did not finish`,
    description: warnings.join(" · "),
  }
}
