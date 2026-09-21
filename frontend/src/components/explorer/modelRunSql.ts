// Which SQL a model's run executed, from the model's edit history.
//
// A run reads the model's SQL just before it executes (saved_query_models.go
// runSavedQueryModel), and its row stores no copy. GET /explorer/saved/:id/versions keeps
// the text as it was BEFORE each edit, stamped with when the edit happened, newest first.
// So the text a run used is the snapshot of the first edit after it started, or the
// current text when nothing was edited since.

/** One pre-edit snapshot, as GET /explorer/saved/:id/versions returns it. */
export interface SqlVersion {
  version: number
  sql_text: string
  /** When the edit that replaced this text happened. */
  created_at: string
}

export interface SqlHistory {
  /** Newest first. The route returns at most 100, and retention can prune the oldest. */
  versions: SqlVersion[]
  current: { sql_text: string }
}

/** An edit this close to a run's start may have landed on either side of the read. */
export const NEAR_EDIT_MS = 60_000

export type RunSql =
  /** No run to place: the model's SQL as it is now. */
  | { kind: "current"; sql: string }
  /** The text the run started with. `editedSince`: the model has been edited after it. */
  | { kind: "at_run"; sql: string; editedSince: boolean; nearEdit: boolean }
  /** The model was edited after the run, and the history no longer reaches back to it. */
  | { kind: "unknown"; current: string }

export function sqlForRun(history: SqlHistory, startedAt: string | undefined | null): RunSql {
  const current = history.current.sql_text
  const start = startedAt ? Date.parse(startedAt) : NaN
  if (Number.isNaN(start)) return { kind: "current", sql: current }

  const dated = history.versions
    .map((v) => ({ v, at: Date.parse(v.created_at) }))
    .filter((x) => !Number.isNaN(x.at))
  const nearEdit = dated.some((x) => Math.abs(x.at - start) <= NEAR_EDIT_MS)
  const after = dated.filter((x) => x.at > start)
  if (after.length === 0) return { kind: "at_run", sql: current, editedSince: false, nearEdit }

  const first = after.reduce((a, b) => (b.v.version < a.v.version ? b : a))
  // The list is the newest versions with nothing missing between them, so it holds every
  // edit since the run when it also holds one from before it, or the first edit ever.
  const complete = first.v.version === 1 || history.versions.some((v) => v.version < first.v.version)
  if (!complete) return { kind: "unknown", current }
  return { kind: "at_run", sql: first.v.sql_text, editedSince: true, nearEdit }
}

/** Reads the versions route's body; null when it is not the shape the route sends. */
export function parseSqlHistory(body: unknown): SqlHistory | null {
  if (!body || typeof body !== "object") return null
  const { versions, current } = body as { versions?: unknown; current?: unknown }
  if (!Array.isArray(versions) || !current || typeof current !== "object") return null
  const sql = (current as { sql_text?: unknown }).sql_text
  if (typeof sql !== "string") return null
  return {
    current: { sql_text: sql },
    versions: versions.filter(
      (v): v is SqlVersion =>
        !!v &&
        typeof v === "object" &&
        typeof (v as SqlVersion).version === "number" &&
        typeof (v as SqlVersion).sql_text === "string" &&
        typeof (v as SqlVersion).created_at === "string",
    ),
  }
}
