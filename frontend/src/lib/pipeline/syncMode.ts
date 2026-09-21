/**
 * The one frontend rule for "is this pipeline CDC?".
 *
 * `cdc_mode` is not a CDC signal on its own. Connections carry a DB default of
 * `cdc_mode = 'initial'` (migration 002) whatever their `sync_mode`, and the chat
 * create path used to copy it onto BATCH pipelines, so the old test
 * `sync_mode === "cdc" || cdc_mode` put a "CDC" badge (and CDC status polling) on
 * a batch pipeline. An explicit `sync_mode` always wins; `cdc_mode` only decides
 * for a legacy row that never persisted `sync_mode`.
 */
export type SyncModeFields = {
  sync_mode?: string | null
  cdc_mode?: string | null
}

function norm(v: unknown): string {
  return typeof v === "string" ? v.trim().toLowerCase() : ""
}

/**
 * Returns true / false when the pipeline row decides the mode, or null when it
 * says nothing (no sync_mode and no cdc_mode), so a caller may consult the
 * source connection.
 */
export function pipelineIsCDC(p: SyncModeFields | null | undefined): boolean | null {
  if (!p) return null
  const sync = norm(p.sync_mode)
  if (sync) return sync === "cdc"
  if (norm(p.cdc_mode)) return true
  return null
}

/**
 * Connection fallback for a pipeline that persisted no mode. Only the
 * connection's `sync_mode` counts: its `cdc_mode` is a column default, present
 * on batch connections too.
 */
export function connectionIsCDC(conn: SyncModeFields | null | undefined): boolean {
  return norm(conn?.sync_mode) === "cdc"
}
