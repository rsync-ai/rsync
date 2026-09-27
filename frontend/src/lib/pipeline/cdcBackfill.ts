/**
 * Client for POST /api/v1/pipelines/:id/cdc/backfill — the Debezium ad-hoc
 * snapshot trigger. `API_ENDPOINTS.PIPELINES.CDC_BACKFILL` has existed in
 * api.ts since the endpoint shipped and was called zero times, so the only way
 * to re-read a table that was already streaming was to rebuild the pipeline
 * ("Edit tables" backfills only the tables you add).
 *
 * The gateway (pipeline_cdc.go:188) passes the body through untouched to the
 * orchestrator, which does all the validation (cdc.go BackfillCDCTables). Its
 * rejections are specific and actionable, and every one of them is a sentence
 * the operator needs — a generic "request failed" toast would hide the fact
 * that, for instance, the pipeline's connector has no signal channel and no
 * amount of retrying will help. `explainBackfillError` is that mapping.
 *
 * `fetchBackfillCapability` asks the same question without acting on it
 * (GET on the same path), so the card can say "not available" before anyone
 * picks tables and clicks.
 */

import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"

export type BackfillMode = "incremental" | "blocking"

export type BackfillErrorBody = {
  error?: string
  message?: string
  tables?: string[]
  connector_class?: string
}

export type BackfillSuccess = {
  success?: boolean
  pipeline_id?: string
  connector_name?: string
  signal_channel?: string
  signal_topic?: string
  snapshot_mode?: string
  data_collections?: string[]
  message?: string
  // Kafka-channel pipelines (PostgreSQL, MongoDB) queue the load and answer
  // "queued" with the request's id; it is sent once the connector runs with the
  // tables, which takes about a minute. MySQL sends at once and has neither.
  status?: string
  request_id?: string
}

export type BackfillResult =
  | { ok: true; data: BackfillSuccess }
  | { ok: false; title: string; detail: string; tables?: string[]; code?: string }

/**
 * The orchestrator's fallback when a connector has no signal channel. Used only
 * when the server sends no message of its own, and kept in step with
 * cdc.go backfillNotSupportedMessage. It used to say the channel came only with
 * an incremental initial snapshot (a large PostgreSQL source); every PostgreSQL
 * and MongoDB connector now gets one when it is created, so recreating an older
 * pipeline is the fix and the copy says so.
 */
export const NO_SIGNAL_CHANNEL_DETAIL =
  "This pipeline's CDC connector has no signal channel, so it cannot be asked to re-read tables: " +
  "neither Re-snapshot nor the backfill of newly added tables can run. " +
  "PostgreSQL and MongoDB pipelines get a Kafka signal channel when their connector is created, but connectors " +
  "created before that was added have none; recreating the pipeline adds it. " +
  "MySQL pipelines use a signal table instead, and other sources have no re-snapshot path."

/**
 * Why a pipeline is offered the blocking mode only, and what that costs. Only
 * MongoDB is blocking-only today (cdc.go backfillModes): Debezium's incremental
 * snapshot on MongoDB writes watermark documents into a collection in the
 * source, and rsync never writes to a source. Fallback for
 * cdc.go backfillModeNotSupportedMessage too.
 */
export const BLOCKING_ONLY_DETAIL =
  "MongoDB pipelines re-read collections with a blocking snapshot only: change streaming for this pipeline " +
  "pauses while the requested collections are read again, then resumes where it stopped, so changes made " +
  "meanwhile arrive late rather than being skipped (as long as the oplog still holds them). Nothing is written to " +
  "your MongoDB database. An incremental snapshot, which keeps streaming during the re-read, is not offered " +
  "because on MongoDB it would write watermark documents into your database."

/**
 * Why an object-storage destination is offered the blocking mode only. The
 * snapshot has to own the table's folder while it rewrites it, so the stream
 * waits. Kept next to BLOCKING_ONLY_DETAIL, which is the MongoDB reason.
 */
export const OBJECT_STORAGE_BLOCKING_DETAIL =
  "Pipelines that write to object storage (GCS, S3, Azure Blob, MinIO) re-read tables with a blocking snapshot " +
  "only: change streaming for this pipeline pauses while the requested tables are read again, then resumes where " +
  "it stopped, so changes made meanwhile arrive late rather than being skipped."

/**
 * Turn the orchestrator's machine-readable `error` code into something an
 * operator can act on. The server's own `message` is kept as the detail
 * wherever it sends one — it is more specific than anything restated here — and
 * the title says what kind of problem it is.
 */
export function explainBackfillError(status: number, body: BackfillErrorBody | null): {
  title: string
  detail: string
  tables?: string[]
} {
  const code = String(body?.error || "").trim()
  const serverMessage = String(body?.message || "").trim()

  switch (code) {
    case "missing_primary_key":
      return {
        title: "These tables have no primary key",
        // The server names the tables; that list IS the fix.
        detail:
          serverMessage ||
          "CDC to a database destination (PostgreSQL, MySQL or MongoDB) needs a PRIMARY KEY on every table. Add a PRIMARY KEY or remove these tables.",
        tables: Array.isArray(body?.tables) ? body!.tables : undefined,
      }
    case "cdc_pk_validation_unsupported":
      return {
        title: "Primary keys could not be checked",
        detail: serverMessage || "This source does not support the primary-key check that a re-snapshot requires.",
      }
    case "cdc_backfill_not_supported":
      return {
        title: "This pipeline cannot be re-snapshotted",
        // Worth being blunt: this one is a property of how the connector was
        // created, not a transient failure, so retrying is pointless. What does
        // help (recreating a PostgreSQL pipeline) is in the detail itself.
        detail:
          (serverMessage || NO_SIGNAL_CHANNEL_DETAIL) +
          " Retrying will not change this — whether a connector has a signal channel is decided when it is created.",
      }
    case "cdc_backfill_mode_not_supported":
      return {
        title: "This snapshot mode is not available for this pipeline",
        detail: serverMessage || BLOCKING_ONLY_DETAIL,
      }
    case "signal_channel_unavailable":
      return {
        title: "The orchestrator has no Kafka producer",
        detail:
          serverMessage ||
          "The connector signals over Kafka but this orchestrator has no Kafka producer. This is a deployment problem, not a pipeline one.",
      }
    case "signal_emit_failed":
      return {
        title: "The signal could not be published",
        detail: serverMessage || "Kafka accepted the connection but refused the signal. Nothing was re-snapshotted.",
      }
    case "orchestrator_unreachable":
      return {
        title: "The orchestrator is unreachable",
        detail: serverMessage || "The API gateway could not reach the orchestrator. Nothing was re-snapshotted.",
      }
  }

  if (status === 404) {
    return {
      title: "No CDC connector for this pipeline",
      detail:
        serverMessage || code || "The pipeline has no running Debezium connector, so there is nothing to signal.",
    }
  }

  return {
    title: "Re-snapshot was rejected",
    detail: serverMessage || code || `HTTP ${status}`,
  }
}

/**
 * Where the fix for a keyless table goes, said for the Edit tables dialog. The
 * orchestrator checks the keys before it touches the connector and the gateway
 * saves nothing on a refusal (cdc.go UpdateCDCTables, cdc_tables.go), so the
 * pipeline keeps its previous table list.
 */
export const MISSING_PRIMARY_KEY_EDIT_DETAIL =
  "This pipeline writes to a database, which updates and deletes each row by its primary key. " +
  "Add a PRIMARY KEY to these tables in the source database, then add them again, or untick them to save the " +
  "rest. The pipeline's table list was not changed."

/**
 * A refused save in the Edit tables dialog, readable where the dialog shows
 * it. Its message names the tables too, for any caller that only prints
 * `message`.
 */
export class TableEditError extends Error {
  readonly title: string
  readonly detail: string
  readonly tables: string[]

  constructor(title: string, detail: string, tables: string[]) {
    super(tables.length > 0 ? `${title}: ${tables.join(", ")}. ${detail}` : `${title}. ${detail}`)
    this.name = "TableEditError"
    this.title = title
    this.detail = detail
    this.tables = tables
  }
}

/**
 * The Edit tables dialog's reading of a refused CDC table-list save
 * (POST /pipelines/:id/cdc/tables). Only the primary-key refusal is mapped: it
 * is the one fixed in the source, so the dialog names the tables and says
 * where. The bare code used to reach the dialog because the error extractor
 * prefers `error` over `message`. Anything else returns null and keeps the
 * generic handling.
 */
export function explainTableEditError(body: BackfillErrorBody | null): TableEditError | null {
  if (String(body?.error || "").trim() !== "missing_primary_key") return null
  const tables = Array.isArray(body?.tables) ? body!.tables.map((t) => String(t).trim()).filter(Boolean) : []
  return new TableEditError(explainBackfillError(400, body).title, MISSING_PRIMARY_KEY_EDIT_DETAIL, tables)
}

/**
 * Fire the ad-hoc snapshot. `tables` must be non-empty — the orchestrator
 * rejects an empty list with 400 "tables must be non-empty", and the caller can
 * give a better message than a round-trip would.
 */
export async function triggerCdcBackfill(args: {
  pipelineId: string
  tables: string[]
  // Omitted = the connector's default (cdc.go backfillModes): incremental where
  // it is offered, blocking on MongoDB.
  mode?: BackfillMode
}): Promise<BackfillResult> {
  const tables = args.tables.map((t) => t.trim()).filter(Boolean)
  if (tables.length === 0) {
    return { ok: false, title: "Pick at least one table", detail: "A re-snapshot has to name the tables to re-read." }
  }

  let res: Response
  try {
    res = await authFetch(API_ENDPOINTS.PIPELINES.CDC_BACKFILL(args.pipelineId), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args.mode ? { tables, mode: args.mode } : { tables }),
    })
  } catch {
    return {
      ok: false,
      title: "The API is unreachable",
      detail: "Nothing was re-snapshotted.",
    }
  }

  const body = (await res.json().catch(() => null)) as (BackfillSuccess & BackfillErrorBody) | null
  if (!res.ok) {
    return { ok: false, code: String(body?.error || "").trim() || undefined, ...explainBackfillError(res.status, body) }
  }
  return { ok: true, data: body || {} }
}

export type BackfillCapability =
  // `modes` = what the POST accepts, default first. An orchestrator older than
  // the MongoDB support sends none; every connector it accepted took both.
  //
  // `objectStorage` / `cleansFolder` describe what a re-snapshot does at the
  // destination: an object store cannot upsert, so a re-read either empties the
  // table's folder first (layout v2) or adds a second copy (the older layout).
  // Absent from an older orchestrator, which is read as "not object storage".
  | {
      state: "supported"
      channel: string
      modes: BackfillMode[]
      defaultMode: BackfillMode
      connectorClass?: string
      destinationType?: string
      objectStorage?: boolean
      cleansFolder?: boolean
    }
  | { state: "unsupported"; detail: string }
  // The question could not be answered: an older gateway without the GET route,
  // no connector yet, Kafka Connect unreachable. The card then behaves as it did
  // before the check existed — it offers the action and lets the POST explain.
  | { state: "unknown" }

const ALL_MODES: BackfillMode[] = ["incremental", "blocking"]

function parseModes(raw: unknown): BackfillMode[] {
  if (!Array.isArray(raw)) return [...ALL_MODES]
  const modes = raw
    .map((m) => String(m).trim().toLowerCase())
    .filter((m): m is BackfillMode => (ALL_MODES as string[]).includes(m))
  // A supported connector with no recognisable mode is a server we do not
  // understand; offering both and letting the POST decide is the old behaviour.
  return modes.length > 0 ? Array.from(new Set(modes)) : [...ALL_MODES]
}

/** True when the pipeline can only re-read with streaming paused (MongoDB). */
export function isBlockingOnly(cap: BackfillCapability): boolean {
  return cap.state === "supported" && cap.modes.length === 1 && cap.modes[0] === "blocking"
}

/**
 * GET /pipelines/:id/cdc/backfill — would a re-snapshot be accepted? Read-only:
 * the orchestrator reads the connector's config and signals nothing.
 */
export async function fetchBackfillCapability(pipelineId: string): Promise<BackfillCapability> {
  try {
    const res = await authFetch(API_ENDPOINTS.PIPELINES.CDC_BACKFILL(pipelineId), { cache: "no-store" })
    if (!res.ok) return { state: "unknown" }
    const body = (await res.json().catch(() => null)) as {
      supported?: unknown
      signal_channel?: unknown
      message?: unknown
      modes?: unknown
      default_mode?: unknown
      connector_class?: unknown
      destination_type?: unknown
      object_storage?: unknown
      cleans_folder?: unknown
    } | null
    if (body?.supported === true) {
      const modes = parseModes(body.modes)
      const def = String(body.default_mode || "")
      return {
        state: "supported",
        channel: String(body.signal_channel || ""),
        modes,
        defaultMode: modes.includes(def as BackfillMode) ? (def as BackfillMode) : modes[0],
        connectorClass: typeof body.connector_class === "string" ? body.connector_class : undefined,
        destinationType: typeof body.destination_type === "string" ? body.destination_type : undefined,
        objectStorage: typeof body.object_storage === "boolean" ? body.object_storage : undefined,
        cleansFolder: typeof body.cleans_folder === "boolean" ? body.cleans_folder : undefined,
      }
    }
    if (body?.supported === false) {
      const message = typeof body.message === "string" ? body.message.trim() : ""
      return { state: "unsupported", detail: message || NO_SIGNAL_CHANNEL_DETAIL }
    }
    return { state: "unknown" }
  } catch {
    return { state: "unknown" }
  }
}

/**
 * The blocking-only reason for this capability: the destination's when it is
 * object storage, else the source's (MongoDB). Null when both modes are offered.
 */
export function blockingOnlyDetail(cap: BackfillCapability): string | null {
  if (!isBlockingOnly(cap)) return null
  return cap.state === "supported" && cap.objectStorage ? OBJECT_STORAGE_BLOCKING_DETAIL : BLOCKING_ONLY_DETAIL
}

/**
 * What a re-snapshot does to the rows already at the destination, said before
 * the click. Object storage cannot upsert: on the current folder layout the
 * orchestrator empties the table's folder and the snapshot writes it again; on
 * the older layout it cannot, and the folder gains a second copy. Anything else
 * (or an orchestrator too old to say) gets the general upsert/append note.
 */
export function resnapshotDestinationNote(cap: BackfillCapability): { tone: "info" | "warning"; text: string } {
  if (cap.state === "supported" && cap.objectStorage) {
    if (cap.cleansFolder) {
      return {
        tone: "info",
        text:
          "This pipeline writes to object storage. Each selected table's folder at the destination is emptied and " +
          "then written again by the snapshot, so it ends up holding one copy of the table — files there are " +
          "missing until the re-read reaches them.",
      }
    }
    return {
      tone: "warning",
      text:
        "This pipeline writes to object storage with the older folder layout, which a re-snapshot cannot empty " +
        "first: each selected table's folder gains a second copy of every row. Remove the old files yourself, or " +
        "read the folder with a query that keeps the newest version of each row.",
    }
  }
  return {
    tone: "warning",
    text:
      "A re-snapshot re-emits every row of the selected tables. A destination that upserts on the primary key " +
      "converges; an append-only destination (object storage, a log table) gains a second copy of those rows.",
  }
}

/**
 * The toast after POST .../cdc/backfill succeeded. A queued answer (Kafka
 * channel) has not reached Debezium yet: it is sent once the connector runs
 * with the tables, so "signalled" would claim something that has not happened.
 */
export function describeBackfillAccepted(
  data: BackfillSuccess,
  fallback: { tables: number; mode: BackfillMode },
): string {
  const n = Array.isArray(data.data_collections) ? data.data_collections.length : fallback.tables
  const used = String(data.snapshot_mode || fallback.mode).toLowerCase()
  const pause =
    used === "blocking" ? "Change streaming for this pipeline pauses until they are re-read, then resumes. " : ""
  if (String(data.status || "").toLowerCase() === "queued") {
    return (
      `Re-snapshot of ${plural(n, "table")} queued (${used}). It starts in about a minute, once the connector ` +
      `picks it up. ${pause}Its progress shows under Snapshot loads on this card.`
    )
  }
  return (
    `Re-snapshot signalled for ${plural(n, "table")} (${used}). ${pause}` +
    "Debezium picks the signal up asynchronously — watch Table statistics for the rows arriving."
  )
}

/** The `backfill` block of POST /pipelines/:id/cdc/tables (cdc_tables.go). */
export type TableEditBackfill = {
  requested?: boolean
  success?: boolean
  mode?: string
  tables?: string[]
  // Present once the orchestrator answered: its status and its body, verbatim.
  status_code?: number
  response?: unknown
  error?: string
  // "queued" = the load waits until the connector runs with the new table list
  // (about 30-90 s); "sent" = already delivered. Absent from an older gateway,
  // which sent it at once.
  status?: string
  request_id?: string
}

export type TableEditResult = {
  success?: boolean
  pending_provision?: boolean
  new_tables?: string[]
  removed_tables?: string[]
  backfill?: TableEditBackfill
  message?: string
  // Sentences the server wants shown as they are (the list could not be saved,
  // a re-added table was not loaded, …).
  warnings?: string[]
  // The pipeline is paused: the added tables start streaming on resume.
  paused?: boolean
}

export type TableEditOutcome = {
  // "warning" = tables were added but their existing rows were NOT loaded and
  // the operator did not ask for that (the case that used to be silent), or the
  // server sent warnings of its own.
  tone: "success" | "info" | "warning"
  title: string
  detail: string
  tables?: string[]
  warnings?: string[]
}

function plural(n: number, word: string): string {
  return `${n} ${word}${n === 1 ? "" : "s"}`
}

function strings(raw: unknown): string[] {
  return Array.isArray(raw) ? raw.map((v) => String(v ?? "").trim()).filter(Boolean) : []
}

// Names a few tables outright; past that, a count reads better than a wall.
function nameTables(tables: string[], noun: string): string {
  if (tables.length === 0) return ""
  if (tables.length > 3) return plural(tables.length, noun)
  if (tables.length === 1) return tables[0]
  return `${tables.slice(0, -1).join(", ")} and ${tables[tables.length - 1]}`
}

/**
 * What removing tables did (#16): they stop streaming, and nothing is deleted
 * — their rows stay at the destination and their counts stay on the statistics
 * tab, marked Removed. Empty when nothing was removed.
 */
export function describeRemovedTables(removed: string[]): string {
  if (removed.length === 0) return ""
  const one = removed.length === 1
  return (
    `${nameTables(removed, "removed table")} stopped streaming; ${one ? "its" : "their"} data already at the ` +
    `destination stays, and ${one ? "its" : "their"} counts stay under Table statistics as Removed.`
  )
}

/**
 * Say what saving the CDC table list actually did. The gateway answers 200
 * even when the backfill it forwarded was refused — the list DID change — and
 * reports the refusal in `backfill`. The dialog used to discard that block and
 * close, so a refused backfill looked exactly like a successful one while the
 * added tables' existing rows never arrived (KI-CDC-EDIT-TABLES-BACKFILL-SILENT).
 *
 * `unavailableDetail` is the capability check's reason when the dialog already
 * knew backfill could not run and so did not ask for it. `removed` and `paused`
 * are the caller's own knowledge, used only when an older gateway's answer
 * lacks `removed_tables` / `paused`.
 */
export function describeTableEditResult(
  result: TableEditResult | null | undefined,
  opts?: { unavailableDetail?: string; removed?: string[]; paused?: boolean },
): TableEditOutcome {
  const added = Array.isArray(result?.new_tables) ? result!.new_tables : []
  const removed = Array.isArray(result?.removed_tables) ? strings(result!.removed_tables) : strings(opts?.removed)
  const paused = typeof result?.paused === "boolean" ? result.paused : Boolean(opts?.paused)
  const warnings = strings(result?.warnings)
  const bf = result?.backfill
  // A paused pipeline streams nothing until it is resumed (#17): "from now on"
  // would promise changes that are not flowing.
  const stillStreams = paused
    ? " They will start streaming when you resume the pipeline."
    : " They still stream every change made from now on."
  const removedNote = describeRemovedTables(removed)

  // Server warnings are always shown, and never shown as a plain success.
  const finish = (out: TableEditOutcome): TableEditOutcome => {
    const detail = removedNote && !out.detail.includes(removedNote) ? `${out.detail} ${removedNote}` : out.detail
    if (warnings.length === 0) return { ...out, detail }
    return { ...out, detail, tone: "warning", warnings }
  }

  if (result?.pending_provision) {
    const out: TableEditOutcome = {
      tone: "info",
      title: "Table selection saved",
      detail:
        result.message ||
        "This pipeline has no CDC connector yet, so the list is applied when CDC is provisioned; the tables load then.",
    }
    // No connector means nothing streamed yet, so there is no removal to report.
    return warnings.length > 0 ? { ...out, tone: "warning", warnings } : out
  }
  if (added.length === 0) {
    if (removed.length > 0) {
      return finish({
        tone: "success",
        title: `${plural(removed.length, "table")} removed`,
        detail: removedNote,
        tables: removed,
      })
    }
    return finish({
      tone: "success",
      title: "CDC tables updated",
      detail:
        "No tables were added, so there was nothing to backfill. Tables already streaming were not reloaded — " +
        "use Re-snapshot tables for that.",
    })
  }
  if (!bf?.requested) {
    if (opts?.unavailableDetail) {
      return finish({
        tone: "warning",
        title: `${plural(added.length, "table")} added, but their existing rows were not loaded`,
        detail: opts.unavailableDetail + stillStreams,
        tables: added,
      })
    }
    return finish({
      tone: "info",
      title: "CDC tables updated",
      detail:
        (paused
          ? `${plural(added.length, "added table")} will start streaming when you resume the pipeline. `
          : `${plural(added.length, "added table")} stream changes from now on. `) +
        "Their existing rows were not loaded because the backfill option was off — use Re-snapshot tables to load them.",
      tables: added,
    })
  }
  if (bf.success) {
    // The gateway reports the mode the orchestrator actually used. Blocking
    // (MongoDB, object storage) pauses streaming for the whole pipeline while
    // the rows load, so "changes stream as they happen" would be false there.
    const blocking = String(bf.mode || "").toLowerCase() === "blocking"
    const blockingCost =
      "change streaming for this pipeline pauses until they are read, then resumes where it stopped, so " +
      "changes made meanwhile arrive late rather than being skipped."
    // Queued (#21): nothing is loading yet. The load goes to Debezium once the
    // connector runs with the new table list, so the toast says when, not "loading".
    if (String(bf.status || "").toLowerCase() === "queued") {
      const when = paused
        ? "starts after you resume the pipeline, once the connector runs with the new table list"
        : "starts in about a minute, once the connector runs with the new table list"
      return finish({
        tone: "success",
        title: "CDC tables updated",
        detail:
          `The load of the existing rows of ${plural(added.length, "added table")} is queued and ${when}. ` +
          (blocking
            ? `It is a blocking snapshot: ${blockingCost} `
            : paused
              ? ""
              : "New changes stream as they happen meanwhile. ") +
          "Tables already streaming were not reloaded. Progress shows under Re-snapshot tables on the Table statistics tab.",
        tables: added,
      })
    }
    return finish({
      tone: "success",
      title: "CDC tables updated",
      detail:
        (blocking
          ? `Loading the existing rows of ${plural(added.length, "added table")} with a blocking snapshot: ` +
            blockingCost +
            " Tables already streaming were not reloaded."
          : `Loading the existing rows of ${plural(added.length, "added table")} in the background; ` +
            "new changes stream as they happen. Tables already streaming were not reloaded.") +
        (paused ? " The added tables start streaming when you resume the pipeline." : ""),
      tables: added,
    })
  }

  // Refused. With an orchestrator answer, explain its code the same way the
  // Re-snapshot card does; without one, the request never got that far.
  const body = bf.response && typeof bf.response === "object" ? (bf.response as BackfillErrorBody) : null
  const reason =
    typeof bf.status_code === "number"
      ? explainBackfillError(bf.status_code, body)
      : { detail: bf.error || "The backfill request did not reach the orchestrator.", tables: undefined }
  return finish({
    tone: "warning",
    title: `${plural(added.length, "table")} added, but their existing rows were NOT loaded`,
    detail: reason.detail + stillStreams,
    tables: reason.tables || added,
  })
}

// ---------------------------------------------------------------------------
// Snapshot loads: GET /pipelines/:id/cdc/snapshot-requests (#21)
// ---------------------------------------------------------------------------

export type SnapshotRequestStatus = "queued" | "sent" | "started" | "completed" | "unconfirmed" | "failed"

export type SnapshotRequest = {
  id: string
  mode: BackfillMode
  tables: string[]
  // initial: the pipeline's first full load, recorded by the orchestrator
  // (migration 118); the API never creates one.
  source: "resnapshot" | "table_edit" | "auto_pickup" | "initial"
  status: SnapshotRequestStatus
  attempts: number
  completed_tables: string[]
  last_error?: string
  requested_at: string
  sent_at?: string
  started_at?: string
  last_progress_at?: string
  completed_at?: string
}

// Still moving: the card keeps polling while any request is in one of these.
const ACTIVE_SNAPSHOT_STATUSES = new Set(["queued", "sent", "started"])

export function snapshotRequestIsActive(r: Pick<SnapshotRequest, "status">): boolean {
  return ACTIVE_SNAPSHOT_STATUSES.has(String(r.status || "").toLowerCase())
}

/**
 * A request's state in plain words. `tone` drives the colour; `detail` is the
 * server's last error where it has one worth reading.
 */
export function describeSnapshotRequest(r: SnapshotRequest): {
  tone: "progress" | "done" | "warning" | "error"
  label: string
  detail?: string
} {
  const lastError = String(r.last_error || "").trim()
  switch (String(r.status || "").toLowerCase()) {
    case "queued":
      // A re-snapshot of tables that already stream only waits for the next
      // pick-up; the connector restart is what an added table waits for.
      return {
        tone: "progress",
        label:
          r.source === "resnapshot"
            ? "Queued — waiting for the connector to pick it up…"
            : "Waiting for the connector to restart with the new tables…",
      }
    case "sent":
      return { tone: "progress", label: "Signal sent — waiting for the first rows…" }
    case "started": {
      const total = Array.isArray(r.tables) ? r.tables.length : 0
      const done = Array.isArray(r.completed_tables) ? r.completed_tables.length : 0
      return {
        tone: "progress",
        label: total > 1 && done > 0 ? `Loading rows… (${done} of ${total} tables done)` : "Loading rows…",
      }
    }
    case "completed":
      return { tone: "done", label: "Done" }
    case "unconfirmed":
      // The server's last_error says why (no rows after N signals, progress not
      // tracked on this deployment, …); the label only says what is known.
      return {
        tone: "warning",
        label: "Not confirmed — no finished snapshot was seen",
        detail: lastError || undefined,
      }
    case "failed":
      return { tone: "error", label: lastError ? `Failed: ${lastError}` : "Failed" }
    default:
      // A status this client does not know yet: say it rather than guess.
      return { tone: "progress", label: String(r.status || "Unknown") }
  }
}

export function describeSnapshotRequestSource(source: string): string {
  switch (source) {
    case "resnapshot":
      return "Re-snapshot"
    case "table_edit":
      return "Edit tables"
    case "auto_pickup":
      return "New table picked up"
    case "initial":
      return "Initial load"
    default:
      return source || "Snapshot"
  }
}

function parseSnapshotRequest(raw: unknown): SnapshotRequest | null {
  if (!raw || typeof raw !== "object") return null
  const o = raw as Record<string, unknown>
  const id = String(o.id ?? "").trim()
  if (!id) return null
  const str = (v: unknown) => (typeof v === "string" && v.trim() ? v : undefined)
  return {
    id,
    mode: String(o.mode || "").toLowerCase() === "blocking" ? "blocking" : "incremental",
    tables: strings(o.tables),
    source: String(o.source || "") as SnapshotRequest["source"],
    status: String(o.status || "") as SnapshotRequestStatus,
    attempts: Number.isFinite(Number(o.attempts)) ? Number(o.attempts) : 0,
    completed_tables: strings(o.completed_tables),
    last_error: str(o.last_error),
    requested_at: String(o.requested_at || ""),
    sent_at: str(o.sent_at),
    started_at: str(o.started_at),
    last_progress_at: str(o.last_progress_at),
    completed_at: str(o.completed_at),
  }
}

/**
 * The latest snapshot loads, newest first. `unavailable` = a gateway without
 * the route (404): the card hides the list and stops asking. `error` = this
 * read failed; the caller keeps what it showed last.
 */
export async function fetchSnapshotRequests(
  pipelineId: string,
): Promise<{ state: "ok"; requests: SnapshotRequest[] } | { state: "unavailable" } | { state: "error" }> {
  try {
    const res = await authFetch(API_ENDPOINTS.PIPELINES.CDC_SNAPSHOT_REQUESTS(pipelineId), { cache: "no-store" })
    if (res.status === 404) return { state: "unavailable" }
    if (!res.ok) return { state: "error" }
    const body = (await res.json().catch(() => null)) as { requests?: unknown } | null
    const list = Array.isArray(body?.requests) ? body!.requests : []
    return { state: "ok", requests: list.map(parseSnapshotRequest).filter((r): r is SnapshotRequest => r !== null) }
  } catch {
    return { state: "error" }
  }
}
