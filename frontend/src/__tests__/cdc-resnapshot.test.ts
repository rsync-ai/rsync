import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"

import { authFetch } from "@/lib/api/auth-fetch"
import {
  BLOCKING_ONLY_DETAIL,
  blockingOnlyDetail,
  describeBackfillAccepted,
  describeSnapshotRequest,
  describeSnapshotRequestSource,
  describeTableEditResult,
  explainBackfillError,
  fetchBackfillCapability,
  fetchSnapshotRequests,
  isBlockingOnly,
  NO_SIGNAL_CHANNEL_DETAIL,
  OBJECT_STORAGE_BLOCKING_DETAIL,
  resnapshotDestinationNote,
  snapshotRequestIsActive,
  triggerCdcBackfill,
  type SnapshotRequest,
} from "@/lib/pipeline/cdcBackfill"
import { cdcTableNames } from "@/components/pipeline/CdcResnapshotCard"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

const mockFetch = authFetch as Mock
afterEach(() => vi.clearAllMocks())

describe("cdcTableNames", () => {
  it("offers only the CDC tables — a batch table has nothing to re-snapshot", () => {
    expect(
      cdcTableNames([
        { table_name: "orders", qualified_name: "public.orders", mode: "cdc" },
        { table_name: "archive", qualified_name: "public.archive", mode: "batch" },
      ])
    ).toEqual(["public.orders"])
  })

  it("prefers the source-side qualified name, which is what the signal carries", () => {
    expect(cdcTableNames([{ table_name: "orders", qualified_name: "sales.orders", mode: "cdc" }])).toEqual([
      "sales.orders",
    ])
  })

  it("falls back to table_name when the row has no qualified name", () => {
    expect(cdcTableNames([{ table_name: "orders", mode: "cdc" }])).toEqual(["orders"])
  })

  it("dedupes and sorts, and survives an empty or missing list", () => {
    expect(
      cdcTableNames([
        { table_name: "b", qualified_name: "s.b", mode: "cdc" },
        { table_name: "a", qualified_name: "s.a", mode: "CDC" },
        { table_name: "b", qualified_name: "s.b", mode: "cdc" },
        { table_name: "", qualified_name: "", mode: "cdc" },
      ])
    ).toEqual(["s.a", "s.b"])
    expect(cdcTableNames(null)).toEqual([])
  })
})

describe("explainBackfillError", () => {
  it("keeps the server's table list, because that list is the fix", () => {
    const out = explainBackfillError(400, {
      error: "missing_primary_key",
      message: "tables without PK: public.events",
      tables: ["public.events"],
    })
    expect(out.title).toContain("no primary key")
    expect(out.detail).toBe("tables without PK: public.events")
    expect(out.tables).toEqual(["public.events"])
  })

  it("says retrying is pointless when the pipeline has no signal channel", () => {
    const out = explainBackfillError(400, { error: "cdc_backfill_not_supported", connector_class: "postgres" })
    expect(out.detail).toContain("Retrying will not change this")
    expect(out.detail).toContain(NO_SIGNAL_CHANNEL_DETAIL)
  })

  it("names the fix that now works — recreating a PostgreSQL pipeline adds the channel — and no size gate", () => {
    // Every PostgreSQL connector gets a signal channel when it is created, so an
    // older pipeline recreated as-is is fixed. The copy used to say the channel
    // came only with a large (1M-row) incremental initial load.
    const out = explainBackfillError(400, { error: "cdc_backfill_not_supported" })
    expect(out.detail).toMatch(/recreating the pipeline adds it/i)
    expect(out.detail).toMatch(/newly added tables/i)
    expect(out.detail).not.toContain("snapshot_strategy")
    expect(out.detail).not.toMatch(/large enough|1,000,000|incremental initial/i)
  })

  it("names MongoDB alongside PostgreSQL as getting the channel at create time", () => {
    // Kept in step with cdc.go backfillNotSupportedMessage.
    expect(NO_SIGNAL_CHANNEL_DETAIL).toMatch(/PostgreSQL and MongoDB pipelines get a Kafka signal channel/)
  })

  it("explains a refused mode by what the allowed one costs, preferring the server's sentence", () => {
    const fallback = explainBackfillError(400, { error: "cdc_backfill_mode_not_supported" })
    expect(fallback.title).toBe("This snapshot mode is not available for this pipeline")
    expect(fallback.detail).toBe(BLOCKING_ONLY_DETAIL)
    expect(fallback.detail).toMatch(/pauses while the requested collections are read again/)
    expect(fallback.detail).toMatch(/Nothing is written to your MongoDB database/)
    expect(fallback.detail).toMatch(/watermark documents/)

    const server = explainBackfillError(400, { error: "cdc_backfill_mode_not_supported", message: "blocking only here" })
    expect(server.detail).toBe("blocking only here")
  })

  it("prefers the orchestrator's own explanation when it sends one", () => {
    const out = explainBackfillError(400, { error: "cdc_backfill_not_supported", message: "the server's reason" })
    expect(out.detail.startsWith("the server's reason")).toBe(true)
    expect(out.detail).not.toContain(NO_SIGNAL_CHANNEL_DETAIL)
  })

  it("distinguishes a deployment problem from a pipeline problem", () => {
    expect(explainBackfillError(503, { error: "signal_channel_unavailable" }).title).toContain("no Kafka producer")
    expect(explainBackfillError(502, { error: "orchestrator_unreachable" }).title).toContain("unreachable")
  })

  it("says nothing was re-snapshotted when the signal was refused", () => {
    expect(explainBackfillError(500, { error: "signal_emit_failed" }).detail).toContain("Nothing was re-snapshotted")
  })

  it("explains a 404 as no running connector rather than as a missing page", () => {
    expect(explainBackfillError(404, null).title).toContain("No CDC connector")
  })

  it("falls back to the server's own message for an unknown code", () => {
    const out = explainBackfillError(500, { error: "something_new", message: "the wire says this" })
    expect(out.title).toBe("Re-snapshot was rejected")
    expect(out.detail).toBe("the wire says this")
  })
})

describe("triggerCdcBackfill", () => {
  it("refuses an empty table list locally instead of spending a 400", async () => {
    const out = await triggerCdcBackfill({ pipelineId: "p1", tables: ["  "], mode: "incremental" })
    expect(mockFetch).not.toHaveBeenCalled()
    expect(out).toMatchObject({ ok: false, title: "Pick at least one table" })
  })

  it("posts the trimmed tables and the chosen mode", async () => {
    mockFetch.mockResolvedValue({ ok: true, status: 200, json: async () => ({ success: true }) })
    const out = await triggerCdcBackfill({ pipelineId: "p1", tables: [" s.a ", "s.b"], mode: "blocking" })

    expect(out.ok).toBe(true)
    const body = JSON.parse(mockFetch.mock.calls[0][1].body)
    expect(body).toEqual({ tables: ["s.a", "s.b"], mode: "blocking" })
    expect(mockFetch.mock.calls[0][0]).toContain("/api/v1/pipelines/p1/cdc/backfill")
  })

  it("sends no mode when none is chosen, so the orchestrator picks the connector's default", async () => {
    // MongoDB refuses incremental; a client that filled in a default would be
    // refused on every MongoDB pipeline.
    mockFetch.mockResolvedValue({ ok: true, status: 200, json: async () => ({ success: true }) })
    await triggerCdcBackfill({ pipelineId: "p1", tables: ["shop.orders"] })
    expect(JSON.parse(mockFetch.mock.calls[0][1].body)).toEqual({ tables: ["shop.orders"] })
  })

  it("turns a rejection into the explained error, not a raw code", async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 400,
      json: async () => ({ error: "missing_primary_key", tables: ["public.events"] }),
    })
    const out = await triggerCdcBackfill({ pipelineId: "p1", tables: ["public.events"], mode: "incremental" })

    expect(out).toMatchObject({ ok: false, tables: ["public.events"] })
    expect(out.ok).toBe(false)
  })

  it("carries the server's error code, so the card can tell 'cannot ever' from 'failed this time'", async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 400, json: async () => ({ error: "cdc_backfill_not_supported" }) })
    const out = await triggerCdcBackfill({ pipelineId: "p1", tables: ["s.a"], mode: "incremental" })
    expect(out).toMatchObject({ ok: false, code: "cdc_backfill_not_supported" })
  })
})

describe("fetchBackfillCapability", () => {
  it("GETs the same path the POST uses", async () => {
    mockFetch.mockResolvedValue({ ok: true, status: 200, json: async () => ({ supported: true, signal_channel: "kafka" }) })
    await fetchBackfillCapability("p1")
    expect(mockFetch.mock.calls[0][0]).toContain("/api/v1/pipelines/p1/cdc/backfill")
    expect(mockFetch.mock.calls[0][1]?.method ?? "GET").toBe("GET")
  })

  it("reports a supported connector with its channel — and both modes when an older orchestrator lists none", async () => {
    mockFetch.mockResolvedValue({ ok: true, status: 200, json: async () => ({ supported: true, signal_channel: "kafka" }) })
    const cap = await fetchBackfillCapability("p1")
    expect(cap).toEqual({
      state: "supported",
      channel: "kafka",
      modes: ["incremental", "blocking"],
      defaultMode: "incremental",
    })
    expect(isBlockingOnly(cap)).toBe(false)
  })

  it("reports MongoDB as blocking-only, from the modes the orchestrator lists", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        supported: true,
        signal_channel: "kafka",
        modes: ["blocking"],
        default_mode: "blocking",
        connector_class: "io.debezium.connector.mongodb.mongodbconnector",
      }),
    })
    const cap = await fetchBackfillCapability("p1")
    expect(cap).toMatchObject({ state: "supported", modes: ["blocking"], defaultMode: "blocking" })
    expect(isBlockingOnly(cap)).toBe(true)
  })

  it("control: a PostgreSQL connector listing both modes is not blocking-only", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ supported: true, signal_channel: "kafka", modes: ["incremental", "blocking"], default_mode: "incremental" }),
    })
    const cap = await fetchBackfillCapability("p1")
    expect(cap).toMatchObject({ modes: ["incremental", "blocking"], defaultMode: "incremental" })
    expect(isBlockingOnly(cap)).toBe(false)
  })

  it("drops modes it does not know and never defaults to one it was not offered", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ supported: true, signal_channel: "kafka", modes: ["BLOCKING", "turbo"], default_mode: "incremental" }),
    })
    expect(await fetchBackfillCapability("p1")).toMatchObject({ modes: ["blocking"], defaultMode: "blocking" })
  })

  it("reports an unsupported connector with the server's reason", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ supported: false, error: "cdc_backfill_not_supported", message: "no channel here" }),
    })
    expect(await fetchBackfillCapability("p1")).toEqual({ state: "unsupported", detail: "no channel here" })
  })

  it("falls back to its own reason when the server gives none", async () => {
    mockFetch.mockResolvedValue({ ok: true, status: 200, json: async () => ({ supported: false }) })
    expect(await fetchBackfillCapability("p1")).toEqual({ state: "unsupported", detail: NO_SIGNAL_CHANNEL_DETAIL })
  })

  // An older gateway has no GET on this path. "unknown" keeps the card exactly
  // as it was before the check existed, rather than hiding the button on a 404.
  it("says unknown — not unsupported — when the check itself fails", async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 404, json: async () => ({}) })
    expect(await fetchBackfillCapability("p1")).toEqual({ state: "unknown" })
    mockFetch.mockRejectedValue(new TypeError("Failed to fetch"))
    expect(await fetchBackfillCapability("p1")).toEqual({ state: "unknown" })
    mockFetch.mockResolvedValue({ ok: true, status: 200, json: async () => ({}) })
    expect(await fetchBackfillCapability("p1")).toEqual({ state: "unknown" })
  })
})

// POST /pipelines/:id/cdc/tables answers 200 even when the backfill it
// forwarded was refused. describeTableEditResult is what stops the Edit tables
// dialog from reading that 200 as "rows loaded" (KI-CDC-EDIT-TABLES-BACKFILL-SILENT).
describe("describeTableEditResult", () => {
  const refused = {
    success: true,
    new_tables: ["public.orders"],
    backfill: {
      requested: true,
      success: false,
      tables: ["public.orders"],
      status_code: 400,
      // The gateway embeds the orchestrator's body verbatim (json.RawMessage).
      response: { error: "cdc_backfill_not_supported", message: "no signal channel on this connector" },
      error: "orchestrator backfill failed (status 400)",
    },
  }

  it("turns a refused backfill into a warning that says the rows were NOT loaded, and why", () => {
    const out = describeTableEditResult(refused)
    expect(out.tone).toBe("warning")
    expect(out.title).toMatch(/NOT loaded/)
    expect(out.detail).toContain("no signal channel on this connector")
    expect(out.detail).toMatch(/still stream/i)
    expect(out.tables).toEqual(["public.orders"])
  })

  it("keeps the orchestrator's own table list when it names the tables at fault", () => {
    const out = describeTableEditResult({
      ...refused,
      backfill: {
        ...refused.backfill,
        response: { error: "missing_primary_key", message: "no PK", tables: ["public.events"] },
      },
    })
    expect(out.tone).toBe("warning")
    expect(out.tables).toEqual(["public.events"])
  })

  it("reports a request that never reached the orchestrator by its transport error", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders"],
      backfill: { requested: true, success: false, error: "dial tcp: connection refused" },
    })
    expect(out.tone).toBe("warning")
    expect(out.detail).toContain("dial tcp: connection refused")
  })

  it("says the rows are loading when the backfill was accepted", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders", "public.items"],
      backfill: { requested: true, success: true, status_code: 200 },
    })
    expect(out.tone).toBe("success")
    expect(out.detail).toMatch(/Loading the existing rows of 2 added tables/)
  })

  it("says streaming pauses when the orchestrator loaded the rows with a blocking snapshot (MongoDB)", () => {
    const out = describeTableEditResult({
      new_tables: ["shop.orders"],
      backfill: { requested: true, success: true, status_code: 200, mode: "blocking" },
    })
    expect(out.tone).toBe("success")
    expect(out.detail).toMatch(/blocking snapshot/)
    expect(out.detail).toMatch(/change streaming for this pipeline pauses until they are read, then resumes/)
    expect(out.detail).not.toMatch(/new changes stream as they happen/)
  })

  it("control: an incremental load says changes keep streaming, and no pause", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders"],
      backfill: { requested: true, success: true, status_code: 200, mode: "incremental" },
    })
    expect(out.detail).toMatch(/new changes stream as they happen/)
    expect(out.detail).not.toMatch(/pauses/)
  })

  it("does not warn when the operator turned the backfill off — but still says what that means", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders"],
      backfill: { requested: false, success: false },
    })
    expect(out.tone).toBe("info")
    expect(out.detail).toMatch(/were not loaded because the backfill option was off/)
    expect(out.detail).toMatch(/Re-snapshot tables/)
  })

  it("warns with the capability reason when the dialog knew backfill could not run", () => {
    const out = describeTableEditResult(
      { new_tables: ["public.orders"], backfill: { requested: false, success: false } },
      { unavailableDetail: "no channel here" },
    )
    expect(out.tone).toBe("warning")
    expect(out.detail).toContain("no channel here")
  })

  it("has nothing to backfill when no table was added", () => {
    const out = describeTableEditResult({ new_tables: [], backfill: { requested: true, success: true } })
    expect(out.tone).toBe("success")
    expect(out.detail).toMatch(/No tables were added/)
  })

  it("defers to provisioning when the pipeline has no connector yet", () => {
    const out = describeTableEditResult({
      pending_provision: true,
      new_tables: ["public.orders"],
      backfill: { requested: true, success: false, error: "no Debezium connector yet" },
      message: "Table selection saved. Applied when CDC is provisioned.",
    })
    expect(out.tone).toBe("info")
    expect(out.detail).toBe("Table selection saved. Applied when CDC is provisioned.")
  })
})

// #16/#17: a table edit that removes tables, runs on a paused pipeline, or
// comes back with server warnings. Each case has the control it differs from.
describe("describeTableEditResult — removals, pause, warnings", () => {
  it("a removal talks about the removal: streaming stops, the data and the counts stay", () => {
    const out = describeTableEditResult({ new_tables: [], removed_tables: ["public.orders"] })
    expect(out.tone).toBe("success")
    expect(out.title).toBe("1 table removed")
    expect(out.detail).toBe(
      "public.orders stopped streaming; its data already at the destination stays, and its counts stay under " +
        "Table statistics as Removed."
    )
    expect(out.detail).not.toMatch(/No tables were added/)
    expect(out.tables).toEqual(["public.orders"])
  })

  it("control: no removal and no addition keeps the nothing-to-backfill wording", () => {
    const out = describeTableEditResult({ new_tables: [], removed_tables: [] })
    expect(out.detail).toMatch(/No tables were added/)
    expect(out.detail).not.toMatch(/stopped streaming/)
  })

  it("an edit that adds and removes says both", () => {
    const out = describeTableEditResult({
      new_tables: ["public.items"],
      removed_tables: ["public.orders", "public.refunds"],
      backfill: { requested: true, success: true, status_code: 200 },
    })
    expect(out.detail).toMatch(/Loading the existing rows of 1 added table/)
    expect(out.detail).toMatch(/public\.orders and public\.refunds stopped streaming; their data/)
  })

  it("names the removed tables from the caller when an older gateway sends no removed_tables", () => {
    const out = describeTableEditResult({ new_tables: [] }, { removed: ["shop.orders"] })
    expect(out.title).toBe("1 table removed")
    expect(out.detail).toMatch(/^shop\.orders stopped streaming/)
  })

  it("the server's removed_tables wins over the caller's guess", () => {
    const out = describeTableEditResult({ new_tables: [], removed_tables: ["a.b"] }, { removed: ["x.y"] })
    expect(out.detail).toMatch(/^a\.b stopped streaming/)
    expect(out.detail).not.toMatch(/x\.y/)
  })

  it("paused: added tables start streaming on resume, not 'from now on'", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders"],
      paused: true,
      backfill: { requested: false, success: false },
    })
    expect(out.detail).toMatch(/1 added table will start streaming when you resume the pipeline/)
    expect(out.detail).not.toMatch(/from now on/)
  })

  it("control: running, the same edit streams changes from now on", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders"],
      paused: false,
      backfill: { requested: false, success: false },
    })
    expect(out.detail).toMatch(/1 added table stream changes from now on/)
    expect(out.detail).not.toMatch(/resume/)
  })

  it("paused, from the caller when an older gateway sends no `paused`", () => {
    const out = describeTableEditResult(
      { new_tables: ["public.orders"], backfill: { requested: false, success: false } },
      { paused: true },
    )
    expect(out.detail).toMatch(/when you resume the pipeline/)
  })

  it("a refused load on a paused pipeline says the tables stream on resume", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders"],
      paused: true,
      backfill: { requested: true, success: false, error: "dial tcp: connection refused" },
    })
    expect(out.tone).toBe("warning")
    expect(out.detail).toMatch(/They will start streaming when you resume the pipeline\./)
    expect(out.detail).not.toMatch(/from now on/)
  })

  it("shows the server's warnings and never as a plain success", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders"],
      warnings: ["The table list could not be saved; it is re-read from the connector."],
      backfill: { requested: true, success: true, status_code: 200 },
    })
    expect(out.tone).toBe("warning")
    expect(out.warnings).toEqual(["The table list could not be saved; it is re-read from the connector."])
    expect(out.detail).toMatch(/Loading the existing rows/)
  })

  it("control: no warnings keeps the success tone and no warnings list", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders"],
      warnings: [],
      backfill: { requested: true, success: true, status_code: 200 },
    })
    expect(out.tone).toBe("success")
    expect(out.warnings).toBeUndefined()
  })

  it("a queued load says it starts in about a minute, not that it is loading (#21)", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders"],
      backfill: { requested: true, success: true, status_code: 202, status: "queued", request_id: "r1" },
    })
    expect(out.tone).toBe("success")
    expect(out.detail).toMatch(/is queued and starts in about a minute, once the connector runs with the new table list/)
    expect(out.detail).toMatch(/Progress shows under Re-snapshot tables/)
    expect(out.detail).not.toMatch(/^Loading/)
  })

  it("a queued load on a paused pipeline starts after resume", () => {
    const out = describeTableEditResult({
      new_tables: ["public.orders"],
      paused: true,
      backfill: { requested: true, success: true, status: "queued" },
    })
    expect(out.detail).toMatch(/is queued and starts after you resume the pipeline/)
    expect(out.detail).not.toMatch(/about a minute/)
    expect(out.detail).not.toMatch(/New changes stream as they happen/)
  })
})

describe("describeBackfillAccepted (the Re-snapshot toast)", () => {
  it("a queued answer says it starts in about a minute and where to watch it", () => {
    const msg = describeBackfillAccepted(
      { status: "queued", request_id: "r1", snapshot_mode: "incremental", data_collections: ["a.b", "a.c"] },
      { tables: 5, mode: "blocking" },
    )
    expect(msg).toMatch(/^Re-snapshot of 2 tables queued \(incremental\)\. It starts in about a minute/)
    expect(msg).toMatch(/Snapshot loads/)
    expect(msg).not.toMatch(/signalled/)
    expect(msg).not.toMatch(/pauses/)
  })

  it("a queued blocking re-snapshot also says streaming pauses", () => {
    const msg = describeBackfillAccepted({ status: "queued" }, { tables: 1, mode: "blocking" })
    expect(msg).toMatch(/^Re-snapshot of 1 table queued \(blocking\)/)
    expect(msg).toMatch(/streaming for this pipeline pauses/)
  })

  it("control: an answer without `queued` (MySQL signal table) was sent at once", () => {
    const msg = describeBackfillAccepted({ snapshot_mode: "incremental" }, { tables: 1, mode: "incremental" })
    expect(msg).toMatch(/^Re-snapshot signalled for 1 table \(incremental\)/)
    expect(msg).not.toMatch(/queued/)
  })
})

describe("object storage in the Re-snapshot card (capability)", () => {
  const supported = { state: "supported" as const, channel: "kafka", modes: ["blocking" as const], defaultMode: "blocking" as const }

  it("blocking-only is explained for object storage, not as MongoDB", () => {
    expect(blockingOnlyDetail({ ...supported, objectStorage: true })).toBe(OBJECT_STORAGE_BLOCKING_DETAIL)
    expect(blockingOnlyDetail(supported)).toBe(BLOCKING_ONLY_DETAIL)
  })

  it("control: a connector that takes both modes has no blocking-only note", () => {
    expect(
      blockingOnlyDetail({ ...supported, modes: ["incremental", "blocking"], defaultMode: "incremental" })
    ).toBeNull()
    expect(blockingOnlyDetail({ state: "unknown" })).toBeNull()
  })

  it("cleans_folder: the folder is emptied and written again", () => {
    const note = resnapshotDestinationNote({ ...supported, objectStorage: true, cleansFolder: true })
    expect(note.tone).toBe("info")
    expect(note.text).toMatch(/emptied and then written again/)
  })

  it("object storage without cleans_folder warns about a second copy", () => {
    const note = resnapshotDestinationNote({ ...supported, objectStorage: true, cleansFolder: false })
    expect(note.tone).toBe("warning")
    expect(note.text).toMatch(/older folder layout/)
    expect(note.text).toMatch(/second copy of every row/)
  })

  it("control: anything else (or an older orchestrator) keeps the general note", () => {
    for (const cap of [supported, { state: "unknown" as const }]) {
      const note = resnapshotDestinationNote(cap)
      expect(note.tone).toBe("warning")
      expect(note.text).toMatch(/upserts on the primary key converges/)
    }
  })
})

// #21: GET /pipelines/:id/cdc/snapshot-requests, in plain words.
describe("snapshot requests", () => {
  const base: SnapshotRequest = {
    id: "r1",
    mode: "blocking",
    tables: ["public.orders"],
    source: "resnapshot",
    status: "queued",
    attempts: 0,
    completed_tables: [],
    requested_at: "2026-09-24T10:00:00Z",
  }
  const label = (over: Partial<SnapshotRequest>) => describeSnapshotRequest({ ...base, ...over })

  it("says each state in plain words", () => {
    expect(label({ status: "queued", source: "table_edit" })).toEqual({
      tone: "progress",
      label: "Waiting for the connector to restart with the new tables…",
    })
    expect(label({ status: "queued", source: "resnapshot" }).label).toBe(
      "Queued — waiting for the connector to pick it up…"
    )
    expect(label({ status: "sent" })).toEqual({ tone: "progress", label: "Signal sent — waiting for the first rows…" })
    expect(label({ status: "started" })).toEqual({ tone: "progress", label: "Loading rows…" })
    expect(label({ status: "completed" })).toEqual({ tone: "done", label: "Done" })
    expect(label({ status: "unconfirmed", last_error: "no rows after 3 signals" })).toEqual({
      tone: "warning",
      label: "Not confirmed — no finished snapshot was seen",
      detail: "no rows after 3 signals",
    })
    expect(label({ status: "failed", last_error: "connector not found" })).toEqual({
      tone: "error",
      label: "Failed: connector not found",
    })
    expect(label({ status: "failed" }).label).toBe("Failed")
  })

  it("counts finished tables only while several load", () => {
    expect(label({ status: "started", tables: ["a", "b", "c"], completed_tables: ["a"] }).label).toBe(
      "Loading rows… (1 of 3 tables done)"
    )
    expect(label({ status: "started", tables: ["a", "b"], completed_tables: [] }).label).toBe("Loading rows…")
  })

  it("keeps polling only while a request is still moving", () => {
    for (const s of ["queued", "sent", "started"] as const) expect(snapshotRequestIsActive({ status: s })).toBe(true)
    for (const s of ["completed", "unconfirmed", "failed"] as const) expect(snapshotRequestIsActive({ status: s })).toBe(false)
  })

  it("names where a load came from", () => {
    expect(describeSnapshotRequestSource("resnapshot")).toBe("Re-snapshot")
    expect(describeSnapshotRequestSource("table_edit")).toBe("Edit tables")
    expect(describeSnapshotRequestSource("auto_pickup")).toBe("New table picked up")
    expect(describeSnapshotRequestSource("initial")).toBe("Initial load")
  })

  it("reads the list, and tells a missing route apart from a failed read", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ requests: [{ ...base, status: "sent", attempts: 2 }, { status: "queued" }] }),
    })
    const ok = await fetchSnapshotRequests("p1")
    expect(String(mockFetch.mock.calls[0][0])).toContain("/pipelines/p1/cdc/snapshot-requests")
    // The row without an id is dropped rather than rendered as a blank load.
    expect(ok).toEqual({ state: "ok", requests: [expect.objectContaining({ id: "r1", status: "sent", attempts: 2 })] })

    mockFetch.mockResolvedValueOnce({ ok: false, status: 404, json: async () => ({}) })
    expect(await fetchSnapshotRequests("p1")).toEqual({ state: "unavailable" })

    mockFetch.mockResolvedValueOnce({ ok: false, status: 500, json: async () => ({}) })
    expect(await fetchSnapshotRequests("p1")).toEqual({ state: "error" })

    mockFetch.mockRejectedValueOnce(new Error("offline"))
    expect(await fetchSnapshotRequests("p1")).toEqual({ state: "error" })
  })
})
