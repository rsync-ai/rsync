/**
 * The "Re-snapshot tables" card (POST /pipelines/:id/cdc/backfill).
 *
 * The behaviours pinned here are the ones that make the card safe to click:
 * batch-mode rows are never offered (a Debezium signal cannot address them),
 * the re-emission warning is on screen BEFORE the button rather than in the
 * toast after it, and a pipeline built without a signal channel is told so up
 * front — before anyone picks tables — instead of after a click that could only
 * fail.
 *
 * Three defects seen on a live Postgres→MongoDB pipeline are guarded below:
 * the card offered a live button on a connector with no signal channel; it
 * listed 6 of 9 tables (no `mode=cdc`, so tables still waiting for their first
 * event were never offered, and no paging past the endpoint's 50-row default);
 * and a long list had no way to find one table. Each "hides X" test has a
 * control that shows X, so a card that never renders the button cannot pass.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

let mockRole = "member"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
}))

const toastError = vi.fn()
const toastSuccess = vi.fn()
vi.mock("sonner", () => ({
  toast: {
    error: (...a: unknown[]) => toastError(...a),
    success: (...a: unknown[]) => toastSuccess(...a),
  },
}))

vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: mockRole,
    isLoading: false,
    error: false,
    activeWorkspace: null,
    can: () => false,
    // Delegate to the real ladder so this test can't drift from roles.ts.
    meets: (min: string) => realMeetsRole(mockRole, min as never),
  }),
}))

import { meetsRole as realMeetsRole } from "@/lib/workspace/roles"
import { emitPipelineRefresh } from "@/lib/events/pipelineRefresh"
import { CdcResnapshotCard, cdcTableNames, loadAllCdcTableRows } from "@/components/pipeline/CdcResnapshotCard"

type Row = { qualified_name?: string; table_name?: string; mode?: string; status?: string }

const NOT_AVAILABLE = /re-snapshot is not available for this pipeline/i

function json(status: number, body: unknown) {
  return { ok: status < 400, status, json: async () => body }
}

/** table-stats answers with `rows`; the backfill POST answers with `post`. */
function serve(rows: Row[], post: { status: number; body: unknown } = { status: 200, body: {} }) {
  authFetch.mockImplementation(async (url: string, init?: RequestInit) => {
    if (init?.method === "POST") return json(post.status, post.body)
    return json(200, { tables: rows })
  })
}

function postBodies() {
  return authFetch.mock.calls
    .filter(([, init]) => (init as RequestInit | undefined)?.method === "POST")
    .map(([, init]) => JSON.parse(String((init as RequestInit).body)))
}

beforeEach(() => {
  mockRole = "member"
  authFetch.mockReset()
  toastError.mockReset()
  toastSuccess.mockReset()
})
afterEach(() => cleanup())

describe("cdcTableNames", () => {
  it("keeps only CDC rows, prefers the source-side qualified name, dedupes and sorts", () => {
    expect(
      cdcTableNames([
        { qualified_name: "shop.orders", table_name: "orders", mode: "cdc" },
        { table_name: "customers", mode: "CDC" },
        { qualified_name: "shop.archive", mode: "batch" },
        { qualified_name: "shop.orders", mode: "cdc" },
      ])
    ).toEqual(["customers", "shop.orders"])
  })

  it("is empty, not throwing, on a missing list", () => {
    expect(cdcTableNames(null)).toEqual([])
    expect(cdcTableNames(undefined)).toEqual([])
  })

  // A removed table keeps its stats row (its data is still at the
  // destination), but the connector no longer captures it: a re-read cannot
  // reach it. The waiting_for_data row is the control — any other status stays.
  it("leaves out removed tables and keeps every other status", () => {
    expect(
      cdcTableNames([
        { qualified_name: "shop.orders", mode: "cdc", status: "running" },
        { qualified_name: "shop.old_orders", mode: "cdc", status: "removed" },
        { qualified_name: "shop.new_table", mode: "cdc", status: "waiting_for_data" },
      ])
    ).toEqual(["shop.new_table", "shop.orders"])
  })
})

describe("CdcResnapshotCard", () => {
  it("offers the CDC tables and not the batch ones", async () => {
    serve([
      { qualified_name: "shop.orders", mode: "cdc" },
      { qualified_name: "shop.nightly_dump", mode: "batch" },
    ])
    render(<CdcResnapshotCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByLabelText("Re-snapshot shop.orders")).toBeInTheDocument())
    expect(screen.queryByLabelText("Re-snapshot shop.nightly_dump")).not.toBeInTheDocument()
  })

  it("warns that every row is re-emitted before anything is clicked", async () => {
    serve([{ qualified_name: "shop.orders", mode: "cdc" }])
    render(<CdcResnapshotCard pipelineId="p1" />)

    await waitFor(() =>
      expect(screen.getByText(/re-emits every row of the selected tables/)).toBeInTheDocument()
    )
    // Said up front: an append-only destination gains a second copy.
    expect(screen.getByText(/append-only destination.*gains a second copy/)).toBeInTheDocument()
  })

  it("keeps the button dead until a table is picked", async () => {
    serve([{ qualified_name: "shop.orders", mode: "cdc" }])
    render(<CdcResnapshotCard pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Re-snapshot shop.orders")).toBeInTheDocument())

    expect(screen.getByRole("button", { name: /^Re-snapshot$/ })).toBeDisabled()

    await userEvent.setup().click(screen.getByLabelText("Re-snapshot shop.orders"))
    expect(screen.getByRole("button", { name: /Re-snapshot 1 table/ })).toBeEnabled()
  })

  it("posts the chosen tables and mode, and clears the selection on success", async () => {
    serve(
      [
        { qualified_name: "shop.orders", mode: "cdc" },
        { qualified_name: "shop.customers", mode: "cdc" },
      ],
      { status: 200, body: { data_collections: ["shop.orders"], snapshot_mode: "blocking" } }
    )
    render(<CdcResnapshotCard pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Re-snapshot shop.orders")).toBeInTheDocument())

    const user = userEvent.setup()
    await user.click(screen.getByLabelText("Re-snapshot shop.orders"))
    await user.click(screen.getByRole("radio", { name: /blocking/i }))
    await user.click(screen.getByRole("button", { name: /Re-snapshot 1 table/ }))

    await waitFor(() => expect(postBodies()).toHaveLength(1))
    expect(postBodies()[0]).toEqual({ tables: ["shop.orders"], mode: "blocking" })
    const post = authFetch.mock.calls.find(([, init]) => (init as RequestInit | undefined)?.method === "POST")
    expect(String(post?.[0])).toContain("/pipelines/p1/cdc/backfill")

    await waitFor(() => expect(screen.getByRole("button", { name: /^Re-snapshot$/ })).toBeDisabled())
    // No `status: "queued"` in the answer (the MySQL channel): sent at once.
    expect(String(toastSuccess.mock.calls[0][0])).toContain("asynchronously")

    // #15: a successful re-snapshot announces itself on the refresh bus, and
    // the card's own listener re-reads the table list after the POST.
    const postIndex = authFetch.mock.calls.findIndex(([, init]) => (init as RequestInit | undefined)?.method === "POST")
    await waitFor(() =>
      expect(
        authFetch.mock.calls.slice(postIndex + 1).some(([u]) => String(u).includes("/pipelines/p1/table-stats"))
      ).toBe(true)
    )
  })

  // The capability check answers "unknown" here (serve() has no GET route for
  // it), so the only way to learn is the POST — and then the card stops
  // offering what cannot work, rather than leaving a red error next to a live
  // button that will fail the same way again.
  it("tells the operator a retry will not help when the pipeline has no signal channel", async () => {
    serve([{ qualified_name: "shop.orders", mode: "cdc" }], {
      status: 400,
      // cdc.go:824 — `error` carries the machine code, `message` the sentence.
      body: {
        error: "cdc_backfill_not_supported",
        message: "CDC backfill needs either a Kafka signal channel or a MySQL source signal table",
      },
    })
    render(<CdcResnapshotCard pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Re-snapshot shop.orders")).toBeInTheDocument())

    const user = userEvent.setup()
    await user.click(screen.getByLabelText("Re-snapshot shop.orders"))
    await user.click(screen.getByRole("button", { name: /Re-snapshot 1 table/ }))

    await waitFor(() => expect(screen.getByText(NOT_AVAILABLE)).toBeInTheDocument())
    expect(toastError).toHaveBeenCalledWith("This pipeline cannot be re-snapshotted")
    expect(screen.getByText(/Retrying will not change this/)).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /^Re-snapshot/ })).not.toBeInTheDocument()
  })

  it("renders a failed table read as an error, not as 'no CDC tables'", async () => {
    authFetch.mockResolvedValue(json(500, {}))
    render(<CdcResnapshotCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent(/Could not load/))
    expect(screen.queryByText(/No CDC tables/)).not.toBeInTheDocument()
  })

  it("blocks a viewer and says which role is needed", async () => {
    mockRole = "viewer"
    serve([{ qualified_name: "shop.orders", mode: "cdc" }])
    render(<CdcResnapshotCard pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Re-snapshot shop.orders")).toBeInTheDocument())

    await userEvent.setup().click(screen.getByLabelText("Re-snapshot shop.orders"))
    expect(screen.getByRole("button", { name: /Re-snapshot 1 table/ })).toBeDisabled()
    expect(screen.getByText(/need at least Member in this workspace/)).toBeInTheDocument()
  })

  it("fails closed while the workspace role is still loading", async () => {
    // `role` is "" until WorkspaceContext resolves; meetsRole ranks that 0.
    mockRole = ""
    serve([{ qualified_name: "shop.orders", mode: "cdc" }])
    render(<CdcResnapshotCard pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Re-snapshot shop.orders")).toBeInTheDocument())

    await userEvent.setup().click(screen.getByLabelText("Re-snapshot shop.orders"))
    expect(screen.getByRole("button", { name: /Re-snapshot 1 table/ })).toBeDisabled()
  })
})

type Reply = { status: number; body: unknown }

/**
 * Route by URL + method: table-stats pages, the capability GET, the POST, and
 * the snapshot-requests GET (404 unless `requests` is given, like a gateway
 * older than the route). `rows` is the full server-side list; it is sliced by
 * limit/offset the way table_stats.go does.
 */
function serveRouted(opts: { rows: Row[]; capability: Reply; post?: Reply; requests?: () => Reply }) {
  authFetch.mockImplementation(async (url: string, init?: RequestInit) => {
    const u = new URL(url, "http://x")
    if (u.pathname.endsWith("/table-stats")) {
      const limit = Number(u.searchParams.get("limit") || 50)
      const offset = Number(u.searchParams.get("offset") || 0)
      return json(200, { tables: opts.rows.slice(offset, offset + limit), total: opts.rows.length })
    }
    if (u.pathname.endsWith("/cdc/backfill")) {
      const r = init?.method === "POST" ? (opts.post ?? { status: 200, body: {} }) : opts.capability
      return json(r.status, r.body)
    }
    if (u.pathname.endsWith("/cdc/snapshot-requests") && opts.requests) {
      const r = opts.requests()
      return json(r.status, r.body)
    }
    return json(404, {})
  })
}

const cdc = (name: string, extra: Partial<Row> = {}): Row => ({ qualified_name: name, mode: "cdc", ...extra })
const SUPPORTED: Reply = { status: 200, body: { supported: true, signal_channel: "kafka" } }
const UNSUPPORTED: Reply = {
  status: 200,
  body: { supported: false, error: "cdc_backfill_not_supported", message: "No signal channel on this connector." },
}
const NO_ROUTE: Reply = { status: 404, body: { error: "not found" } } // an older gateway
const runButton = () => screen.queryByRole("button", { name: /^Re-snapshot/ })
const tableStatsUrls = () =>
  authFetch.mock.calls.map(([u]) => new URL(String(u), "http://x")).filter((u) => u.pathname.endsWith("/table-stats"))
const requestReads = () =>
  authFetch.mock.calls.filter(([u]) => new URL(String(u), "http://x").pathname.endsWith("/cdc/snapshot-requests")).length

describe("CdcResnapshotCard — asks before offering", () => {
  it("says up front that a connector with no signal channel cannot be re-snapshotted, and offers no button", async () => {
    serveRouted({ rows: [cdc("shop.orders")], capability: UNSUPPORTED })
    render(<CdcResnapshotCard pipelineId="p1" />)

    expect(await screen.findByText(NOT_AVAILABLE)).toBeInTheDocument()
    expect(screen.getByText("No signal channel on this connector.")).toBeInTheDocument()
    expect(runButton()).not.toBeInTheDocument()
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument()
    expect(screen.queryByText(/snapshot mode/i)).not.toBeInTheDocument()
  })

  it("control: a supported connector gets the table list and the button", async () => {
    serveRouted({ rows: [cdc("shop.orders")], capability: SUPPORTED })
    render(<CdcResnapshotCard pipelineId="p1" />)

    expect(await screen.findByLabelText("Re-snapshot shop.orders")).toBeInTheDocument()
    expect(runButton()).toBeInTheDocument()
    expect(screen.queryByText(NOT_AVAILABLE)).not.toBeInTheDocument()
  })

  it("an unanswerable check (older gateway) leaves the card as it was — the button stays", async () => {
    serveRouted({ rows: [cdc("shop.orders")], capability: NO_ROUTE })
    render(<CdcResnapshotCard pipelineId="p1" />)

    expect(await screen.findByLabelText("Re-snapshot shop.orders")).toBeInTheDocument()
    expect(runButton()).toBeInTheDocument()
    expect(screen.queryByText(NOT_AVAILABLE)).not.toBeInTheDocument()
  })

  it("control: any other refusal stays an error next to a button you can try again", async () => {
    serveRouted({
      rows: [cdc("shop.orders")],
      capability: SUPPORTED,
      post: { status: 400, body: { error: "missing_primary_key", tables: ["shop.orders"] } },
    })
    render(<CdcResnapshotCard pipelineId="p1" />)

    const user = userEvent.setup()
    await user.click(await screen.findByLabelText("Re-snapshot shop.orders"))
    await user.click(screen.getByRole("button", { name: /Re-snapshot 1 table/ }))

    expect(await screen.findByRole("alert")).toHaveTextContent(/no primary key/i)
    expect(screen.queryByText(NOT_AVAILABLE)).not.toBeInTheDocument()
    expect(runButton()).toBeInTheDocument()
  })
})

// cdc.go backfillModes: a MongoDB connector accepts blocking only (an
// incremental snapshot would write watermark documents into the source).
const MONGO: Reply = {
  status: 200,
  body: {
    supported: true,
    signal_channel: "kafka",
    modes: ["blocking"],
    default_mode: "blocking",
    connector_class: "io.debezium.connector.mongodb.mongodbconnector",
  },
}
const PG_BOTH: Reply = {
  status: 200,
  body: { supported: true, signal_channel: "kafka", modes: ["incremental", "blocking"], default_mode: "incremental" },
}

describe("CdcResnapshotCard — MongoDB is blocking only", () => {
  it("shows the one mode and that streaming pauses, offers no incremental choice, and posts blocking", async () => {
    serveRouted({
      rows: [cdc("shop.orders")],
      capability: MONGO,
      post: { status: 200, body: { data_collections: ["shop.orders"], snapshot_mode: "blocking" } },
    })
    render(<CdcResnapshotCard pipelineId="p1" />)

    const note = await screen.findByTestId("cdc-resnapshot-blocking-only")
    expect(note).toHaveTextContent(/Snapshot mode: blocking — streaming pauses during the re-read/)
    expect(note).toHaveTextContent(/then resumes where it stopped/)
    expect(note).toHaveTextContent(/Nothing is written to your MongoDB database/)
    expect(screen.queryByRole("radio")).not.toBeInTheDocument()

    const user = userEvent.setup()
    await user.click(screen.getByLabelText("Re-snapshot shop.orders"))
    await user.click(screen.getByRole("button", { name: /Re-snapshot 1 table/ }))

    await waitFor(() => expect(postBodies()).toHaveLength(1))
    expect(postBodies()[0]).toEqual({ tables: ["shop.orders"], mode: "blocking" })
    expect(String(toastSuccess.mock.calls[0][0])).toMatch(/streaming for this pipeline pauses/)
  })

  it("control: a connector that takes both modes gets the choice, incremental first, and no pause note", async () => {
    serveRouted({ rows: [cdc("public.orders")], capability: PG_BOTH })
    render(<CdcResnapshotCard pipelineId="p1" />)

    await screen.findByLabelText("Re-snapshot public.orders")
    expect(screen.queryByTestId("cdc-resnapshot-blocking-only")).not.toBeInTheDocument()
    expect(screen.getByRole("radio", { name: /incremental/i })).toBeChecked()
    expect(screen.getByRole("radio", { name: /blocking/i })).not.toBeChecked()

    const user = userEvent.setup()
    await user.click(screen.getByLabelText("Re-snapshot public.orders"))
    await user.click(screen.getByRole("button", { name: /Re-snapshot 1 table/ }))
    await waitFor(() => expect(postBodies()).toHaveLength(1))
    expect(postBodies()[0]).toEqual({ tables: ["public.orders"], mode: "incremental" })
  })
})

describe("CdcResnapshotCard — which tables it offers", () => {
  it("asks for CDC mode, which is the branch that lists tables still waiting for data", async () => {
    serveRouted({
      rows: [cdc("shop.orders"), cdc("shop.new_table", { status: "waiting_for_data" })],
      capability: SUPPORTED,
    })
    render(<CdcResnapshotCard pipelineId="p1" />)

    expect(await screen.findByLabelText("Re-snapshot shop.new_table")).toBeInTheDocument()
    expect(tableStatsUrls()[0].searchParams.get("mode")).toBe("cdc")
  })

  it("control: one short page is one request", async () => {
    serveRouted({ rows: [cdc("shop.a"), cdc("shop.b")], capability: SUPPORTED })
    render(<CdcResnapshotCard pipelineId="p1" />)

    expect(await screen.findByText(/0 of 2 selected/)).toBeInTheDocument()
    expect(tableStatsUrls()).toHaveLength(1)
  })
})

// Paging is proven on the loader, not through a render: a card with 1000+
// checkboxes costs seconds in jsdom and tens of seconds under a loaded CI run.
describe("loadAllCdcTableRows", () => {
  const many = (n: number) => Array.from({ length: n }, (_, i) => cdc(`shop.t${String(i).padStart(4, "0")}`))

  it("pages past the API's page size instead of stopping at the first page", async () => {
    serveRouted({ rows: many(1002), capability: SUPPORTED })
    const out = await loadAllCdcTableRows("p1")

    expect(tableStatsUrls().map((u) => u.searchParams.get("offset"))).toEqual(["0", "1000"])
    expect("rows" in out && out.rows.length).toBe(1002)
    expect("rows" in out && out.rows[1001].qualified_name).toBe("shop.t1001")
  })

  it("control: a list of exactly one page stops on the total, without an empty second request", async () => {
    serveRouted({ rows: many(1000), capability: SUPPORTED })
    const out = await loadAllCdcTableRows("p1")

    expect(tableStatsUrls()).toHaveLength(1)
    expect("rows" in out && out.rows.length).toBe(1000)
  })

  it("stops at the page cap when the server never reports a total", async () => {
    authFetch.mockImplementation(async () => json(200, { tables: many(1000) }))
    await loadAllCdcTableRows("p1")

    expect(tableStatsUrls()).toHaveLength(20)
  })

  it("returns the status of a failed page instead of a partial list", async () => {
    let calls = 0
    authFetch.mockImplementation(async () => (++calls === 1 ? json(200, { tables: many(1000), total: 1500 }) : json(500, {})))

    expect(await loadAllCdcTableRows("p1")).toEqual({ status: 500 })
  })
})

describe("CdcResnapshotCard — long lists", () => {
  const twelve = [...Array.from({ length: 10 }, (_, i) => cdc(`shop.orders_${i}`)), cdc("shop.users"), cdc("shop.users_archive")]

  it("filters the list, and Select shown selects only what the filter shows", async () => {
    serveRouted({ rows: twelve, capability: SUPPORTED })
    render(<CdcResnapshotCard pipelineId="p1" />)

    const user = userEvent.setup()
    await user.type(await screen.findByLabelText("Filter tables"), "users")
    expect(screen.queryByLabelText("Re-snapshot shop.orders_0")).not.toBeInTheDocument()
    expect(screen.getByLabelText("Re-snapshot shop.users")).toBeInTheDocument()

    await user.click(screen.getByRole("button", { name: "Select shown" }))
    expect(screen.getByText(/2 of 12 selected/)).toBeInTheDocument()
    expect(runButton()).toHaveTextContent("Re-snapshot 2 tables")
  })

  it("control: with no filter, Select all selects every table", async () => {
    serveRouted({ rows: twelve, capability: SUPPORTED })
    render(<CdcResnapshotCard pipelineId="p1" />)

    await userEvent.setup().click(await screen.findByRole("button", { name: "Select all" }))
    expect(screen.getByText(/12 of 12 selected/)).toBeInTheDocument()
  })

  it("a short list gets no filter box", async () => {
    serveRouted({ rows: [cdc("shop.a"), cdc("shop.b")], capability: SUPPORTED })
    render(<CdcResnapshotCard pipelineId="p1" />)

    await screen.findByLabelText("Re-snapshot shop.a")
    expect(screen.queryByLabelText("Filter tables")).not.toBeInTheDocument()
  })
})

// Object storage (capability `object_storage`): an object store cannot upsert,
// so what a re-read does to the rows already there is said before the click —
// the folder is emptied first (layout v2, `cleans_folder`) or gains a second
// copy (the older layout). Blocking is the only mode offered.
const objectStore = (cleansFolder: boolean): Reply => ({
  status: 200,
  body: {
    supported: true,
    signal_channel: "kafka",
    modes: ["blocking"],
    default_mode: "blocking",
    destination_type: "gcs",
    object_storage: true,
    cleans_folder: cleansFolder,
  },
})

describe("CdcResnapshotCard — object storage", () => {
  it("says the table's folder is emptied and written again when the orchestrator cleans it", async () => {
    serveRouted({ rows: [cdc("public.orders")], capability: objectStore(true) })
    render(<CdcResnapshotCard pipelineId="p1" />)

    const note = await screen.findByTestId("cdc-resnapshot-destination-note")
    expect(note).toHaveAttribute("data-tone", "info")
    expect(note).toHaveTextContent(/folder at the destination is emptied and then written again/)
    expect(note).not.toHaveTextContent(/second copy/)
    // Blocking only, explained for object storage rather than for MongoDB.
    const blocking = screen.getByTestId("cdc-resnapshot-blocking-only")
    expect(blocking).toHaveTextContent(/write to object storage/)
    expect(blocking).not.toHaveTextContent(/MongoDB/)
    expect(screen.queryByRole("radio")).not.toBeInTheDocument()
  })

  it("warns about a second copy when the older folder layout cannot be emptied", async () => {
    serveRouted({ rows: [cdc("public.orders")], capability: objectStore(false) })
    render(<CdcResnapshotCard pipelineId="p1" />)

    const note = await screen.findByTestId("cdc-resnapshot-destination-note")
    expect(note).toHaveAttribute("data-tone", "warning")
    expect(note).toHaveTextContent(/gains a second copy of every row/)
  })

  it("control: a database destination keeps the general upsert / append note", async () => {
    serveRouted({ rows: [cdc("public.orders")], capability: PG_BOTH })
    render(<CdcResnapshotCard pipelineId="p1" />)

    const note = await screen.findByTestId("cdc-resnapshot-destination-note")
    expect(note).toHaveAttribute("data-tone", "warning")
    expect(note).toHaveTextContent(/upserts on the primary key converges/)
  })
})

type ReqStatus = "queued" | "sent" | "started" | "completed" | "unconfirmed" | "failed"
const req = (id: string, status: ReqStatus, extra: Record<string, unknown> = {}) => ({
  id,
  mode: "blocking",
  tables: ["public.orders"],
  source: "resnapshot",
  status,
  attempts: 1,
  completed_tables: [],
  requested_at: "2026-09-24T10:00:00Z",
  ...extra,
})
const requestsReply = (...list: ReturnType<typeof req>[]) => () => ({ status: 200, body: { requests: list } })

describe("CdcResnapshotCard — snapshot loads (#21)", () => {
  it("lists the latest loads with their state in plain words", async () => {
    serveRouted({
      rows: [cdc("public.orders")],
      capability: PG_BOTH,
      requests: requestsReply(
        req("r1", "queued", { source: "table_edit", tables: ["public.new_table"] }),
        req("r2", "sent"),
        req("r3", "started", { tables: ["public.a", "public.b"], completed_tables: ["public.a"] }),
        req("r4", "completed"),
        req("r5", "unconfirmed", { last_error: "no snapshot rows after 3 signals" }),
        req("r6", "failed", { last_error: "connector not found" })
      ),
    })
    render(<CdcResnapshotCard pipelineId="p1" />)

    const section = await screen.findByTestId("cdc-snapshot-loads")
    expect(section).toHaveTextContent("Snapshot loads")
    const items = screen.getAllByTestId("cdc-snapshot-load")
    expect(items.map((i) => i.getAttribute("data-status"))).toEqual([
      "queued",
      "sent",
      "started",
      "completed",
      "unconfirmed",
      "failed",
    ])
    expect(items[0]).toHaveTextContent("Edit tables")
    expect(items[0]).toHaveTextContent("public.new_table")
    expect(items[0]).toHaveTextContent("Waiting for the connector to restart with the new tables…")
    expect(items[1]).toHaveTextContent("Re-snapshot")
    expect(items[1]).toHaveTextContent("Signal sent — waiting for the first rows…")
    expect(items[2]).toHaveTextContent("Loading rows… (1 of 2 tables done)")
    expect(items[3]).toHaveTextContent("Done")
    expect(items[4]).toHaveTextContent("Not confirmed — no finished snapshot was seen")
    expect(items[4]).toHaveTextContent("no snapshot rows after 3 signals")
    expect(items[5]).toHaveTextContent("Failed: connector not found")
  })

  it("a gateway without the route shows no section, and the card stops asking", async () => {
    serveRouted({ rows: [cdc("public.orders")], capability: PG_BOTH, requests: () => ({ status: 404, body: {} }) })
    render(<CdcResnapshotCard pipelineId="p1" />)

    await screen.findByLabelText("Re-snapshot public.orders")
    await waitFor(() => expect(requestReads()).toBe(1))
    expect(screen.queryByTestId("cdc-snapshot-loads")).not.toBeInTheDocument()

    act(() => emitPipelineRefresh("p1"))
    await waitFor(() => expect(tableStatsUrls()).toHaveLength(2))
    expect(requestReads()).toBe(1)
  })

  it("says a queued re-snapshot will start in about a minute, not that it was signalled", async () => {
    let list: ReturnType<typeof req>[] = []
    serveRouted({
      rows: [cdc("public.orders")],
      capability: PG_BOTH,
      post: {
        status: 202,
        body: {
          success: true,
          status: "queued",
          request_id: "r9",
          snapshot_mode: "incremental",
          data_collections: ["public.orders"],
          signal_channel: "kafka",
        },
      },
      requests: () => ({ status: 200, body: { requests: list } }),
    })
    render(<CdcResnapshotCard pipelineId="p1" />)

    const user = userEvent.setup()
    await user.click(await screen.findByLabelText("Re-snapshot public.orders"))
    list = [req("r9", "queued", { mode: "incremental" })]
    await user.click(screen.getByRole("button", { name: /Re-snapshot 1 table/ }))

    await waitFor(() => expect(toastSuccess).toHaveBeenCalledTimes(1))
    const msg = String(toastSuccess.mock.calls[0][0])
    expect(msg).toMatch(/queued/)
    expect(msg).toMatch(/about a minute/)
    expect(msg).not.toMatch(/signalled/)
    // The refresh after the POST brings the queued load onto the card.
    expect(await screen.findByText("Queued — waiting for the connector to pick it up…")).toBeInTheDocument()
  })
})

describe("CdcResnapshotCard — refresh and polling", () => {
  afterEach(() => vi.useRealTimers())

  it("re-reads its tables and loads when this pipeline's refresh event fires (#15)", async () => {
    serveRouted({ rows: [cdc("public.orders")], capability: PG_BOTH, requests: requestsReply(req("r1", "completed")) })
    render(<CdcResnapshotCard pipelineId="p1" />)
    await screen.findByTestId("cdc-snapshot-loads")
    await waitFor(() => expect(requestReads()).toBe(1))
    expect(tableStatsUrls()).toHaveLength(1)

    // Control: another pipeline's event changes nothing here.
    act(() => emitPipelineRefresh("p2"))
    expect(tableStatsUrls()).toHaveLength(1)
    expect(requestReads()).toBe(1)

    act(() => emitPipelineRefresh("p1"))
    await waitFor(() => expect(tableStatsUrls()).toHaveLength(2))
    await waitFor(() => expect(requestReads()).toBe(2))
  })

  it("polls every 5 s while a load is still moving, and stops once none is", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    let status: ReqStatus = "started"
    serveRouted({
      rows: [cdc("public.orders")],
      capability: PG_BOTH,
      requests: () => ({ status: 200, body: { requests: [req("r1", status)] } }),
    })
    render(<CdcResnapshotCard pipelineId="p1" />)
    await screen.findByText("Loading rows…")
    const first = requestReads()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000)
    })
    await waitFor(() => expect(requestReads()).toBe(first + 1))

    status = "completed"
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000)
    })
    await screen.findByText("Done")
    const settled = requestReads()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(15000)
    })
    expect(requestReads()).toBe(settled)
  })
})
