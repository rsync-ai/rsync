import { describe, it, expect, vi, beforeEach, type Mock } from "vitest"
import { fireEvent, render, screen, within } from "@testing-library/react"
import "@testing-library/jest-dom"

// CDC "Edit tables": adding a table with no primary key to a pipeline that
// writes to a database is refused by the orchestrator with
// {"error":"missing_primary_key","message":…,"tables":[…]}. The dialog showed
// the bare code "missing_primary_key" (prod, 2026-09-25): the error extractor
// prefers `error` over `message`, and the table names were dropped. It now
// names the tables and says the key goes in the source.
//
// The save runs through the real updatePipelineCDCTables, so the path from the
// HTTP body to the banner is the one the monitoring panel uses.

const json = (status: number, data: unknown) => ({ ok: status >= 200 && status < 300, status, json: async () => data })

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(),
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { updatePipelineCDCTables } from "@/lib/api/pipelines"
import { explainTableEditError, MISSING_PRIMARY_KEY_EDIT_DETAIL, TableEditError } from "@/lib/pipeline/cdcBackfill"
import { PipelineTableSelector, type AvailableTable } from "../PipelineTableSelector"

const tables: AvailableTable[] = [
  { name: "orders", schema: "public", row_count: 10, columns: 3 },
  { name: "items", schema: "public", row_count: 5, columns: 2 },
  { name: "audit_log", schema: "public", row_count: 7, columns: 4 },
]

// cdc.go missingPrimaryKeyMessage, as the orchestrator sends it.
const SERVER_MESSAGE =
  "CDC to a database destination (PostgreSQL, MySQL or MongoDB) needs a PRIMARY KEY on every table: the destination upserts and deletes on it. Add a PRIMARY KEY or remove these tables."

function mockSaveResponse(status: number, body: unknown) {
  ;(authFetch as Mock).mockImplementation(async (url: string) =>
    String(url).includes("/cdc/tables") ? json(status, body) : json(200, {}),
  )
}

function renderDialog(onClose = vi.fn()) {
  render(
    <PipelineTableSelector
      isOpen
      onClose={onClose}
      pipelineId="pl_cdc"
      availableTables={tables}
      suggestedTables={[]}
      title="Edit CDC tables"
      showCdcBackfillToggle
      initialSelectedTables={["public.items"]}
      cdcBackfillNewTables={false}
      onCdcBackfillNewTablesChange={() => {}}
      onTablesSelected={async (picked) => {
        await updatePipelineCDCTables("pl_cdc", picked)
      }}
    />,
  )
  return onClose
}

const tableBox = (key: string) => {
  const el = document.getElementById(`tbl-${key}`)
  if (!el) throw new Error(`no checkbox for ${key}`)
  return el
}

describe("Edit CDC tables: a table with no primary key", () => {
  beforeEach(() => {
    ;(authFetch as Mock).mockReset()
  })

  it("names the keyless tables and says the key goes in the source", async () => {
    mockSaveResponse(400, {
      error: "missing_primary_key",
      message: SERVER_MESSAGE,
      tables: ["public.orders", "public.audit_log"],
    })
    const onClose = renderDialog()

    fireEvent.click(tableBox("public.orders"))
    fireEvent.click(tableBox("public.audit_log"))
    fireEvent.click(screen.getByRole("button", { name: "Sync 3 tables" }))

    const alert = await screen.findByRole("alert")
    expect(within(alert).getByText("These tables have no primary key")).toBeInTheDocument()
    expect(within(alert).getByText("public.orders")).toBeInTheDocument()
    expect(within(alert).getByText("public.audit_log")).toBeInTheDocument()
    expect(within(alert).getByText(MISSING_PRIMARY_KEY_EDIT_DETAIL)).toBeInTheDocument()
    expect(alert).toHaveTextContent("in the source database")
    expect(alert).not.toHaveTextContent("missing_primary_key")
    // The dialog stays open so the tables can be unticked.
    expect(onClose).not.toHaveBeenCalled()
  })

  it("control: any other refusal keeps the plain message", async () => {
    mockSaveResponse(502, { error: "Kafka Connect is unreachable, so the CDC table list was not changed" })
    const onClose = renderDialog()

    fireEvent.click(tableBox("public.orders"))
    fireEvent.click(screen.getByRole("button", { name: "Sync 2 tables" }))

    const alert = await screen.findByRole("alert")
    expect(alert).toHaveTextContent("Kafka Connect is unreachable, so the CDC table list was not changed")
    expect(alert).not.toHaveTextContent("no primary key")
    expect(onClose).not.toHaveBeenCalled()
  })
})

describe("explainTableEditError", () => {
  it("maps the primary-key refusal and keeps its table list", () => {
    const err = explainTableEditError({ error: "missing_primary_key", message: SERVER_MESSAGE, tables: [" public.orders ", ""] })
    expect(err).toBeInstanceOf(TableEditError)
    expect(err!.tables).toEqual(["public.orders"])
    // A caller that prints only `message` still gets the table names.
    expect(err!.message).toBe(`These tables have no primary key: public.orders. ${MISSING_PRIMARY_KEY_EDIT_DETAIL}`)
  })

  it("still reads without a table list", () => {
    const err = explainTableEditError({ error: "missing_primary_key" })
    expect(err!.tables).toEqual([])
    expect(err!.message).toBe(`These tables have no primary key. ${MISSING_PRIMARY_KEY_EDIT_DETAIL}`)
  })

  it("control: other codes, and no body, are left to the generic handling", () => {
    expect(explainTableEditError({ error: "cdc_pk_validation_unsupported", message: "x" })).toBeNull()
    expect(explainTableEditError({ error: "Failed to update CDC tables" })).toBeNull()
    expect(explainTableEditError(null)).toBeNull()
  })
})
