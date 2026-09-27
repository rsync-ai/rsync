import { describe, it, expect, vi, beforeEach, type Mock } from "vitest"
import { fireEvent, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"
import { useState } from "react"

// CDC "Edit tables" after the bug sweep.
//
//  #5   A table removed earlier and now added back missed every change made
//       while it was out. Loading its rows becomes the default for it, and
//       adding it back WITHOUT loading (unticked, or not possible) is warned about.
//  #17  On a paused pipeline, "tables you add start streaming now" is false:
//       nothing streams until it is resumed.
//
// Every "shows X" test has the control that does not.

const json = (status: number, data: unknown) => ({ ok: status >= 200 && status < 300, status, json: async () => data })

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(),
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { PipelineTableSelector, type AvailableTable } from "../PipelineTableSelector"

const tables: AvailableTable[] = [
  { name: "orders", schema: "public", row_count: 10, columns: 3 },
  { name: "items", schema: "public", row_count: 5, columns: 2 },
]

/** The parent owns the backfill flag, as the monitoring panel does. */
function Harness(props: Record<string, unknown> & { startLoading?: boolean }) {
  const { startLoading = false, ...rest } = props
  const [loading, setLoading] = useState(startLoading)
  return (
    <PipelineTableSelector
      isOpen
      onClose={() => {}}
      pipelineId="pl_cdc"
      availableTables={tables}
      suggestedTables={[]}
      title="Edit CDC tables"
      showCdcBackfillToggle
      initialSelectedTables={["public.items"]}
      cdcBackfillNewTables={loading}
      onCdcBackfillNewTablesChange={setLoading}
      {...rest}
    />
  )
}

const tableBox = (key: string) => {
  const el = document.getElementById(`tbl-${key}`)
  if (!el) throw new Error(`no checkbox for ${key}`)
  return el
}
const loadBox = () => screen.getByRole("checkbox", { name: /Also load the existing rows of the tables you add/ })
const warning = () => screen.queryByTestId("cdc-readded-without-load-warning")

describe("#5 re-adding a table that was removed before", () => {
  beforeEach(() => {
    ;(authFetch as Mock).mockReset()
    ;(authFetch as Mock).mockResolvedValue(json(200, {}))
  })

  it("turns loading on when a previously removed table is added back", async () => {
    render(<Harness previouslyLoadedTables={["public.orders"]} />)
    await screen.findByTestId("cdc-edit-tables-help")
    expect(loadBox()).not.toBeChecked()

    fireEvent.click(tableBox("public.orders"))

    expect(loadBox()).toBeChecked()
    // Loading is on, so there is nothing to warn about.
    expect(warning()).not.toBeInTheDocument()
  })

  it("control: adding a table this pipeline never loaded leaves the option as it was", async () => {
    render(<Harness previouslyLoadedTables={["public.customers"]} />)
    await screen.findByTestId("cdc-edit-tables-help")

    fireEvent.click(tableBox("public.orders"))

    expect(loadBox()).not.toBeChecked()
    expect(warning()).not.toBeInTheDocument()
  })

  it("control: a table still in the saved selection is not being re-added", async () => {
    // items is selected when the dialog opens: keeping it is not adding it back.
    render(<Harness previouslyLoadedTables={["public.items"]} />)
    await screen.findByTestId("cdc-edit-tables-help")
    expect(loadBox()).not.toBeChecked()
    expect(warning()).not.toBeInTheDocument()
  })

  it("warns, and does not fight the user, when loading is turned off for a re-added table", async () => {
    render(<Harness previouslyLoadedTables={["public.orders"]} />)
    await screen.findByTestId("cdc-edit-tables-help")
    fireEvent.click(tableBox("public.orders"))
    expect(loadBox()).toBeChecked()

    fireEvent.click(loadBox())

    expect(loadBox()).not.toBeChecked()
    const w = warning()
    expect(w).toBeInTheDocument()
    expect(w).toHaveTextContent("public.orders was removed from this pipeline before.")
    expect(w).toHaveTextContent("The changes made while the table was removed are not streamed")
    expect(w).toHaveTextContent("without loading, those rows stay missing or stale at the destination")
  })

  it("counts several re-added tables", async () => {
    render(<Harness initialSelectedTables={[]} previouslyLoadedTables={["public.orders", "public.items"]} startLoading />)
    await screen.findByTestId("cdc-edit-tables-help")
    fireEvent.click(loadBox()) // the user's choice, before any table is added
    fireEvent.click(tableBox("public.orders"))
    fireEvent.click(tableBox("public.items"))

    expect(loadBox()).not.toBeChecked()
    expect(warning()).toHaveTextContent("2 of the tables you add were removed from this pipeline before.")
  })

  it("warns when the connector cannot load rows at all", async () => {
    render(
      <Harness
        previouslyLoadedTables={["public.orders"]}
        cdcBackfillUnavailableDetail="This pipeline's CDC connector has no signal channel."
      />
    )
    await screen.findByTestId("cdc-edit-tables-help")
    fireEvent.click(tableBox("public.orders"))

    expect(screen.queryByRole("checkbox", { name: /Also load the existing rows/ })).not.toBeInTheDocument()
    expect(warning()).toHaveTextContent("public.orders was removed from this pipeline before.")
  })
})

describe("#17 paused pipeline", () => {
  beforeEach(() => {
    ;(authFetch as Mock).mockReset()
    ;(authFetch as Mock).mockResolvedValue(json(200, {}))
  })

  it("says added tables start streaming when the pipeline is resumed", async () => {
    render(<Harness pipelineStatus="paused" />)
    const help = await screen.findByTestId("cdc-edit-tables-help")
    expect(help).toHaveTextContent("The pipeline is paused: tables you add start streaming when you resume it")
    expect(help).not.toHaveTextContent("Tables you add start streaming now")
  })

  it("says so in the cannot-load note too", async () => {
    render(<Harness pipelineStatus="paused" cdcBackfillUnavailableDetail="No signal channel." />)
    const help = await screen.findByTestId("cdc-edit-tables-help")
    expect(help).toHaveTextContent("they will stream every change made once you resume the pipeline")
    expect(help).not.toHaveTextContent("from now on")
  })

  it("control: a running pipeline's added tables start streaming now", async () => {
    render(<Harness pipelineStatus="running" cdcBackfillUnavailableDetail="No signal channel." />)
    const help = await screen.findByTestId("cdc-edit-tables-help")
    expect(help).toHaveTextContent("Tables you add start streaming now")
    expect(help).toHaveTextContent("they will stream every change made from now on")
    expect(help).not.toHaveTextContent("paused")
  })
})
