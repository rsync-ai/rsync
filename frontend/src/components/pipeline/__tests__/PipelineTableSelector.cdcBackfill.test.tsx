import { describe, it, expect, vi, beforeEach, type Mock } from "vitest"
import { render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

// The CDC "Edit tables" dialog used to offer "Backfill existing rows for newly
// added tables" pre-ticked and "Recommended" on every pipeline — including the
// ones whose connector could never run it — and never said how it differs from
// Re-snapshot tables. It now says what it does, and when the capability check
// says backfill cannot run, it shows that reason instead of the option.

const json = (status: number, data: unknown) => ({ ok: status >= 200 && status < 300, status, json: async () => data })

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(),
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { PipelineTableSelector, type AvailableTable } from "../PipelineTableSelector"

const tables: AvailableTable[] = [{ name: "orders", schema: "public", row_count: 10, columns: 3 }]

function renderSelector(props: Record<string, unknown>) {
  return render(
    <PipelineTableSelector
      isOpen
      onClose={() => {}}
      pipelineId="pl_cdc"
      availableTables={tables}
      suggestedTables={[]}
      title="Edit CDC tables"
      {...props}
    />
  )
}

describe("PipelineTableSelector — CDC table edit", () => {
  beforeEach(() => {
    ;(authFetch as Mock).mockReset()
    ;(authFetch as Mock).mockResolvedValue(json(200, {}))
  })

  it("says what the dialog does and points to Re-snapshot for tables already streaming", async () => {
    renderSelector({ showCdcBackfillToggle: true })
    const help = await screen.findByTestId("cdc-edit-tables-help")
    expect(help).toHaveTextContent("Choose which tables this pipeline streams")
    expect(help).toHaveTextContent("Tables that are already streaming are not reloaded")
    expect(help).toHaveTextContent("Re-snapshot tables")
  })

  it("offers the backfill, ticked, when nothing says it cannot run", async () => {
    renderSelector({ showCdcBackfillToggle: true, cdcBackfillNewTables: true })
    const box = await screen.findByRole("checkbox", { name: /Also load the existing rows of the tables you add/ })
    expect(box).toBeChecked()
    expect(screen.queryByText(/cannot be loaded/)).not.toBeInTheDocument()
  })

  it("says, next to the option, that a MongoDB backfill pauses streaming", async () => {
    renderSelector({ showCdcBackfillToggle: true, cdcBackfillNewTables: true, cdcBackfillBlockingOnly: true })
    const note = await screen.findByTestId("cdc-backfill-blocking-note")
    expect(note).toHaveTextContent("blocking snapshot")
    expect(note).toHaveTextContent("change streaming for the whole pipeline pauses until the added collections are read")
    expect(note).toHaveTextContent("Nothing is written to your database")
    // Still offered: blocking is a cost, not a refusal.
    expect(screen.getByRole("checkbox", { name: /Also load the existing rows/ })).toBeChecked()
  })

  it("control: no pause note for a connector that loads rows while streaming", async () => {
    renderSelector({ showCdcBackfillToggle: true, cdcBackfillNewTables: true })
    await screen.findByRole("checkbox", { name: /Also load the existing rows/ })
    expect(screen.queryByTestId("cdc-backfill-blocking-note")).not.toBeInTheDocument()
  })

  it("replaces the option with the reason when the connector cannot backfill", async () => {
    renderSelector({
      showCdcBackfillToggle: true,
      cdcBackfillNewTables: true,
      cdcBackfillUnavailableDetail: "This pipeline's CDC connector has no signal channel.",
    })
    const help = await screen.findByTestId("cdc-edit-tables-help")
    expect(help).toHaveTextContent("Existing rows of the tables you add cannot be loaded")
    expect(help).toHaveTextContent("This pipeline's CDC connector has no signal channel.")
    expect(help).toHaveTextContent("You can still add tables")
    // No pre-ticked "Recommended" box that the server would refuse.
    expect(screen.queryByRole("checkbox", { name: /Also load the existing rows/ })).not.toBeInTheDocument()
    expect(help).not.toHaveTextContent("Recommended")
  })

  it("shows none of it outside a CDC table edit", async () => {
    renderSelector({ showCdcBackfillToggle: false })
    await screen.findByText("Edit CDC tables")
    expect(screen.queryByTestId("cdc-edit-tables-help")).not.toBeInTheDocument()
  })
})
