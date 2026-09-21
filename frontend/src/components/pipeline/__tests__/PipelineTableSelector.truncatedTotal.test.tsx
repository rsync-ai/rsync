import { describe, it, expect, vi, beforeEach, type Mock } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

// Discovery now lists up to 5000 tables, and the connector reports how many
// the source has. The picker says "Showing N of M" only when the list was cut,
// using those totals, instead of guessing from a fixed 100-row count.

const json = (status: number, data: unknown) => ({ ok: status >= 200 && status < 300, status, json: async () => data })

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(),
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { truncatedTableTotal } from "@/lib/api/connections"
import { PipelineTableSelector, type AvailableTable } from "../PipelineTableSelector"

const tables = (n: number): AvailableTable[] =>
  Array.from({ length: n }, (_, i) => ({ name: `t${i}`, schema: "public", row_count: i, columns: 1 }))

function renderSelector(props: Record<string, unknown>) {
  return render(
    <PipelineTableSelector
      isOpen
      onClose={() => {}}
      pipelineId="pl_trunc"
      availableTables={[] as never}
      suggestedTables={[]}
      {...props}
    />
  )
}

describe("truncatedTableTotal", () => {
  it("returns the source total when the list was cut", () => {
    expect(truncatedTableTotal({ total_tables_available: 7500, tables_truncated: true })).toBe(7500)
  })

  it("is undefined for a complete list", () => {
    expect(truncatedTableTotal({ total_tables_available: 40, tables_truncated: false })).toBeUndefined()
    expect(truncatedTableTotal({ total_tables_available: 40, total_tables_discovered: 40 })).toBeUndefined()
  })

  it("falls back to discovered < available when the flag is absent (/metadata envelope)", () => {
    expect(truncatedTableTotal({ total_tables_available: 7500, total_tables_discovered: 5000 })).toBe(7500)
  })

  it("an explicit false beats the counts", () => {
    expect(truncatedTableTotal({ total_tables_available: 7500, total_tables_discovered: 5000, tables_truncated: false })).toBeUndefined()
  })

  it("is undefined when no total is reported (v1 envelopes, empty waits)", () => {
    expect(truncatedTableTotal({ tables_truncated: true })).toBeUndefined()
    expect(truncatedTableTotal({ total_tables_available: 0, tables_truncated: true })).toBeUndefined()
    expect(truncatedTableTotal(undefined)).toBeUndefined()
    expect(truncatedTableTotal(null)).toBeUndefined()
  })
})

describe("PipelineTableSelector truncated-list notice", () => {
  beforeEach(() => {
    ;(authFetch as Mock).mockReset()
    ;(authFetch as Mock).mockResolvedValue(json(200, {}))
  })

  it("shows N of M when the parent passes a total", async () => {
    renderSelector({ availableTables: tables(3), truncatedTotal: 7500 })
    const notice = await screen.findByTestId("tables-truncated-notice")
    expect(notice).toHaveTextContent("Showing 3 of 7,500 tables")
  })

  it("no total, no notice (control)", async () => {
    renderSelector({ availableTables: tables(150) })
    await screen.findByText("Select entire database")
    expect(screen.queryByTestId("tables-truncated-notice")).not.toBeInTheDocument()
  })

  it("the fallback fetch asks for a full page and reads its totals", async () => {
    const calls: string[] = []
    ;(authFetch as Mock).mockImplementation(async (url: string) => {
      calls.push(String(url))
      if (String(url).includes("/metadata")) {
        return json(200, { tables: tables(4), total_tables_available: 9000, total_tables_discovered: 4 })
      }
      return json(200, {})
    })
    renderSelector({ sourceConnectionId: "conn-big" })
    const notice = await screen.findByTestId("tables-truncated-notice")
    expect(notice).toHaveTextContent("Showing 4 of 9,000 tables")
    const metadataCall = calls.find((u) => u.includes("/metadata"))
    expect(metadataCall).toMatch(/conn-big\/metadata\?limit=5000$/)
  })

  it("the fallback fetch of a complete source shows no notice (control)", async () => {
    ;(authFetch as Mock).mockImplementation(async (url: string) =>
      String(url).includes("/metadata")
        ? json(200, { tables: tables(4), total_tables_available: 4, total_tables_discovered: 4 })
        : json(200, {})
    )
    renderSelector({ sourceConnectionId: "conn-small" })
    await screen.findByText("Select entire database")
    await waitFor(() => expect(screen.queryByTestId("tables-truncated-notice")).not.toBeInTheDocument())
  })
})
