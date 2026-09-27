import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from "vitest"
import { act, render, renderHook, screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import { authFetch } from "@/lib/api/auth-fetch"
import { ColumnsPanel, columnsPanelHeight, useWarehouseTables, type WarehouseTables } from "@/components/explorer/AssetColumns"
import type { AssetNode } from "@/components/explorer/assetLineage"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
const mockFetch = authFetch as unknown as Mock

const ORDERS: AssetNode = { id: "table:c-1:public.orders", kind: "table", name: "public.orders", connection_id: "c-1" }
const columns = Array.from({ length: 23 }, (_, i) => ({
  name: i === 0 ? "id" : `field_${String(i).padStart(2, "0")}`,
  type: i === 0 ? "bigint" : "text",
  is_primary_key: i === 0,
}))
const ready = (tables: { name: string; schema: string; columns: typeof columns }[]): WarehouseTables => ({
  status: "ready",
  tables,
})

function panel(tables: WarehouseTables | undefined, node: AssetNode = ORDERS, inCanvas = false) {
  const onRetry = vi.fn()
  render(<ColumnsPanel node={node} tables={tables} onRetry={onRetry} inCanvas={inCanvas} />)
  return onRetry
}

describe("ColumnsPanel", () => {
  it("says why it has no columns to show", async () => {
    const { rerender } = render(<ColumnsPanel node={{ ...ORDERS, connection_id: undefined }} tables={undefined} onRetry={() => {}} />)
    expect(screen.getByText("The graph does not say which warehouse this table is in.")).toBeInTheDocument()

    rerender(<ColumnsPanel node={ORDERS} tables={{ status: "loading" }} onRetry={() => {}} />)
    expect(screen.getByText("Loading columns…")).toBeInTheDocument()

    rerender(<ColumnsPanel node={ORDERS} tables={ready([])} onRetry={() => {}} />)
    expect(screen.getByText(/Not in this warehouse's schema/)).toBeInTheDocument()

    rerender(
      <ColumnsPanel
        node={{ ...ORDERS, name: "orders" }}
        tables={ready([
          { name: "orders", schema: "public", columns },
          { name: "orders", schema: "staging", columns },
        ])}
        onRetry={() => {}}
      />,
    )
    expect(screen.getByText("2 tables in different schemas have this name, so which one is meant is unclear.")).toBeInTheDocument()

    rerender(<ColumnsPanel node={ORDERS} tables={ready([{ name: "orders", schema: "public", columns: [] }])} onRetry={() => {}} />)
    expect(screen.getByText("The warehouse lists no columns for this table.")).toBeInTheDocument()
  })

  it("offers a retry when the schema could not be read", async () => {
    const onRetry = panel({ status: "error", message: "The warehouse's schema could not be read (HTTP 502)." })
    expect(screen.getByText("The warehouse's schema could not be read (HTTP 502).")).toBeInTheDocument()
    await userEvent.click(screen.getByRole("button", { name: "Retry" }))
    expect(onRetry).toHaveBeenCalledTimes(1)
  })

  it("lists ten columns at a time, keys marked, and pages through the rest", async () => {
    panel(ready([{ name: "orders", schema: "public", columns }]))
    const list = () => screen.getByRole("list", { name: "Columns of public.orders" })
    const names = () => within(list()).getAllByRole("listitem").map((li) => li.querySelector(".font-mono")!.textContent)
    expect(names()).toEqual(columns.slice(0, 10).map((c) => c.name))
    expect(within(list()).getByText("key").closest("li")).toHaveTextContent("id")
    expect(screen.getByText("1–10 of 23")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Previous" })).toBeDisabled()

    await userEvent.click(screen.getByRole("button", { name: "Next" }))
    await userEvent.click(screen.getByRole("button", { name: "Next" }))
    expect(screen.getByText("21–23 of 23")).toBeInTheDocument()
    expect(names()).toEqual(["field_20", "field_21", "field_22"])
    expect(screen.getByRole("button", { name: "Next" })).toBeDisabled()
  })

  it("finds a column by part of its name, from the first page of matches", async () => {
    panel(ready([{ name: "orders", schema: "public", columns }]))
    await userEvent.click(screen.getByRole("button", { name: "Next" }))
    const search = screen.getByRole("searchbox", { name: "Find a column in public.orders" })
    expect(search).toHaveAttribute("placeholder", "Find one of 23 columns")
    await userEvent.type(search, "FIELD_1")
    expect(within(screen.getByRole("list", { name: "Columns of public.orders" })).getAllByRole("listitem")).toHaveLength(10)
    // Ten matches fit on one page, so there is no pager.
    expect(screen.queryByRole("button", { name: "Next" })).toBeNull()
    await userEvent.type(search, "9")
    expect(within(screen.getByRole("list", { name: "Columns of public.orders" })).getAllByRole("listitem")).toHaveLength(1)
    expect(screen.getByRole("list", { name: "Columns of public.orders" })).toHaveTextContent("field_19")
    await userEvent.clear(search)
    await userEvent.type(search, "nope")
    expect(screen.getByText("No column matches.")).toBeInTheDocument()
  })

  it("keeps its controls out of the tab order inside a canvas card", () => {
    panel(ready([{ name: "orders", schema: "public", columns }]), ORDERS, true)
    expect(screen.getByRole("searchbox")).toHaveAttribute("tabindex", "-1")
    expect(screen.getByRole("searchbox")).toHaveClass("nodrag", "nopan")
    for (const name of ["Previous", "Next"]) expect(screen.getByRole("button", { name })).toHaveAttribute("tabindex", "-1")
  })
})

describe("columnsPanelHeight", () => {
  const withColumns = (n: number) => ready([{ name: "orders", schema: "public", columns: columns.slice(0, n) }])
  // The search box (24) and a gap (6), 20 a row, and a gap and the pager (30) past ten.
  it("is as tall as the rows a page lists, with room for the pager past ten", () => {
    expect(columnsPanelHeight(ORDERS, withColumns(1))).toBe(50)
    expect(columnsPanelHeight(ORDERS, withColumns(10))).toBe(230)
    expect(columnsPanelHeight(ORDERS, withColumns(11))).toBe(260)
    expect(columnsPanelHeight(ORDERS, withColumns(23))).toBe(260)
  })

  it("keeps a note short, and makes room for Retry under an error", () => {
    const note = columnsPanelHeight(ORDERS, { status: "loading" })
    expect(columnsPanelHeight({ ...ORDERS, connection_id: undefined }, undefined)).toBe(note)
    expect(columnsPanelHeight(ORDERS, ready([]))).toBe(note)
    expect(columnsPanelHeight(ORDERS, withColumns(0))).toBe(note)
    expect(columnsPanelHeight(ORDERS, { status: "error", message: "HTTP 500" })).toBe(note + 32)
    expect(note).toBeLessThan(columnsPanelHeight(ORDERS, withColumns(3)))
  })
})

describe("useWarehouseTables", () => {
  let answer: (res: { ok: boolean; status: number; json: () => Promise<unknown> }) => void

  beforeEach(() => {
    mockFetch.mockReset()
    mockFetch.mockImplementation(
      () =>
        new Promise((resolve) => {
          answer = resolve
        }),
    )
  })
  afterEach(() => vi.clearAllMocks())

  it("asks each warehouse once, however many tables open at once", async () => {
    const { result } = renderHook(() => useWarehouseTables())
    act(() => {
      result.current.load("c-1")
      result.current.load("c-1")
    })
    expect(mockFetch).toHaveBeenCalledTimes(1)
    expect(mockFetch).toHaveBeenCalledWith("/api/v1/explorer/connections/c-1/schema-index")
    expect(result.current.tablesOf("c-1")).toEqual({ status: "loading" })
    await act(async () => answer({ ok: true, status: 200, json: async () => ({ tables: [{ name: "orders", schema: "public", columns }] }) }))
    expect(result.current.tablesOf("c-1")).toMatchObject({ status: "ready", tables: [{ name: "orders" }] })
    act(() => result.current.load("c-1"))
    expect(mockFetch).toHaveBeenCalledTimes(1)
    expect(result.current.tablesOf("c-2")).toBeUndefined()
  })

  it("asks again after a failure", async () => {
    const { result } = renderHook(() => useWarehouseTables())
    act(() => result.current.load("c 1"))
    expect(mockFetch).toHaveBeenCalledWith("/api/v1/explorer/connections/c%201/schema-index")
    await act(async () => answer({ ok: false, status: 502, json: async () => ({}) }))
    expect(result.current.tablesOf("c 1")).toEqual({ status: "error", message: "The warehouse's schema could not be read (HTTP 502)." })
    act(() => result.current.load("c 1"))
    expect(mockFetch).toHaveBeenCalledTimes(2)
    expect(result.current.tablesOf("c 1")).toEqual({ status: "loading" })
  })

  it("says when the server could not be reached", async () => {
    mockFetch.mockRejectedValueOnce(new TypeError("Failed to fetch"))
    const { result } = renderHook(() => useWarehouseTables())
    await act(async () => result.current.load("c-1"))
    expect(result.current.tablesOf("c-1")).toEqual({
      status: "error",
      message: "Could not reach the server to read the warehouse's schema.",
    })
  })
})
