import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, fireEvent, waitFor } from "@testing-library/react"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: (...args: unknown[]) => authFetch(...args) }))

import { DocumentExplorer } from "../DocumentExplorer"

const COLLECTIONS = [
  { name: "customers", schema: "shop", columns: [] },
  { name: "orders", schema: "shop", columns: [] },
]

function reply(status: number, body: unknown) {
  return { ok: status < 400, status, text: async () => JSON.stringify(body) }
}

function sentBodies(): Record<string, unknown>[] {
  return authFetch.mock.calls.map((c) => JSON.parse((c[1] as { body: string }).body))
}

function Harness({ initial = "orders" }: { initial?: string }) {
  return (
    <DocumentExplorer
      connectionId="conn-1"
      collections={COLLECTIONS}
      collection={initial}
      onCollectionChange={() => {}}
    />
  )
}

describe("DocumentExplorer", () => {
  beforeEach(() => authFetch.mockReset())

  it("browses the picked collection and renders one column per field", async () => {
    authFetch.mockResolvedValueOnce(
      reply(200, {
        collection: "orders",
        columns: ["_id", "status", "total"],
        documents: [
          { _id: { $oid: "65f000000000000000000001" }, status: "paid", total: 12.5 },
          { _id: { $oid: "65f000000000000000000002" }, status: null },
        ],
        returned: 2,
        has_more: false,
        paging_mode: "keyset",
        execution_time_ms: 4,
      }),
    )
    render(<Harness />)

    await waitFor(() => expect(screen.getAllByTestId("doc-row")).toHaveLength(2))
    expect(authFetch.mock.calls[0][0]).toBe("/api/v1/explorer/documents/find")
    expect(sentBodies()[0]).toEqual({ connection_id: "conn-1", collection: "orders", limit: 50 })
    expect(screen.getByText(`ObjectId("65f000000000000000000001")`)).toBeInTheDocument()
    expect(screen.getByText("12.5")).toBeInTheDocument()
    expect(screen.getByText("null")).toBeInTheDocument()
    // total is absent from the second document
    expect(screen.getByTitle("Field not present in this document")).toBeInTheDocument()
    expect(screen.getByTestId("doc-summary")).toHaveTextContent("2 documents · 4 ms")
    expect(screen.queryByRole("button", { name: /load more/i })).not.toBeInTheDocument()
  })

  it("pages with the returned cursor and appends the next page", async () => {
    authFetch
      .mockResolvedValueOnce(
        reply(200, {
          collection: "orders",
          columns: ["_id", "a"],
          documents: [{ _id: 1, a: "first" }],
          has_more: true,
          paging_mode: "keyset",
          next_cursor: "CURSOR-1",
        }),
      )
      .mockResolvedValueOnce(
        reply(200, {
          collection: "orders",
          columns: ["_id", "b"],
          documents: [{ _id: 2, b: "second" }],
          has_more: false,
          paging_mode: "keyset",
        }),
      )
    render(<Harness />)

    fireEvent.click(await screen.findByRole("button", { name: /load more/i }))
    await waitFor(() => expect(screen.getAllByTestId("doc-row")).toHaveLength(2))
    expect(sentBodies()[1]).toMatchObject({ collection: "orders", cursor: "CURSOR-1" })
    expect(screen.getByText("first")).toBeInTheDocument()
    expect(screen.getByText("second")).toBeInTheDocument()
    expect(screen.getByRole("columnheader", { name: "b" })).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /load more/i })).not.toBeInTheDocument()
  })

  it("shows the gateway's refusal with the path it points at", async () => {
    authFetch.mockResolvedValueOnce(reply(200, { collection: "orders", documents: [], has_more: false }))
    authFetch.mockResolvedValueOnce(
      reply(400, { error: "operator $where is not allowed", error_code: "operator_not_allowed", path: "filter.$where" }),
    )
    render(<Harness />)
    await screen.findByText("No documents match this filter")

    fireEvent.change(screen.getByLabelText("Filter"), { target: { value: `{"$where":"1"}` } })
    fireEvent.click(screen.getByRole("button", { name: "Find" }))

    const alert = await screen.findByRole("alert")
    expect(alert).toHaveTextContent("operator $where is not allowed")
    expect(alert).toHaveTextContent("at filter.$where")
  })

  it("catches malformed JSON before sending anything", async () => {
    authFetch.mockResolvedValueOnce(reply(200, { collection: "orders", documents: [], has_more: false }))
    render(<Harness />)
    await screen.findByText("No documents match this filter")

    fireEvent.change(screen.getByLabelText("Sort"), { target: { value: `{"total":` } })
    fireEvent.click(screen.getByRole("button", { name: "Find" }))

    expect(await screen.findByRole("alert")).toHaveTextContent(/Sort is not valid JSON/)
    expect(screen.getByLabelText("Sort")).toHaveAttribute("aria-invalid", "true")
    expect(authFetch).toHaveBeenCalledTimes(1)
  })
})
