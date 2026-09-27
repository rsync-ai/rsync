import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"

import { authFetch } from "@/lib/api/auth-fetch"
import {
  describeColumnLink,
  flattenResolvedColumns,
  resolveExplorerColumns,
  toIndexTables,
} from "@/lib/explorer/resolveColumns"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

const mockFetch = authFetch as Mock
afterEach(() => vi.clearAllMocks())

describe("toIndexTables", () => {
  it("renames nullable to is_nullable, which the gateway binds", () => {
    // cache.ExplorerColumnIndex tags this `is_nullable` (explorer_cache.go:58)
    // while the explorer page calls it `nullable`. A pass-through binds every
    // column as NOT NULL with no error anywhere, so this is the whole point of
    // the function.
    const [t] = toIndexTables([
      { name: "orders", schema: "sales", columns: [{ name: "note", type: "text", nullable: true }] },
    ])
    expect(t.columns[0].is_nullable).toBe(true)
    expect(t.columns[0]).not.toHaveProperty("nullable")
  })

  it("defaults the schema the way a Postgres source does", () => {
    expect(toIndexTables([{ name: "orders" }])[0].schema).toBe("public")
  })

  it("sends only metadata fields — never a value", () => {
    const [t] = toIndexTables([
      {
        name: "orders",
        columns: [{ name: "id", type: "int", is_primary_key: true }],
      } as never,
    ])
    expect(Object.keys(t).sort()).toEqual(["columns", "name", "schema"])
    expect(Object.keys(t.columns[0]).sort()).toEqual(["is_nullable", "is_primary_key", "name", "type"])
  })

  it("survives a table the page knows nothing about", () => {
    expect(toIndexTables([{ name: "orders" }])[0].columns).toEqual([])
  })
})

describe("flattenResolvedColumns", () => {
  it("orders by how a query reads and drops repeats", () => {
    expect(
      flattenResolvedColumns({
        select_cols: ["o.total"],
        where_cols: ["o.status", "o.total"],
        group_by_cols: ["o.region"],
        order_by_cols: ["o.total"],
      })
    ).toEqual(["o.total", "o.status", "o.region"])
  })

  it("returns nothing for an empty or missing mapping", () => {
    expect(flattenResolvedColumns(null)).toEqual([])
    expect(flattenResolvedColumns({})).toEqual([])
  })
})

describe("describeColumnLink", () => {
  it("leads with the review request, which is the only new signal this step has", () => {
    const s = describeColumnLink({
      columns: { select_cols: ["a", "b"] },
      confidence: 0.4,
      needs_hitl: true,
      hitl_reason: "two columns named total",
      ambiguous_columns: ["total"],
    })
    expect(s).toContain("Needs review")
    expect(s).toContain("two columns named total")
    expect(s).toContain("ambiguous: total")
  })

  it("counts columns and joins when the model is confident", () => {
    expect(
      describeColumnLink({
        columns: { select_cols: ["a", "b"] },
        join_plan: [{ join_type: "INNER", left_table: "a", right_table: "b", condition: "a.id = b.a_id" }],
        confidence: 0.9,
        needs_hitl: false,
      })
    ).toBe("2 columns linked, 1 join planned")
  })

  it("uses the singular for one column and omits joins when there are none", () => {
    expect(describeColumnLink({ columns: { select_cols: ["a"] }, confidence: 1, needs_hitl: false })).toBe(
      "1 column linked"
    )
  })
})

describe("resolveExplorerColumns", () => {
  it("posts metadata-only tables in the gateway's field names", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ columns: { select_cols: ["o.id"] }, confidence: 0.77, needs_hitl: false }),
    })

    const out = await resolveExplorerColumns({
      connectionId: "conn-1",
      question: "total by region",
      tables: [{ name: "orders", schema: "sales", columns: [{ name: "id", type: "int", nullable: false }] }],
    })

    expect(mockFetch.mock.calls[0][0]).toBe("/api/v1/explorer/nl/resolve-columns")
    const body = JSON.parse(mockFetch.mock.calls[0][1].body)
    expect(body.connection_id).toBe("conn-1")
    expect(body.question).toBe("total by region")
    expect(body.selected_tables[0].columns[0]).toEqual({
      name: "id",
      type: "int",
      is_primary_key: false,
      is_nullable: false,
    })
    // The server's number, not a constant — that was the bug this replaces.
    expect(out.confidence).toBe(0.77)
  })

  it("substitutes a question for a raw-SQL run, because the field is required", async () => {
    mockFetch.mockResolvedValue({ ok: true, status: 200, json: async () => ({ columns: {}, confidence: 0 }) })
    await resolveExplorerColumns({ connectionId: "c", question: "   ", tables: [] })

    expect(JSON.parse(mockFetch.mock.calls[0][1].body).question).toBe("Columns relevant to this query")
  })

  it("throws the server's sentence so the caller can show it", async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 404, json: async () => ({ error: "Connection not found" }) })
    await expect(resolveExplorerColumns({ connectionId: "c", question: "q", tables: [] })).rejects.toThrow(
      "Connection not found"
    )
  })
})
