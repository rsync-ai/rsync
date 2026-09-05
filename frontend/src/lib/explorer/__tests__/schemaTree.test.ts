import { describe, it, expect } from "vitest"
import { groupTablesByDatabase, type SchemaTableLike } from "../schemaTree"

const tbl = (name: string, schema?: string, extra: Partial<SchemaTableLike> = {}): SchemaTableLike => ({
  name,
  schema,
  ...extra,
})

describe("groupTablesByDatabase", () => {
  it("groups tables by their schema/database name", () => {
    const groups = groupTablesByDatabase([
      tbl("orders", "sales"),
      tbl("users", "sales"),
      tbl("events", "analytics"),
    ])
    // databases are alphabetised
    expect(groups.map((g) => g.database)).toEqual(["analytics", "sales"])
    const sales = groups.find((g) => g.database === "sales")!
    expect(sales.tables.map((t) => t.name)).toEqual(["orders", "users"])
    expect(sales.tableCount).toBe(2)
  })

  it("buckets tables without a schema under '(default)'", () => {
    const groups = groupTablesByDatabase([tbl("lonely")])
    expect(groups).toHaveLength(1)
    expect(groups[0].database).toBe("(default)")
    expect(groups[0].tables[0].name).toBe("lonely")
  })

  it("sorts databases and tables alphabetically (case-insensitive)", () => {
    const groups = groupTablesByDatabase([
      tbl("Zebra", "main"),
      tbl("apple", "main"),
      tbl("x", "Beta"),
    ])
    expect(groups.map((g) => g.database)).toEqual(["Beta", "main"])
    expect(groups.find((g) => g.database === "main")!.tables.map((t) => t.name)).toEqual([
      "apple",
      "Zebra",
    ])
  })

  it("returns an empty array for empty input", () => {
    expect(groupTablesByDatabase([])).toEqual([])
  })

  it("preserves columns and row_count on each table", () => {
    const groups = groupTablesByDatabase([
      tbl("orders", "sales", { row_count: 50, columns: [{ name: "id", type: "int" }] }),
    ])
    expect(groups[0].tables[0].row_count).toBe(50)
    expect(groups[0].tables[0].columns).toEqual([{ name: "id", type: "int" }])
  })

  it("treats empty-string schema the same as missing (→ default bucket)", () => {
    const groups = groupTablesByDatabase([tbl("a", ""), tbl("b", undefined)])
    expect(groups).toHaveLength(1)
    expect(groups[0].database).toBe("(default)")
    expect(groups[0].tables.map((t) => t.name)).toEqual(["a", "b"])
  })
})
