import { describe, it, expect } from "vitest"
import {
  groupForeignKeysByTable,
  groupTablesByDatabase,
  isInferredForeignKey,
  relationshipsFor,
  type SchemaForeignKeyLike,
  type SchemaTableLike,
} from "../schemaTree"

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

describe("groupForeignKeysByTable", () => {
  const fk = (
    fromTable: string,
    fromColumn: string,
    toTable: string,
    toColumn: string,
    extra: Partial<SchemaForeignKeyLike> = {},
  ): SchemaForeignKeyLike => ({
    from_schema: "sales",
    from_table: fromTable,
    from_column: fromColumn,
    to_schema: "sales",
    to_table: toTable,
    to_column: toColumn,
    ...extra,
  })

  it("indexes both endpoints so each table sees both directions", () => {
    const index = groupForeignKeysByTable([fk("orders", "user_id", "users", "id")])
    const orders = relationshipsFor(index, { name: "orders", schema: "sales" })
    const users = relationshipsFor(index, { name: "users", schema: "sales" })
    expect(orders.outgoing.map((f) => f.to_table)).toEqual(["users"])
    expect(orders.incoming).toEqual([])
    expect(users.incoming.map((f) => f.from_table)).toEqual(["orders"])
    expect(users.outgoing).toEqual([])
  })

  it("matches case-insensitively and falls back to the bare table name", () => {
    const index = groupForeignKeysByTable([
      fk("Orders", "user_id", "Users", "id", { from_schema: "", to_schema: "" }),
    ])
    // Table carries a schema the foreign key does not — bare-name fallback.
    expect(relationshipsFor(index, { name: "orders", schema: "sales" }).outgoing).toHaveLength(1)
  })

  it("reports a table with no relationships as empty, not undefined", () => {
    const index = groupForeignKeysByTable([fk("orders", "user_id", "users", "id")])
    expect(relationshipsFor(index, { name: "events", schema: "analytics" })).toEqual({
      outgoing: [],
      incoming: [],
    })
  })

  it("separates inferred relationships from real constraints", () => {
    expect(isInferredForeignKey(fk("orders", "user_id", "users", "id", { confidence: 1 }))).toBe(false)
    expect(isInferredForeignKey(fk("orders", "user_id", "users", "id", { confidence: 0.8 }))).toBe(true)
    // No confidence at all means the backend did not say — treat as a constraint.
    expect(isInferredForeignKey(fk("orders", "user_id", "users", "id"))).toBe(false)
  })

  it("skips malformed entries and handles an absent list", () => {
    expect(groupForeignKeysByTable(undefined).size).toBe(0)
    const index = groupForeignKeysByTable([
      { from_table: "", from_column: "x", to_table: "users", to_column: "id" },
    ])
    expect(index.size).toBe(0)
  })
})
