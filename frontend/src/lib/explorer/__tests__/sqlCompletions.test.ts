import { describe, it, expect } from "vitest"
import {
  buildSchemaLookup,
  parseTableAliases,
  columnCompletionsFor,
  wordCompletions,
  foreignKeyJoinHints,
  tablesInSchema,
  type CompletionTable,
} from "../sqlCompletions"

const TABLES: CompletionTable[] = [
  {
    name: "orders",
    schema: "sales",
    columns: [
      { name: "id", is_primary_key: true },
      { name: "user_id", type: "int" },
      { name: "total", type: "decimal" },
    ],
  },
  {
    name: "users",
    schema: "sales",
    columns: [
      { name: "id", is_primary_key: true },
      { name: "email", type: "varchar" },
    ],
  },
]

describe("parseTableAliases", () => {
  it("maps FROM/JOIN aliases (with and without AS) to their tables", () => {
    const a = parseTableAliases("SELECT * FROM sales.orders o JOIN users AS u ON o.user_id = u.id")
    expect(a.get("o")).toBe("sales.orders")
    expect(a.get("u")).toBe("users")
  })

  it("handles comma-separated tables", () => {
    const a = parseTableAliases("SELECT * FROM orders o, users u")
    expect(a.get("o")).toBe("orders")
    expect(a.get("u")).toBe("users")
  })

  it("does not treat SQL keywords as aliases", () => {
    const a = parseTableAliases("SELECT * FROM orders WHERE id = 1")
    expect(a.has("where")).toBe(false)
    expect(a.size).toBe(0)
  })
})

describe("columnCompletionsFor", () => {
  const lookup = buildSchemaLookup(TABLES)

  it("resolves an alias to its table's columns", () => {
    const aliases = parseTableAliases("FROM sales.orders o")
    expect(columnCompletionsFor("o", lookup, aliases).map((o) => o.label)).toEqual([
      "id",
      "user_id",
      "total",
    ])
  })

  it("resolves a bare table name directly", () => {
    expect(columnCompletionsFor("users", lookup, new Map()).map((o) => o.label)).toEqual([
      "id",
      "email",
    ])
  })

  it("marks primary-key columns distinctly", () => {
    expect(columnCompletionsFor("users", lookup, new Map()).find((o) => o.label === "id")?.type).toBe(
      "constant",
    )
  })

  it("returns [] for an unknown qualifier", () => {
    expect(columnCompletionsFor("nope", lookup, new Map())).toEqual([])
  })
})

describe("wordCompletions", () => {
  it("offers qualified tables (boosted) plus de-duplicated columns", () => {
    const opts = wordCompletions(buildSchemaLookup(TABLES))
    expect(opts.filter((o) => o.type === "class").map((o) => o.label)).toEqual([
      "sales.orders",
      "sales.users",
    ])
    expect(opts.filter((o) => o.label === "id").length).toBe(1) // deduped across tables
  })
})

describe("foreignKeyJoinHints", () => {
  it("renders join-key equalities from foreign keys", () => {
    const hints = foreignKeyJoinHints([
      {
        from_schema: "sales",
        from_table: "orders",
        from_column: "user_id",
        to_schema: "sales",
        to_table: "users",
        to_column: "id",
      },
    ])
    expect(hints[0].label).toBe("sales.orders.user_id = sales.users.id")
  })

  it("filters to FKs whose both endpoints are in scope when a scope set is given", () => {
    const fks = [
      { from_table: "orders", from_column: "user_id", to_table: "users", to_column: "id" },
      { from_table: "payments", from_column: "invoice_id", to_table: "invoices", to_column: "id" },
    ]
    expect(foreignKeyJoinHints(fks)).toHaveLength(2) // no scope → all
    const scoped = foreignKeyJoinHints(fks, new Set(["orders", "users"]))
    expect(scoped).toHaveLength(1)
    expect(scoped[0].label).toContain("orders")
  })
})

describe("tablesInSchema", () => {
  it("lists the tables in a schema by bare name", () => {
    const lookup = buildSchemaLookup(TABLES)
    expect(tablesInSchema("sales", lookup).map((o) => o.label).sort()).toEqual(["orders", "users"])
  })

  it("is case-insensitive and returns [] for an unknown schema", () => {
    const lookup = buildSchemaLookup(TABLES)
    expect(tablesInSchema("SALES", lookup)).toHaveLength(2)
    expect(tablesInSchema("public", lookup)).toEqual([])
  })
})

describe("columnCompletionsFor — schema-qualified + collisions", () => {
  const COLLIDING: CompletionTable[] = [
    { name: "orders", schema: "sales", columns: [{ name: "total", type: "decimal" }] },
    { name: "orders", schema: "archive", columns: [{ name: "archived_at", type: "timestamp" }] },
  ]

  it("resolves a fully-qualified schema.table to its columns", () => {
    const lookup = buildSchemaLookup(COLLIDING)
    expect(columnCompletionsFor("sales.orders", lookup, new Map()).map((o) => o.label)).toEqual([
      "total",
    ])
    expect(columnCompletionsFor("archive.orders", lookup, new Map()).map((o) => o.label)).toEqual([
      "archived_at",
    ])
  })

  it("declines (returns []) a bare name that is ambiguous across schemas instead of guessing", () => {
    const lookup = buildSchemaLookup(COLLIDING)
    expect(columnCompletionsFor("orders", lookup, new Map())).toEqual([])
  })
})

describe("parseTableAliases — quoted identifiers", () => {
  it("parses double-quoted (Postgres) table refs and aliases", () => {
    const a = parseTableAliases('SELECT * FROM "Users" u WHERE u.id = 1')
    expect(a.get("u")).toBe("users")
  })

  it("parses backtick-quoted (MySQL) table refs", () => {
    const a = parseTableAliases("SELECT * FROM `Orders` o")
    expect(a.get("o")).toBe("orders")
  })
})

describe("wordCompletions — cross-table column collisions", () => {
  it("neutralizes the type detail when a column name collides with differing types", () => {
    const tables: CompletionTable[] = [
      { name: "orders", schema: "s", columns: [{ name: "status", type: "int" }] },
      { name: "shipments", schema: "s", columns: [{ name: "status", type: "varchar" }] },
    ]
    const opts = wordCompletions(buildSchemaLookup(tables))
    expect(opts.filter((o) => o.label === "status")).toHaveLength(1) // still deduped
    expect(opts.find((o) => o.label === "status")?.detail).toBe("column") // neutral, not int/varchar
  })
})
