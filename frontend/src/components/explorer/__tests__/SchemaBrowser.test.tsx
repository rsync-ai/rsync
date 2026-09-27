import { describe, it, expect, vi } from "vitest"
import { render, screen, fireEvent } from "@testing-library/react"
import { SchemaBrowser } from "../SchemaBrowser"
import type { SchemaForeignKeyLike, SchemaTableLike } from "@/lib/explorer/schemaTree"

const TABLES: SchemaTableLike[] = [
  {
    name: "orders",
    schema: "sales",
    row_count: 50,
    columns: [{ name: "id", is_primary_key: true }, { name: "total", type: "decimal" }],
  },
  { name: "users", schema: "sales", columns: [{ name: "id" }, { name: "email" }] },
  { name: "events", schema: "analytics", columns: [{ name: "ts", type: "timestamp" }] },
]

// Athena-style layout: a Database *dropdown* selects one namespace at a time,
// its tables are listed directly under a "Tables (N)" header, and a table
// expands to reveal its columns. Selection checkboxes + click-to-insert are
// preserved (rsync uses them for pipeline / NL→SQL table picking).
describe("SchemaBrowser (Athena layout)", () => {
  it("renders a Database selector listing every database (alphabetised)", () => {
    render(<SchemaBrowser tables={TABLES} />)
    expect(screen.getByRole("combobox", { name: "Database" })).toBeInTheDocument()
    expect(screen.getByRole("option", { name: "analytics" })).toBeInTheDocument()
    expect(screen.getByRole("option", { name: "sales" })).toBeInTheDocument()
  })

  it("shows the selected database's tables directly, under a Tables (N) header", () => {
    render(<SchemaBrowser tables={TABLES} />)
    // default = first database alphabetically (analytics) → its table is shown
    // directly, no node to expand first
    expect(screen.getByRole("button", { name: "events" })).toBeInTheDocument()
    expect(screen.getByText(/Tables \(1\)/)).toBeInTheDocument()
    // another database's tables are NOT shown until it is selected
    expect(screen.queryByRole("button", { name: "orders" })).not.toBeInTheDocument()
  })

  it("switches the table list when the Database selector changes", () => {
    render(<SchemaBrowser tables={TABLES} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    expect(screen.getByRole("button", { name: "orders" })).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "users" })).toBeInTheDocument()
    expect(screen.getByText(/Tables \(2\)/)).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "events" })).not.toBeInTheDocument()
  })

  it("expands a table to reveal its columns", () => {
    render(<SchemaBrowser tables={TABLES} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    fireEvent.click(screen.getByRole("button", { name: "orders" }))
    expect(screen.getByText("total")).toBeInTheDocument()
    expect(screen.getByText("id")).toBeInTheDocument()
  })

  it("inserts the schema-qualified table name on demand", () => {
    const onInsertTable = vi.fn()
    render(<SchemaBrowser tables={TABLES} onInsertTable={onInsertTable} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    fireEvent.click(screen.getByRole("button", { name: "Insert table orders" }))
    expect(onInsertTable).toHaveBeenCalledWith("sales.orders")
  })

  it("inserts a column name on demand", () => {
    const onInsertColumn = vi.fn()
    render(<SchemaBrowser tables={TABLES} onInsertColumn={onInsertColumn} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    fireEvent.click(screen.getByRole("button", { name: "orders" }))
    fireEvent.click(screen.getByRole("button", { name: "Insert column total" }))
    expect(onInsertColumn).toHaveBeenCalledWith("total")
  })

  it("keeps a per-table selection checkbox (custom key)", () => {
    const onToggleTable = vi.fn()
    render(
      <SchemaBrowser
        tables={TABLES}
        onToggleTable={onToggleTable}
        selectionKey={(t) => `db:${t.name}`}
        selectedTables={["db:orders"]}
      />,
    )
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    expect(screen.getByRole("checkbox", { name: "Select orders" })).toBeChecked()
    fireEvent.click(screen.getByRole("checkbox", { name: "Select users" }))
    expect(onToggleTable).toHaveBeenCalledWith("db:users")
  })

  it("filters tables within the selected database", () => {
    render(<SchemaBrowser tables={TABLES} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    fireEvent.change(screen.getByPlaceholderText(/filter/i), { target: { value: "user" } })
    expect(screen.getByRole("button", { name: "users" })).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "orders" })).not.toBeInTheDocument()
  })

  it("names the items with itemLabel (document mode: collections)", () => {
    const onInsertTable = vi.fn()
    render(
      <SchemaBrowser
        tables={TABLES}
        itemLabel="collections"
        insertTitle="Open collection"
        insertLabel="Open collection"
        onInsertTable={onInsertTable}
      />,
    )
    expect(screen.getByText("Collections (1)")).toBeInTheDocument()
    expect(screen.queryByText(/Tables \(/)).not.toBeInTheDocument()
    expect(screen.getByPlaceholderText("Filter collections…")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Open collection events" })).toHaveAttribute("title", "Open collection")
    expect(screen.queryByRole("button", { name: /^Insert table/ })).not.toBeInTheDocument()
    fireEvent.change(screen.getByPlaceholderText("Filter collections…"), { target: { value: "zzz" } })
    expect(screen.getByText(/No collections match/)).toBeInTheDocument()
  })

  // Hidden until hover is fine for a mouse; a keyboard user tabbing onto the
  // button must see it, or the only control that opens a collection is invisible.
  it("reveals the row buttons on keyboard focus, not only on hover", () => {
    render(<SchemaBrowser tables={TABLES} onInsertTable={vi.fn()} onInsertColumn={vi.fn()} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    const insert = screen.getByRole("button", { name: "Insert table orders" })
    expect(insert.className).toContain("opacity-0")
    expect(insert.className).toContain("focus-visible:opacity-100")
    expect(insert.className).toContain("focus-visible:ring-1")
    fireEvent.click(screen.getByRole("button", { name: "orders" }))
    const column = screen.getByRole("button", { name: "Insert column total" })
    expect(column.className).toContain("focus-visible:opacity-100")
  })

  it("shows a loading state", () => {
    render(<SchemaBrowser tables={[]} loading />)
    expect(screen.getByText(/loading/i)).toBeInTheDocument()
  })

  // #54: on Postgres the dropdown read "Database" over "public", which is a schema.
  it("names the dropdown after what the namespace is", () => {
    render(<SchemaBrowser tables={[{ name: "users", schema: "public" }]} namespaceLabel="Schema" />)
    expect(screen.getByRole("combobox", { name: "Schema" })).toHaveValue("public")
    expect(screen.queryByRole("combobox", { name: "Database" })).toBeNull()
  })

  it("shows an empty hint when there are no tables", () => {
    render(<SchemaBrowser tables={[]} emptyHint="Select a connection to browse its schema" />)
    expect(screen.getByText("Select a connection to browse its schema")).toBeInTheDocument()
  })
})

// The schema-index response has always carried `foreign_keys`, and the page has
// always fed them to NL→SQL generation, but nothing showed them — so a user
// could not tell which joins the AI had to work with, nor which of them the
// backend merely guessed (confidence < 1).
describe("SchemaBrowser relationships", () => {
  const FKS: SchemaForeignKeyLike[] = [
    {
      from_schema: "sales",
      from_table: "orders",
      from_column: "user_id",
      to_schema: "sales",
      to_table: "users",
      to_column: "id",
      confidence: 1,
    },
    {
      from_schema: "sales",
      from_table: "orders",
      from_column: "coupon_code",
      to_schema: "sales",
      to_table: "coupons",
      to_column: "code",
      confidence: 0.7,
    },
  ]

  const openSales = () => {
    render(<SchemaBrowser tables={TABLES} foreignKeys={FKS} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
  }

  it("counts a table's relationships without changing the expander's name", () => {
    openSales()
    // The count is a sibling of the expand button, so click-to-expand and the
    // page's own table lookups keep addressing the table by its bare name.
    expect(screen.getByRole("button", { name: "orders" })).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "users" })).toBeInTheDocument()
  })

  it("lists both directions when a table is expanded", () => {
    openSales()
    fireEvent.click(screen.getByRole("button", { name: "orders" }))
    const rels = screen.getByRole("list", { name: "Relationships for orders" })
    // outgoing: orders.user_id → sales.users.id
    expect(rels).toHaveTextContent("user_id → sales.users.id")
    expect(rels).toHaveTextContent("coupon_code → sales.coupons.code")

    fireEvent.click(screen.getByRole("button", { name: "users" }))
    // incoming on the other endpoint, pointing back at users.id
    expect(screen.getByRole("list", { name: "Relationships for users" })).toHaveTextContent(
      "sales.orders.user_id → id",
    )
  })

  it("marks only the inferred relationship, and explains the mark", () => {
    openSales()
    fireEvent.click(screen.getByRole("button", { name: "orders" }))
    const rels = screen.getByRole("list", { name: "Relationships for orders" })
    // one tag for the confidence-0.7 row, none for the real constraint
    expect(rels.querySelectorAll("[title^='Guessed from column names']")).toHaveLength(1)
    expect(screen.getByText(/guessed from column names, not declared/i)).toBeInTheDocument()
  })

  it("says nothing about relationships when none were supplied", () => {
    render(<SchemaBrowser tables={TABLES} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    fireEvent.click(screen.getByRole("button", { name: "orders" }))
    expect(screen.queryByRole("list", { name: "Relationships for orders" })).toBeNull()
    expect(screen.queryByText(/guessed from column names/i)).toBeNull()
  })
})
